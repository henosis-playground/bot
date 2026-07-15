use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::LazyLock;

use minicbor::Encoder;
use regex::Regex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::{Builder, TempPath};
use walkdir::{DirEntry, WalkDir};

pub const BUNDLE_FORMAT_VERSION: u32 = 1;
pub const RUNTIME_API_VERSION: &str = "henosis-component-api-v1";
pub const ESBUILD_VERSION: &str = "0.27.0";
pub const ESBUILD_SHA256: &str = "e4d41ef34045d7d1751c375cd4a90d07d929832f838830dc5212689a6567ee58";
const ESBUILD_CONFIG: &str = "bundle=true;charset=utf8;external=henosis:*;format=esm;legal-comments=none;minify=false;packages=bundle;platform=browser;splitting=false;target=esnext;tree-shaking=true";

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
const PACKAGED_ESBUILD: &[u8] = include_bytes!("../../../tools/esbuild/linux-x64/esbuild");

static COMPONENT_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?s)export\s+default\s+defineComponent\s*\(\s*\{.*?\bname\s*:\s*["']([a-z][a-z0-9_-]{0,62})["']"#,
    )
    .expect("component discovery pattern is valid")
});
static ESM_IMPORT_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?m)^\s*(?:import|export)\s+(?:[^;]*?\s+from\s+)?["']([^"']+)["']"#)
        .expect("ESM import pattern is valid")
});

#[derive(Debug, thiserror::Error)]
pub enum BundleError {
    #[error("cannot inspect repository `{path}`: {source}")]
    InspectRepository {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("cannot read component source `{path}`: {source}")]
    ReadSource {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("component `{name}` is declared by both `{first}` and `{second}`")]
    DuplicateComponent {
        name: String,
        first: PathBuf,
        second: PathBuf,
    },
    #[error("no Henosis components were found under `{0}`")]
    NoComponents(PathBuf),
    #[error(
        "the packaged esbuild executable failed its checksum: expected {expected}, got {actual}"
    )]
    EsbuildChecksum {
        expected: &'static str,
        actual: String,
    },
    #[error("cannot prepare packaged esbuild: {0}")]
    PrepareEsbuild(std::io::Error),
    #[error("cannot prepare the generated entry for component `{component}`: {source}")]
    PrepareEntry {
        component: String,
        source: std::io::Error,
    },
    #[error("esbuild failed for component `{component}`\n{stderr}")]
    Esbuild { component: String, stderr: String },
    #[error("bundle for component `{component}` retained forbidden import `{specifier}`")]
    ExternalImport {
        component: String,
        specifier: String,
    },
    #[error("cannot write bundle output `{path}`: {source}")]
    WriteOutput {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("cannot encode bundle manifest: {0}")]
    EncodeManifest(serde_json::Error),
    #[error("cannot encode bundle identity: {0}")]
    EncodeIdentity(minicbor::encode::Error<std::convert::Infallible>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComponentSource {
    pub name: String,
    pub entry: PathBuf,
}

#[derive(Debug, Clone)]
pub struct BundleRequest {
    pub repository: PathBuf,
    pub output: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundlerIdentity {
    pub name: String,
    pub version: String,
    pub config_hash: String,
    pub executable_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleManifestV1 {
    pub format_version: u32,
    pub module_format: String,
    pub entrypoint: String,
    pub executable_sha256: String,
    pub runtime_api_version: String,
    pub bundler: BundlerIdentity,
    pub dependency_lock_hash: Option<String>,
    pub sdk_package_hashes: BTreeMap<String, String>,
    pub declared_capabilities: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleArtifact {
    pub component: String,
    pub bundle_id: String,
    pub module: PathBuf,
    pub manifest: PathBuf,
    pub executable_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleSetManifest {
    pub format_version: u32,
    pub bundles: Vec<BundleArtifact>,
}

pub trait Bundler {
    fn bundle(&self, request: &BundleRequest) -> Result<BundleSetManifest, BundleError>;
}

#[derive(Debug, Default)]
pub struct EsbuildBundler;

impl Bundler for EsbuildBundler {
    fn bundle(&self, request: &BundleRequest) -> Result<BundleSetManifest, BundleError> {
        let repository =
            request
                .repository
                .canonicalize()
                .map_err(|source| BundleError::InspectRepository {
                    path: request.repository.clone(),
                    source,
                })?;
        let components = discover_components(&repository)?;
        let lock_hash = dependency_lock_hash(&repository)?;
        fs::create_dir_all(&request.output).map_err(|source| BundleError::WriteOutput {
            path: request.output.clone(),
            source,
        })?;
        let esbuild = packaged_esbuild()?;
        let mut artifacts = Vec::with_capacity(components.len());
        for component in components {
            artifacts.push(bundle_component(
                esbuild.as_ref(),
                &repository,
                &request.output,
                &component,
                lock_hash.clone(),
            )?);
        }
        artifacts.sort_by(|left, right| left.component.cmp(&right.component));
        let set = BundleSetManifest {
            format_version: BUNDLE_FORMAT_VERSION,
            bundles: artifacts,
        };
        write_json(&request.output.join("manifest.json"), &set)?;
        Ok(set)
    }
}

pub fn discover_components(repository: &Path) -> Result<Vec<ComponentSource>, BundleError> {
    let mut components = BTreeMap::<String, PathBuf>::new();
    for entry in WalkDir::new(repository)
        .follow_links(false)
        .into_iter()
        .filter_entry(should_visit)
    {
        let entry = entry.map_err(|error| BundleError::InspectRepository {
            path: repository.to_path_buf(),
            source: error
                .into_io_error()
                .unwrap_or_else(|| std::io::Error::other("directory traversal failed")),
        })?;
        if !entry.file_type().is_file()
            || !matches!(
                entry.path().extension().and_then(OsStr::to_str),
                Some("ts" | "tsx")
            )
            || entry.path().file_name() == Some(OsStr::new("index.ts"))
        {
            continue;
        }
        let source =
            fs::read_to_string(entry.path()).map_err(|source| BundleError::ReadSource {
                path: entry.path().to_path_buf(),
                source,
            })?;
        let Some(captures) = COMPONENT_PATTERN.captures(&source) else {
            continue;
        };
        let name = captures[1].to_string();
        if let Some(first) = components.insert(name.clone(), entry.path().to_path_buf()) {
            return Err(BundleError::DuplicateComponent {
                name,
                first,
                second: entry.path().to_path_buf(),
            });
        }
    }
    if components.is_empty() {
        return Err(BundleError::NoComponents(repository.to_path_buf()));
    }
    Ok(components
        .into_iter()
        .map(|(name, entry)| ComponentSource { name, entry })
        .collect())
}

fn should_visit(entry: &DirEntry) -> bool {
    !matches!(
        entry.file_name().to_str(),
        Some("node_modules" | "dist" | ".git" | ".henosis" | "target")
    )
}

fn bundle_component(
    esbuild: &Path,
    repository: &Path,
    output_root: &Path,
    component: &ComponentSource,
    dependency_lock_hash: Option<String>,
) -> Result<BundleArtifact, BundleError> {
    let relative_entry = component
        .entry
        .strip_prefix(repository)
        .expect("discovery only returns repository children");
    let import_path = format!("./{}", relative_entry.to_string_lossy().replace('\\', "/"));
    let entry_source = format!(
        "import componentDefinition from {};\nimport {{ createBundle }} from \"@henosis/core\";\nconst bundle = createBundle(componentDefinition);\nexport const protocolVersion = bundle.protocolVersion;\nexport const component = bundle.component;\nexport const evaluate = bundle.evaluate;\n",
        serde_json::to_string(&import_path).expect("path string is JSON encodable")
    );
    let build_dir = tempfile::tempdir().map_err(BundleError::PrepareEsbuild)?;
    let module_path = build_dir.path().join("module.js");
    let mut child = Command::new(esbuild)
        .current_dir(repository)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .arg("--bundle")
        .arg("--format=esm")
        .arg("--platform=browser")
        .arg("--target=esnext")
        .arg("--charset=utf8")
        .arg("--tree-shaking=true")
        .arg("--legal-comments=none")
        .arg("--packages=bundle")
        .arg("--external:henosis:*")
        .arg("--sourcefile=henosis-component.ts")
        .arg(format!("--outfile={}", module_path.display()))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(BundleError::PrepareEsbuild)?;
    child
        .stdin
        .take()
        .expect("piped esbuild stdin is present")
        .write_all(entry_source.as_bytes())
        .map_err(|source| BundleError::PrepareEntry {
            component: component.name.clone(),
            source,
        })?;
    let output = child
        .wait_with_output()
        .map_err(BundleError::PrepareEsbuild)?;
    if !output.status.success() {
        return Err(BundleError::Esbuild {
            component: component.name.clone(),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    let bytes = fs::read(&module_path).map_err(|source| BundleError::ReadSource {
        path: module_path,
        source,
    })?;
    reject_external_imports(&component.name, &bytes)?;
    let executable_sha256 = sha256(&bytes);
    let config_hash = sha256(ESBUILD_CONFIG.as_bytes());
    let manifest = BundleManifestV1 {
        format_version: BUNDLE_FORMAT_VERSION,
        module_format: "esm".to_string(),
        entrypoint: "henosis:component".to_string(),
        executable_sha256: executable_sha256.clone(),
        runtime_api_version: RUNTIME_API_VERSION.to_string(),
        bundler: BundlerIdentity {
            name: "esbuild".to_string(),
            version: ESBUILD_VERSION.to_string(),
            config_hash,
            executable_sha256: ESBUILD_SHA256.to_string(),
        },
        dependency_lock_hash,
        sdk_package_hashes: BTreeMap::new(),
        declared_capabilities: Vec::new(),
    };
    let bundle_id = bundle_id(&manifest)?;
    let artifact_dir = output_root.join(&bundle_id);
    fs::create_dir_all(&artifact_dir).map_err(|source| BundleError::WriteOutput {
        path: artifact_dir.clone(),
        source,
    })?;
    let final_module = artifact_dir.join("module.js");
    fs::write(&final_module, bytes).map_err(|source| BundleError::WriteOutput {
        path: final_module.clone(),
        source,
    })?;
    let final_manifest = artifact_dir.join("manifest.json");
    write_json(&final_manifest, &manifest)?;
    Ok(BundleArtifact {
        component: component.name.clone(),
        bundle_id,
        module: final_module,
        manifest: final_manifest,
        executable_sha256,
    })
}

fn reject_external_imports(component: &str, bytes: &[u8]) -> Result<(), BundleError> {
    let source = String::from_utf8_lossy(bytes);
    for captures in ESM_IMPORT_PATTERN.captures_iter(&source) {
        let specifier = &captures[1];
        if !specifier.starts_with("henosis:") {
            return Err(BundleError::ExternalImport {
                component: component.to_string(),
                specifier: specifier.to_string(),
            });
        }
    }
    Ok(())
}

fn packaged_esbuild() -> Result<TempPath, BundleError> {
    #[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
    compile_error!("the D26 prototype currently packages esbuild only for linux-x86_64");

    let actual = sha256(PACKAGED_ESBUILD);
    if actual != ESBUILD_SHA256 {
        return Err(BundleError::EsbuildChecksum {
            expected: ESBUILD_SHA256,
            actual,
        });
    }
    let mut executable = Builder::new()
        .prefix("henosis-esbuild-")
        .tempfile()
        .map_err(BundleError::PrepareEsbuild)?;
    executable
        .write_all(PACKAGED_ESBUILD)
        .map_err(BundleError::PrepareEsbuild)?;
    executable.flush().map_err(BundleError::PrepareEsbuild)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(executable.path(), fs::Permissions::from_mode(0o700))
            .map_err(BundleError::PrepareEsbuild)?;
    }
    Ok(executable.into_temp_path())
}

fn dependency_lock_hash(repository: &Path) -> Result<Option<String>, BundleError> {
    let mut current = Some(repository);
    while let Some(directory) = current {
        for name in [
            "pnpm-lock.yaml",
            "package-lock.json",
            "yarn.lock",
            "bun.lockb",
        ] {
            let path = directory.join(name);
            if path.is_file() {
                let bytes =
                    fs::read(&path).map_err(|source| BundleError::ReadSource { path, source })?;
                return Ok(Some(sha256(&bytes)));
            }
        }
        current = directory.parent();
    }
    Ok(None)
}

fn bundle_id(manifest: &BundleManifestV1) -> Result<String, BundleError> {
    let mut bytes = Vec::new();
    let mut encoder = Encoder::new(&mut bytes);
    encoder.map(9).map_err(BundleError::EncodeIdentity)?;
    encoder.u8(0).map_err(BundleError::EncodeIdentity)?;
    encoder
        .u32(manifest.format_version)
        .map_err(BundleError::EncodeIdentity)?;
    encoder.u8(1).map_err(BundleError::EncodeIdentity)?;
    encoder
        .str(&manifest.module_format)
        .map_err(BundleError::EncodeIdentity)?;
    encoder.u8(2).map_err(BundleError::EncodeIdentity)?;
    encoder
        .str(&manifest.entrypoint)
        .map_err(BundleError::EncodeIdentity)?;
    encoder.u8(3).map_err(BundleError::EncodeIdentity)?;
    encoder
        .str(&manifest.executable_sha256)
        .map_err(BundleError::EncodeIdentity)?;
    encoder.u8(4).map_err(BundleError::EncodeIdentity)?;
    encoder
        .str(&manifest.runtime_api_version)
        .map_err(BundleError::EncodeIdentity)?;
    encoder.u8(5).map_err(BundleError::EncodeIdentity)?;
    encoder.map(4).map_err(BundleError::EncodeIdentity)?;
    encoder.u8(0).map_err(BundleError::EncodeIdentity)?;
    encoder
        .str(&manifest.bundler.name)
        .map_err(BundleError::EncodeIdentity)?;
    encoder.u8(1).map_err(BundleError::EncodeIdentity)?;
    encoder
        .str(&manifest.bundler.version)
        .map_err(BundleError::EncodeIdentity)?;
    encoder.u8(2).map_err(BundleError::EncodeIdentity)?;
    encoder
        .str(&manifest.bundler.config_hash)
        .map_err(BundleError::EncodeIdentity)?;
    encoder.u8(3).map_err(BundleError::EncodeIdentity)?;
    encoder
        .str(&manifest.bundler.executable_sha256)
        .map_err(BundleError::EncodeIdentity)?;
    encoder.u8(6).map_err(BundleError::EncodeIdentity)?;
    match &manifest.dependency_lock_hash {
        Some(hash) => encoder.str(hash).map_err(BundleError::EncodeIdentity)?,
        None => encoder.null().map_err(BundleError::EncodeIdentity)?,
    };
    encoder.u8(7).map_err(BundleError::EncodeIdentity)?;
    encoder
        .map(manifest.sdk_package_hashes.len() as u64)
        .map_err(BundleError::EncodeIdentity)?;
    for (name, hash) in &manifest.sdk_package_hashes {
        encoder.str(name).map_err(BundleError::EncodeIdentity)?;
        encoder.str(hash).map_err(BundleError::EncodeIdentity)?;
    }
    encoder.u8(8).map_err(BundleError::EncodeIdentity)?;
    encoder
        .array(manifest.declared_capabilities.len() as u64)
        .map_err(BundleError::EncodeIdentity)?;
    for capability in &manifest.declared_capabilities {
        encoder
            .str(capability)
            .map_err(BundleError::EncodeIdentity)?;
    }
    Ok(sha256(&bytes))
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<(), BundleError> {
    let mut bytes = serde_json::to_vec_pretty(value).map_err(BundleError::EncodeManifest)?;
    bytes.push(b'\n');
    fs::write(path, bytes).map_err(|source| BundleError::WriteOutput {
        path: path.to_path_buf(),
        source,
    })
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_finds_multiple_default_components() {
        let repo = tempfile::tempdir().unwrap();
        fs::create_dir(repo.path().join("src")).unwrap();
        fs::write(
            repo.path().join("src/a.ts"),
            "export default defineComponent({ name: \"a\", outputs: {}, build() {} });",
        )
        .unwrap();
        fs::write(
            repo.path().join("src/b.ts"),
            "export default defineComponent({\n name: 'b_service', outputs: {}, build() {} });",
        )
        .unwrap();

        let found = discover_components(repo.path()).unwrap();
        assert_eq!(
            found
                .iter()
                .map(|component| component.name.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b_service"]
        );
    }

    #[test]
    fn bundle_identity_is_stable() {
        let manifest = BundleManifestV1 {
            format_version: 1,
            module_format: "esm".to_string(),
            entrypoint: "henosis:component".to_string(),
            executable_sha256: "a".repeat(64),
            runtime_api_version: RUNTIME_API_VERSION.to_string(),
            bundler: BundlerIdentity {
                name: "esbuild".to_string(),
                version: ESBUILD_VERSION.to_string(),
                config_hash: "b".repeat(64),
                executable_sha256: ESBUILD_SHA256.to_string(),
            },
            dependency_lock_hash: Some("c".repeat(64)),
            sdk_package_hashes: BTreeMap::new(),
            declared_capabilities: Vec::new(),
        };
        assert_eq!(bundle_id(&manifest).unwrap(), bundle_id(&manifest).unwrap());
    }

    #[test]
    fn same_source_produces_same_content_addressed_protocol_module() {
        let repo = tempfile::tempdir().unwrap();
        fs::create_dir_all(repo.path().join("src")).unwrap();
        fs::create_dir_all(repo.path().join("node_modules/@henosis/core")).unwrap();
        fs::write(
            repo.path().join("src/web.ts"),
            "import { defineComponent } from '@henosis/core'; export default defineComponent({ name: 'web', outputs: {}, build() { return {}; } });",
        )
        .unwrap();
        fs::write(
            repo.path().join("node_modules/@henosis/core/package.json"),
            r#"{"name":"@henosis/core","type":"module","exports":"./index.js"}"#,
        )
        .unwrap();
        fs::write(
            repo.path().join("node_modules/@henosis/core/index.js"),
            "export function defineComponent(spec) { return spec; } export function createBundle(component) { return { protocolVersion: 1, component: { name: component.name, inputs: {}, outputs: {} }, evaluate() { return { protocolVersion: 1, status: 'complete', resources: [], outputs: {}, observedOutputs: {}, reads: [] }; } }; }",
        )
        .unwrap();
        fs::write(
            repo.path().join("pnpm-lock.yaml"),
            "lockfileVersion: '9.0'\n",
        )
        .unwrap();
        let first_output = repo.path().join("first");
        let second_output = repo.path().join("second");

        let first = EsbuildBundler
            .bundle(&BundleRequest {
                repository: repo.path().to_path_buf(),
                output: first_output,
            })
            .unwrap();
        let second = EsbuildBundler
            .bundle(&BundleRequest {
                repository: repo.path().to_path_buf(),
                output: second_output,
            })
            .unwrap();

        assert_eq!(first.bundles[0].bundle_id, second.bundles[0].bundle_id);
        assert_eq!(
            first.bundles[0].executable_sha256,
            second.bundles[0].executable_sha256
        );
        let module = fs::read_to_string(&first.bundles[0].module).unwrap();
        assert!(module.contains("protocolVersion"));
        assert!(module.contains("component"));
        assert!(module.contains("evaluate"));
        assert!(module.contains("export {"));
    }
}
