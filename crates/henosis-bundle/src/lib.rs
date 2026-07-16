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
static FILES_MANIFEST_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?s)\bfiles\s*:\s*\[(.*?)\]"#).expect("files manifest pattern is valid")
});
static CONFIG_FILE_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"config\.file\s*\(\s*["']([^"']+)["'](?:\s*,\s*["'](sha256:[0-9a-f]{64})["'])?\s*\)"#,
    )
    .expect("configuration file pattern is valid")
});
static DEFAULT_IMPORT_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?m)^\s*import\s+([A-Za-z][A-Za-z0-9]*)\s+from\s+["']([^"']+)["']\s*;?"#)
        .expect("default import pattern is valid")
});
static OUTPUT_REFERENCE_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"\b([A-Za-z][A-Za-z0-9]*)\.outputs\.([A-Za-z][A-Za-z0-9]{0,62})\b"#)
        .expect("output reference pattern is valid")
});
static WORKER_SOURCE_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?s)\bworker\.create\s*\([^,]+,\s*\{.*?\bsource\s*:\s*\{(.*?)\}"#)
        .expect("worker source pattern is valid")
});
static WORKER_SOURCE_FIELD_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"\b(entry|assets)\s*:\s*["']([^"']+)["']"#)
        .expect("worker source field pattern is valid")
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
    #[error("cannot decode esbuild dependency metadata for component `{component}`: {source}")]
    DecodeMetafile {
        component: String,
        source: serde_json::Error,
    },
    #[error("cannot encode bundle identity: {0}")]
    EncodeIdentity(minicbor::encode::Error<std::convert::Infallible>),
    #[error(
        "error[HENOSIS_BUNDLE_FILE_MANIFEST]: component `{component}` has a non-static files manifest\n  --> {source_path}\n  = help: declare configuration files directly as config.file(\"path\") calls"
    )]
    InvalidFileManifest {
        component: String,
        source_path: PathBuf,
    },
    #[error(
        "error[HENOSIS_BUNDLE_FILE_MISSING]: component `{component}` declares missing native {kind} `{path}`\n  --> {source_path}:{line}:{column}\n   |\n  = help: create the referenced path or remove it from the component files manifest"
    )]
    MissingNativeFile {
        component: String,
        kind: String,
        path: String,
        source_path: PathBuf,
        line: usize,
        column: usize,
    },
    #[error(
        "error[HENOSIS_BUNDLE_FILE_PATH]: component `{component}` declares invalid native path `{path}`\n  --> {source_path}:{line}:{column}\n  = help: use a normalized repository-relative path without dot, parent, empty, or backslash segments"
    )]
    InvalidNativePath {
        component: String,
        path: String,
        source_path: PathBuf,
        line: usize,
        column: usize,
    },
    #[error(
        "error[HENOSIS_BUNDLE_FILE_DIGEST]: component `{component}` expected {expected} for `{path}`, but the file hashes to {actual}\n  --> {source_path}:{line}:{column}\n  = help: update the optional expected digest or restore the intended file bytes"
    )]
    NativeDigestMismatch {
        component: String,
        path: String,
        expected: String,
        actual: String,
        source_path: Box<PathBuf>,
        line: usize,
        column: usize,
    },
    #[error(
        "error[HENOSIS_WORKER_SOURCE]: component `{component}` has a non-static Worker source\n  --> {source_path}\n  = help: write source: {{ entry: \"workers/name.ts\" }} with an optional static assets path"
    )]
    InvalidWorkerSource {
        component: String,
        source_path: PathBuf,
    },
    #[error(
        "error[HENOSIS_ARTIFACT_MISSING]: component `{component}` declares missing {kind} source `{path}`\n  --> {source_path}:{line}:{column}"
    )]
    MissingArtifactSource {
        component: String,
        kind: String,
        path: String,
        source_path: PathBuf,
        line: usize,
        column: usize,
    },
    #[error("cannot build workload artifact for component `{component}`\n{stderr}")]
    ArtifactBuild { component: String, stderr: String },
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
pub struct ConfigFileEntry {
    pub path: String,
    pub sha256: String,
    pub size: u64,
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
    pub config_files: Vec<ConfigFileEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleArtifact {
    pub component: String,
    pub bundle_id: String,
    pub module: PathBuf,
    pub manifest: PathBuf,
    pub executable_sha256: String,
    /// Canonical source and package files observed while producing this component bundle.
    #[serde(default)]
    pub dependencies: Vec<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleSetManifest {
    pub format_version: u32,
    pub bundles: Vec<BundleArtifact>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkloadArtifactKind {
    CloudflareWorker,
    StaticAssets,
}

impl WorkloadArtifactKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CloudflareWorker => "cloudflare-worker",
            Self::StaticAssets => "static-assets",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BuiltArtifact {
    pub component: String,
    pub input: String,
    pub kind: WorkloadArtifactKind,
    pub digest: String,
    pub source: PathBuf,
    pub stored: PathBuf,
}

#[derive(Clone, Debug)]
struct ArtifactBuildDeclaration {
    component: String,
    input: String,
    kind: WorkloadArtifactKind,
    path: String,
    source_path: PathBuf,
    line: usize,
    column: usize,
}

#[derive(Clone, Debug)]
struct OutputInputDeclaration {
    input: String,
    specifier: String,
    output: String,
}

#[derive(Clone, Debug)]
enum DerivedInputDeclaration {
    Output(OutputInputDeclaration),
    Artifact(ArtifactBuildDeclaration),
}

#[derive(Clone, Debug)]
pub struct DirectoryArtifactStore {
    root: PathBuf,
}

impl DirectoryArtifactStore {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn push(&self, bytes: &[u8]) -> Result<(String, PathBuf), BundleError> {
        let hexadecimal = sha256(bytes);
        let digest = format!("sha256:{hexadecimal}");
        let path = self.root.join("sha256").join(hexadecimal);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| BundleError::WriteOutput {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        fs::write(&path, bytes).map_err(|source| BundleError::WriteOutput {
            path: path.clone(),
            source,
        })?;
        Ok((digest, path))
    }
}

pub fn build_workload_artifacts(
    repository: &Path,
    output: &Path,
) -> Result<Vec<BuiltArtifact>, BundleError> {
    let repository =
        repository
            .canonicalize()
            .map_err(|source| BundleError::InspectRepository {
                path: repository.to_path_buf(),
                source,
            })?;
    let declarations = discover_artifact_builds(&repository)?;
    let store = DirectoryArtifactStore::new(output);
    let esbuild = packaged_esbuild()?;
    let mut built = Vec::with_capacity(declarations.len());
    for declaration in declarations {
        let source = repository.join(&declaration.path);
        let bytes = match declaration.kind {
            WorkloadArtifactKind::CloudflareWorker => {
                if !source.is_file() {
                    return Err(missing_artifact_source(&declaration));
                }
                build_worker_artifact(esbuild.as_ref(), &repository, &declaration, &source)?
            }
            WorkloadArtifactKind::StaticAssets => {
                if !source.is_dir() {
                    return Err(missing_artifact_source(&declaration));
                }
                build_assets_artifact(&source)?
            }
        };
        let (digest, stored) = store.push(&bytes)?;
        built.push(BuiltArtifact {
            component: declaration.component,
            input: declaration.input,
            kind: declaration.kind,
            digest,
            source,
            stored,
        });
    }
    built.sort_by(|left, right| {
        (&left.component, &left.input).cmp(&(&right.component, &right.input))
    });
    Ok(built)
}

pub trait Bundler {
    fn bundle(&self, request: &BundleRequest) -> Result<BundleSetManifest, BundleError>;

    fn build_artifacts(
        &self,
        repository: &Path,
        output: &Path,
    ) -> Result<Vec<BuiltArtifact>, BundleError> {
        build_workload_artifacts(repository, output)
    }
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

#[derive(Clone, Debug)]
struct ConfigFileDeclaration {
    path: String,
    expected_sha256: Option<String>,
    line: usize,
    column: usize,
}

#[derive(Clone, Debug)]
struct ResolvedConfigFile {
    entry: ConfigFileEntry,
    source: PathBuf,
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
    let declarations = read_config_file_declarations(component)?;
    let config_files = resolve_config_files(repository, component, &declarations)?;
    let derived_inputs = discover_derived_inputs(component)?;
    let closure_wire = config_files
        .iter()
        .map(|file| {
            serde_json::json!({
                "path": file.entry.path,
                "sha256": file.entry.sha256,
            })
        })
        .collect::<Vec<_>>();
    let entry_source = generated_entry_source(&import_path, &closure_wire, &derived_inputs);
    let build_dir = tempfile::tempdir().map_err(BundleError::PrepareEsbuild)?;
    let module_path = build_dir.path().join("module.js");
    let metafile_path = build_dir.path().join("metafile.json");
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
        .arg(format!("--metafile={}", metafile_path.display()))
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
    let mut dependencies = read_dependencies(&component.name, repository, &metafile_path)?;
    dependencies.extend(config_files.iter().map(|file| file.source.clone()));
    dependencies.sort();
    dependencies.dedup();
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
        config_files: config_files.iter().map(|file| file.entry.clone()).collect(),
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
    for file in &config_files {
        let destination = artifact_dir.join("files").join(&file.entry.path);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).map_err(|source| BundleError::WriteOutput {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        fs::copy(&file.source, &destination).map_err(|source| BundleError::WriteOutput {
            path: destination,
            source,
        })?;
    }
    let final_manifest = artifact_dir.join("manifest.json");
    write_json(&final_manifest, &manifest)?;
    Ok(BundleArtifact {
        component: component.name.clone(),
        bundle_id,
        module: final_module,
        manifest: final_manifest,
        executable_sha256,
        dependencies,
    })
}

fn read_config_file_declarations(
    component: &ComponentSource,
) -> Result<Vec<ConfigFileDeclaration>, BundleError> {
    let source =
        fs::read_to_string(&component.entry).map_err(|source| BundleError::ReadSource {
            path: component.entry.clone(),
            source,
        })?;
    let Some(manifest) = FILES_MANIFEST_PATTERN.captures(&source) else {
        return Ok(Vec::new());
    };
    let contents = manifest.get(1).expect("files capture exists");
    let mut declarations = Vec::new();
    let mut consumed = String::with_capacity(contents.as_str().len());
    let mut cursor = 0;
    for captures in CONFIG_FILE_PATTERN.captures_iter(contents.as_str()) {
        let full = captures.get(0).expect("configuration file capture exists");
        consumed.push_str(&contents.as_str()[cursor..full.start()]);
        consumed.push_str(&" ".repeat(full.len()));
        cursor = full.end();
        let offset = contents.start() + full.start();
        let (line, column) = line_column(&source, offset);
        declarations.push(ConfigFileDeclaration {
            path: captures[1].to_string(),
            expected_sha256: captures.get(2).map(|value| value.as_str().to_owned()),
            line,
            column,
        });
    }
    consumed.push_str(&contents.as_str()[cursor..]);
    if consumed
        .chars()
        .any(|character| !character.is_whitespace() && character != ',')
    {
        return Err(BundleError::InvalidFileManifest {
            component: component.name.clone(),
            source_path: component.entry.clone(),
        });
    }
    Ok(declarations)
}

fn resolve_config_files(
    repository: &Path,
    component: &ComponentSource,
    declarations: &[ConfigFileDeclaration],
) -> Result<Vec<ResolvedConfigFile>, BundleError> {
    let mut files = BTreeMap::<String, ResolvedConfigFile>::new();
    for declaration in declarations {
        validate_config_path(component, declaration)?;
        let source_path = repository.join(&declaration.path);
        if !source_path.is_file() {
            return Err(BundleError::MissingNativeFile {
                component: component.name.clone(),
                kind: "configuration file".to_owned(),
                path: declaration.path.clone(),
                source_path: component.entry.clone(),
                line: declaration.line,
                column: declaration.column,
            });
        }
        let canonical = source_path
            .canonicalize()
            .map_err(|source| BundleError::ReadSource {
                path: source_path.clone(),
                source,
            })?;
        if !canonical.starts_with(repository) {
            return Err(BundleError::InvalidNativePath {
                component: component.name.clone(),
                path: declaration.path.clone(),
                source_path: component.entry.clone(),
                line: declaration.line,
                column: declaration.column,
            });
        }
        let bytes = fs::read(&canonical).map_err(|source| BundleError::ReadSource {
            path: canonical.clone(),
            source,
        })?;
        let actual = format!("sha256:{}", sha256(&bytes));
        if declaration
            .expected_sha256
            .as_ref()
            .is_some_and(|expected| expected != &actual)
        {
            return Err(BundleError::NativeDigestMismatch {
                component: component.name.clone(),
                path: declaration.path.clone(),
                expected: declaration
                    .expected_sha256
                    .clone()
                    .expect("expected digest exists"),
                actual,
                source_path: Box::new(component.entry.clone()),
                line: declaration.line,
                column: declaration.column,
            });
        }
        let resolved = ResolvedConfigFile {
            entry: ConfigFileEntry {
                path: declaration.path.clone(),
                sha256: actual,
                size: bytes.len() as u64,
            },
            source: canonical,
        };
        files.insert(resolved.entry.path.clone(), resolved);
    }
    Ok(files.into_values().collect())
}

fn validate_config_path(
    component: &ComponentSource,
    declaration: &ConfigFileDeclaration,
) -> Result<(), BundleError> {
    let valid = !declaration.path.is_empty()
        && !declaration.path.starts_with('/')
        && !declaration.path.contains('\\')
        && declaration
            .path
            .split('/')
            .all(|part| !matches!(part, "" | "." | ".."));
    if valid {
        Ok(())
    } else {
        Err(BundleError::InvalidNativePath {
            component: component.name.clone(),
            path: declaration.path.clone(),
            source_path: component.entry.clone(),
            line: declaration.line,
            column: declaration.column,
        })
    }
}

fn discover_artifact_builds(
    repository: &Path,
) -> Result<Vec<ArtifactBuildDeclaration>, BundleError> {
    let mut declarations = Vec::new();
    for component in discover_components(repository)? {
        declarations.extend(
            discover_derived_inputs(&component)?
                .into_iter()
                .filter_map(|input| match input {
                    DerivedInputDeclaration::Artifact(declaration) => Some(declaration),
                    DerivedInputDeclaration::Output(_) => None,
                }),
        );
    }
    Ok(declarations)
}

fn discover_derived_inputs(
    component: &ComponentSource,
) -> Result<Vec<DerivedInputDeclaration>, BundleError> {
    let source =
        fs::read_to_string(&component.entry).map_err(|source| BundleError::ReadSource {
            path: component.entry.clone(),
            source,
        })?;
    let imports = DEFAULT_IMPORT_PATTERN
        .captures_iter(&source)
        .map(|captures| (captures[1].to_owned(), captures[2].to_owned()))
        .collect::<BTreeMap<_, _>>();
    let mut used_names = BTreeMap::<String, usize>::new();
    let mut outputs = BTreeMap::<(String, String), OutputInputDeclaration>::new();
    for captures in OUTPUT_REFERENCE_PATTERN.captures_iter(&source) {
        let alias = captures[1].to_owned();
        let output = captures[2].to_owned();
        let Some(specifier) = imports.get(&alias) else {
            continue;
        };
        let key = (specifier.clone(), output.clone());
        outputs
            .entry(key)
            .or_insert_with(|| OutputInputDeclaration {
                input: unique_input_name(
                    &format!("{alias}{}", capitalize(&output)),
                    &mut used_names,
                ),
                specifier: specifier.clone(),
                output,
            });
    }

    let mut artifacts = Vec::new();
    for worker_source in WORKER_SOURCE_PATTERN.captures_iter(&source) {
        let contents = worker_source.get(1).expect("worker source capture exists");
        let mut found = 0;
        for captures in WORKER_SOURCE_FIELD_PATTERN.captures_iter(contents.as_str()) {
            found += 1;
            let field = &captures[1];
            let path = captures[2].to_owned();
            let full = captures.get(0).expect("worker source field capture exists");
            let offset = contents.start() + full.start();
            let (line, column) = line_column(&source, offset);
            if !valid_repository_path(&path) {
                return Err(BundleError::InvalidNativePath {
                    component: component.name.clone(),
                    path,
                    source_path: component.entry.clone(),
                    line,
                    column,
                });
            }
            artifacts.push(ArtifactBuildDeclaration {
                component: component.name.clone(),
                input: unique_input_name(
                    if field == "entry" {
                        "workerEntry"
                    } else {
                        "workerAssets"
                    },
                    &mut used_names,
                ),
                kind: if field == "entry" {
                    WorkloadArtifactKind::CloudflareWorker
                } else {
                    WorkloadArtifactKind::StaticAssets
                },
                path,
                source_path: component.entry.clone(),
                line,
                column,
            });
        }
        if found == 0 {
            return Err(BundleError::InvalidWorkerSource {
                component: component.name.clone(),
                source_path: component.entry.clone(),
            });
        }
    }

    let mut derived = outputs
        .into_values()
        .map(DerivedInputDeclaration::Output)
        .chain(artifacts.into_iter().map(DerivedInputDeclaration::Artifact))
        .collect::<Vec<_>>();
    derived.sort_by(|left, right| derived_input_name(left).cmp(derived_input_name(right)));
    Ok(derived)
}

fn generated_entry_source(
    import_path: &str,
    closure_wire: &[serde_json::Value],
    derived_inputs: &[DerivedInputDeclaration],
) -> String {
    let mut imports = String::new();
    let mut entries = Vec::with_capacity(derived_inputs.len());
    for (index, input) in derived_inputs.iter().enumerate() {
        match input {
            DerivedInputDeclaration::Output(output) => {
                let generated = format!("__henosis_input_{index}");
                imports.push_str(&format!(
                    "import {generated} from {};\n",
                    serde_json::to_string(&output.specifier)
                        .expect("module specifier is JSON encodable")
                ));
                entries.push(format!(
                    "{}: {generated}.outputs.{}",
                    serde_json::to_string(&output.input).expect("input name is JSON encodable"),
                    output.output,
                ));
            }
            DerivedInputDeclaration::Artifact(artifact) => {
                entries.push(format!(
                    "{}: {{ source: \"artifact\", kind: {}, path: {} }}",
                    serde_json::to_string(&artifact.input).expect("input name is JSON encodable"),
                    serde_json::to_string(artifact.kind.as_str())
                        .expect("artifact kind is JSON encodable"),
                    serde_json::to_string(&artifact.path).expect("artifact path is JSON encodable"),
                ));
            }
        }
    }
    format!(
        "import componentDefinition from {};\n{imports}import {{ createBundle }} from \"@henosis/core\";\nconst bundle = createBundle(componentDefinition, {}, {{ {} }});\nexport const protocolVersion = bundle.protocolVersion;\nexport const component = bundle.component;\nexport const evaluate = bundle.evaluate;\n",
        serde_json::to_string(import_path).expect("path string is JSON encodable"),
        serde_json::to_string(closure_wire).expect("closure manifest is JSON encodable"),
        entries.join(", "),
    )
}

fn derived_input_name(input: &DerivedInputDeclaration) -> &str {
    match input {
        DerivedInputDeclaration::Output(output) => &output.input,
        DerivedInputDeclaration::Artifact(artifact) => &artifact.input,
    }
}

fn unique_input_name(base: &str, used: &mut BTreeMap<String, usize>) -> String {
    let count = used.entry(base.to_owned()).or_default();
    *count += 1;
    if *count == 1 {
        base.to_owned()
    } else {
        format!("{base}{count}")
    }
}

fn capitalize(value: &str) -> String {
    let mut characters = value.chars();
    match characters.next() {
        Some(first) => first.to_ascii_uppercase().to_string() + characters.as_str(),
        None => String::new(),
    }
}

fn build_worker_artifact(
    esbuild: &Path,
    repository: &Path,
    declaration: &ArtifactBuildDeclaration,
    source: &Path,
) -> Result<Vec<u8>, BundleError> {
    let output = tempfile::NamedTempFile::new().map_err(BundleError::PrepareEsbuild)?;
    let result = Command::new(esbuild)
        .current_dir(repository)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .arg(source)
        .arg("--bundle")
        .arg("--format=esm")
        .arg("--platform=browser")
        .arg("--target=esnext")
        .arg("--charset=utf8")
        .arg("--tree-shaking=true")
        .arg("--legal-comments=none")
        .arg("--minify")
        .arg(format!("--outfile={}", output.path().display()))
        .output()
        .map_err(BundleError::PrepareEsbuild)?;
    if !result.status.success() {
        return Err(BundleError::ArtifactBuild {
            component: declaration.component.clone(),
            stderr: String::from_utf8_lossy(&result.stderr).trim().to_owned(),
        });
    }
    fs::read(output.path()).map_err(|source| BundleError::ReadSource {
        path: output.path().to_path_buf(),
        source,
    })
}

fn build_assets_artifact(source: &Path) -> Result<Vec<u8>, BundleError> {
    let mut files = BTreeMap::new();
    for entry in WalkDir::new(source).follow_links(false) {
        let entry = entry.map_err(|error| BundleError::InspectRepository {
            path: source.to_path_buf(),
            source: error
                .into_io_error()
                .unwrap_or_else(|| std::io::Error::other("static assets traversal failed")),
        })?;
        if !entry.file_type().is_file() {
            continue;
        }
        let relative = entry
            .path()
            .strip_prefix(source)
            .expect("assets traversal stays below root")
            .to_string_lossy()
            .replace('\\', "/");
        let bytes = fs::read(entry.path()).map_err(|source| BundleError::ReadSource {
            path: entry.path().to_path_buf(),
            source,
        })?;
        files.insert(relative, bytes);
    }
    serde_json::to_vec(&serde_json::json!({
        "format": "henosis-static-assets-v1",
        "files": files,
    }))
    .map_err(BundleError::EncodeManifest)
}

fn missing_artifact_source(declaration: &ArtifactBuildDeclaration) -> BundleError {
    BundleError::MissingArtifactSource {
        component: declaration.component.clone(),
        kind: declaration.kind.as_str().to_owned(),
        path: declaration.path.clone(),
        source_path: declaration.source_path.clone(),
        line: declaration.line,
        column: declaration.column,
    }
}

fn valid_repository_path(path: &str) -> bool {
    !path.is_empty()
        && !path.starts_with('/')
        && !path.contains('\\')
        && path.split('/').all(|part| !matches!(part, "" | "." | ".."))
}

fn line_column(source: &str, offset: usize) -> (usize, usize) {
    let prefix = &source[..offset];
    let line = prefix.bytes().filter(|byte| *byte == b'\n').count() + 1;
    let column = prefix
        .rsplit_once('\n')
        .map_or(prefix.len() + 1, |(_, tail)| tail.len() + 1);
    (line, column)
}

#[derive(Deserialize)]
struct EsbuildMetafile {
    inputs: BTreeMap<String, serde_json::Value>,
}

fn read_dependencies(
    component: &str,
    repository: &Path,
    metafile_path: &Path,
) -> Result<Vec<PathBuf>, BundleError> {
    let bytes = fs::read(metafile_path).map_err(|source| BundleError::ReadSource {
        path: metafile_path.to_path_buf(),
        source,
    })?;
    let metafile: EsbuildMetafile =
        serde_json::from_slice(&bytes).map_err(|source| BundleError::DecodeMetafile {
            component: component.to_string(),
            source,
        })?;
    let mut dependencies = metafile
        .inputs
        .into_keys()
        .filter(|path| path != "henosis-component.ts")
        .filter_map(|path| {
            let path = repository.join(path);
            path.is_file().then(|| path.canonicalize().unwrap_or(path))
        })
        .collect::<Vec<_>>();
    dependencies.sort();
    dependencies.dedup();
    Ok(dependencies)
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
    encoder.map(10).map_err(BundleError::EncodeIdentity)?;
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
    encoder.u8(9).map_err(BundleError::EncodeIdentity)?;
    encoder
        .array(manifest.config_files.len() as u64)
        .map_err(BundleError::EncodeIdentity)?;
    for file in &manifest.config_files {
        encoder.array(3).map_err(BundleError::EncodeIdentity)?;
        encoder
            .str(&file.path)
            .map_err(BundleError::EncodeIdentity)?;
        encoder
            .str(&file.sha256)
            .map_err(BundleError::EncodeIdentity)?;
        encoder
            .u64(file.size)
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
            config_files: Vec::new(),
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

    #[test]
    fn configuration_file_bytes_round_trip_and_change_bundle_identity() {
        let repo = tempfile::tempdir().unwrap();
        fs::create_dir_all(repo.path().join("src")).unwrap();
        fs::create_dir_all(repo.path().join("migrations")).unwrap();
        fs::create_dir_all(repo.path().join("node_modules/@henosis/core")).unwrap();
        fs::write(
            repo.path().join("src/database.ts"),
            "import { config, defineComponent } from '@henosis/core'; export default defineComponent({ name: 'database', files: [config.file('migrations/001.sql')], outputs: {}, build() { return {}; } });",
        )
        .unwrap();
        fs::write(
            repo.path().join("node_modules/@henosis/core/package.json"),
            r#"{"name":"@henosis/core","type":"module","exports":"./index.js"}"#,
        )
        .unwrap();
        fs::write(
            repo.path().join("node_modules/@henosis/core/index.js"),
            "export const config = { file(path) { return { path }; } }; export function defineComponent(spec) { return spec; } export function createBundle(component, files) { return { protocolVersion: 1, component: { name: component.name, inputs: {}, outputs: {}, files }, evaluate() { return { protocolVersion: 1, status: 'complete', resources: [], outputs: {}, observedOutputs: {}, reads: [] }; } }; }",
        )
        .unwrap();
        fs::write(repo.path().join("migrations/001.sql"), "select 1;\n").unwrap();

        let first = EsbuildBundler
            .bundle(&BundleRequest {
                repository: repo.path().to_path_buf(),
                output: repo.path().join("bundles-one"),
            })
            .unwrap();
        let manifest: BundleManifestV1 =
            serde_json::from_slice(&fs::read(&first.bundles[0].manifest).unwrap()).unwrap();
        assert_eq!(manifest.config_files.len(), 1);
        assert_eq!(
            fs::read(
                first.bundles[0]
                    .manifest
                    .parent()
                    .unwrap()
                    .join("files/migrations/001.sql")
            )
            .unwrap(),
            b"select 1;\n"
        );

        fs::write(repo.path().join("migrations/001.sql"), "select 2;\n").unwrap();
        let second = EsbuildBundler
            .bundle(&BundleRequest {
                repository: repo.path().to_path_buf(),
                output: repo.path().join("bundles-two"),
            })
            .unwrap();
        assert_ne!(first.bundles[0].bundle_id, second.bundles[0].bundle_id);
    }

    #[test]
    fn imported_outputs_and_worker_sources_become_hidden_bundle_inputs() {
        let repo = tempfile::tempdir().unwrap();
        fs::create_dir_all(repo.path().join("src")).unwrap();
        let entry = repo.path().join("src/web.ts");
        fs::write(
            &entry,
            "import a from '@henosis/service-a'; import { worker } from '@henosis/platform-cloudflare'; export default defineComponent({ name: 'web', outputs: {}, build(ctx) { ctx.emit(worker.create('web', { source: { entry: 'workers/web.ts', assets: 'public' }, vars: { BACKEND_URL: a.outputs.api.value } })); return {}; } });",
        )
        .unwrap();
        let component = ComponentSource {
            name: "web".to_owned(),
            entry,
        };

        let inputs = discover_derived_inputs(&component).unwrap();
        assert_eq!(
            inputs.iter().map(derived_input_name).collect::<Vec<_>>(),
            vec!["aApi", "workerAssets", "workerEntry"]
        );
        let entry_source = generated_entry_source("./src/web.ts", &[], &inputs);
        assert!(entry_source.contains("\"aApi\": __henosis_input_0.outputs.api"));
        assert!(entry_source.contains(
            "\"workerEntry\": { source: \"artifact\", kind: \"cloudflare-worker\", path: \"workers/web.ts\" }"
        ));
        assert!(entry_source.contains(
            "\"workerAssets\": { source: \"artifact\", kind: \"static-assets\", path: \"public\" }"
        ));
    }

    #[test]
    fn worker_rebuild_changes_binding_without_changing_config_bundle_identity() {
        let repo = tempfile::tempdir().unwrap();
        fs::create_dir_all(repo.path().join("src")).unwrap();
        fs::create_dir_all(repo.path().join("workers")).unwrap();
        fs::create_dir_all(repo.path().join("node_modules/@henosis/core")).unwrap();
        fs::create_dir_all(
            repo.path()
                .join("node_modules/@henosis/platform-cloudflare"),
        )
        .unwrap();
        fs::write(
            repo.path().join("src/web.ts"),
            "import { defineComponent } from '@henosis/core'; import { worker } from '@henosis/platform-cloudflare'; export default defineComponent({ name: 'web', outputs: {}, build(ctx) { ctx.emit(worker.create('web', { source: { entry: 'workers/web.ts' } })); return {}; } });",
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
            repo.path()
                .join("node_modules/@henosis/platform-cloudflare/package.json"),
            r#"{"name":"@henosis/platform-cloudflare","type":"module","exports":"./index.js"}"#,
        )
        .unwrap();
        fs::write(
            repo.path().join("node_modules/@henosis/platform-cloudflare/index.js"),
            "export const worker = { create(name, body) { return { kind: 'cloudflare/worker@1', name, body, outputs: {}, configFiles: [] }; } };",
        )
        .unwrap();
        fs::write(
            repo.path().join("workers/web.ts"),
            "export default { fetch() { return new Response('one'); } };\n",
        )
        .unwrap();

        let first_bundle = EsbuildBundler
            .bundle(&BundleRequest {
                repository: repo.path().to_path_buf(),
                output: repo.path().join("bundles-one"),
            })
            .unwrap();
        let first_artifact =
            build_workload_artifacts(repo.path(), &repo.path().join("artifacts")).unwrap();
        fs::write(
            repo.path().join("workers/web.ts"),
            "export default { fetch() { return new Response('two'); } };\n",
        )
        .unwrap();
        let second_bundle = EsbuildBundler
            .bundle(&BundleRequest {
                repository: repo.path().to_path_buf(),
                output: repo.path().join("bundles-two"),
            })
            .unwrap();
        let second_artifact =
            build_workload_artifacts(repo.path(), &repo.path().join("artifacts")).unwrap();

        assert_eq!(
            first_bundle.bundles[0].bundle_id,
            second_bundle.bundles[0].bundle_id
        );
        assert_ne!(first_artifact[0].digest, second_artifact[0].digest);
        assert_eq!(first_artifact[0].input, "workerEntry");
    }
}
