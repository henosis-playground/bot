use std::collections::BTreeMap;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use henosis_app::{ArtifactBinding, ArtifactRequirement, ArtifactService, WorkloadArtifactKind};
use henosis_types::{ArtifactDigest, ComponentName, InputName};
use sha2::{Digest as _, Sha256};
use tempfile::{Builder, TempPath};
use walkdir::WalkDir;

const ESBUILD_SHA256: &str = "e4d41ef34045d7d1751c375cd4a90d07d929832f838830dc5212689a6567ee58";

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
const PACKAGED_ESBUILD: &[u8] = include_bytes!("../../../tools/esbuild/linux-x64/esbuild");

#[derive(Debug, thiserror::Error)]
pub enum ArtifactError {
    #[error("cannot inspect artifact source `{path}`: {source}")]
    Inspect {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error(
        "component `{component}` declares missing {kind} source `{path}` at {source_path}:{line}:{column}"
    )]
    Missing {
        component: String,
        kind: String,
        path: String,
        source_path: PathBuf,
        line: usize,
        column: usize,
    },
    #[error("cannot prepare packaged esbuild: {0}")]
    Prepare(std::io::Error),
    #[error(
        "the packaged esbuild executable failed its checksum: expected {expected}, got {actual}"
    )]
    EsbuildChecksum {
        expected: &'static str,
        actual: String,
    },
    #[error("cannot build workload artifact for component `{component}`\n{stderr}")]
    Build { component: String, stderr: String },
    #[error("artifact requirement has an invalid domain value: {0}")]
    InvalidRequirement(String),
    #[error("cannot encode static assets: {0}")]
    Encode(serde_json::Error),
}

#[derive(Clone, Debug)]
pub struct DirectoryArtifactService {
    root: PathBuf,
}

impl DirectoryArtifactService {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

impl ArtifactService for DirectoryArtifactService {
    type Error = ArtifactError;

    fn build(
        &self,
        repository: &Path,
        requirements: &[ArtifactRequirement],
    ) -> Result<Vec<ArtifactBinding>, Self::Error> {
        build_requirements(repository, &self.root, requirements)
    }
}

pub fn build_requirements(
    repository: &Path,
    output: &Path,
    requirements: &[ArtifactRequirement],
) -> Result<Vec<ArtifactBinding>, ArtifactError> {
    let repository = repository
        .canonicalize()
        .map_err(|source| ArtifactError::Inspect {
            path: repository.to_path_buf(),
            source,
        })?;
    let esbuild = packaged_esbuild()?;
    let mut built = Vec::with_capacity(requirements.len());
    for requirement in requirements {
        let source = repository.join(&requirement.path);
        let bytes = match requirement.kind {
            WorkloadArtifactKind::CloudflareWorker => {
                if !source.is_file() {
                    return Err(missing(requirement));
                }
                build_worker(esbuild.as_ref(), &repository, requirement, &source)?
            }
            WorkloadArtifactKind::StaticAssets => {
                if !source.is_dir() {
                    return Err(missing(requirement));
                }
                build_assets(&source)?
            }
        };
        let (digest, stored) = store(output, &bytes)?;
        built.push(ArtifactBinding {
            component: ComponentName::new(requirement.component.clone())
                .map_err(|error| ArtifactError::InvalidRequirement(error.to_string()))?,
            input: InputName::new(requirement.input.clone())
                .map_err(|error| ArtifactError::InvalidRequirement(error.to_string()))?,
            kind: requirement.kind,
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

fn build_worker(
    esbuild: &Path,
    repository: &Path,
    requirement: &ArtifactRequirement,
    source: &Path,
) -> Result<Vec<u8>, ArtifactError> {
    let output = tempfile::NamedTempFile::new().map_err(ArtifactError::Prepare)?;
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
        .map_err(ArtifactError::Prepare)?;
    if !result.status.success() {
        return Err(ArtifactError::Build {
            component: requirement.component.clone(),
            stderr: String::from_utf8_lossy(&result.stderr).trim().to_owned(),
        });
    }
    fs::read(output.path()).map_err(|source_error| ArtifactError::Inspect {
        path: output.path().to_path_buf(),
        source: source_error,
    })
}

fn build_assets(source: &Path) -> Result<Vec<u8>, ArtifactError> {
    let mut files = BTreeMap::new();
    for entry in WalkDir::new(source).follow_links(false) {
        let entry = entry.map_err(|error| ArtifactError::Inspect {
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
        let bytes = fs::read(entry.path()).map_err(|source_error| ArtifactError::Inspect {
            path: entry.path().to_path_buf(),
            source: source_error,
        })?;
        files.insert(relative, bytes);
    }
    serde_json::to_vec(&serde_json::json!({
        "format": "henosis-static-assets-v1",
        "files": files,
    }))
    .map_err(ArtifactError::Encode)
}

fn store(root: &Path, bytes: &[u8]) -> Result<(ArtifactDigest, PathBuf), ArtifactError> {
    let digest = ArtifactDigest::from_bytes(Sha256::digest(bytes).into());
    let hexadecimal = hex::encode(digest.as_bytes());
    let path = root.join("sha256").join(hexadecimal);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| ArtifactError::Inspect {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    fs::write(&path, bytes).map_err(|source| ArtifactError::Inspect {
        path: path.clone(),
        source,
    })?;
    Ok((digest, path))
}

fn missing(requirement: &ArtifactRequirement) -> ArtifactError {
    ArtifactError::Missing {
        component: requirement.component.clone(),
        kind: requirement.kind.as_str().to_owned(),
        path: requirement.path.clone(),
        source_path: requirement.source_path.clone(),
        line: requirement.line,
        column: requirement.column,
    }
}

fn packaged_esbuild() -> Result<TempPath, ArtifactError> {
    #[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
    compile_error!("the D26 prototype currently packages esbuild only for linux-x86_64");

    let actual = sha256(PACKAGED_ESBUILD);
    if actual != ESBUILD_SHA256 {
        return Err(ArtifactError::EsbuildChecksum {
            expected: ESBUILD_SHA256,
            actual,
        });
    }
    let mut executable = Builder::new()
        .prefix("henosis-esbuild-")
        .tempfile()
        .map_err(ArtifactError::Prepare)?;
    executable
        .write_all(PACKAGED_ESBUILD)
        .map_err(ArtifactError::Prepare)?;
    executable.flush().map_err(ArtifactError::Prepare)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(executable.path(), fs::Permissions::from_mode(0o700))
            .map_err(ArtifactError::Prepare)?;
    }
    Ok(executable.into_temp_path())
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
