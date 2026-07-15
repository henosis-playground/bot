use std::path::PathBuf;

use henosis_bundle::{BundleError, BundleRequest, Bundler};

use crate::henosis::core_client::{
    BundlePin, CoreBoundary, CoreBoundaryError, GraphIntent, GraphPhase, GraphStatus,
};
use crate::henosis::status::{STATUS_END, STATUS_START};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviewRequest {
    pub repository: String,
    pub pull_request: u64,
    pub checkout: PathBuf,
    pub environment: String,
}

#[derive(Debug, thiserror::Error)]
pub enum PreviewError {
    #[error(transparent)]
    Bundle(#[from] BundleError),
    #[error(transparent)]
    Core(#[from] CoreBoundaryError),
}

pub struct PreviewWorkflow<C, B> {
    core: C,
    bundler: B,
    artifact_root: PathBuf,
}

impl<C, B> PreviewWorkflow<C, B>
where
    C: CoreBoundary,
    B: Bundler,
{
    pub fn new(core: C, bundler: B, artifact_root: impl Into<PathBuf>) -> Self {
        Self {
            core,
            bundler,
            artifact_root: artifact_root.into(),
        }
    }

    pub async fn p_plus(&self, request: &PreviewRequest) -> Result<GraphStatus, PreviewError> {
        let output = self.artifact_root.join(&request.environment).join(format!(
            "{}-{}",
            repository_slug(&request.repository),
            request.pull_request
        ));
        let bundles = self.bundler.bundle(&BundleRequest {
            repository: request.checkout.clone(),
            output,
        })?;
        let pins = bundles
            .bundles
            .into_iter()
            .map(|bundle| BundlePin {
                component: bundle.component,
                bundle_id: bundle.bundle_id,
            })
            .collect();
        let intent = match self.core.status(&request.environment).await {
            Ok(_) => GraphIntent::Update {
                graph: request.environment.clone(),
                bundles: pins,
            },
            Err(CoreBoundaryError::GraphNotFound(_)) => GraphIntent::Create {
                graph: request.environment.clone(),
                bundles: pins,
            },
            Err(error) => return Err(error.into()),
        };
        self.core.apply(intent).await.map_err(Into::into)
    }

    pub async fn p_minus(&self, environment: &str) -> Result<GraphStatus, PreviewError> {
        self.core
            .apply(GraphIntent::Retire {
                graph: environment.to_string(),
            })
            .await
            .map_err(Into::into)
    }

    pub async fn status_section(&self, environment: &str) -> Result<String, PreviewError> {
        let status = self.core.status(environment).await?;
        Ok(render_core_status(&status))
    }
}

pub fn render_core_status(status: &GraphStatus) -> String {
    let phase = match status.phase {
        GraphPhase::Planning => ":hourglass_flowing_sand: planning",
        GraphPhase::Blocked => ":pause_button: blocked",
        GraphPhase::Reconciling => ":hourglass_flowing_sand: reconciling",
        GraphPhase::Ready => ":white_check_mark: ready",
        GraphPhase::Failed => ":x: failed",
        GraphPhase::Retired => ":heavy_minus_sign: retired",
    };
    let blocked = if status.blocked_on.is_empty() {
        "none".to_string()
    } else {
        status
            .blocked_on
            .iter()
            .map(|input| format!("`{}.{}`", input.component, input.input))
            .collect::<Vec<_>>()
            .join(" · ")
    };
    format!(
        "{STATUS_START}\n### Henosis status\n\n| | |\n|---|---|\n| Environment | `{}` |\n| Plan | generation {} · {} resource(s) |\n| Blocked on | {} |\n| Observed ready | {} |\n| Status | {} |\n{STATUS_END}",
        status.graph,
        status.generation,
        status.planned_resources,
        blocked,
        status.observed_ready,
        phase,
    )
}

fn repository_slug(repository: &str) -> &str {
    repository.rsplit('/').next().unwrap_or(repository)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use henosis_bundle::{BundleArtifact, BundleSetManifest};

    use super::*;
    use crate::henosis::core_client::{FakeCoreBoundary, GraphIntent};

    struct FakeBundler;

    impl Bundler for FakeBundler {
        fn bundle(&self, request: &BundleRequest) -> Result<BundleSetManifest, BundleError> {
            fs::create_dir_all(&request.output).unwrap();
            Ok(BundleSetManifest {
                format_version: 1,
                bundles: vec![BundleArtifact {
                    component: "web".to_string(),
                    bundle_id: "a".repeat(64),
                    module: request.output.join("module.js"),
                    manifest: request.output.join("bundle.json"),
                    executable_sha256: "b".repeat(64),
                }],
            })
        }
    }

    #[tokio::test]
    async fn pr_opened_then_p_plus_records_intent_and_renders_status() {
        let core = FakeCoreBoundary::default();
        let workflow = PreviewWorkflow::new(
            core.clone(),
            FakeBundler,
            tempfile::tempdir().unwrap().path(),
        );
        let request = PreviewRequest {
            repository: "henosis-playground/web".to_string(),
            pull_request: 42,
            checkout: Path::new("/checkout/web").to_path_buf(),
            environment: "preview_01k00000000000000000000000".to_string(),
        };

        let accepted = workflow.p_plus(&request).await.unwrap();
        assert_eq!(accepted.generation, 1);
        assert_eq!(
            core.intents().await,
            vec![GraphIntent::Create {
                graph: request.environment.clone(),
                bundles: vec![BundlePin {
                    component: "web".to_string(),
                    bundle_id: "a".repeat(64),
                }],
            }]
        );

        let status = workflow.status_section(&request.environment).await.unwrap();
        insta::assert_snapshot!(status, @r#"
<!-- henosis:status -->
### Henosis status

| | |
|---|---|
| Environment | `preview_01k00000000000000000000000` |
| Plan | generation 1 · 0 resource(s) |
| Blocked on | none |
| Observed ready | 0 |
| Status | :hourglass_flowing_sand: planning |
<!-- /henosis:status -->
"#);
    }
}
