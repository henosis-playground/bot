use std::path::PathBuf;

use henosis_app::{
    ApplyGraph, ArtifactService, Bundler, CheckoutService, GraphOperation, GraphSourcePolicy,
    GraphStatus, SourceRequest,
};

use crate::henosis::core_client::{CoreBoundary, CoreBoundaryError, GraphPhase};
use crate::henosis::status::{STATUS_END, STATUS_START};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviewRequest {
    pub repository: String,
    pub pull_request: u64,
    pub checkout: PathBuf,
    pub revision: String,
    pub reference: Option<String>,
    pub environment: String,
}

#[derive(Debug, thiserror::Error)]
pub enum PreviewError {
    #[error(transparent)]
    Operation(#[from] henosis_app::OperationError),
    #[error(transparent)]
    Core(#[from] CoreBoundaryError),
}

pub struct PreviewWorkflow<C, B, A, K> {
    core: C,
    operation: GraphOperation<C, B, A, K>,
}

impl<C, B, A, K> PreviewWorkflow<C, B, A, K>
where
    C: CoreBoundary + henosis_app::CoreClient + Clone,
    B: Bundler,
    A: ArtifactService,
    K: CheckoutService,
{
    pub fn new(
        core: C,
        bundler: B,
        artifacts: A,
        checkouts: K,
        bundle_root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            core: core.clone(),
            operation: GraphOperation::new(core, bundler, artifacts, checkouts, bundle_root),
        }
    }

    pub async fn p_plus(&self, request: &PreviewRequest) -> Result<GraphStatus, PreviewError> {
        Ok(self
            .operation
            .apply(ApplyGraph {
                graph: request.environment.clone(),
                sources: vec![SourceRequest {
                    repository: request.repository.clone(),
                    revision: Some(request.revision.clone()),
                    reference: request.reference.clone(),
                    component: None,
                }],
                create: true,
                source_policy: GraphSourcePolicy::AcceptLocal,
                preserve_unmentioned: false,
            })
            .await?
            .status)
    }

    pub async fn p_minus(&self, environment: &str) -> Result<GraphStatus, PreviewError> {
        self.operation.retire(environment).await.map_err(Into::into)
    }

    pub async fn status_section(
        &self,
        environment_name: &str,
        graph: &str,
    ) -> Result<String, PreviewError> {
        let status = CoreBoundary::status(&self.core, graph).await?;
        Ok(render_core_status(environment_name, &status))
    }
}

pub fn render_core_status(environment_name: &str, status: &GraphStatus) -> String {
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
        "{STATUS_START}\n### Henosis status\n\n| | |\n|---|---|\n| Environment | **{}** (`{}`) |\n| Plan | generation {} · {} resource(s) |\n| Blocked on | {} |\n| Observed ready | {} |\n| Status | {} |\n{STATUS_END}",
        environment_name,
        status.graph,
        status.generation,
        status.planned_resources,
        blocked,
        status.observed_ready,
        phase,
    )
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use henosis_app::{
        ArtifactBinding, ArtifactRequirement, BundleArtifact, BundleError, BundleRequest,
        BundleSetManifest, PreparedSource, SourceProvenance,
    };

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
                    artifact_requirements: Vec::new(),
                    dependencies: Vec::new(),
                }],
            })
        }
    }

    struct NoArtifacts;

    impl ArtifactService for NoArtifacts {
        type Error = std::convert::Infallible;

        fn build(
            &self,
            _repository: &Path,
            _requirements: &[ArtifactRequirement],
        ) -> Result<Vec<ArtifactBinding>, Self::Error> {
            Ok(Vec::new())
        }
    }

    #[derive(Clone)]
    struct PreparedCheckout;

    impl CheckoutService for PreparedCheckout {
        type Error = std::convert::Infallible;

        async fn checkout(&self, request: &SourceRequest) -> Result<PreparedSource, Self::Error> {
            Ok(PreparedSource {
                repository: Path::new("/checkout/web").to_path_buf(),
                provenance: SourceProvenance::Vcs {
                    repository: request.repository.clone(),
                    revision: request.revision.clone().unwrap(),
                    reference: request.reference.clone(),
                },
                component: request.component.clone(),
                lease: None,
            })
        }
    }

    #[tokio::test]
    async fn pr_opened_then_p_plus_records_intent_and_renders_status() {
        let core = FakeCoreBoundary::default();
        let root = tempfile::tempdir().unwrap();
        let workflow = PreviewWorkflow::new(
            core.clone(),
            FakeBundler,
            NoArtifacts,
            PreparedCheckout,
            root.path().join("bundles"),
        );
        let request = PreviewRequest {
            repository: "henosis-playground/web".to_string(),
            pull_request: 42,
            checkout: Path::new("/checkout/web").to_path_buf(),
            revision: "deadbeef".to_string(),
            reference: Some("refs/pull/42/head".to_string()),
            environment: "preview_01k00000000000000000000000".to_string(),
        };

        let accepted = workflow.p_plus(&request).await.unwrap();
        assert_eq!(accepted.generation, 1);
        assert_eq!(
            core.intents().await,
            vec![GraphIntent::Create {
                graph: request.environment.clone(),
                bundles: vec![henosis_app::BundlePin {
                    component: "web".to_string(),
                    bundle_id: "a".repeat(64),
                    input_bindings: std::collections::BTreeMap::new(),
                    source: Some(SourceProvenance::Vcs {
                        repository: request.repository.clone(),
                        revision: request.revision.clone(),
                        reference: request.reference.clone(),
                    }),
                }],
                source_policy: GraphSourcePolicy::AcceptLocal,
            }]
        );

        let status = workflow
            .status_section("shared-demo", &request.environment)
            .await
            .unwrap();
        insta::assert_snapshot!(status, @r#"
<!-- henosis:status -->
### Henosis status

| | |
|---|---|
| Environment | **shared-demo** (`preview_01k00000000000000000000000`) |
| Plan | generation 1 · 0 resource(s) |
| Blocked on | none |
| Observed ready | 0 |
| Status | :hourglass_flowing_sand: planning |
<!-- /henosis:status -->
"#);
    }
}
