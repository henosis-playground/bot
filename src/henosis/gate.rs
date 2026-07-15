use std::collections::BTreeMap;
use std::sync::Mutex;

use anyhow::Context;

use crate::henosis::gate_report::GateReport;
use crate::henosis::queue::GateRun;

pub trait GateExecutor {
    async fn execute(&self, gate_run: &GateRun) -> anyhow::Result<GateReport>;
}

/// Compile-time seam retained while the old queue is still present.
///
/// D26 removed renderer/gate-runner process execution. Henosis repositories now
/// submit graph intent through the core boundary; non-Henosis repositories keep
/// using bors' native merge queue unchanged.
pub struct CliGateExecutor;

impl CliGateExecutor {
    pub fn new<D>(_legacy_command: impl Into<String>, _legacy_dev_manifest: D) -> Self {
        Self
    }
}

impl GateExecutor for CliGateExecutor {
    async fn execute(&self, _gate_run: &GateRun) -> anyhow::Result<GateReport> {
        anyhow::bail!(
            "the legacy renderer-backed merge gate was removed by D26; submit bundle graph intent and present core plan status instead"
        )
    }
}

#[derive(Debug, Default)]
pub struct FakeGateExecutor {
    reports: Mutex<BTreeMap<String, GateReport>>,
}

impl FakeGateExecutor {
    pub fn new(reports: BTreeMap<String, GateReport>) -> Self {
        Self {
            reports: Mutex::new(reports),
        }
    }
}

impl GateExecutor for FakeGateExecutor {
    async fn execute(&self, gate_run: &GateRun) -> anyhow::Result<GateReport> {
        self.reports
            .lock()
            .unwrap()
            .get(&gate_run.external_id)
            .cloned()
            .with_context(|| format!("No fake gate report for `{}`", gate_run.external_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::henosis::gate_report::GateFailure;
    use crate::henosis::queue::{
        CandidateComponent, CandidateWorld, PENDING_STATUS, QueuePullRequest, RecordedGateRun,
    };

    fn gate_run(external_id: &str) -> RecordedGateRun {
        RecordedGateRun {
            id: 1,
            external_id: external_id.to_string(),
            status: PENDING_STATUS.to_string(),
            world: CandidateWorld {
                members: vec![QueuePullRequest::new(
                    "henosis-playground/service-a",
                    3,
                    "service-a",
                    "a-pr",
                    "pr/3",
                    "a-pr",
                )],
                components: vec![CandidateComponent {
                    name: "service-a".to_string(),
                    repo: "henosis-playground/service-a".to_string(),
                    r#ref: "a-pr".to_string(),
                    digest: "sha256:a".to_string(),
                    candidate: true,
                }],
            },
            merge_commit_sha: None,
            dev_bump_commit_sha: None,
        }
    }

    #[tokio::test]
    async fn fake_gate_executor_returns_pass_report() {
        let report = GateReport {
            ok: true,
            failures: vec![],
        };
        let executor =
            FakeGateExecutor::new(BTreeMap::from([("gate-1".to_string(), report.clone())]));

        assert_eq!(executor.execute(&gate_run("gate-1")).await.unwrap(), report);
    }

    #[tokio::test]
    async fn fake_gate_executor_returns_fail_report() {
        let report = GateReport {
            ok: false,
            failures: vec![GateFailure {
                consumer: "service-b".to_string(),
                producer: "service-a".to_string(),
                pinned_sha: Some("1111111111111111111111111111111111111111".to_string()),
                resolved_sha: Some("2222222222222222222222222222222222222222".to_string()),
                outputs_schema_at_pinned: Some(serde_json::json!({
                    "kind": "object",
                    "shape": { "url": { "kind": "url" } }
                })),
                outputs_schema_at_resolved: Some(serde_json::json!({
                    "kind": "object",
                    "shape": {}
                })),
                consumed_paths: vec!["url".to_string()],
                kind: "compile".to_string(),
                message: "service-b consumes service-a.url which no longer exists".to_string(),
                excerpt: "error TS2339".to_string(),
                source_url: None,
            }],
        };
        let executor =
            FakeGateExecutor::new(BTreeMap::from([("gate-1".to_string(), report.clone())]));

        assert_eq!(executor.execute(&gate_run("gate-1")).await.unwrap(), report);
    }

    #[tokio::test]
    async fn legacy_cli_gate_is_explicitly_retired() {
        let error = CliGateExecutor::new("henosis-gate", ())
            .execute(&gate_run("gate-1"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("removed by D26"));
    }
}
