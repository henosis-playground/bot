use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Mutex;

use anyhow::Context;
use tokio::process::Command;

use crate::henosis::gate_report::GateReport;
use crate::henosis::lockfile::{self, EnvironmentSection, Lockfile};
use crate::henosis::queue::{GateRun, candidate_world_components};

pub trait GateExecutor {
    async fn execute(&self, gate_run: &GateRun) -> anyhow::Result<GateReport>;
}

#[derive(Debug, Clone)]
pub struct CliGateExecutor {
    gate_command: String,
    dev_lockfile_path: String,
}

impl CliGateExecutor {
    pub fn new(gate_command: impl Into<String>, dev_lockfile_path: impl Into<String>) -> Self {
        Self {
            gate_command: gate_command.into(),
            dev_lockfile_path: dev_lockfile_path.into(),
        }
    }
}

impl GateExecutor for CliGateExecutor {
    async fn execute(&self, gate_run: &GateRun) -> anyhow::Result<GateReport> {
        let tempdir = tempfile::Builder::new()
            .prefix("henosis-gate-")
            .tempdir_in("/tmp")
            .context("Cannot create Henosis gate tempdir")?;

        let lockfile_path = tempdir.path().join("candidate.toml");
        let scratch_dir = tempdir.path().join("scratch");
        let output_dir = tempdir.path().join("output");
        tokio::fs::create_dir_all(&scratch_dir)
            .await
            .context("Cannot create Henosis gate scratch dir")?;
        tokio::fs::create_dir_all(&output_dir)
            .await
            .context("Cannot create Henosis gate output dir")?;

        let lockfile = Lockfile {
            environment: EnvironmentSection {
                id: "dev".to_string(),
            },
            components: candidate_world_components(&gate_run.world),
        };
        let toml = lockfile::to_toml(&lockfile).context("Cannot serialize candidate lockfile")?;
        tokio::fs::write(&lockfile_path, toml)
            .await
            .with_context(|| {
                format!(
                    "Cannot write candidate lockfile to {}",
                    lockfile_path.display()
                )
            })?;

        let output = Command::new(&self.gate_command)
            .arg(&lockfile_path)
            .arg("--scratch")
            .arg(&scratch_dir)
            .arg("--output")
            .arg(&output_dir)
            .arg("--dev-lockfile")
            .arg(PathBuf::from(&self.dev_lockfile_path))
            .output()
            .await
            .with_context(|| format!("Cannot execute gate command `{}`", self.gate_command))?;

        let report_path = output_dir.join("report.json");
        let report_json = tokio::fs::read_to_string(&report_path)
            .await
            .with_context(|| {
                format!(
                    "Cannot read gate report from {}. stdout:\n{}\nstderr:\n{}",
                    report_path.display(),
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr),
                )
            })?;
        let report = GateReport::parse(&report_json).context("Cannot parse gate report JSON")?;

        if !output.status.success() && report.ok {
            anyhow::bail!(
                "Gate command `{}` exited with {} but reported ok=true",
                self.gate_command,
                output.status
            );
        }

        Ok(report)
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
    use indexmap::IndexMap;

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
                component: "service-b".to_string(),
                consumer_of: "service-a".to_string(),
                kind: "compile".to_string(),
                message: "service-b consumes service-a.url which no longer exists".to_string(),
                excerpt: "error TS2339".to_string(),
            }],
        };
        let executor =
            FakeGateExecutor::new(BTreeMap::from([("gate-1".to_string(), report.clone())]));

        assert_eq!(executor.execute(&gate_run("gate-1")).await.unwrap(), report);
    }

    #[test]
    fn candidate_world_materializes_to_lockfile_components() {
        let run = gate_run("gate-1");
        assert_eq!(
            candidate_world_components(&run.world),
            IndexMap::from([(
                "service-a".to_string(),
                lockfile::pinned("henosis-playground/service-a", "a-pr", "sha256:a")
            )])
        );
    }
}
