use std::collections::BTreeMap;
use std::sync::Mutex;

use anyhow::Context;
use tokio::process::Command;

use crate::henosis::environment::DevLockfileReader;
use crate::henosis::gate_report::GateReport;
use crate::henosis::lockfile::{self, EnvironmentSection, Lockfile};
use crate::henosis::queue::{GateRun, candidate_world_components};

pub trait GateExecutor {
    async fn execute(&self, gate_run: &GateRun) -> anyhow::Result<GateReport>;
}

pub struct CliGateExecutor<D> {
    gate_command: String,
    dev_lockfiles: D,
}

impl<D> CliGateExecutor<D> {
    pub fn new(gate_command: impl Into<String>, dev_lockfiles: D) -> Self {
        Self {
            gate_command: gate_command.into(),
            dev_lockfiles,
        }
    }
}

impl<D> GateExecutor for CliGateExecutor<D>
where
    D: DevLockfileReader + Send + Sync,
{
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

        let dev_lockfile_path = tempdir.path().join("dev.toml");
        let dev_lockfile = self.dev_lockfiles.read_dev_lockfile().await?;
        let dev_toml = lockfile::to_toml(&dev_lockfile).context("Cannot serialize dev lockfile")?;
        tokio::fs::write(&dev_lockfile_path, dev_toml)
            .await
            .with_context(|| {
                format!(
                    "Cannot write dev lockfile to {}",
                    dev_lockfile_path.display()
                )
            })?;

        let output = Command::new(&self.gate_command)
            .arg(&lockfile_path)
            .arg("--scratch")
            .arg(&scratch_dir)
            .arg("--output")
            .arg(&output_dir)
            .arg("--dev-lockfile")
            .arg(&dev_lockfile_path)
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
    use crate::henosis::environment::DevLockfileReader;
    use crate::henosis::gate_report::GateFailure;
    use crate::henosis::queue::{
        CandidateComponent, CandidateWorld, PENDING_STATUS, QueuePullRequest, RecordedGateRun,
    };

    #[derive(Clone)]
    struct StaticDevLockfile(Lockfile);

    impl DevLockfileReader for StaticDevLockfile {
        async fn read_dev_lockfile(&self) -> anyhow::Result<Lockfile> {
            Ok(self.0.clone())
        }
    }

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

    #[tokio::test]
    async fn cli_gate_executor_writes_dev_lockfile_into_gate_tempdir() {
        let script_dir = tempfile::tempdir().unwrap();
        let script_path = script_dir.path().join("gate.sh");
        tokio::fs::write(
            &script_path,
            r#"#!/bin/sh
set -eu
output=""
dev=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --output)
      output="$2"
      shift 2
      ;;
    --dev-lockfile)
      dev="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
test -n "$output"
test -n "$dev"
test -f "$dev"
case "$dev" in
  /tmp/henosis-gate-*/dev.toml) ;;
  *) exit 12 ;;
esac
grep -q 'b-main' "$dev"
printf '{"ok":true,"failures":[]}\n' > "$output/report.json"
"#,
        )
        .await
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mut permissions = tokio::fs::metadata(&script_path)
                .await
                .unwrap()
                .permissions();
            permissions.set_mode(0o755);
            tokio::fs::set_permissions(&script_path, permissions)
                .await
                .unwrap();
        }

        let dev_lockfile = Lockfile {
            environment: EnvironmentSection {
                id: "dev".to_string(),
            },
            components: IndexMap::from([
                (
                    "service-a".to_string(),
                    lockfile::pinned("henosis-playground/service-a", "a-main", "sha256:a"),
                ),
                (
                    "service-b".to_string(),
                    lockfile::pinned("henosis-playground/service-b", "b-main", "sha256:b"),
                ),
            ]),
        };
        let executor = CliGateExecutor::new(
            script_path.to_string_lossy().to_string(),
            StaticDevLockfile(dev_lockfile),
        );

        let report = executor.execute(&gate_run("gate-1")).await.unwrap();

        assert!(report.ok);
    }
}
