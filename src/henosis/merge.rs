use anyhow::Context;

use crate::henosis::queue::{
    BUMPING_DEV_STATUS, GATE_PASSED_STATUS, GateRun, MERGED_STATUS, MERGING_PR_STATUS, PrCommenter,
    QueuePullRequest,
};

pub trait MergeExecutor {
    async fn execute(&self, gate_run: &GateRun) -> anyhow::Result<()>;
}

pub trait MergeStore {
    async fn mark_gate_run_status(&self, external_id: &str, status: &str) -> anyhow::Result<()>;
    async fn record_merge_commit_sha(
        &self,
        external_id: &str,
        merge_commit_sha: &str,
    ) -> anyhow::Result<()>;
    async fn record_dev_bump_commit_sha(
        &self,
        external_id: &str,
        dev_bump_commit_sha: &str,
    ) -> anyhow::Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DevBump {
    pub commit_sha: String,
    pub commit_url: String,
}

pub trait PullRequestMerger {
    async fn squash_merge(&self, pr: &QueuePullRequest) -> anyhow::Result<String>;
}

pub trait DevLockfileBumper {
    async fn bump_dev_lockfile(
        &self,
        gate_run: &GateRun,
        merge_commit_sha: &str,
    ) -> anyhow::Result<DevBump>;
}

pub struct StateMachineMergeExecutor<S, M, B, C> {
    store: S,
    merger: M,
    bumper: B,
    commenter: C,
}

impl<S, M, B, C> StateMachineMergeExecutor<S, M, B, C> {
    pub fn new(store: S, merger: M, bumper: B, commenter: C) -> Self {
        Self {
            store,
            merger,
            bumper,
            commenter,
        }
    }
}

impl<S, M, B, C> MergeExecutor for StateMachineMergeExecutor<S, M, B, C>
where
    S: MergeStore,
    M: PullRequestMerger,
    B: DevLockfileBumper,
    C: PrCommenter,
{
    async fn execute(&self, gate_run: &GateRun) -> anyhow::Result<()> {
        let merge_commit_sha = self.ensure_merged(gate_run).await?;
        let bump = self.ensure_dev_bumped(gate_run, &merge_commit_sha).await?;

        self.store
            .mark_gate_run_status(&gate_run.external_id, MERGED_STATUS)
            .await?;
        for pr in &gate_run.world.members {
            self.commenter
                .post_comment(
                    pr,
                    &format!(
                        "Landed as {merge_commit_sha}. Dev bumped: {}",
                        bump.commit_url
                    ),
                )
                .await?;
        }

        Ok(())
    }
}

impl<S, M, B, C> StateMachineMergeExecutor<S, M, B, C>
where
    S: MergeStore,
    M: PullRequestMerger,
    B: DevLockfileBumper,
    C: PrCommenter,
{
    async fn ensure_merged(&self, gate_run: &GateRun) -> anyhow::Result<String> {
        if let Some(merge_commit_sha) = &gate_run.merge_commit_sha {
            return Ok(merge_commit_sha.clone());
        }

        if matches!(
            gate_run.status.as_str(),
            GATE_PASSED_STATUS | MERGING_PR_STATUS | BUMPING_DEV_STATUS
        ) {
            self.store
                .mark_gate_run_status(&gate_run.external_id, MERGING_PR_STATUS)
                .await?;
        }

        let mut merge_commit_shas = Vec::with_capacity(gate_run.world.members.len());
        for pr in &gate_run.world.members {
            merge_commit_shas.push(self.merger.squash_merge(pr).await?);
        }
        let merge_commit_sha = merge_commit_shas
            .first()
            .cloned()
            .context("gate run has no source PRs to merge")?;
        self.store
            .record_merge_commit_sha(&gate_run.external_id, &merge_commit_sha)
            .await?;

        Ok(merge_commit_sha)
    }

    async fn ensure_dev_bumped(
        &self,
        gate_run: &GateRun,
        merge_commit_sha: &str,
    ) -> anyhow::Result<DevBump> {
        if let Some(dev_bump_commit_sha) = &gate_run.dev_bump_commit_sha {
            return Ok(DevBump {
                commit_sha: dev_bump_commit_sha.clone(),
                commit_url: dev_bump_commit_sha.clone(),
            });
        }

        self.store
            .mark_gate_run_status(&gate_run.external_id, BUMPING_DEV_STATUS)
            .await?;
        let bump = self
            .bumper
            .bump_dev_lockfile(gate_run, merge_commit_sha)
            .await?;
        self.store
            .record_dev_bump_commit_sha(&gate_run.external_id, &bump.commit_sha)
            .await?;
        Ok(bump)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use indexmap::IndexMap;

    use super::*;
    use crate::henosis::lockfile::{EnvironmentSection, Lockfile, pinned};
    use crate::henosis::queue::{
        CandidateComponent, CandidateWorld, GATE_PASSED_STATUS, QueuePullRequest, RecordedGateRun,
    };

    #[derive(Default)]
    struct MemoryMergeStore {
        statuses: Mutex<Vec<String>>,
        merge_commit_sha: Mutex<Option<String>>,
        dev_bump_commit_sha: Mutex<Option<String>>,
    }

    impl MergeStore for MemoryMergeStore {
        async fn mark_gate_run_status(
            &self,
            _external_id: &str,
            status: &str,
        ) -> anyhow::Result<()> {
            self.statuses.lock().unwrap().push(status.to_string());
            Ok(())
        }

        async fn record_merge_commit_sha(
            &self,
            _external_id: &str,
            merge_commit_sha: &str,
        ) -> anyhow::Result<()> {
            *self.merge_commit_sha.lock().unwrap() = Some(merge_commit_sha.to_string());
            Ok(())
        }

        async fn record_dev_bump_commit_sha(
            &self,
            _external_id: &str,
            dev_bump_commit_sha: &str,
        ) -> anyhow::Result<()> {
            *self.dev_bump_commit_sha.lock().unwrap() = Some(dev_bump_commit_sha.to_string());
            Ok(())
        }
    }

    #[derive(Default)]
    struct FakeMerger {
        calls: Mutex<Vec<QueuePullRequest>>,
    }

    impl PullRequestMerger for FakeMerger {
        async fn squash_merge(&self, pr: &QueuePullRequest) -> anyhow::Result<String> {
            self.calls.lock().unwrap().push(pr.clone());
            Ok("merge-sha".to_string())
        }
    }

    #[derive(Default)]
    struct FakeBumper {
        calls: Mutex<Vec<(String, String)>>,
    }

    impl DevLockfileBumper for FakeBumper {
        async fn bump_dev_lockfile(
            &self,
            gate_run: &GateRun,
            merge_commit_sha: &str,
        ) -> anyhow::Result<DevBump> {
            self.calls
                .lock()
                .unwrap()
                .push((gate_run.external_id.clone(), merge_commit_sha.to_string()));
            Ok(DevBump {
                commit_sha: "dev-bump-sha".to_string(),
                commit_url: "https://github.com/henosis-playground/deploy/commit/dev-bump-sha"
                    .to_string(),
            })
        }
    }

    #[derive(Default)]
    struct MemoryCommenter {
        comments: Mutex<Vec<(QueuePullRequest, String)>>,
    }

    impl PrCommenter for MemoryCommenter {
        async fn post_comment(&self, pr: &QueuePullRequest, body: &str) -> anyhow::Result<()> {
            self.comments
                .lock()
                .unwrap()
                .push((pr.clone(), body.to_string()));
            Ok(())
        }
    }

    fn gate_run(status: &str) -> RecordedGateRun {
        RecordedGateRun {
            id: 1,
            external_id: "gate-1".to_string(),
            status: status.to_string(),
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
    async fn fake_pass_marks_merged_bumps_dev_and_posts_comment() {
        let store = MemoryMergeStore::default();
        let merger = FakeMerger::default();
        let bumper = FakeBumper::default();
        let commenter = MemoryCommenter::default();
        let executor = StateMachineMergeExecutor::new(store, merger, bumper, commenter);

        executor
            .execute(&gate_run(GATE_PASSED_STATUS))
            .await
            .unwrap();

        assert_eq!(
            executor.store.statuses.lock().unwrap().as_slice(),
            [MERGING_PR_STATUS, BUMPING_DEV_STATUS, MERGED_STATUS]
        );
        assert_eq!(
            executor.store.merge_commit_sha.lock().unwrap().as_deref(),
            Some("merge-sha")
        );
        assert_eq!(
            executor
                .store
                .dev_bump_commit_sha
                .lock()
                .unwrap()
                .as_deref(),
            Some("dev-bump-sha")
        );
        let comments = executor.commenter.comments.lock().unwrap();
        assert_eq!(comments.len(), 1);
        assert!(comments[0].1.contains("Landed as merge-sha"));
        assert!(comments[0].1.contains("Dev bumped: https://github.com/"));
    }

    #[tokio::test]
    async fn restart_resume_bumping_dev_does_not_remerge() {
        let mut run = gate_run(BUMPING_DEV_STATUS);
        run.merge_commit_sha = Some("merge-sha".to_string());
        let store = MemoryMergeStore::default();
        let merger = FakeMerger::default();
        let bumper = FakeBumper::default();
        let commenter = MemoryCommenter::default();
        let executor = StateMachineMergeExecutor::new(store, merger, bumper, commenter);

        executor.execute(&run).await.unwrap();

        assert!(executor.merger.calls.lock().unwrap().is_empty());
        assert_eq!(
            executor.bumper.calls.lock().unwrap().as_slice(),
            [("gate-1".to_string(), "merge-sha".to_string())]
        );
        assert_eq!(
            executor.store.statuses.lock().unwrap().as_slice(),
            [BUMPING_DEV_STATUS, MERGED_STATUS]
        );
    }

    #[test]
    fn test_lockfile_fixture_stays_pinned() {
        let lockfile = Lockfile {
            environment: EnvironmentSection {
                id: "dev".to_string(),
            },
            components: IndexMap::from([(
                "service-a".to_string(),
                pinned("henosis-playground/service-a", "merge-sha", "sha256:a"),
            )]),
        };

        assert_eq!(lockfile.environment.id, "dev");
    }
}
