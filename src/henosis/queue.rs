use std::collections::BTreeMap;

use anyhow::Context;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::henosis::config::RegisteredComponent;
use crate::henosis::environment::{DevManifestReader, PullRequestKey};
use crate::henosis::gate::GateExecutor;
use crate::henosis::manifest::{ComponentEntry, PinnedEntry};
use crate::henosis::merge::MergeExecutor;

pub const GLOBAL_QUEUE_LOCK_KEY: i64 = 0x4845_4e4f_5155_4555;
pub const PENDING_STATUS: &str = "pending";
pub const PENDING_EXECUTOR_STATUS: &str = "pending-executor";
pub const RUNNING_STATUS: &str = "running";
pub const GATE_FAILED_STATUS: &str = "gate-failed";
pub const GATE_PASSED_STATUS: &str = "gate-passed";
pub const MERGING_PR_STATUS: &str = "merging-pr";
pub const BUMPING_DEV_STATUS: &str = "bumping-dev";
pub const MERGED_STATUS: &str = "merged";
pub const INVALIDATED_STATUS: &str = "invalidated";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueuePullRequest {
    pub key: PullRequestKey,
    pub component: String,
    pub head_sha: String,
    pub head_branch: String,
    pub approved_sha: String,
}

impl QueuePullRequest {
    pub fn new(
        repo: impl Into<String>,
        number: u64,
        component: impl Into<String>,
        head_sha: impl Into<String>,
        head_branch: impl Into<String>,
        approved_sha: impl Into<String>,
    ) -> Self {
        Self {
            key: PullRequestKey::new(repo, number),
            component: component.into(),
            head_sha: head_sha.into(),
            head_branch: head_branch.into(),
            approved_sha: approved_sha.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateComponent {
    pub name: String,
    pub repo: String,
    pub r#ref: String,
    pub digest: String,
    pub candidate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateWorld {
    pub members: Vec<QueuePullRequest>,
    pub components: Vec<CandidateComponent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedGateRun {
    pub id: i64,
    pub external_id: String,
    pub status: String,
    pub world: CandidateWorld,
    pub merge_commit_sha: Option<String>,
    pub dev_bump_commit_sha: Option<String>,
}

pub type GateRun = RecordedGateRun;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateStatus {
    pub external_id: String,
    pub status: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckConclusion {
    Success,
    Failure,
    Neutral,
}

pub trait QueueStore {
    async fn try_acquire_global_lock(&mut self) -> anyhow::Result<bool>;
    async fn release_global_lock(&mut self) -> anyhow::Result<()>;
    async fn oldest_ready_candidate(
        &mut self,
        components: &[RegisteredComponent],
    ) -> anyhow::Result<Option<QueuePullRequest>>;
    async fn oldest_resumable_merge(&mut self) -> anyhow::Result<Option<RecordedGateRun>>;
    async fn record_gate_run(&mut self, world: &CandidateWorld) -> anyhow::Result<RecordedGateRun>;
    async fn mark_gate_run_status(&mut self, external_id: &str, status: &str)
    -> anyhow::Result<()>;
    async fn latest_gate_status(&self, key: &PullRequestKey) -> anyhow::Result<Option<GateStatus>>;
    async fn invalidate_active_gate_runs(
        &mut self,
        key: &PullRequestKey,
    ) -> anyhow::Result<Vec<GateStatus>>;
    async fn reenqueue_pr(&mut self, pr: &QueuePullRequest) -> anyhow::Result<()>;
}

pub trait GateCheckReporter {
    async fn create_in_progress_check(
        &self,
        pr: &QueuePullRequest,
        external_id: &str,
    ) -> anyhow::Result<()>;
    async fn resolve_check_run(
        &self,
        external_id: &str,
        conclusion: CheckConclusion,
        summary: &str,
    ) -> anyhow::Result<()>;
}

pub trait PrCommenter {
    async fn post_comment(&self, pr: &QueuePullRequest, body: &str) -> anyhow::Result<()>;
}

pub struct QueueManager {
    components: Vec<RegisteredComponent>,
}

impl QueueManager {
    pub fn new(components: Vec<RegisteredComponent>) -> Self {
        Self { components }
    }

    pub async fn tick<S, D, R, E, M, C>(
        &self,
        store: &mut S,
        dev_manifests: &D,
        check_reporter: &R,
        gate_executor: &E,
        merge_executor: &M,
        commenter: &C,
    ) -> anyhow::Result<Option<RecordedGateRun>>
    where
        S: QueueStore,
        D: DevManifestReader,
        R: GateCheckReporter,
        E: GateExecutor,
        M: MergeExecutor,
        C: PrCommenter,
    {
        if !store.try_acquire_global_lock().await? {
            return Ok(None);
        }

        let result = self
            .tick_with_lock(
                store,
                dev_manifests,
                check_reporter,
                gate_executor,
                merge_executor,
                commenter,
            )
            .await;
        let release = store.release_global_lock().await;
        match (result, release) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Err(error), Err(release_error)) => Err(error.context(release_error)),
        }
    }

    async fn tick_with_lock<S, D, R, E, M, C>(
        &self,
        store: &mut S,
        dev_manifests: &D,
        check_reporter: &R,
        gate_executor: &E,
        merge_executor: &M,
        commenter: &C,
    ) -> anyhow::Result<Option<RecordedGateRun>>
    where
        S: QueueStore,
        D: DevManifestReader,
        R: GateCheckReporter,
        E: GateExecutor,
        M: MergeExecutor,
        C: PrCommenter,
    {
        if let Some(gate_run) = store.oldest_resumable_merge().await? {
            merge_executor.execute(&gate_run).await?;
            return Ok(Some(gate_run));
        }

        let Some(candidate) = store.oldest_ready_candidate(&self.components).await? else {
            return Ok(None);
        };

        let world = self
            .candidate_world(dev_manifests, vec![candidate.clone()])
            .await?;
        let mut gate_run = store.record_gate_run(&world).await?;
        for member in &world.members {
            check_reporter
                .create_in_progress_check(member, &gate_run.external_id)
                .await?;
        }

        store
            .mark_gate_run_status(&gate_run.external_id, PENDING_EXECUTOR_STATUS)
            .await?;
        gate_run.status = PENDING_EXECUTOR_STATUS.to_string();

        store
            .mark_gate_run_status(&gate_run.external_id, RUNNING_STATUS)
            .await?;
        gate_run.status = RUNNING_STATUS.to_string();

        let report = gate_executor.execute(&gate_run).await?;
        if report.ok {
            store
                .mark_gate_run_status(&gate_run.external_id, GATE_PASSED_STATUS)
                .await?;
            gate_run.status = GATE_PASSED_STATUS.to_string();
            check_reporter
                .resolve_check_run(
                    &gate_run.external_id,
                    CheckConclusion::Success,
                    &report.check_run_summary(),
                )
                .await?;
            merge_executor.execute(&gate_run).await?;
        } else {
            store
                .mark_gate_run_status(&gate_run.external_id, GATE_FAILED_STATUS)
                .await?;
            gate_run.status = GATE_FAILED_STATUS.to_string();
            check_reporter
                .resolve_check_run(
                    &gate_run.external_id,
                    CheckConclusion::Failure,
                    &report.check_run_summary(),
                )
                .await?;
            let comment = report.pr_comment();
            for member in &gate_run.world.members {
                commenter.post_comment(member, &comment).await?;
            }
        }

        Ok(Some(gate_run))
    }

    pub async fn invalidate_pr_push<S, R>(
        &self,
        store: &mut S,
        check_reporter: &R,
        pr: &QueuePullRequest,
    ) -> anyhow::Result<bool>
    where
        S: QueueStore,
        R: GateCheckReporter,
    {
        let invalidated = store.invalidate_active_gate_runs(&pr.key).await?;
        if invalidated.is_empty() {
            return Ok(false);
        }

        store.reenqueue_pr(pr).await?;
        for gate in invalidated {
            check_reporter
                .resolve_check_run(
                    &gate.external_id,
                    CheckConclusion::Neutral,
                    "Henosis gate cancelled because a new commit was pushed.",
                )
                .await?;
        }

        Ok(true)
    }

    async fn candidate_world<D>(
        &self,
        dev_manifests: &D,
        members: Vec<QueuePullRequest>,
    ) -> anyhow::Result<CandidateWorld>
    where
        D: DevManifestReader,
    {
        let dev = dev_manifests.read_dev_manifest().await?;
        let dev_pins = dev
            .components
            .iter()
            .map(|(name, entry)| match entry {
                ComponentEntry::Pinned(pin) => Ok((name.clone(), pin.clone())),
                ComponentEntry::Follower(_) => {
                    anyhow::bail!("dev manifest contains follower entry for `{name}`")
                }
            })
            .collect::<anyhow::Result<BTreeMap<String, PinnedEntry>>>()?;

        let members_by_component = members
            .iter()
            .map(|member| (member.component.clone(), member))
            .collect::<BTreeMap<_, _>>();

        let mut components = Vec::with_capacity(self.components.len());
        for component in &self.components {
            let dev_pin = dev_pins
                .get(&component.name)
                .with_context(|| format!("No dev pin found for component `{}`", component.name))?;
            if let Some(member) = members_by_component.get(&component.name) {
                components.push(CandidateComponent {
                    name: component.name.clone(),
                    repo: member.key.repo.clone(),
                    r#ref: member.head_sha.clone(),
                    digest: dev_pin.digest.clone(),
                    candidate: true,
                });
            } else {
                components.push(CandidateComponent {
                    name: component.name.clone(),
                    repo: dev_pin.repo.clone(),
                    r#ref: dev_pin.r#ref.clone(),
                    digest: dev_pin.digest.clone(),
                    candidate: false,
                });
            }
        }

        Ok(CandidateWorld {
            members,
            components,
        })
    }
}

pub fn candidate_world_components(world: &CandidateWorld) -> IndexMap<String, ComponentEntry> {
    world
        .components
        .iter()
        .map(|component| {
            (
                component.name.clone(),
                ComponentEntry::Pinned(PinnedEntry {
                    repo: component.repo.clone(),
                    r#ref: component.r#ref.clone(),
                    digest: component.digest.clone(),
                }),
            )
        })
        .collect()
}

pub fn gate_external_id(world: &CandidateWorld) -> anyhow::Result<String> {
    let Some(first) = world.members.first() else {
        anyhow::bail!("candidate world must contain at least one member");
    };
    Ok(format!(
        "gate-{}-{}-{}",
        first.key.repo.replace('/', "-"),
        first.key.number,
        first.head_sha
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::henosis::environment::DevManifestReader;
    use crate::henosis::gate::FakeGateExecutor;
    use crate::henosis::gate_report::{GateFailure, GateReport};
    use crate::henosis::manifest::{EnvironmentSection, Manifest, pinned};
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::Mutex;

    #[derive(Clone)]
    struct StaticDevManifest(Manifest);

    impl DevManifestReader for StaticDevManifest {
        async fn read_dev_manifest(&self) -> anyhow::Result<Manifest> {
            Ok(self.0.clone())
        }
    }

    #[derive(Default)]
    struct MemoryQueueStore {
        lock_taken: bool,
        ready: VecDeque<QueuePullRequest>,
        recorded: Vec<RecordedGateRun>,
        statuses: BTreeMap<String, String>,
        reenqueued: Vec<QueuePullRequest>,
    }

    impl QueueStore for MemoryQueueStore {
        async fn try_acquire_global_lock(&mut self) -> anyhow::Result<bool> {
            if self.lock_taken {
                Ok(false)
            } else {
                self.lock_taken = true;
                Ok(true)
            }
        }

        async fn release_global_lock(&mut self) -> anyhow::Result<()> {
            self.lock_taken = false;
            Ok(())
        }

        async fn oldest_ready_candidate(
            &mut self,
            components: &[RegisteredComponent],
        ) -> anyhow::Result<Option<QueuePullRequest>> {
            let component_repos = components
                .iter()
                .map(|component| component.repo.clone())
                .collect::<Vec<_>>();
            Ok(self
                .ready
                .iter()
                .position(|pr| component_repos.contains(&pr.key.repo))
                .and_then(|index| self.ready.remove(index)))
        }

        async fn oldest_resumable_merge(&mut self) -> anyhow::Result<Option<RecordedGateRun>> {
            Ok(self
                .recorded
                .iter()
                .find(|run| {
                    matches!(
                        run.status.as_str(),
                        GATE_PASSED_STATUS | MERGING_PR_STATUS | BUMPING_DEV_STATUS
                    )
                })
                .cloned())
        }

        async fn record_gate_run(
            &mut self,
            world: &CandidateWorld,
        ) -> anyhow::Result<RecordedGateRun> {
            let id = self.recorded.len() as i64 + 1;
            let gate_run = RecordedGateRun {
                id,
                external_id: gate_external_id(world)?,
                status: PENDING_STATUS.to_string(),
                world: world.clone(),
                merge_commit_sha: None,
                dev_bump_commit_sha: None,
            };
            self.recorded.push(gate_run.clone());
            Ok(gate_run)
        }

        async fn mark_gate_run_status(
            &mut self,
            external_id: &str,
            status: &str,
        ) -> anyhow::Result<()> {
            self.statuses
                .insert(external_id.to_string(), status.to_string());
            if let Some(gate_run) = self
                .recorded
                .iter_mut()
                .find(|gate_run| gate_run.external_id == external_id)
            {
                gate_run.status = status.to_string();
            }
            Ok(())
        }

        async fn latest_gate_status(
            &self,
            key: &PullRequestKey,
        ) -> anyhow::Result<Option<GateStatus>> {
            let Some(run) = self
                .recorded
                .iter()
                .rev()
                .find(|run| run.world.members.iter().any(|member| member.key == *key))
            else {
                return Ok(None);
            };
            Ok(Some(GateStatus {
                external_id: run.external_id.clone(),
                status: self
                    .statuses
                    .get(&run.external_id)
                    .cloned()
                    .unwrap_or_else(|| "pending".to_string()),
            }))
        }

        async fn invalidate_active_gate_runs(
            &mut self,
            key: &PullRequestKey,
        ) -> anyhow::Result<Vec<GateStatus>> {
            let mut invalidated = Vec::new();
            let active = [PENDING_STATUS, PENDING_EXECUTOR_STATUS, RUNNING_STATUS];
            let external_ids = self
                .recorded
                .iter()
                .filter(|run| run.world.members.iter().any(|member| member.key == *key))
                .map(|run| run.external_id.clone())
                .collect::<Vec<_>>();

            for external_id in external_ids {
                let status = self
                    .statuses
                    .get(&external_id)
                    .cloned()
                    .unwrap_or_else(|| PENDING_STATUS.to_string());
                if active.contains(&status.as_str()) {
                    self.mark_gate_run_status(&external_id, INVALIDATED_STATUS)
                        .await?;
                    invalidated.push(GateStatus {
                        external_id,
                        status: INVALIDATED_STATUS.to_string(),
                    });
                }
            }
            Ok(invalidated)
        }

        async fn reenqueue_pr(&mut self, pr: &QueuePullRequest) -> anyhow::Result<()> {
            self.reenqueued.push(pr.clone());
            if !self.ready.iter().any(|ready| ready.key == pr.key) {
                self.ready.push_back(pr.clone());
            }
            Ok(())
        }
    }

    #[derive(Default)]
    struct MemoryCheckReporter {
        checks: Mutex<Vec<(PullRequestKey, String)>>,
        resolved: Mutex<Vec<(String, CheckConclusion, String)>>,
    }

    impl GateCheckReporter for MemoryCheckReporter {
        async fn create_in_progress_check(
            &self,
            pr: &QueuePullRequest,
            external_id: &str,
        ) -> anyhow::Result<()> {
            self.checks
                .lock()
                .unwrap()
                .push((pr.key.clone(), external_id.to_string()));
            Ok(())
        }

        async fn resolve_check_run(
            &self,
            external_id: &str,
            conclusion: CheckConclusion,
            summary: &str,
        ) -> anyhow::Result<()> {
            self.resolved.lock().unwrap().push((
                external_id.to_string(),
                conclusion,
                summary.to_string(),
            ));
            Ok(())
        }
    }

    #[derive(Default)]
    struct MemoryMergeExecutor {
        calls: Mutex<Vec<String>>,
    }

    impl MergeExecutor for MemoryMergeExecutor {
        async fn execute(&self, gate_run: &GateRun) -> anyhow::Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(gate_run.external_id.clone());
            Ok(())
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

    fn components() -> Vec<RegisteredComponent> {
        vec![
            RegisteredComponent {
                name: "service-a".to_string(),
                repo: "henosis-playground/service-a".to_string(),
                main_branch: "main".to_string(),
            },
            RegisteredComponent {
                name: "service-b".to_string(),
                repo: "henosis-playground/service-b".to_string(),
                main_branch: "main".to_string(),
            },
        ]
    }

    fn dev_manifest() -> Manifest {
        Manifest {
            environment: EnvironmentSection {
                id: "dev".to_string(),
            },
            components: IndexMap::from([
                (
                    "service-a".to_string(),
                    pinned("henosis-playground/service-a", "a-main", "sha256:a"),
                ),
                (
                    "service-b".to_string(),
                    pinned("henosis-playground/service-b", "b-main", "sha256:b"),
                ),
            ]),
        }
    }

    fn ready_pr(head_sha: &str) -> QueuePullRequest {
        QueuePullRequest::new(
            "henosis-playground/service-a",
            3,
            "service-a",
            head_sha,
            "pr/3",
            head_sha,
        )
    }

    fn failure_report() -> GateReport {
        GateReport {
            ok: false,
            failures: vec![GateFailure {
                component: "service-b".to_string(),
                consumer_of: Some("service-a".to_string()),
                kind: "compile".to_string(),
                message: "service-b consumes service-a.databaseUrl which no longer exists"
                    .to_string(),
                excerpt: "Property 'databaseUrl' does not exist on type".to_string(),
            }],
        }
    }

    fn recorded_run(status: &str) -> RecordedGateRun {
        RecordedGateRun {
            id: 1,
            external_id: "gate-henosis-playground-service-a-3-a-pr".to_string(),
            status: status.to_string(),
            world: CandidateWorld {
                members: vec![ready_pr("a-pr")],
                components: vec![
                    CandidateComponent {
                        name: "service-a".to_string(),
                        repo: "henosis-playground/service-a".to_string(),
                        r#ref: "a-pr".to_string(),
                        digest: "sha256:a".to_string(),
                        candidate: true,
                    },
                    CandidateComponent {
                        name: "service-b".to_string(),
                        repo: "henosis-playground/service-b".to_string(),
                        r#ref: "b-main".to_string(),
                        digest: "sha256:b".to_string(),
                        candidate: false,
                    },
                ],
            },
            merge_commit_sha: None,
            dev_bump_commit_sha: None,
        }
    }

    #[tokio::test]
    async fn tick_records_gate_run_check_run_and_passes_gate() {
        let manager = QueueManager::new(components());
        let dev = StaticDevManifest(dev_manifest());
        let mut store = MemoryQueueStore::default();
        store.ready.push_back(QueuePullRequest::new(
            "henosis-playground/service-a",
            3,
            "service-a",
            "a-pr",
            "pr/3",
            "a-pr",
        ));
        let reporter = MemoryCheckReporter::default();
        let external_id = "gate-henosis-playground-service-a-3-a-pr".to_string();
        let executor = FakeGateExecutor::new(BTreeMap::from([(
            external_id.clone(),
            GateReport {
                ok: true,
                failures: vec![],
            },
        )]));
        let merge_executor = MemoryMergeExecutor::default();
        let commenter = MemoryCommenter::default();

        let gate_run = manager
            .tick(
                &mut store,
                &dev,
                &reporter,
                &executor,
                &merge_executor,
                &commenter,
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            gate_run
                .world
                .components
                .iter()
                .map(|component| (
                    component.name.as_str(),
                    component.r#ref.as_str(),
                    component.candidate
                ))
                .collect::<Vec<_>>(),
            vec![("service-a", "a-pr", true), ("service-b", "b-main", false)]
        );
        assert_eq!(
            reporter.checks.lock().unwrap().as_slice(),
            [(
                PullRequestKey::new("henosis-playground/service-a", 3),
                gate_run.external_id.clone()
            )]
        );
        assert_eq!(
            store
                .statuses
                .get(&gate_run.external_id)
                .map(String::as_str),
            Some(GATE_PASSED_STATUS)
        );
        assert_eq!(
            reporter.resolved.lock().unwrap().as_slice(),
            [(
                external_id.clone(),
                CheckConclusion::Success,
                "Henosis gate passed. The candidate world compiled and rendered.".to_string()
            )]
        );
        assert_eq!(
            merge_executor.calls.lock().unwrap().as_slice(),
            [external_id]
        );
        assert!(commenter.comments.lock().unwrap().is_empty());
        assert_eq!(gate_run.status, GATE_PASSED_STATUS);
    }

    #[tokio::test]
    async fn fake_gate_executor_fail_updates_status_resolves_check_and_posts_comment() {
        let manager = QueueManager::new(components());
        let dev = StaticDevManifest(dev_manifest());
        let mut store = MemoryQueueStore::default();
        store.ready.push_back(ready_pr("a-pr"));
        let reporter = MemoryCheckReporter::default();
        let external_id = "gate-henosis-playground-service-a-3-a-pr".to_string();
        let executor =
            FakeGateExecutor::new(BTreeMap::from([(external_id.clone(), failure_report())]));
        let merge_executor = MemoryMergeExecutor::default();
        let commenter = MemoryCommenter::default();

        let gate_run = manager
            .tick(
                &mut store,
                &dev,
                &reporter,
                &executor,
                &merge_executor,
                &commenter,
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(gate_run.status, GATE_FAILED_STATUS);
        assert_eq!(
            store
                .statuses
                .get(&gate_run.external_id)
                .map(String::as_str),
            Some(GATE_FAILED_STATUS)
        );
        let resolved = reporter.resolved.lock().unwrap();
        assert_eq!(resolved[0].0, external_id);
        assert_eq!(resolved[0].1, CheckConclusion::Failure);
        assert!(resolved[0].2.contains("service-b"));
        assert!(resolved[0].2.contains("service-a.databaseUrl"));
        assert!(merge_executor.calls.lock().unwrap().is_empty());
        let comments = commenter.comments.lock().unwrap();
        assert_eq!(comments.len(), 1);
        assert!(comments[0].1.contains("Henosis gate failed"));
        assert!(comments[0].1.contains("service-b"));
        assert!(comments[0].1.contains("service-a"));
    }

    #[tokio::test]
    async fn push_invalidation_marks_running_gate_invalidated_and_reenqueues() {
        let manager = QueueManager::new(components());
        let mut store = MemoryQueueStore::default();
        let run = recorded_run(RUNNING_STATUS);
        store
            .statuses
            .insert(run.external_id.clone(), RUNNING_STATUS.to_string());
        store.recorded.push(run);
        let reporter = MemoryCheckReporter::default();
        let pushed = ready_pr("a-new");

        let invalidated = manager
            .invalidate_pr_push(&mut store, &reporter, &pushed)
            .await
            .unwrap();

        assert!(invalidated);
        assert_eq!(
            store
                .statuses
                .get("gate-henosis-playground-service-a-3-a-pr")
                .map(String::as_str),
            Some(INVALIDATED_STATUS)
        );
        assert_eq!(store.reenqueued, vec![pushed.clone()]);
        let resolved = reporter.resolved.lock().unwrap();
        assert_eq!(resolved[0].1, CheckConclusion::Neutral);
        assert!(resolved[0].2.contains("cancelled"));
    }
}
