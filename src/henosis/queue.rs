use std::collections::BTreeMap;

use anyhow::Context;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::henosis::config::{ComponentMode, RegisteredComponent};
use crate::henosis::environment::{DevManifestReader, PullRequestKey};
use crate::henosis::gate::GateExecutor;
use crate::henosis::manifest::{ComponentEntry, PinnedEntry, synthetic_digest_for_ref};
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
pub const ADVISORY_PASSED_STATUS: &str = "advisory-passed";
pub const ADVISORY_FAILED_STATUS: &str = "advisory-failed";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueuePullRequest {
    pub key: PullRequestKey,
    pub component: String,
    pub head_sha: String,
    pub head_branch: String,
    pub approved_sha: String,
    #[serde(default = "default_base_branch")]
    pub base_branch: String,
    #[serde(default)]
    pub repo_validation: Option<RepoValidation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoValidation {
    pub tested_commit_sha: String,
    pub base_sha: String,
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
        Self::with_base_branch(
            repo,
            number,
            component,
            head_sha,
            head_branch,
            approved_sha,
            default_base_branch(),
        )
    }

    pub fn with_base_branch(
        repo: impl Into<String>,
        number: u64,
        component: impl Into<String>,
        head_sha: impl Into<String>,
        head_branch: impl Into<String>,
        approved_sha: impl Into<String>,
        base_branch: impl Into<String>,
    ) -> Self {
        Self {
            key: PullRequestKey::new(repo, number),
            component: component.into(),
            head_sha: head_sha.into(),
            head_branch: head_branch.into(),
            approved_sha: approved_sha.into(),
            base_branch: base_branch.into(),
            repo_validation: None,
        }
    }

    pub fn with_repo_validation(mut self, tested_commit_sha: String, base_sha: String) -> Self {
        self.head_sha = tested_commit_sha.clone();
        self.repo_validation = Some(RepoValidation {
            tested_commit_sha,
            base_sha,
        });
        self
    }

    pub fn mode(&self) -> ComponentMode {
        if self.repo_validation.is_some() {
            ComponentMode::Chained
        } else {
            ComponentMode::GateOnly
        }
    }
}

fn default_base_branch() -> String {
    "main".to_string()
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
    pub head_sha: String,
    pub status: String,
    pub diagnostic: Option<String>,
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
    async fn oldest_resumable_gate(&mut self) -> anyhow::Result<Option<RecordedGateRun>>;
    async fn oldest_resumable_merge(&mut self) -> anyhow::Result<Option<RecordedGateRun>>;
    async fn record_gate_run(&mut self, world: &CandidateWorld) -> anyhow::Result<RecordedGateRun>;
    async fn mark_gate_run_status(&mut self, external_id: &str, status: &str)
    -> anyhow::Result<()>;
    async fn record_gate_run_diagnostic(
        &mut self,
        external_id: &str,
        diagnostic: &str,
    ) -> anyhow::Result<()>;
    async fn latest_gate_status(&self, key: &PullRequestKey) -> anyhow::Result<Option<GateStatus>>;
    async fn invalidate_active_gate_runs(
        &mut self,
        key: &PullRequestKey,
    ) -> anyhow::Result<Vec<GateStatus>>;
    async fn reenqueue_pr(&mut self, pr: &QueuePullRequest) -> anyhow::Result<()>;
    async fn clear_repo_validation(&mut self, key: &PullRequestKey) -> anyhow::Result<()>;
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

pub trait AdvisoryGateStore {
    async fn record_advisory_gate_status(
        &mut self,
        pr: &QueuePullRequest,
        external_id: &str,
        status: &str,
        diagnostic: Option<&str>,
    ) -> anyhow::Result<()>;
    async fn latest_advisory_gate_status(
        &self,
        key: &PullRequestKey,
    ) -> anyhow::Result<Option<GateStatus>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepoValidationStatus {
    Current,
    Stale,
}

pub trait RepoValidationChecker {
    async fn check_repo_validation(
        &self,
        pr: &QueuePullRequest,
    ) -> anyhow::Result<RepoValidationStatus>;
}

pub struct QueueManager {
    components: Vec<RegisteredComponent>,
}

impl QueueManager {
    pub fn new(components: Vec<RegisteredComponent>) -> Self {
        Self { components }
    }

    pub async fn tick<S, D, R, E, M, C, V>(
        &self,
        store: &mut S,
        dev_manifests: &D,
        check_reporter: &R,
        gate_executor: &E,
        merge_executor: &M,
        commenter: &C,
        repo_validation: &V,
    ) -> anyhow::Result<Option<RecordedGateRun>>
    where
        S: QueueStore,
        D: DevManifestReader,
        R: GateCheckReporter,
        E: GateExecutor,
        M: MergeExecutor,
        C: PrCommenter,
        V: RepoValidationChecker,
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
                repo_validation,
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

    async fn tick_with_lock<S, D, R, E, M, C, V>(
        &self,
        store: &mut S,
        dev_manifests: &D,
        check_reporter: &R,
        gate_executor: &E,
        merge_executor: &M,
        commenter: &C,
        repo_validation: &V,
    ) -> anyhow::Result<Option<RecordedGateRun>>
    where
        S: QueueStore,
        D: DevManifestReader,
        R: GateCheckReporter,
        E: GateExecutor,
        M: MergeExecutor,
        C: PrCommenter,
        V: RepoValidationChecker,
    {
        if let Some(gate_run) = store.oldest_resumable_merge().await? {
            merge_executor.execute(&gate_run).await?;
            return Ok(Some(gate_run));
        }

        if let Some(mut gate_run) = store.oldest_resumable_gate().await? {
            self.run_gate_and_finish(
                store,
                check_reporter,
                gate_executor,
                merge_executor,
                commenter,
                &mut gate_run,
            )
            .await?;
            return Ok(Some(gate_run));
        }

        let Some(candidate) = store.oldest_ready_candidate(&self.components).await? else {
            return Ok(None);
        };
        if candidate.repo_validation.is_some()
            && repo_validation.check_repo_validation(&candidate).await?
                == RepoValidationStatus::Stale
        {
            store.clear_repo_validation(&candidate.key).await?;
            return Ok(None);
        }

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

        self.run_gate_and_finish(
            store,
            check_reporter,
            gate_executor,
            merge_executor,
            commenter,
            &mut gate_run,
        )
        .await?;

        Ok(Some(gate_run))
    }

    async fn run_gate_and_finish<S, R, E, M, C>(
        &self,
        store: &mut S,
        check_reporter: &R,
        gate_executor: &E,
        merge_executor: &M,
        commenter: &C,
        gate_run: &mut RecordedGateRun,
    ) -> anyhow::Result<()>
    where
        S: QueueStore,
        R: GateCheckReporter,
        E: GateExecutor,
        M: MergeExecutor,
        C: PrCommenter,
    {
        let report = gate_executor.execute(gate_run).await?;
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
            let diagnostic = report.pr_comment();
            store
                .mark_gate_run_status(&gate_run.external_id, GATE_FAILED_STATUS)
                .await?;
            store
                .record_gate_run_diagnostic(&gate_run.external_id, &diagnostic)
                .await?;
            gate_run.status = GATE_FAILED_STATUS.to_string();
            check_reporter
                .resolve_check_run(
                    &gate_run.external_id,
                    CheckConclusion::Failure,
                    &report.check_run_summary(),
                )
                .await?;
            for member in &gate_run.world.members {
                commenter.post_comment(member, &diagnostic).await?;
            }
        }

        Ok(())
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
                    "Henosis merge gate cancelled because a new commit was pushed.",
                )
                .await?;
        }

        Ok(true)
    }

    pub async fn candidate_world<D>(
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
                    digest: synthetic_digest_for_ref(&member.head_sha),
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

pub fn advisory_gate_external_id(pr: &QueuePullRequest) -> String {
    format!(
        "gate-{}-{}-advisory-{}",
        pr.key.repo.replace('/', "-"),
        pr.key.number,
        pr.head_sha
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::henosis::environment::DevManifestReader;
    use crate::henosis::gate::FakeGateExecutor;
    use crate::henosis::gate_report::{GateFailure, GateReport};
    use crate::henosis::manifest::{
        EnvironmentSection, Manifest, pinned, synthetic_digest_for_ref,
    };
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
        diagnostics: BTreeMap<String, String>,
        reenqueued: Vec<QueuePullRequest>,
        cleared_repo_validations: Vec<PullRequestKey>,
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

        async fn oldest_resumable_gate(&mut self) -> anyhow::Result<Option<RecordedGateRun>> {
            Ok(self
                .recorded
                .iter()
                .find(|run| run.status == RUNNING_STATUS)
                .cloned())
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

        async fn record_gate_run_diagnostic(
            &mut self,
            external_id: &str,
            diagnostic: &str,
        ) -> anyhow::Result<()> {
            self.diagnostics
                .insert(external_id.to_string(), diagnostic.to_string());
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
            let head_sha = run
                .world
                .members
                .iter()
                .find(|member| member.key == *key)
                .map(|member| member.head_sha.clone())
                .unwrap_or_default();
            Ok(Some(GateStatus {
                external_id: run.external_id.clone(),
                head_sha,
                status: self
                    .statuses
                    .get(&run.external_id)
                    .cloned()
                    .unwrap_or_else(|| "pending".to_string()),
                diagnostic: self.diagnostics.get(&run.external_id).cloned(),
            }))
        }

        async fn invalidate_active_gate_runs(
            &mut self,
            key: &PullRequestKey,
        ) -> anyhow::Result<Vec<GateStatus>> {
            let mut invalidated = Vec::new();
            let active = [PENDING_STATUS, PENDING_EXECUTOR_STATUS, RUNNING_STATUS];
            let runs = self
                .recorded
                .iter()
                .filter_map(|run| {
                    run.world
                        .members
                        .iter()
                        .find(|member| member.key == *key)
                        .map(|member| (run.external_id.clone(), member.head_sha.clone()))
                })
                .collect::<Vec<_>>();

            for (external_id, head_sha) in runs {
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
                        head_sha,
                        status: INVALIDATED_STATUS.to_string(),
                        diagnostic: None,
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

        async fn clear_repo_validation(&mut self, key: &PullRequestKey) -> anyhow::Result<()> {
            self.cleared_repo_validations.push(key.clone());
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

    struct StaticRepoValidation(RepoValidationStatus);

    impl RepoValidationChecker for StaticRepoValidation {
        async fn check_repo_validation(
            &self,
            _pr: &QueuePullRequest,
        ) -> anyhow::Result<RepoValidationStatus> {
            Ok(self.0)
        }
    }

    fn components() -> Vec<RegisteredComponent> {
        vec![
            RegisteredComponent {
                name: "service-a".to_string(),
                repo: "henosis-playground/service-a".to_string(),
                main_branch: "main".to_string(),
                mode: ComponentMode::GateOnly,
            },
            RegisteredComponent {
                name: "service-b".to_string(),
                repo: "henosis-playground/service-b".to_string(),
                main_branch: "main".to_string(),
                mode: ComponentMode::GateOnly,
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
                consumer: "service-b".to_string(),
                producer: "service-a".to_string(),
                pinned_sha: Some("a-main".repeat(8)),
                resolved_sha: Some("a-pr".repeat(10)),
                outputs_schema_at_pinned: Some(serde_json::json!({
                    "kind": "object",
                    "shape": {
                        "databaseUrl": { "kind": "url" }
                    }
                })),
                outputs_schema_at_resolved: Some(serde_json::json!({
                    "kind": "object",
                    "shape": {
                        "apiUrl": { "kind": "url" }
                    }
                })),
                consumed_paths: vec!["databaseUrl".to_string()],
                kind: "compile".to_string(),
                message: "service-b consumes service-a.databaseUrl which no longer exists"
                    .to_string(),
                excerpt: "Property 'databaseUrl' does not exist on type".to_string(),
                source_url: None,
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
                        digest: synthetic_digest_for_ref("a-pr"),
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
                &StaticRepoValidation(RepoValidationStatus::Current),
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
                "Henosis merge gate passed. The candidate world compiled and rendered.".to_string()
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
                &StaticRepoValidation(RepoValidationStatus::Current),
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
        assert!(resolved[0].2.contains("databaseUrl (removed)"));
        assert!(merge_executor.calls.lock().unwrap().is_empty());
        let comments = commenter.comments.lock().unwrap();
        assert_eq!(comments.len(), 1);
        assert!(comments[0].1.contains("Henosis merge gate failed"));
        assert!(comments[0].1.contains("service-b"));
        assert!(comments[0].1.contains("service-a"));
    }

    #[tokio::test]
    async fn tick_resumes_running_gate_without_creating_duplicate_check() {
        let manager = QueueManager::new(components());
        let dev = StaticDevManifest(dev_manifest());
        let mut store = MemoryQueueStore::default();
        let run = recorded_run(RUNNING_STATUS);
        let external_id = run.external_id.clone();
        store
            .statuses
            .insert(external_id.clone(), RUNNING_STATUS.to_string());
        store.recorded.push(run);
        let reporter = MemoryCheckReporter::default();
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
                &StaticRepoValidation(RepoValidationStatus::Current),
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(gate_run.status, GATE_FAILED_STATUS);
        assert!(reporter.checks.lock().unwrap().is_empty());
        assert_eq!(
            store.statuses.get(&external_id).map(String::as_str),
            Some(GATE_FAILED_STATUS)
        );
        let resolved = reporter.resolved.lock().unwrap();
        assert_eq!(resolved[0].0, external_id);
        assert_eq!(resolved[0].1, CheckConclusion::Failure);
        assert!(resolved[0].2.contains("service-b"));
        assert!(merge_executor.calls.lock().unwrap().is_empty());
        assert_eq!(commenter.comments.lock().unwrap().len(), 1);
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
