use std::collections::BTreeMap;

use anyhow::Context;
use indexmap::IndexMap;

use crate::henosis::config::RegisteredComponent;
use crate::henosis::environment::{DevLockfileReader, PullRequestKey};
use crate::henosis::lockfile::{ComponentEntry, PinnedEntry};

pub const GLOBAL_QUEUE_LOCK_KEY: i64 = 0x4845_4e4f_5155_4555;
pub const PENDING_EXECUTOR_STATUS: &str = "pending-executor";

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateComponent {
    pub name: String,
    pub repo: String,
    pub r#ref: String,
    pub digest: String,
    pub candidate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateWorld {
    pub members: Vec<QueuePullRequest>,
    pub components: Vec<CandidateComponent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedGateRun {
    pub id: i64,
    pub external_id: String,
    pub world: CandidateWorld,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateStatus {
    pub external_id: String,
    pub status: String,
}

pub trait QueueStore {
    async fn try_acquire_global_lock(&mut self) -> anyhow::Result<bool>;
    async fn release_global_lock(&mut self) -> anyhow::Result<()>;
    async fn oldest_ready_candidate(
        &mut self,
        components: &[RegisteredComponent],
    ) -> anyhow::Result<Option<QueuePullRequest>>;
    async fn record_gate_run(&mut self, world: &CandidateWorld) -> anyhow::Result<RecordedGateRun>;
    async fn mark_gate_run_status(&mut self, external_id: &str, status: &str)
    -> anyhow::Result<()>;
    async fn latest_gate_status(&self, key: &PullRequestKey) -> anyhow::Result<Option<GateStatus>>;
}

pub trait GateCheckReporter {
    async fn create_in_progress_check(
        &self,
        pr: &QueuePullRequest,
        external_id: &str,
    ) -> anyhow::Result<()>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateExecutionRequest {
    PendingExecutor,
}

pub trait GateExecutor {
    async fn request_execution(
        &self,
        gate_run: &RecordedGateRun,
    ) -> anyhow::Result<GateExecutionRequest>;
}

#[derive(Debug, Default)]
pub struct PendingGateExecutor;

impl GateExecutor for PendingGateExecutor {
    async fn request_execution(
        &self,
        _gate_run: &RecordedGateRun,
    ) -> anyhow::Result<GateExecutionRequest> {
        Ok(GateExecutionRequest::PendingExecutor)
    }
}

pub struct QueueManager {
    components: Vec<RegisteredComponent>,
}

impl QueueManager {
    pub fn new(components: Vec<RegisteredComponent>) -> Self {
        Self { components }
    }

    pub async fn tick<S, D, R, E>(
        &self,
        store: &mut S,
        dev_lockfiles: &D,
        check_reporter: &R,
        executor: &E,
    ) -> anyhow::Result<Option<RecordedGateRun>>
    where
        S: QueueStore,
        D: DevLockfileReader,
        R: GateCheckReporter,
        E: GateExecutor,
    {
        if !store.try_acquire_global_lock().await? {
            return Ok(None);
        }

        let result = self
            .tick_with_lock(store, dev_lockfiles, check_reporter, executor)
            .await;
        let release = store.release_global_lock().await;
        match (result, release) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Err(error), Err(release_error)) => Err(error.context(release_error)),
        }
    }

    async fn tick_with_lock<S, D, R, E>(
        &self,
        store: &mut S,
        dev_lockfiles: &D,
        check_reporter: &R,
        executor: &E,
    ) -> anyhow::Result<Option<RecordedGateRun>>
    where
        S: QueueStore,
        D: DevLockfileReader,
        R: GateCheckReporter,
        E: GateExecutor,
    {
        let Some(candidate) = store.oldest_ready_candidate(&self.components).await? else {
            return Ok(None);
        };

        let world = self
            .candidate_world(dev_lockfiles, vec![candidate.clone()])
            .await?;
        let gate_run = store.record_gate_run(&world).await?;
        check_reporter
            .create_in_progress_check(&candidate, &gate_run.external_id)
            .await?;

        match executor.request_execution(&gate_run).await? {
            GateExecutionRequest::PendingExecutor => {
                store
                    .mark_gate_run_status(&gate_run.external_id, PENDING_EXECUTOR_STATUS)
                    .await?;
            }
        }

        Ok(Some(gate_run))
    }

    async fn candidate_world<D>(
        &self,
        dev_lockfiles: &D,
        members: Vec<QueuePullRequest>,
    ) -> anyhow::Result<CandidateWorld>
    where
        D: DevLockfileReader,
    {
        let dev = dev_lockfiles.read_dev_lockfile().await?;
        let dev_pins = dev
            .components
            .iter()
            .map(|(name, entry)| match entry {
                ComponentEntry::Pinned(pin) => Ok((name.clone(), pin.clone())),
                ComponentEntry::Follower(_) => {
                    anyhow::bail!("dev lockfile contains follower entry for `{name}`")
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
    use crate::henosis::environment::DevLockfileReader;
    use crate::henosis::lockfile::{EnvironmentSection, Lockfile, pinned};
    use std::collections::VecDeque;

    #[derive(Clone)]
    struct StaticDevLockfile(Lockfile);

    impl DevLockfileReader for StaticDevLockfile {
        async fn read_dev_lockfile(&self) -> anyhow::Result<Lockfile> {
            Ok(self.0.clone())
        }
    }

    #[derive(Default)]
    struct MemoryQueueStore {
        lock_taken: bool,
        ready: VecDeque<QueuePullRequest>,
        recorded: Vec<RecordedGateRun>,
        statuses: BTreeMap<String, String>,
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

        async fn record_gate_run(
            &mut self,
            world: &CandidateWorld,
        ) -> anyhow::Result<RecordedGateRun> {
            let id = self.recorded.len() as i64 + 1;
            let gate_run = RecordedGateRun {
                id,
                external_id: gate_external_id(world)?,
                world: world.clone(),
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
    }

    #[derive(Default)]
    struct MemoryCheckReporter {
        checks: std::sync::Mutex<Vec<(PullRequestKey, String)>>,
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

    fn dev_lockfile() -> Lockfile {
        Lockfile {
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

    #[tokio::test]
    async fn tick_records_gate_run_check_run_and_pending_executor_status() {
        let manager = QueueManager::new(components());
        let dev = StaticDevLockfile(dev_lockfile());
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
        let executor = PendingGateExecutor;

        let gate_run = manager
            .tick(&mut store, &dev, &reporter, &executor)
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
            Some(PENDING_EXECUTOR_STATUS)
        );
    }
}
