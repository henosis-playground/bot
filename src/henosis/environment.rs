use std::collections::BTreeMap;

use anyhow::Context;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::henosis::config::RegisteredComponent;
use crate::henosis::graph::{ComponentGraph, ComponentPackageReader, ComponentRef};
use crate::henosis::lockfile::{
    self, ComponentEntry, EnvironmentSection, Lockfile, PinnedEntry, follower_dev, pinned,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PullRequestKey {
    pub repo: String,
    pub number: u64,
}

impl PullRequestKey {
    pub fn new(repo: impl Into<String>, number: u64) -> Self {
        Self {
            repo: repo.into(),
            number,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviewPullRequest {
    pub key: PullRequestKey,
    pub component: String,
    pub head_branch: String,
    pub head_sha: String,
}

impl PreviewPullRequest {
    pub fn new(
        repo: impl Into<String>,
        number: u64,
        component: impl Into<String>,
        head_branch: impl Into<String>,
        head_sha: impl Into<String>,
    ) -> Self {
        Self {
            key: PullRequestKey::new(repo, number),
            component: component.into(),
            head_branch: head_branch.into(),
            head_sha: head_sha.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvironmentState {
    pub id: String,
    pub lockfile_path: String,
    pub is_preview: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvironmentWrite {
    pub id: String,
    pub lockfile_path: String,
    pub branch: String,
    pub commit_sha: String,
    pub members: Vec<PreviewPullRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetiredEnvironment {
    pub id: String,
    pub lockfile_path: String,
    pub branch: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EnvironmentChange {
    pub written: Vec<EnvironmentWrite>,
    pub retired: Vec<RetiredEnvironment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvironmentStatus {
    pub environment: EnvironmentState,
    pub branch: String,
    pub members: Vec<PreviewPullRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeployWriteResult {
    pub commit_sha: String,
}

pub trait DevLockfileReader {
    async fn read_dev_lockfile(&self) -> anyhow::Result<Lockfile>;
}

pub trait ImageDigestResolver {
    async fn image_digest(&self, repo: &str, sha: &str) -> anyhow::Result<Option<String>>;
}

pub trait DeployRepoWriter {
    async fn write_lockfile(
        &mut self,
        path: &str,
        contents: &str,
    ) -> anyhow::Result<DeployWriteResult>;
    async fn delete_lockfile(&mut self, path: &str) -> anyhow::Result<()>;
    async fn create_branch(&mut self, branch: &str) -> anyhow::Result<()>;
    async fn delete_branch(&mut self, branch: &str) -> anyhow::Result<()>;
}

pub trait EnvironmentStore {
    async fn upsert_environment(
        &mut self,
        id: &str,
        lockfile_path: &str,
        is_preview: bool,
    ) -> anyhow::Result<()>;
    async fn retire_environment(&mut self, id: &str) -> anyhow::Result<()>;
    async fn put_member(
        &mut self,
        environment_id: &str,
        member: &PreviewPullRequest,
    ) -> anyhow::Result<()>;
    async fn retire_member(&mut self, key: &PullRequestKey) -> anyhow::Result<()>;
    async fn environment_for_pr(
        &self,
        key: &PullRequestKey,
    ) -> anyhow::Result<Option<EnvironmentState>>;
    async fn active_members(&self, environment_id: &str)
    -> anyhow::Result<Vec<PreviewPullRequest>>;
    async fn record_lockfile_revision(
        &mut self,
        environment_id: &str,
        commit_sha: &str,
    ) -> anyhow::Result<()>;
}

pub struct EnvironmentManager {
    components: Vec<RegisteredComponent>,
}

impl EnvironmentManager {
    pub fn new(components: Vec<RegisteredComponent>) -> Self {
        Self { components }
    }

    pub async fn open_pr<S, W, R, D>(
        &self,
        store: &mut S,
        writer: &mut W,
        package_reader: &R,
        dev_lockfiles: &D,
        digest_resolver: &impl ImageDigestResolver,
        pr: PreviewPullRequest,
    ) -> anyhow::Result<EnvironmentChange>
    where
        S: EnvironmentStore,
        W: DeployRepoWriter,
        R: ComponentPackageReader,
        D: DevLockfileReader,
    {
        let id = solo_environment_id(&pr.key.repo, pr.key.number);
        let lockfile_path = environment_lockfile_path(&id);
        store.upsert_environment(&id, &lockfile_path, true).await?;
        store.retire_member(&pr.key).await?;
        store.put_member(&id, &pr).await?;

        let write = self
            .write_environment(
                store,
                writer,
                package_reader,
                dev_lockfiles,
                digest_resolver,
                &id,
            )
            .await?;

        Ok(EnvironmentChange {
            written: vec![write],
            retired: vec![],
        })
    }

    pub async fn reopen_pr<S, W, R, D>(
        &self,
        store: &mut S,
        writer: &mut W,
        package_reader: &R,
        dev_lockfiles: &D,
        digest_resolver: &impl ImageDigestResolver,
        pr: PreviewPullRequest,
    ) -> anyhow::Result<EnvironmentChange>
    where
        S: EnvironmentStore,
        W: DeployRepoWriter,
        R: ComponentPackageReader,
        D: DevLockfileReader,
    {
        self.open_pr(
            store,
            writer,
            package_reader,
            dev_lockfiles,
            digest_resolver,
            pr,
        )
        .await
    }

    pub async fn retire_pr<S, W>(
        &self,
        store: &mut S,
        writer: &mut W,
        key: PullRequestKey,
    ) -> anyhow::Result<EnvironmentChange>
    where
        S: EnvironmentStore,
        W: DeployRepoWriter,
    {
        let Some(environment) = store.environment_for_pr(&key).await? else {
            return Ok(EnvironmentChange::default());
        };
        store.retire_member(&key).await?;
        self.retire_if_empty(store, writer, &environment).await
    }

    pub async fn join<S, W, R, D>(
        &self,
        store: &mut S,
        writer: &mut W,
        package_reader: &R,
        dev_lockfiles: &D,
        digest_resolver: &impl ImageDigestResolver,
        pr: PreviewPullRequest,
        name: &str,
    ) -> anyhow::Result<EnvironmentChange>
    where
        S: EnvironmentStore,
        W: DeployRepoWriter,
        R: ComponentPackageReader,
        D: DevLockfileReader,
    {
        let target_id = shared_environment_id(name);
        let target_path = environment_lockfile_path(&target_id);
        let previous = store.environment_for_pr(&pr.key).await?;

        store
            .upsert_environment(&target_id, &target_path, true)
            .await?;
        store.retire_member(&pr.key).await?;
        store.put_member(&target_id, &pr).await?;

        let mut change = EnvironmentChange::default();
        if let Some(previous) = previous.filter(|previous| previous.id != target_id) {
            change.extend(self.retire_if_empty(store, writer, &previous).await?);
            if store
                .active_members(&previous.id)
                .await
                .map(|members| !members.is_empty())
                .unwrap_or(false)
            {
                change.written.push(
                    self.write_environment(
                        store,
                        writer,
                        package_reader,
                        dev_lockfiles,
                        digest_resolver,
                        &previous.id,
                    )
                    .await?,
                );
            }
        }

        change.written.push(
            self.write_environment(
                store,
                writer,
                package_reader,
                dev_lockfiles,
                digest_resolver,
                &target_id,
            )
            .await?,
        );
        Ok(change)
    }

    pub async fn leave<S, W, R, D>(
        &self,
        store: &mut S,
        writer: &mut W,
        package_reader: &R,
        dev_lockfiles: &D,
        digest_resolver: &impl ImageDigestResolver,
        pr: PreviewPullRequest,
    ) -> anyhow::Result<EnvironmentChange>
    where
        S: EnvironmentStore,
        W: DeployRepoWriter,
        R: ComponentPackageReader,
        D: DevLockfileReader,
    {
        let previous = store.environment_for_pr(&pr.key).await?;
        let solo_id = solo_environment_id(&pr.key.repo, pr.key.number);
        let solo_path = environment_lockfile_path(&solo_id);

        store.upsert_environment(&solo_id, &solo_path, true).await?;
        store.retire_member(&pr.key).await?;
        store.put_member(&solo_id, &pr).await?;

        let mut change = EnvironmentChange::default();
        if let Some(previous) = previous.filter(|previous| previous.id != solo_id) {
            change.extend(self.retire_if_empty(store, writer, &previous).await?);
            if store
                .active_members(&previous.id)
                .await
                .map(|members| !members.is_empty())
                .unwrap_or(false)
            {
                change.written.push(
                    self.write_environment(
                        store,
                        writer,
                        package_reader,
                        dev_lockfiles,
                        digest_resolver,
                        &previous.id,
                    )
                    .await?,
                );
            }
        }

        change.written.push(
            self.write_environment(
                store,
                writer,
                package_reader,
                dev_lockfiles,
                digest_resolver,
                &solo_id,
            )
            .await?,
        );
        Ok(change)
    }

    pub async fn status<S>(
        &self,
        store: &S,
        key: &PullRequestKey,
    ) -> anyhow::Result<Option<EnvironmentStatus>>
    where
        S: EnvironmentStore,
    {
        let Some(environment) = store.environment_for_pr(key).await? else {
            return Ok(None);
        };
        let members = store.active_members(&environment.id).await?;
        Ok(Some(EnvironmentStatus {
            branch: environment_branch(&environment.id),
            environment,
            members,
        }))
    }

    async fn write_environment<S, W, R, D>(
        &self,
        store: &mut S,
        writer: &mut W,
        package_reader: &R,
        dev_lockfiles: &D,
        digest_resolver: &impl ImageDigestResolver,
        environment_id: &str,
    ) -> anyhow::Result<EnvironmentWrite>
    where
        S: EnvironmentStore,
        W: DeployRepoWriter,
        R: ComponentPackageReader,
        D: DevLockfileReader,
    {
        let environment = store
            .active_members(environment_id)
            .await
            .with_context(|| format!("Cannot load members for environment `{environment_id}`"))?;
        anyhow::ensure!(
            !environment.is_empty(),
            "cannot write environment `{environment_id}` with no active members"
        );
        let lockfile = self
            .build_lockfile(
                package_reader,
                dev_lockfiles,
                digest_resolver,
                environment_id,
                &environment,
            )
            .await?;
        let contents = lockfile::to_toml(&lockfile)?;
        let lockfile_path = environment_lockfile_path(environment_id);
        let result = writer.write_lockfile(&lockfile_path, &contents).await?;
        store
            .record_lockfile_revision(environment_id, &result.commit_sha)
            .await?;
        let branch = environment_branch(environment_id);
        writer.create_branch(&branch).await?;

        Ok(EnvironmentWrite {
            id: environment_id.to_string(),
            lockfile_path,
            branch,
            commit_sha: result.commit_sha,
            members: environment,
        })
    }

    async fn build_lockfile<R, D>(
        &self,
        package_reader: &R,
        dev_lockfiles: &D,
        digest_resolver: &impl ImageDigestResolver,
        environment_id: &str,
        members: &[PreviewPullRequest],
    ) -> anyhow::Result<Lockfile>
    where
        R: ComponentPackageReader,
        D: DevLockfileReader,
    {
        let dev = dev_lockfiles.read_dev_lockfile().await?;
        let dev_pins = dev_pins(&dev)?;
        let members_by_component = members
            .iter()
            .map(|member| (member.component.clone(), member))
            .collect::<BTreeMap<_, _>>();
        let refs = self
            .components
            .iter()
            .map(|component| {
                let r#ref = members_by_component
                    .get(&component.name)
                    .map(|member| member.head_sha.clone())
                    .or_else(|| dev_pins.get(&component.name).map(|pin| pin.r#ref.clone()))
                    .with_context(|| {
                        format!("No dev pin found for component `{}`", component.name)
                    })?;
                Ok((component.name.clone(), r#ref))
            })
            .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
        let graph_refs = ComponentGraph::from_registered_components(&self.components, &refs)?;
        let graph = ComponentGraph::read(&graph_refs, package_reader).await?;
        let changed_components = members
            .iter()
            .map(|member| member.component.as_str())
            .collect::<Vec<_>>();
        let closure = graph.preview_closure(changed_components);

        let mut components = IndexMap::new();
        for component in &self.components {
            let entry = match members_by_component.get(&component.name) {
                Some(member) => {
                    let dev_pin = dev_pins.get(&component.name).with_context(|| {
                        format!("No dev pin found for component `{}`", component.name)
                    })?;
                    let digest = digest_resolver
                        .image_digest(&member.key.repo, &member.head_sha)
                        .await?
                        .unwrap_or_else(|| dev_pin.digest.clone());
                    pinned(member.key.repo.clone(), member.head_branch.clone(), digest)
                }
                None if closure.contains(&component.name) => {
                    let dev_pin = dev_pins.get(&component.name).with_context(|| {
                        format!("No dev pin found for component `{}`", component.name)
                    })?;
                    pinned(
                        dev_pin.repo.clone(),
                        dev_pin.r#ref.clone(),
                        dev_pin.digest.clone(),
                    )
                }
                None => follower_dev(),
            };
            components.insert(component.name.clone(), entry);
        }

        Ok(Lockfile {
            environment: EnvironmentSection {
                id: environment_id.to_string(),
            },
            components,
        })
    }

    async fn retire_if_empty<S, W>(
        &self,
        store: &mut S,
        writer: &mut W,
        environment: &EnvironmentState,
    ) -> anyhow::Result<EnvironmentChange>
    where
        S: EnvironmentStore,
        W: DeployRepoWriter,
    {
        let members = store.active_members(&environment.id).await?;
        if !members.is_empty() {
            return Ok(EnvironmentChange::default());
        }

        writer.delete_lockfile(&environment.lockfile_path).await?;
        let branch = environment_branch(&environment.id);
        writer.delete_branch(&branch).await?;
        store.retire_environment(&environment.id).await?;
        Ok(EnvironmentChange {
            written: vec![],
            retired: vec![RetiredEnvironment {
                id: environment.id.clone(),
                lockfile_path: environment.lockfile_path.clone(),
                branch,
            }],
        })
    }
}

impl EnvironmentChange {
    fn extend(&mut self, other: EnvironmentChange) {
        self.written.extend(other.written);
        self.retired.extend(other.retired);
    }
}

pub fn solo_environment_id(repo: &str, number: u64) -> String {
    let repo_name = repo.rsplit('/').next().unwrap_or(repo);
    slugify(&format!("pr-{repo_name}-{number}"))
}

pub fn shared_environment_id(name: &str) -> String {
    slugify(name)
}

pub fn environment_lockfile_path(environment_id: &str) -> String {
    format!("{environment_id}.toml")
}

pub fn environment_branch(environment_id: &str) -> String {
    format!("env/{environment_id}")
}

pub fn slugify(input: &str) -> String {
    let mut out = String::new();
    let mut last_was_dash = false;
    for ch in input.chars().flat_map(char::to_lowercase) {
        let normalized = if ch.is_ascii_alphanumeric() { ch } else { '-' };
        if normalized == '-' {
            if out.is_empty() || last_was_dash {
                continue;
            }
            last_was_dash = true;
        } else {
            last_was_dash = false;
        }
        out.push(normalized);
        if out.len() >= 63 {
            break;
        }
    }

    while out.ends_with('-') {
        out.pop();
    }

    if out.is_empty() {
        "env".to_string()
    } else {
        out
    }
}

fn dev_pins(lockfile: &Lockfile) -> anyhow::Result<BTreeMap<String, PinnedEntry>> {
    lockfile
        .components
        .iter()
        .map(|(name, entry)| match entry {
            ComponentEntry::Pinned(pin) => Ok((name.clone(), pin.clone())),
            ComponentEntry::Follower(_) => {
                anyhow::bail!("dev lockfile contains follower entry for `{name}`")
            }
        })
        .collect()
}

#[allow(dead_code)]
fn _graph_refs_for_docs(_refs: &[ComponentRef]) {}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::henosis::graph::{PackageHenosis, PackageJson};

    #[derive(Clone)]
    struct StaticDevLockfile(Lockfile);

    impl DevLockfileReader for StaticDevLockfile {
        async fn read_dev_lockfile(&self) -> anyhow::Result<Lockfile> {
            Ok(self.0.clone())
        }
    }

    struct NoDigestResolver;

    impl ImageDigestResolver for NoDigestResolver {
        async fn image_digest(&self, _repo: &str, _sha: &str) -> anyhow::Result<Option<String>> {
            Ok(None)
        }
    }

    #[derive(Default)]
    struct MemoryWriter {
        writes: BTreeMap<String, String>,
        deleted_lockfiles: Vec<String>,
        created_branches: Vec<String>,
        deleted_branches: Vec<String>,
        commit_counter: u64,
    }

    impl DeployRepoWriter for MemoryWriter {
        async fn write_lockfile(
            &mut self,
            path: &str,
            contents: &str,
        ) -> anyhow::Result<DeployWriteResult> {
            self.commit_counter += 1;
            self.writes.insert(path.to_string(), contents.to_string());
            Ok(DeployWriteResult {
                commit_sha: format!("commit-{}", self.commit_counter),
            })
        }

        async fn delete_lockfile(&mut self, path: &str) -> anyhow::Result<()> {
            self.deleted_lockfiles.push(path.to_string());
            self.writes.remove(path);
            Ok(())
        }

        async fn create_branch(&mut self, branch: &str) -> anyhow::Result<()> {
            self.created_branches.push(branch.to_string());
            Ok(())
        }

        async fn delete_branch(&mut self, branch: &str) -> anyhow::Result<()> {
            self.deleted_branches.push(branch.to_string());
            Ok(())
        }
    }

    #[derive(Default)]
    struct MemoryStore {
        environments: BTreeMap<String, EnvironmentState>,
        retired_environments: BTreeSet<String>,
        members: BTreeMap<PullRequestKey, (String, PreviewPullRequest)>,
        revisions: Vec<(String, String)>,
    }

    impl EnvironmentStore for MemoryStore {
        async fn upsert_environment(
            &mut self,
            id: &str,
            lockfile_path: &str,
            is_preview: bool,
        ) -> anyhow::Result<()> {
            self.retired_environments.remove(id);
            self.environments.insert(
                id.to_string(),
                EnvironmentState {
                    id: id.to_string(),
                    lockfile_path: lockfile_path.to_string(),
                    is_preview,
                },
            );
            Ok(())
        }

        async fn retire_environment(&mut self, id: &str) -> anyhow::Result<()> {
            self.retired_environments.insert(id.to_string());
            Ok(())
        }

        async fn put_member(
            &mut self,
            environment_id: &str,
            member: &PreviewPullRequest,
        ) -> anyhow::Result<()> {
            self.members.insert(
                member.key.clone(),
                (environment_id.to_string(), member.clone()),
            );
            Ok(())
        }

        async fn retire_member(&mut self, key: &PullRequestKey) -> anyhow::Result<()> {
            self.members.remove(key);
            Ok(())
        }

        async fn environment_for_pr(
            &self,
            key: &PullRequestKey,
        ) -> anyhow::Result<Option<EnvironmentState>> {
            Ok(self
                .members
                .get(key)
                .and_then(|(environment_id, _)| self.environments.get(environment_id))
                .cloned())
        }

        async fn active_members(
            &self,
            environment_id: &str,
        ) -> anyhow::Result<Vec<PreviewPullRequest>> {
            let mut members = self
                .members
                .values()
                .filter(|(id, _)| id == environment_id)
                .map(|(_, member)| member.clone())
                .collect::<Vec<_>>();
            members.sort_by_key(|member| member.key.clone());
            Ok(members)
        }

        async fn record_lockfile_revision(
            &mut self,
            environment_id: &str,
            commit_sha: &str,
        ) -> anyhow::Result<()> {
            self.revisions
                .push((environment_id.to_string(), commit_sha.to_string()));
            Ok(())
        }
    }

    struct MemoryPackageReader {
        packages: BTreeMap<(String, String), PackageJson>,
    }

    impl ComponentPackageReader for MemoryPackageReader {
        async fn fetch_package_json(&self, repo: &str, sha: &str) -> anyhow::Result<PackageJson> {
            self.packages
                .get(&(repo.to_string(), sha.to_string()))
                .cloned()
                .with_context(|| format!("missing package {repo}@{sha}"))
        }
    }

    fn package(name: &str, component: &str, surface: bool, deps: &[&str]) -> PackageJson {
        PackageJson {
            name: name.to_string(),
            dependencies: deps
                .iter()
                .map(|dep| (dep.to_string(), "*".to_string()))
                .collect(),
            henosis: PackageHenosis {
                component: Some(component.to_string()),
                surface,
            },
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

    fn package_reader() -> MemoryPackageReader {
        let service_a = package("@henosis/service-a", "service-a", false, &["@henosis/sdk"]);
        let service_b = package(
            "@henosis/service-b",
            "service-b",
            true,
            &["@henosis/sdk", "@henosis/service-a"],
        );
        MemoryPackageReader {
            packages: [
                (
                    (
                        "henosis-playground/service-a".to_string(),
                        "a-main".to_string(),
                    ),
                    service_a.clone(),
                ),
                (
                    (
                        "henosis-playground/service-a".to_string(),
                        "a-pr".to_string(),
                    ),
                    service_a.clone(),
                ),
                (
                    (
                        "henosis-playground/service-b".to_string(),
                        "b-main".to_string(),
                    ),
                    service_b.clone(),
                ),
                (
                    (
                        "henosis-playground/service-b".to_string(),
                        "b-pr".to_string(),
                    ),
                    service_b,
                ),
            ]
            .into_iter()
            .collect(),
        }
    }

    fn read_written(writer: &MemoryWriter, path: &str) -> Lockfile {
        lockfile::parse_toml(writer.writes.get(path).unwrap()).unwrap()
    }

    #[tokio::test]
    async fn opens_and_retires_solo_environment() {
        let manager = EnvironmentManager::new(components());
        let mut store = MemoryStore::default();
        let mut writer = MemoryWriter::default();
        let packages = package_reader();
        let dev = StaticDevLockfile(dev_lockfile());
        let digest = NoDigestResolver;

        let change = manager
            .open_pr(
                &mut store,
                &mut writer,
                &packages,
                &dev,
                &digest,
                PreviewPullRequest::new(
                    "henosis-playground/service-a",
                    3,
                    "service-a",
                    "pr/3",
                    "a-pr",
                ),
            )
            .await
            .unwrap();

        assert_eq!(change.written[0].id, "pr-service-a-3");
        assert_eq!(writer.created_branches, vec!["env/pr-service-a-3"]);
        let lockfile = read_written(&writer, "pr-service-a-3.toml");
        assert!(matches!(
            lockfile.components.get("service-a"),
            Some(ComponentEntry::Pinned(PinnedEntry { r#ref, digest, .. }))
                if r#ref == "pr/3" && digest == "sha256:a"
        ));
        assert!(matches!(
            lockfile.components.get("service-b"),
            Some(ComponentEntry::Pinned(PinnedEntry { r#ref, .. })) if r#ref == "b-main"
        ));

        let close = manager
            .retire_pr(
                &mut store,
                &mut writer,
                PullRequestKey::new("henosis-playground/service-a", 3),
            )
            .await
            .unwrap();

        assert_eq!(close.retired[0].id, "pr-service-a-3");
        assert_eq!(writer.deleted_lockfiles, vec!["pr-service-a-3.toml"]);
        assert_eq!(writer.deleted_branches, vec!["env/pr-service-a-3"]);
    }

    #[tokio::test]
    async fn join_and_leave_regenerate_affected_lockfiles() {
        let manager = EnvironmentManager::new(components());
        let mut store = MemoryStore::default();
        let mut writer = MemoryWriter::default();
        let packages = package_reader();
        let dev = StaticDevLockfile(dev_lockfile());
        let digest = NoDigestResolver;
        let service_a = PreviewPullRequest::new(
            "henosis-playground/service-a",
            3,
            "service-a",
            "pr/3",
            "a-pr",
        );
        let service_b = PreviewPullRequest::new(
            "henosis-playground/service-b",
            7,
            "service-b",
            "pr/7",
            "b-pr",
        );

        manager
            .open_pr(
                &mut store,
                &mut writer,
                &packages,
                &dev,
                &digest,
                service_a.clone(),
            )
            .await
            .unwrap();
        manager
            .open_pr(
                &mut store,
                &mut writer,
                &packages,
                &dev,
                &digest,
                service_b.clone(),
            )
            .await
            .unwrap();
        manager
            .join(
                &mut store,
                &mut writer,
                &packages,
                &dev,
                &digest,
                service_a.clone(),
                "demo stack",
            )
            .await
            .unwrap();
        manager
            .join(
                &mut store,
                &mut writer,
                &packages,
                &dev,
                &digest,
                service_b.clone(),
                "demo stack",
            )
            .await
            .unwrap();

        let shared = read_written(&writer, "demo-stack.toml");
        assert!(matches!(
            shared.components.get("service-a"),
            Some(ComponentEntry::Pinned(PinnedEntry { r#ref, .. })) if r#ref == "pr/3"
        ));
        assert!(matches!(
            shared.components.get("service-b"),
            Some(ComponentEntry::Pinned(PinnedEntry { r#ref, .. })) if r#ref == "pr/7"
        ));
        assert!(
            writer
                .deleted_lockfiles
                .contains(&"pr-service-a-3.toml".to_string())
        );
        assert!(
            writer
                .deleted_lockfiles
                .contains(&"pr-service-b-7.toml".to_string())
        );

        manager
            .leave(&mut store, &mut writer, &packages, &dev, &digest, service_a)
            .await
            .unwrap();

        let shared = read_written(&writer, "demo-stack.toml");
        assert!(matches!(
            shared.components.get("service-a"),
            Some(ComponentEntry::Follower(_))
        ));
        assert!(matches!(
            shared.components.get("service-b"),
            Some(ComponentEntry::Pinned(PinnedEntry { r#ref, .. })) if r#ref == "pr/7"
        ));
        let solo = read_written(&writer, "pr-service-a-3.toml");
        assert!(matches!(
            solo.components.get("service-a"),
            Some(ComponentEntry::Pinned(PinnedEntry { r#ref, .. })) if r#ref == "pr/3"
        ));
    }

    #[test]
    fn slug_ids_are_lowercase_ascii_hyphenated_and_capped() {
        assert_eq!(solo_environment_id("Org/Service_A", 42), "pr-service-a-42");
        assert_eq!(shared_environment_id("Demo Stack!!"), "demo-stack");
        assert_eq!(slugify("----"), "env");
        assert_eq!(slugify(&"a".repeat(100)).len(), 63);
    }
}
