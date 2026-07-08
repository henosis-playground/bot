use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Context;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::henosis::config::RegisteredComponent;
use crate::henosis::graph::{ComponentGraph, ComponentPackageReader, ComponentRef};
use crate::henosis::manifest::{
    self, ComponentEntry, EnvironmentSection, Manifest, PinnedEntry, follower_dev, pinned,
    synthetic_digest_for_ref,
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
    pub manifest_path: String,
    pub is_preview: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvironmentWrite {
    pub id: String,
    pub manifest_path: String,
    pub branch: String,
    pub commit_sha: String,
    pub members: Vec<PreviewPullRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetiredEnvironment {
    pub id: String,
    pub manifest_path: String,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenderStatus {
    Success,
    Failure,
}

impl RenderStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
        }
    }
}

impl TryFrom<&str> for RenderStatus {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "success" => Ok(Self::Success),
            "failure" => Ok(Self::Failure),
            _ => anyhow::bail!("unknown render status `{value}`"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderOutcome {
    pub environment_id: String,
    pub commit_sha: String,
    pub status: RenderStatus,
    pub run_url: String,
    pub excerpt: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeployWriteResult {
    pub commit_sha: String,
}

pub trait DevManifestReader {
    async fn read_dev_manifest(&self) -> anyhow::Result<Manifest>;
}

pub trait ImageDigestResolver {
    async fn image_digest(&self, repo: &str, sha: &str) -> anyhow::Result<Option<String>>;
}

pub trait DeployRepoWriter {
    async fn write_manifest(
        &mut self,
        path: &str,
        contents: &str,
    ) -> anyhow::Result<DeployWriteResult>;
    async fn delete_manifest(&mut self, path: &str) -> anyhow::Result<()>;
    async fn create_branch(&mut self, branch: &str) -> anyhow::Result<()>;
    async fn delete_branch(&mut self, branch: &str) -> anyhow::Result<()>;
}

pub trait EnvironmentStore {
    async fn upsert_environment(
        &mut self,
        id: &str,
        manifest_path: &str,
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
    async fn active_environment(&self, id: &str) -> anyhow::Result<Option<EnvironmentState>>;
    async fn active_members(&self, environment_id: &str)
    -> anyhow::Result<Vec<PreviewPullRequest>>;
    async fn record_manifest_revision(
        &mut self,
        environment_id: &str,
        commit_sha: &str,
    ) -> anyhow::Result<()>;
}

pub trait EnvironmentIdGenerator {
    fn new_preview_environment_id(&self) -> String;
}

#[derive(Default)]
pub struct RandomEnvironmentIdGenerator;

impl EnvironmentIdGenerator for RandomEnvironmentIdGenerator {
    fn new_preview_environment_id(&self) -> String {
        let mut bytes = rand::random::<[u8; 16]>();
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        preview_environment_id(bytes)
    }
}

pub struct EnvironmentManager {
    components: Vec<RegisteredComponent>,
    id_generator: Arc<dyn EnvironmentIdGenerator + Send + Sync>,
}

impl EnvironmentManager {
    pub fn new(components: Vec<RegisteredComponent>) -> Self {
        Self {
            components,
            id_generator: Arc::new(RandomEnvironmentIdGenerator),
        }
    }

    pub fn with_id_generator(
        components: Vec<RegisteredComponent>,
        id_generator: Arc<dyn EnvironmentIdGenerator + Send + Sync>,
    ) -> Self {
        Self {
            components,
            id_generator,
        }
    }

    pub async fn open_pr<S, W, R, D>(
        &self,
        store: &mut S,
        writer: &mut W,
        package_reader: &R,
        dev_manifests: &D,
        digest_resolver: &impl ImageDigestResolver,
        pr: PreviewPullRequest,
    ) -> anyhow::Result<EnvironmentChange>
    where
        S: EnvironmentStore,
        W: DeployRepoWriter,
        R: ComponentPackageReader,
        D: DevManifestReader,
    {
        let previous = store.environment_for_pr(&pr.key).await?;
        let id = self.id_generator.new_preview_environment_id();
        let manifest_path = environment_manifest_path(&id);
        store.upsert_environment(&id, &manifest_path, true).await?;
        store.retire_member(&pr.key).await?;
        store.put_member(&id, &pr).await?;

        let mut change = EnvironmentChange::default();
        if let Some(previous) = previous.filter(|previous| previous.id != id) {
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
                        dev_manifests,
                        digest_resolver,
                        &previous.id,
                    )
                    .await?,
                );
            }
        }

        let write = self
            .write_environment(
                store,
                writer,
                package_reader,
                dev_manifests,
                digest_resolver,
                &id,
            )
            .await?;
        change.written.push(write);

        Ok(change)
    }

    pub async fn reopen_pr<S, W, R, D>(
        &self,
        store: &mut S,
        writer: &mut W,
        package_reader: &R,
        dev_manifests: &D,
        digest_resolver: &impl ImageDigestResolver,
        pr: PreviewPullRequest,
    ) -> anyhow::Result<EnvironmentChange>
    where
        S: EnvironmentStore,
        W: DeployRepoWriter,
        R: ComponentPackageReader,
        D: DevManifestReader,
    {
        self.open_pr(
            store,
            writer,
            package_reader,
            dev_manifests,
            digest_resolver,
            pr,
        )
        .await
    }

    pub async fn refresh_pr<S, W, R, D>(
        &self,
        store: &mut S,
        writer: &mut W,
        package_reader: &R,
        dev_manifests: &D,
        digest_resolver: &impl ImageDigestResolver,
        pr: PreviewPullRequest,
    ) -> anyhow::Result<EnvironmentChange>
    where
        S: EnvironmentStore,
        W: DeployRepoWriter,
        R: ComponentPackageReader,
        D: DevManifestReader,
    {
        let Some(environment) = store.environment_for_pr(&pr.key).await? else {
            return self
                .open_pr(
                    store,
                    writer,
                    package_reader,
                    dev_manifests,
                    digest_resolver,
                    pr,
                )
                .await;
        };

        store.put_member(&environment.id, &pr).await?;
        let write = self
            .write_environment(
                store,
                writer,
                package_reader,
                dev_manifests,
                digest_resolver,
                &environment.id,
            )
            .await?;

        Ok(EnvironmentChange {
            written: vec![write],
            retired: vec![],
        })
    }

    pub async fn retire_pr<S, W>(
        &self,
        store: &mut S,
        writer: &mut W,
        package_reader: &impl ComponentPackageReader,
        dev_manifests: &impl DevManifestReader,
        digest_resolver: &impl ImageDigestResolver,
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
        let mut change = self.retire_if_empty(store, writer, &environment).await?;
        if change.retired.is_empty() {
            change.written.push(
                self.write_environment(
                    store,
                    writer,
                    package_reader,
                    dev_manifests,
                    digest_resolver,
                    &environment.id,
                )
                .await?,
            );
        }
        Ok(change)
    }

    pub async fn join<S, W, R, D>(
        &self,
        store: &mut S,
        writer: &mut W,
        package_reader: &R,
        dev_manifests: &D,
        digest_resolver: &impl ImageDigestResolver,
        pr: PreviewPullRequest,
        name: &str,
    ) -> anyhow::Result<EnvironmentChange>
    where
        S: EnvironmentStore,
        W: DeployRepoWriter,
        R: ComponentPackageReader,
        D: DevManifestReader,
    {
        let target_id = name.trim();
        anyhow::ensure!(
            is_preview_environment_id(target_id),
            "`{target_id}` is not a preview environment id"
        );
        let Some(target) = store.active_environment(target_id).await? else {
            anyhow::bail!("preview environment `{target_id}` is not active");
        };
        anyhow::ensure!(
            target.is_preview,
            "environment `{target_id}` is not a preview environment"
        );
        let previous = store.environment_for_pr(&pr.key).await?;

        store.retire_member(&pr.key).await?;
        store.put_member(target_id, &pr).await?;

        let mut change = EnvironmentChange::default();
        if let Some(previous) = previous.filter(|previous| previous.id != target.id) {
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
                        dev_manifests,
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
                dev_manifests,
                digest_resolver,
                &target.id,
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
        dev_manifests: &D,
        digest_resolver: &impl ImageDigestResolver,
        pr: PreviewPullRequest,
    ) -> anyhow::Result<EnvironmentChange>
    where
        S: EnvironmentStore,
        W: DeployRepoWriter,
        R: ComponentPackageReader,
        D: DevManifestReader,
    {
        let previous = store.environment_for_pr(&pr.key).await?;
        let solo_id = self.id_generator.new_preview_environment_id();
        let solo_path = environment_manifest_path(&solo_id);

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
                        dev_manifests,
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
                dev_manifests,
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
        dev_manifests: &D,
        digest_resolver: &impl ImageDigestResolver,
        environment_id: &str,
    ) -> anyhow::Result<EnvironmentWrite>
    where
        S: EnvironmentStore,
        W: DeployRepoWriter,
        R: ComponentPackageReader,
        D: DevManifestReader,
    {
        let environment = store
            .active_members(environment_id)
            .await
            .with_context(|| format!("Cannot load members for environment `{environment_id}`"))?;
        anyhow::ensure!(
            !environment.is_empty(),
            "cannot write environment `{environment_id}` with no active members"
        );
        let manifest = self
            .build_manifest(
                package_reader,
                dev_manifests,
                digest_resolver,
                environment_id,
                &environment,
            )
            .await?;
        let contents = manifest::to_toml(&manifest)?;
        let manifest_path = environment_manifest_path(environment_id);
        let result = writer.write_manifest(&manifest_path, &contents).await?;
        store
            .record_manifest_revision(environment_id, &result.commit_sha)
            .await?;
        let branch = environment_branch(environment_id);
        writer.create_branch(&branch).await?;

        Ok(EnvironmentWrite {
            id: environment_id.to_string(),
            manifest_path,
            branch,
            commit_sha: result.commit_sha,
            members: environment,
        })
    }

    async fn build_manifest<R, D>(
        &self,
        package_reader: &R,
        dev_manifests: &D,
        digest_resolver: &impl ImageDigestResolver,
        environment_id: &str,
        members: &[PreviewPullRequest],
    ) -> anyhow::Result<Manifest>
    where
        R: ComponentPackageReader,
        D: DevManifestReader,
    {
        let dev = dev_manifests.read_dev_manifest().await?;
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
                    dev_pins.get(&component.name).with_context(|| {
                        format!("No dev pin found for component `{}`", component.name)
                    })?;
                    let digest = digest_resolver
                        .image_digest(&member.key.repo, &member.head_sha)
                        .await?
                        .unwrap_or_else(|| synthetic_digest_for_ref(&member.head_sha));
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

        Ok(Manifest {
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

        writer.delete_manifest(&environment.manifest_path).await?;
        let branch = environment_branch(&environment.id);
        writer.delete_branch(&branch).await?;
        store.retire_environment(&environment.id).await?;
        Ok(EnvironmentChange {
            written: vec![],
            retired: vec![RetiredEnvironment {
                id: environment.id.clone(),
                manifest_path: environment.manifest_path.clone(),
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

pub fn preview_environment_id(mut bytes: [u8; 16]) -> String {
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "preview-{0:02x}{1:02x}{2:02x}{3:02x}-{4:02x}{5:02x}-{6:02x}{7:02x}-{8:02x}{9:02x}-{10:02x}{11:02x}{12:02x}{13:02x}{14:02x}{15:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15],
    )
}

pub fn is_preview_environment_id(id: &str) -> bool {
    let Some(uuid) = id.strip_prefix("preview-") else {
        return false;
    };
    if uuid.len() != 36 {
        return false;
    }
    for (index, ch) in uuid.chars().enumerate() {
        match index {
            8 | 13 | 18 | 23 if ch == '-' => {}
            14 if ch == '4' => {}
            19 if matches!(ch, '8' | '9' | 'a' | 'b') => {}
            8 | 13 | 18 | 23 | 14 | 19 => return false,
            _ if ch.is_ascii_digit() || matches!(ch, 'a'..='f') => {}
            _ => return false,
        }
    }
    true
}

pub fn environment_manifest_path(environment_id: &str) -> String {
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

fn dev_pins(manifest: &Manifest) -> anyhow::Result<BTreeMap<String, PinnedEntry>> {
    manifest
        .components
        .iter()
        .map(|(name, entry)| match entry {
            ComponentEntry::Pinned(pin) => Ok((name.clone(), pin.clone())),
            ComponentEntry::Follower(_) => {
                anyhow::bail!("dev manifest contains follower entry for `{name}`")
            }
        })
        .collect()
}

#[allow(dead_code)]
fn _graph_refs_for_docs(_refs: &[ComponentRef]) {}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::*;
    use crate::henosis::config::ComponentMode;
    use crate::henosis::graph::{PackageHenosis, PackageJson};

    #[derive(Clone)]
    struct StaticDevManifest(Manifest);

    impl DevManifestReader for StaticDevManifest {
        async fn read_dev_manifest(&self) -> anyhow::Result<Manifest> {
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
        deleted_manifests: Vec<String>,
        created_branches: Vec<String>,
        deleted_branches: Vec<String>,
        commit_counter: u64,
    }

    impl DeployRepoWriter for MemoryWriter {
        async fn write_manifest(
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

        async fn delete_manifest(&mut self, path: &str) -> anyhow::Result<()> {
            self.deleted_manifests.push(path.to_string());
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
            manifest_path: &str,
            is_preview: bool,
        ) -> anyhow::Result<()> {
            anyhow::ensure!(
                !self.retired_environments.contains(id),
                "Cannot reuse retired environment id `{id}`"
            );
            self.environments.insert(
                id.to_string(),
                EnvironmentState {
                    id: id.to_string(),
                    manifest_path: manifest_path.to_string(),
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

        async fn active_environment(&self, id: &str) -> anyhow::Result<Option<EnvironmentState>> {
            Ok(self
                .environments
                .get(id)
                .filter(|environment| !self.retired_environments.contains(&environment.id))
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

        async fn record_manifest_revision(
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

    fn package(name: &str, component: &str, deps: &[&str]) -> PackageJson {
        PackageJson {
            name: name.to_string(),
            dependencies: deps
                .iter()
                .map(|dep| (dep.to_string(), "*".to_string()))
                .collect(),
            henosis: PackageHenosis {
                component: Some(component.to_string()),
            },
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

    fn package_reader() -> MemoryPackageReader {
        let service_a = package("@henosis/service-a", "service-a", &["@henosis/sdk"]);
        let service_b = package(
            "@henosis/service-b",
            "service-b",
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
                        "henosis-playground/service-a".to_string(),
                        "a-pr-2".to_string(),
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

    fn read_written(writer: &MemoryWriter, path: &str) -> Manifest {
        manifest::parse_toml(writer.writes.get(path).unwrap()).unwrap()
    }

    struct FixedIdGenerator(Mutex<VecDeque<String>>);

    impl FixedIdGenerator {
        fn new(ids: &[&str]) -> Arc<Self> {
            Arc::new(Self(Mutex::new(
                ids.iter().map(|id| id.to_string()).collect(),
            )))
        }
    }

    impl EnvironmentIdGenerator for FixedIdGenerator {
        fn new_preview_environment_id(&self) -> String {
            self.0
                .lock()
                .unwrap()
                .pop_front()
                .expect("test exhausted preview ids")
        }
    }

    fn manager_with_ids(ids: &[&str]) -> EnvironmentManager {
        EnvironmentManager::with_id_generator(components(), FixedIdGenerator::new(ids))
    }

    #[tokio::test]
    async fn opens_and_retires_solo_environment() {
        let manager = manager_with_ids(&["preview-00000000-0000-4000-8000-000000000001"]);
        let mut store = MemoryStore::default();
        let mut writer = MemoryWriter::default();
        let packages = package_reader();
        let dev = StaticDevManifest(dev_manifest());
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

        assert_eq!(
            change.written[0].id,
            "preview-00000000-0000-4000-8000-000000000001"
        );
        assert_eq!(
            writer.created_branches,
            vec!["env/preview-00000000-0000-4000-8000-000000000001"]
        );
        let manifest = read_written(&writer, "preview-00000000-0000-4000-8000-000000000001.toml");
        assert!(matches!(
            manifest.components.get("service-a"),
            Some(ComponentEntry::Pinned(PinnedEntry { r#ref, digest, .. }))
                if r#ref == "pr/3" && digest == &synthetic_digest_for_ref("a-pr")
        ));
        assert!(matches!(
            manifest.components.get("service-b"),
            Some(ComponentEntry::Pinned(PinnedEntry { r#ref, .. })) if r#ref == "b-main"
        ));

        let close = manager
            .retire_pr(
                &mut store,
                &mut writer,
                &packages,
                &dev,
                &digest,
                PullRequestKey::new("henosis-playground/service-a", 3),
            )
            .await
            .unwrap();

        assert_eq!(
            close.retired[0].id,
            "preview-00000000-0000-4000-8000-000000000001"
        );
        assert_eq!(
            writer.deleted_manifests,
            vec!["preview-00000000-0000-4000-8000-000000000001.toml"]
        );
        assert_eq!(
            writer.deleted_branches,
            vec!["env/preview-00000000-0000-4000-8000-000000000001"]
        );
    }

    #[tokio::test]
    async fn join_and_leave_regenerate_affected_manifests() {
        let manager = manager_with_ids(&[
            "preview-00000000-0000-4000-8000-000000000001",
            "preview-00000000-0000-4000-8000-000000000002",
            "preview-00000000-0000-4000-8000-000000000003",
        ]);
        let mut store = MemoryStore::default();
        let mut writer = MemoryWriter::default();
        let packages = package_reader();
        let dev = StaticDevManifest(dev_manifest());
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
                "preview-00000000-0000-4000-8000-000000000001",
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
                "preview-00000000-0000-4000-8000-000000000001",
            )
            .await
            .unwrap();

        let shared = read_written(&writer, "preview-00000000-0000-4000-8000-000000000001.toml");
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
                .deleted_manifests
                .contains(&"preview-00000000-0000-4000-8000-000000000002.toml".to_string())
        );

        manager
            .leave(&mut store, &mut writer, &packages, &dev, &digest, service_a)
            .await
            .unwrap();

        let shared = read_written(&writer, "preview-00000000-0000-4000-8000-000000000001.toml");
        assert!(matches!(
            shared.components.get("service-a"),
            Some(ComponentEntry::Follower(_))
        ));
        assert!(matches!(
            shared.components.get("service-b"),
            Some(ComponentEntry::Pinned(PinnedEntry { r#ref, .. })) if r#ref == "pr/7"
        ));
        let solo = read_written(&writer, "preview-00000000-0000-4000-8000-000000000003.toml");
        assert!(matches!(
            solo.components.get("service-a"),
            Some(ComponentEntry::Pinned(PinnedEntry { r#ref, .. })) if r#ref == "pr/3"
        ));
    }

    #[tokio::test]
    async fn refresh_preserves_shared_environment_membership() {
        let manager = manager_with_ids(&[
            "preview-00000000-0000-4000-8000-000000000001",
            "preview-00000000-0000-4000-8000-000000000002",
        ]);
        let mut store = MemoryStore::default();
        let mut writer = MemoryWriter::default();
        let packages = package_reader();
        let dev = StaticDevManifest(dev_manifest());
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
                service_a,
                "preview-00000000-0000-4000-8000-000000000001",
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
                service_b,
                "preview-00000000-0000-4000-8000-000000000001",
            )
            .await
            .unwrap();

        let change = manager
            .refresh_pr(
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
                    "a-pr-2",
                ),
            )
            .await
            .unwrap();

        assert_eq!(
            change.written[0].id,
            "preview-00000000-0000-4000-8000-000000000001"
        );
        let shared = read_written(&writer, "preview-00000000-0000-4000-8000-000000000001.toml");
        assert!(matches!(
            shared.components.get("service-a"),
            Some(ComponentEntry::Pinned(PinnedEntry { r#ref, digest, .. }))
                if r#ref == "pr/3" && digest == &synthetic_digest_for_ref("a-pr-2")
        ));
        assert!(matches!(
            shared.components.get("service-b"),
            Some(ComponentEntry::Pinned(PinnedEntry { r#ref, .. })) if r#ref == "pr/7"
        ));
    }

    #[test]
    fn preview_ids_are_uuid_v4_and_legacy_slug_helper_still_behaves() {
        let id = preview_environment_id([0; 16]);
        assert_eq!(id, "preview-00000000-0000-4000-8000-000000000000");
        assert!(is_preview_environment_id(&id));
        assert!(!is_preview_environment_id("pr-service-a-42"));
        assert!(!is_preview_environment_id(
            "preview-00000000-0000-5000-8000-000000000000"
        ));
        assert_eq!(solo_environment_id("Org/Service_A", 42), "pr-service-a-42");
        assert_eq!(shared_environment_id("Demo Stack!!"), "demo-stack");
        assert_eq!(slugify("----"), "env");
        assert_eq!(slugify(&"a".repeat(100)).len(), 63);
    }
}
