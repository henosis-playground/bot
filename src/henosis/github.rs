use anyhow::Context;
use octocrab::models::CheckRunId;
use octocrab::params::checks::{
    CheckRunConclusion, CheckRunOutput as OctoCheckRunOutput, CheckRunStatus,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use crate::PgDbClient;
use crate::bors::Comment;
use crate::bors::RepositoryStore;
use crate::github::api::client::{CheckRunOutput, GithubRepositoryClient};
use crate::github::{CommitSha, GithubRepoName, PullRequestNumber};
use crate::henosis::config::HenosisConfig;
use crate::henosis::environment::{
    DeployRepoWriter, DeployWriteResult, DevManifestReader, ImageDigestResolver,
};
use crate::henosis::graph::{ComponentPackageReader, PackageJson};
use crate::henosis::manifest::{
    self, ComponentEntry, Manifest, PinnedEntry, synthetic_digest_for_ref,
};
use crate::henosis::merge::{
    DevBump, DevManifestBumper, MergeExecutor, PullRequestMerger, StateMachineMergeExecutor,
};
use crate::henosis::queue::{
    CheckConclusion, GateCheckReporter, GateRun, PrCommenter, QueuePullRequest,
};

pub struct GithubDeployRepoWriter<'a> {
    client: &'a GithubRepositoryClient,
    manifest_branch: String,
}

impl<'a> GithubDeployRepoWriter<'a> {
    pub fn new(client: &'a GithubRepositoryClient, manifest_branch: impl Into<String>) -> Self {
        Self {
            client,
            manifest_branch: manifest_branch.into(),
        }
    }
}

impl DeployRepoWriter for GithubDeployRepoWriter<'_> {
    async fn write_manifest(
        &mut self,
        path: &str,
        contents: &str,
    ) -> anyhow::Result<DeployWriteResult> {
        let commit = self
            .client
            .write_file_to_branch(
                path,
                &self.manifest_branch,
                &format!("Update Henosis manifest {path}"),
                contents,
            )
            .await?;
        Ok(DeployWriteResult {
            commit_sha: commit.to_string(),
        })
    }

    async fn delete_manifest(&mut self, path: &str) -> anyhow::Result<()> {
        self.client
            .delete_file_from_branch(
                path,
                &self.manifest_branch,
                &format!("Delete Henosis manifest {path}"),
            )
            .await?;
        Ok(())
    }

    async fn create_branch(&mut self, branch: &str) -> anyhow::Result<()> {
        if self.client.get_branch_sha(branch).await.is_ok() {
            return Ok(());
        }

        let base = self.client.get_branch_sha(&self.manifest_branch).await?;
        if let Err(error) = self.client.create_branch(branch, &base).await {
            if self.client.get_branch_sha(branch).await.is_ok() {
                return Ok(());
            }
            return Err(error);
        }

        Ok(())
    }

    async fn delete_branch(&mut self, branch: &str) -> anyhow::Result<()> {
        self.client.delete_branch(branch).await
    }
}

pub struct GithubDevManifestReader<'a> {
    client: &'a GithubRepositoryClient,
    manifest_branch: String,
    dev_manifest_path: String,
}

impl<'a> GithubDevManifestReader<'a> {
    pub fn new(
        client: &'a GithubRepositoryClient,
        manifest_branch: impl Into<String>,
        dev_manifest_path: impl Into<String>,
    ) -> Self {
        Self {
            client,
            manifest_branch: manifest_branch.into(),
            dev_manifest_path: dev_manifest_path.into(),
        }
    }
}

impl DevManifestReader for GithubDevManifestReader<'_> {
    async fn read_dev_manifest(&self) -> anyhow::Result<Manifest> {
        let file = self
            .client
            .read_file_at_ref(&self.dev_manifest_path, &self.manifest_branch)
            .await?;
        manifest::parse_toml(&file.content).context("Cannot parse dev manifest")
    }
}

pub struct GithubComponentPackageReader<'a> {
    repositories: &'a RepositoryStore,
}

impl<'a> GithubComponentPackageReader<'a> {
    pub fn new(repositories: &'a RepositoryStore) -> Self {
        Self { repositories }
    }
}

impl ComponentPackageReader for GithubComponentPackageReader<'_> {
    async fn fetch_package_json(&self, repo: &str, sha: &str) -> anyhow::Result<PackageJson> {
        let repo_name: GithubRepoName = repo
            .parse()
            .map_err(|error| anyhow::anyhow!("Invalid GitHub repo `{repo}`: {error}"))?;
        let repo = self
            .repositories
            .get(&repo_name)
            .with_context(|| format!("Repository `{repo_name}` is not loaded"))?;
        let file = repo
            .client
            .read_file_at_ref("henosis/package.json", sha)
            .await?;
        serde_json::from_str(&file.content).with_context(|| {
            format!("Cannot parse henosis/package.json for `{repo_name}` at `{sha}`")
        })
    }
}

pub struct GithubImageDigestResolver<'a> {
    repositories: &'a RepositoryStore,
}

impl<'a> GithubImageDigestResolver<'a> {
    pub fn new(repositories: &'a RepositoryStore) -> Self {
        Self { repositories }
    }
}

impl ImageDigestResolver for GithubImageDigestResolver<'_> {
    async fn image_digest(&self, repo: &str, sha: &str) -> anyhow::Result<Option<String>> {
        let repo_name: GithubRepoName = repo
            .parse()
            .map_err(|error| anyhow::anyhow!("Invalid GitHub repo `{repo}`: {error}"))?;
        let Some(repo) = self.repositories.get(&repo_name) else {
            return Ok(None);
        };

        #[derive(Debug, Deserialize)]
        struct ArtifactList {
            artifacts: Vec<Artifact>,
        }

        #[derive(Debug, Deserialize)]
        struct Artifact {
            name: String,
        }

        let route =
            format!("/repos/{repo_name}/actions/artifacts?per_page=100&name=image-digest-{sha}");
        let artifacts = repo
            .client
            .client()
            .get::<ArtifactList, _, ()>(&route, None)
            .await;
        match artifacts {
            Ok(artifacts) => {
                let digest = artifacts
                    .artifacts
                    .iter()
                    .find_map(|artifact| artifact.name.strip_prefix("image-digest-sha256-"))
                    .map(|suffix| format!("sha256:{suffix}"));
                Ok(digest)
            }
            Err(error) => {
                tracing::debug!(
                    "Could not resolve image digest artifact for `{repo_name}` at `{sha}`: {error:?}"
                );
                Ok(None)
            }
        }
    }
}

pub struct GithubGateCheckReporter<'a> {
    repositories: &'a RepositoryStore,
    check_name: String,
}

impl<'a> GithubGateCheckReporter<'a> {
    pub fn new(repositories: &'a RepositoryStore, check_name: impl Into<String>) -> Self {
        Self {
            repositories,
            check_name: check_name.into(),
        }
    }
}

impl GateCheckReporter for GithubGateCheckReporter<'_> {
    async fn create_in_progress_check(
        &self,
        pr: &QueuePullRequest,
        external_id: &str,
    ) -> anyhow::Result<()> {
        let repo_name: GithubRepoName =
            pr.key.repo.parse().map_err(|error| {
                anyhow::anyhow!("Invalid GitHub repo `{}`: {error}", pr.key.repo)
            })?;
        let repo = self
            .repositories
            .get(&repo_name)
            .with_context(|| format!("Repository `{repo_name}` is not loaded"))?;
        repo.client
            .create_check_run(
                &self.check_name,
                &CommitSha(pr.head_sha.clone()),
                CheckRunStatus::InProgress,
                CheckRunOutput {
                    title: "Henosis gate".to_string(),
                    summary: "Gate run created; waiting for executor.".to_string(),
                },
                external_id,
            )
            .await?;
        Ok(())
    }

    async fn resolve_check_run(
        &self,
        external_id: &str,
        conclusion: CheckConclusion,
        summary: &str,
    ) -> anyhow::Result<()> {
        let Some(head_sha) = external_id.rsplit_once('-').map(|(_, sha)| sha) else {
            anyhow::bail!("Cannot extract head SHA from gate external id `{external_id}`");
        };

        let mut resolved = false;
        for repo in self.repositories.repositories() {
            let repo_external_id_prefix = format!(
                "gate-{}-",
                repo.client.repository().to_string().replace('/', "-")
            );
            if !external_id.starts_with(&repo_external_id_prefix) {
                continue;
            }

            let check_runs = match list_check_runs_for_ref(&repo.client, head_sha).await {
                Ok(runs) => runs,
                Err(e) => {
                    tracing::debug!(
                        "Skipping repo {} when resolving check run (SHA not found or other error): {e:#}",
                        repo.client.repository()
                    );
                    continue;
                }
            };
            for check_run in check_runs
                .into_iter()
                .filter(|check_run| check_run.name == self.check_name)
                .filter(|check_run| check_run.external_id.as_deref() == Some(external_id))
            {
                update_check_run_output(
                    &repo.client,
                    check_run.id,
                    conclusion,
                    &self.check_name,
                    summary,
                )
                .await?;
                resolved = true;
            }
        }

        if !resolved {
            tracing::warn!("No Henosis check run found for external id `{external_id}`");
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct CheckRunList {
    check_runs: Vec<CheckRunForResolution>,
}

#[derive(Debug, Deserialize)]
struct CheckRunForResolution {
    id: CheckRunId,
    name: String,
    external_id: Option<String>,
}

async fn list_check_runs_for_ref(
    client: &GithubRepositoryClient,
    r#ref: &str,
) -> anyhow::Result<Vec<CheckRunForResolution>> {
    let route = format!(
        "/repos/{}/commits/{}/check-runs?per_page=100",
        client.repository(),
        r#ref
    );
    let runs = client
        .client()
        .get::<CheckRunList, _, ()>(&route, None)
        .await
        .with_context(|| {
            format!(
                "Cannot list check runs for `{}` at `{ref}`",
                client.repository()
            )
        })?;
    Ok(runs.check_runs)
}

async fn update_check_run_output(
    client: &GithubRepositoryClient,
    check_run_id: CheckRunId,
    conclusion: CheckConclusion,
    title: &str,
    summary: &str,
) -> anyhow::Result<()> {
    client
        .client()
        .checks(client.repository().owner(), client.repository().name())
        .update_check_run(check_run_id)
        .status(CheckRunStatus::Completed)
        .conclusion(match conclusion {
            CheckConclusion::Success => CheckRunConclusion::Success,
            CheckConclusion::Failure => CheckRunConclusion::Failure,
            CheckConclusion::Neutral => CheckRunConclusion::Neutral,
        })
        .output(OctoCheckRunOutput {
            title: title.to_string(),
            summary: summary.to_string(),
            text: None,
            annotations: vec![],
            images: vec![],
        })
        .send()
        .await
        .with_context(|| {
            format!(
                "Cannot update check run `{}` in {}",
                check_run_id.0,
                client.repository()
            )
        })?;
    Ok(())
}

pub struct GithubPrCommenter<'a> {
    repositories: &'a RepositoryStore,
    db: &'a PgDbClient,
}

impl<'a> GithubPrCommenter<'a> {
    pub fn new(repositories: &'a RepositoryStore, db: &'a PgDbClient) -> Self {
        Self { repositories, db }
    }
}

impl PrCommenter for GithubPrCommenter<'_> {
    async fn post_comment(&self, pr: &QueuePullRequest, body: &str) -> anyhow::Result<()> {
        let repo_name: GithubRepoName =
            pr.key.repo.parse().map_err(|error| {
                anyhow::anyhow!("Invalid GitHub repo `{}`: {error}", pr.key.repo)
            })?;
        let repo = self
            .repositories
            .get(&repo_name)
            .with_context(|| format!("Repository `{repo_name}` is not loaded"))?;
        repo.client
            .post_comment(
                PullRequestNumber(pr.key.number),
                Comment::new(body.to_string()),
                self.db,
            )
            .await?;
        Ok(())
    }
}

pub struct GitHubMergeExecutor<'a> {
    pool: PgPool,
    repositories: &'a RepositoryStore,
    db: &'a PgDbClient,
    config: &'a HenosisConfig,
}

impl<'a> GitHubMergeExecutor<'a> {
    pub fn new(
        pool: PgPool,
        repositories: &'a RepositoryStore,
        db: &'a PgDbClient,
        config: &'a HenosisConfig,
    ) -> Self {
        Self {
            pool,
            repositories,
            db,
            config,
        }
    }
}

impl MergeExecutor for GitHubMergeExecutor<'_> {
    async fn execute(&self, gate_run: &GateRun) -> anyhow::Result<()> {
        let store = crate::henosis::db::PgQueueStore::new(self.pool.clone());
        let merger = GithubPullRequestMerger::new(self.repositories);
        let bumper = GithubDevManifestBumper::new(self.repositories, self.config);
        let commenter = GithubPrCommenter::new(self.repositories, self.db);
        StateMachineMergeExecutor::new(store, merger, bumper, commenter)
            .execute(gate_run)
            .await
    }
}

pub struct GithubPullRequestMerger<'a> {
    repositories: &'a RepositoryStore,
}

impl<'a> GithubPullRequestMerger<'a> {
    pub fn new(repositories: &'a RepositoryStore) -> Self {
        Self { repositories }
    }
}

impl PullRequestMerger for GithubPullRequestMerger<'_> {
    async fn squash_merge(&self, pr: &QueuePullRequest) -> anyhow::Result<String> {
        let repo_name: GithubRepoName =
            pr.key.repo.parse().map_err(|error| {
                anyhow::anyhow!("Invalid GitHub repo `{}`: {error}", pr.key.repo)
            })?;
        let repo = self
            .repositories
            .get(&repo_name)
            .with_context(|| format!("Repository `{repo_name}` is not loaded"))?;
        let sha = squash_merge_pull_request(&repo.client, PullRequestNumber(pr.key.number)).await?;
        Ok(sha.to_string())
    }
}

#[derive(Debug, Serialize)]
struct PullRequestMergeRequest<'a> {
    merge_method: &'a str,
}

#[derive(Debug, Deserialize)]
struct PullRequestMergeResponse {
    sha: String,
}

async fn squash_merge_pull_request(
    client: &GithubRepositoryClient,
    pr: PullRequestNumber,
) -> anyhow::Result<CommitSha> {
    let route = format!("/repos/{}/pulls/{}/merge", client.repository(), pr.0);
    let request = PullRequestMergeRequest {
        merge_method: "squash",
    };
    let response: PullRequestMergeResponse = client
        .client()
        .post(route, Some(&request))
        .await
        .with_context(|| format!("Cannot squash-merge PR {}#{}", client.repository(), pr.0))?;
    Ok(CommitSha(response.sha))
}

pub struct GithubDevManifestBumper<'a> {
    repositories: &'a RepositoryStore,
    config: &'a HenosisConfig,
}

impl<'a> GithubDevManifestBumper<'a> {
    pub fn new(repositories: &'a RepositoryStore, config: &'a HenosisConfig) -> Self {
        Self {
            repositories,
            config,
        }
    }
}

impl DevManifestBumper for GithubDevManifestBumper<'_> {
    async fn bump_dev_manifest(
        &self,
        gate_run: &GateRun,
        merge_commit_sha: &str,
    ) -> anyhow::Result<DevBump> {
        let deploy_repo: GithubRepoName = self.config.deploy_repo.parse().map_err(|error| {
            anyhow::anyhow!("Invalid deploy repo `{}`: {error}", self.config.deploy_repo)
        })?;
        let deploy_repo = self
            .repositories
            .get(&deploy_repo)
            .with_context(|| format!("Repository `{}` is not loaded", self.config.deploy_repo))?;

        let current = deploy_repo
            .client
            .read_file_at_ref(&self.config.dev_manifest_path, &self.config.manifest_branch)
            .await?;
        let mut manifest =
            manifest::parse_toml(&current.content).context("Cannot parse dev manifest")?;
        let digest = GithubImageDigestResolver::new(self.repositories);

        for component in gate_run
            .world
            .components
            .iter()
            .filter(|component| component.candidate)
        {
            let resolved_digest = digest
                .image_digest(&component.repo, merge_commit_sha)
                .await?
                .unwrap_or_else(|| synthetic_digest_for_ref(merge_commit_sha));
            manifest.components.insert(
                component.name.clone(),
                ComponentEntry::Pinned(PinnedEntry {
                    repo: component.repo.clone(),
                    r#ref: merge_commit_sha.to_string(),
                    digest: resolved_digest,
                }),
            );
        }

        let serialized = manifest::to_toml(&manifest).context("Cannot serialize dev manifest")?;
        let commit_sha = deploy_repo
            .client
            .write_file_to_branch(
                &self.config.dev_manifest_path,
                &self.config.manifest_branch,
                "Bump Henosis dev manifest",
                &serialized,
            )
            .await?;
        let commit_sha = commit_sha.to_string();

        Ok(DevBump {
            commit_url: format!(
                "https://github.com/{}/commit/{commit_sha}",
                self.config.deploy_repo
            ),
            commit_sha,
        })
    }
}

pub fn deploy_manifest_url(deploy_repo: &str, branch: &str, path: &str) -> String {
    format!("https://github.com/{deploy_repo}/blob/{branch}/{path}")
}

pub fn deploy_branch_url(deploy_repo: &str, branch: &str) -> String {
    format!("https://github.com/{deploy_repo}/tree/{branch}")
}
