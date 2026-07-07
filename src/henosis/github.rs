use anyhow::Context;
use octocrab::params::checks::CheckRunStatus;
use serde::Deserialize;

use crate::bors::RepositoryStore;
use crate::github::api::client::{CheckRunOutput, GithubRepositoryClient};
use crate::github::{CommitSha, GithubRepoName};
use crate::henosis::environment::{
    DeployRepoWriter, DeployWriteResult, DevLockfileReader, ImageDigestResolver,
};
use crate::henosis::graph::{ComponentPackageReader, PackageJson};
use crate::henosis::lockfile::{self, Lockfile};
use crate::henosis::queue::{GateCheckReporter, QueuePullRequest};

pub struct GithubDeployRepoWriter<'a> {
    client: &'a GithubRepositoryClient,
    lockfile_branch: String,
}

impl<'a> GithubDeployRepoWriter<'a> {
    pub fn new(client: &'a GithubRepositoryClient, lockfile_branch: impl Into<String>) -> Self {
        Self {
            client,
            lockfile_branch: lockfile_branch.into(),
        }
    }
}

impl DeployRepoWriter for GithubDeployRepoWriter<'_> {
    async fn write_lockfile(
        &mut self,
        path: &str,
        contents: &str,
    ) -> anyhow::Result<DeployWriteResult> {
        let commit = self
            .client
            .write_file_to_branch(
                path,
                &self.lockfile_branch,
                &format!("Update Henosis lockfile {path}"),
                contents,
            )
            .await?;
        Ok(DeployWriteResult {
            commit_sha: commit.to_string(),
        })
    }

    async fn delete_lockfile(&mut self, path: &str) -> anyhow::Result<()> {
        self.client
            .delete_file_from_branch(
                path,
                &self.lockfile_branch,
                &format!("Delete Henosis lockfile {path}"),
            )
            .await?;
        Ok(())
    }

    async fn create_branch(&mut self, branch: &str) -> anyhow::Result<()> {
        let base = self.client.get_branch_sha(&self.lockfile_branch).await?;
        match self.client.create_branch(branch, &base).await {
            Ok(()) => Ok(()),
            Err(error) if error.to_string().contains("Reference already exists") => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn delete_branch(&mut self, branch: &str) -> anyhow::Result<()> {
        self.client.delete_branch(branch).await
    }
}

pub struct GithubDevLockfileReader<'a> {
    client: &'a GithubRepositoryClient,
    lockfile_branch: String,
    dev_lockfile_path: String,
}

impl<'a> GithubDevLockfileReader<'a> {
    pub fn new(
        client: &'a GithubRepositoryClient,
        lockfile_branch: impl Into<String>,
        dev_lockfile_path: impl Into<String>,
    ) -> Self {
        Self {
            client,
            lockfile_branch: lockfile_branch.into(),
            dev_lockfile_path: dev_lockfile_path.into(),
        }
    }
}

impl DevLockfileReader for GithubDevLockfileReader<'_> {
    async fn read_dev_lockfile(&self) -> anyhow::Result<Lockfile> {
        let file = self
            .client
            .read_file_at_ref(&self.dev_lockfile_path, &self.lockfile_branch)
            .await?;
        lockfile::parse_toml(&file.content).context("Cannot parse dev lockfile")
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
}

pub fn deploy_lockfile_url(deploy_repo: &str, branch: &str, path: &str) -> String {
    format!("https://github.com/{deploy_repo}/blob/{branch}/{path}")
}

pub fn deploy_branch_url(deploy_repo: &str, branch: &str) -> String {
    format!("https://github.com/{deploy_repo}/tree/{branch}")
}
