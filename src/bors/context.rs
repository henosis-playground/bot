use std::sync::Arc;

use super::{RepositoryState, RepositoryStore};
use crate::bors::gitops::Git;
use crate::github::api::operations::CommitAuthor;
use crate::henosis::config::HenosisConfig;
use crate::{PgDbClient, bors::command::CommandParser, github::GithubRepoName};

pub struct BorsContext {
    pub parser: CommandParser,
    pub db: Arc<PgDbClient>,
    pub repositories: Arc<RepositoryStore>,
    pub henosis_config: Option<HenosisConfig>,
    pub commit_author: CommitAuthor,
    pub auto_build_check_run_name: String,
    pub try_build_check_run_name: String,
    pub merge_commit_message_prefix: String,
    pub service_name: String,
    git: Option<Git>,
    web_url: String,
}

impl BorsContext {
    pub fn new(
        parser: CommandParser,
        db: Arc<PgDbClient>,
        repositories: Arc<RepositoryStore>,
        git: Option<Git>,
        web_url: &str,
        henosis_config: Option<HenosisConfig>,
        commit_author: CommitAuthor,
        auto_build_check_run_name: String,
        try_build_check_run_name: String,
        merge_commit_message_prefix: String,
        service_name: String,
    ) -> Self {
        Self {
            parser,
            db,
            repositories,
            henosis_config,
            commit_author,
            auto_build_check_run_name,
            try_build_check_run_name,
            merge_commit_message_prefix,
            service_name,
            git,
            web_url: web_url.trim_end_matches('/').to_string(),
        }
    }

    /// Returns a URL where the bot's website is publicly accessible.
    pub fn get_web_url(&self) -> &str {
        &self.web_url
    }

    pub fn local_git_available(&self) -> bool {
        self.git.is_some()
    }

    pub fn get_git(&self) -> Option<Git> {
        self.git.clone()
    }

    pub fn get_repo(&self, name: &GithubRepoName) -> anyhow::Result<Arc<RepositoryState>> {
        let repo_state = match self.repositories.get(name) {
            Some(state) => state.clone(),
            None => {
                return Err(anyhow::anyhow!("Repository not found: {name}"));
            }
        };
        Ok(repo_state)
    }
}
