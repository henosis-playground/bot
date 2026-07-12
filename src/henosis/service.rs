use crate::BorsContext;
use crate::bors::Comment;
use crate::bors::event::WorkflowRunCompleted;
use crate::database::WorkflowStatus;
use crate::github::{GithubRepoName, PullRequest, PullRequestNumber};
use crate::henosis::config::{HenosisConfig, PreviewMode};
use crate::henosis::core_client::{
    CoreClient, CoreEnvironmentIdGenerator, CoreFailurePresentation, CoreGraphWriter,
};
use crate::henosis::db::{PgEnvironmentStore, PgQueueStore};
use crate::henosis::environment::{
    DeployRepoWriter, DeployWriteResult, EnvironmentChange, EnvironmentManager, EnvironmentStatus,
    EnvironmentStore, PreviewPullRequest, PullRequestKey, RenderOutcome, RenderStatus,
    environment_branch,
};
use crate::henosis::gate::{CliGateExecutor, GateExecutor};
use crate::henosis::github::{
    GitHubMergeExecutor, GithubComponentPackageReader, GithubDeployRepoWriter,
    GithubDevManifestReader, GithubGateCheckReporter, GithubImageDigestResolver, GithubPrCommenter,
    GithubRepoValidationChecker, deploy_manifest_url,
};
use crate::henosis::queue::{
    ADVISORY_FAILED_STATUS, ADVISORY_PASSED_STATUS, AdvisoryGateStore, CheckConclusion,
    GateCheckReporter, MERGED_STATUS, PrCommenter, QueueManager, QueuePullRequest, QueueStore,
    RecordedGateRun, advisory_gate_external_id,
};
use crate::henosis::render_diagnostics::{
    fallback_render_failure_diagnostic, generate_render_failure_diagnostic, render_failure_comment,
};
use crate::henosis::status::{
    StatusSnapshot, remove_status_section, render_status_section, upsert_status_section,
};
use anyhow::Context;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::LazyLock;
use tokio::sync::Notify;

static CORE_STATUS_WAKE: LazyLock<Notify> = LazyLock::new(Notify::new);

pub async fn wait_for_core_status_wake() {
    CORE_STATUS_WAKE.notified().await;
}

enum PreviewWriter<'a> {
    Deploy(GithubDeployRepoWriter<'a>),
    Core(CoreGraphWriter<'a, GithubComponentPackageReader<'a>>),
}

impl DeployRepoWriter for PreviewWriter<'_> {
    async fn write_manifest(
        &mut self,
        path: &str,
        contents: &str,
    ) -> anyhow::Result<DeployWriteResult> {
        match self {
            Self::Deploy(writer) => writer.write_manifest(path, contents).await,
            Self::Core(writer) => writer.write_manifest(path, contents).await,
        }
    }

    async fn delete_manifest(&mut self, path: &str) -> anyhow::Result<()> {
        match self {
            Self::Deploy(writer) => writer.delete_manifest(path).await,
            Self::Core(writer) => writer.delete_manifest(path).await,
        }
    }

    async fn create_branch(&mut self, branch: &str) -> anyhow::Result<()> {
        match self {
            Self::Deploy(writer) => writer.create_branch(branch).await,
            Self::Core(writer) => writer.create_branch(branch).await,
        }
    }

    async fn delete_branch(&mut self, branch: &str) -> anyhow::Result<()> {
        match self {
            Self::Deploy(writer) => writer.delete_branch(branch).await,
            Self::Core(writer) => writer.delete_branch(branch).await,
        }
    }
}

fn environment_manager(config: &HenosisConfig) -> EnvironmentManager {
    let components = config.registered_components();
    if config.core_api.is_some() {
        EnvironmentManager::with_core_previews(components, Arc::new(CoreEnvironmentIdGenerator))
    } else {
        EnvironmentManager::new(components)
    }
}

fn preview_writer<'a>(
    config: &HenosisConfig,
    deploy_repo: &'a crate::bors::RepositoryState,
    packages: &'a GithubComponentPackageReader<'a>,
) -> anyhow::Result<PreviewWriter<'a>> {
    match config.core_api.as_ref() {
        Some(core_api) => Ok(PreviewWriter::Core(CoreGraphWriter::new(
            core_api,
            config.registered_components(),
            packages,
        )?)),
        None => Ok(PreviewWriter::Deploy(GithubDeployRepoWriter::new(
            &deploy_repo.client,
            &config.manifest_branch,
        ))),
    }
}

pub fn is_henosis_component_repo(ctx: &BorsContext, repo: &GithubRepoName) -> bool {
    ctx.henosis_config
        .as_ref()
        .map(|config| config.is_component_repo(&repo.to_string()))
        .unwrap_or(false)
}

pub fn is_chained_component_repo(ctx: &BorsContext, repo: &GithubRepoName) -> bool {
    ctx.henosis_config
        .as_ref()
        .map(|config| config.is_chained_component_repo(&repo.to_string()))
        .unwrap_or(false)
}

pub async fn open_preview_environment(
    ctx: &BorsContext,
    repo: &GithubRepoName,
    pr: &PullRequest,
) -> anyhow::Result<Option<EnvironmentChange>> {
    let Some(config) = ctx.henosis_config.as_ref() else {
        return Ok(None);
    };
    if config.preview_mode == PreviewMode::OnDemand {
        return Ok(None);
    }
    create_preview_environment(ctx, repo, pr).await
}

pub async fn create_preview_environment(
    ctx: &BorsContext,
    repo: &GithubRepoName,
    pr: &PullRequest,
) -> anyhow::Result<Option<EnvironmentChange>> {
    let Some(config) = ctx.henosis_config.as_ref() else {
        return Ok(None);
    };
    let Some(pr) = preview_pull_request(config, repo, pr) else {
        return Ok(None);
    };
    let manager = environment_manager(config);
    let mut store = PgEnvironmentStore::new(ctx.db.pool().clone());
    let deploy_repo = deploy_repo(ctx, config)?;
    let packages = GithubComponentPackageReader::new(&ctx.repositories);
    let mut writer = preview_writer(config, &deploy_repo, &packages)?;
    let dev = GithubDevManifestReader::new(
        &deploy_repo.client,
        &config.manifest_branch,
        &config.dev_manifest_path,
    );
    let digest = GithubImageDigestResolver::new(&ctx.repositories);

    let change = manager
        .open_pr(&mut store, &mut writer, &packages, &dev, &digest, pr)
        .await?;
    reconcile_status_for_change(ctx, &change).await?;
    Ok(Some(change))
}

pub async fn reopen_preview_environment(
    ctx: &BorsContext,
    repo: &GithubRepoName,
    pr: &PullRequest,
) -> anyhow::Result<Option<EnvironmentChange>> {
    let Some(config) = ctx.henosis_config.as_ref() else {
        return Ok(None);
    };
    let Some(pr) = preview_pull_request(config, repo, pr) else {
        return Ok(None);
    };
    if config.preview_mode == PreviewMode::OnDemand {
        return Ok(None);
    }

    let manager = environment_manager(config);
    let mut store = PgEnvironmentStore::new(ctx.db.pool().clone());
    let deploy_repo = deploy_repo(ctx, config)?;
    let packages = GithubComponentPackageReader::new(&ctx.repositories);
    let mut writer = preview_writer(config, &deploy_repo, &packages)?;
    let dev = GithubDevManifestReader::new(
        &deploy_repo.client,
        &config.manifest_branch,
        &config.dev_manifest_path,
    );
    let digest = GithubImageDigestResolver::new(&ctx.repositories);

    let change = manager
        .reopen_pr(&mut store, &mut writer, &packages, &dev, &digest, pr)
        .await?;
    reconcile_status_for_change(ctx, &change).await?;
    Ok(Some(change))
}

pub async fn refresh_preview_environment(
    ctx: &BorsContext,
    repo: &GithubRepoName,
    pr: &PullRequest,
) -> anyhow::Result<Option<EnvironmentChange>> {
    let Some(config) = ctx.henosis_config.as_ref() else {
        return Ok(None);
    };
    let Some(pr) = preview_pull_request(config, repo, pr) else {
        return Ok(None);
    };

    let manager = environment_manager(config);
    let mut store = PgEnvironmentStore::new(ctx.db.pool().clone());
    if config.preview_mode == PreviewMode::OnDemand
        && store
            .environment_for_pr(&pr.key)
            .await
            .with_context(|| {
                format!(
                    "Cannot load environment for `{}`#{}",
                    pr.key.repo, pr.key.number
                )
            })?
            .is_none()
    {
        return Ok(None);
    }
    let deploy_repo = deploy_repo(ctx, config)?;
    let packages = GithubComponentPackageReader::new(&ctx.repositories);
    let mut writer = preview_writer(config, &deploy_repo, &packages)?;
    let dev = GithubDevManifestReader::new(
        &deploy_repo.client,
        &config.manifest_branch,
        &config.dev_manifest_path,
    );
    let digest = GithubImageDigestResolver::new(&ctx.repositories);

    let change = manager
        .refresh_pr(&mut store, &mut writer, &packages, &dev, &digest, pr)
        .await?;
    reconcile_status_for_change(ctx, &change).await?;
    Ok(Some(change))
}

pub async fn join_environment(
    ctx: &BorsContext,
    repo: &GithubRepoName,
    pr: &PullRequest,
    name: &str,
) -> anyhow::Result<Option<EnvironmentChange>> {
    let Some(config) = ctx.henosis_config.as_ref() else {
        return Ok(None);
    };
    let Some(pr) = preview_pull_request(config, repo, pr) else {
        return Ok(None);
    };

    let manager = environment_manager(config);
    let mut store = PgEnvironmentStore::new(ctx.db.pool().clone());
    let deploy_repo = deploy_repo(ctx, config)?;
    let packages = GithubComponentPackageReader::new(&ctx.repositories);
    let mut writer = preview_writer(config, &deploy_repo, &packages)?;
    let dev = GithubDevManifestReader::new(
        &deploy_repo.client,
        &config.manifest_branch,
        &config.dev_manifest_path,
    );
    let digest = GithubImageDigestResolver::new(&ctx.repositories);

    let change = manager
        .join(&mut store, &mut writer, &packages, &dev, &digest, pr, name)
        .await?;
    reconcile_status_for_change(ctx, &change).await?;
    Ok(Some(change))
}

pub async fn leave_environment(
    ctx: &BorsContext,
    repo: &GithubRepoName,
    pr: &PullRequest,
) -> anyhow::Result<Option<EnvironmentChange>> {
    let Some(config) = ctx.henosis_config.as_ref() else {
        return Ok(None);
    };
    let pr_number = pr.number;
    let Some(pr) = preview_pull_request(config, repo, pr) else {
        return Ok(None);
    };

    let manager = environment_manager(config);
    let mut store = PgEnvironmentStore::new(ctx.db.pool().clone());
    let deploy_repo = deploy_repo(ctx, config)?;
    let packages = GithubComponentPackageReader::new(&ctx.repositories);
    let mut writer = preview_writer(config, &deploy_repo, &packages)?;
    let dev = GithubDevManifestReader::new(
        &deploy_repo.client,
        &config.manifest_branch,
        &config.dev_manifest_path,
    );
    let digest = GithubImageDigestResolver::new(&ctx.repositories);

    let change = manager
        .leave(&mut store, &mut writer, &packages, &dev, &digest, pr)
        .await?;
    reconcile_status_for_change(ctx, &change).await?;
    clear_status_for_pr(ctx, repo, pr_number).await?;
    Ok(Some(change))
}

pub async fn tick_queue(ctx: &BorsContext) -> anyhow::Result<Option<RecordedGateRun>> {
    let Some(config) = ctx.henosis_config.as_ref() else {
        return Ok(None);
    };
    let manager = QueueManager::new(config.registered_components());
    let mut store = PgQueueStore::new(ctx.db.pool().clone());
    let deploy_repo = deploy_repo(ctx, config)?;
    let dev = GithubDevManifestReader::new(
        &deploy_repo.client,
        &config.manifest_branch,
        &config.dev_manifest_path,
    );
    let reporter = GithubGateCheckReporter::new(&ctx.repositories, &config.gate_check_run_name);
    let gate_dev = GithubDevManifestReader::new(
        &deploy_repo.client,
        &config.manifest_branch,
        &config.dev_manifest_path,
    );
    let gate_executor = CliGateExecutor::new(&config.gate_command, gate_dev);
    let merge_executor = GitHubMergeExecutor::new(
        ctx.db.pool().clone(),
        ctx.repositories.as_ref(),
        ctx.db.as_ref(),
        config,
    );
    let commenter = GithubPrCommenter::new(ctx.repositories.as_ref(), ctx.db.as_ref());
    let repo_validation = GithubRepoValidationChecker::new(ctx.repositories.as_ref());
    let result = manager
        .tick(
            &mut store,
            &dev,
            &reporter,
            &gate_executor,
            &merge_executor,
            &commenter,
            &repo_validation,
        )
        .await?;
    if let Some(gate_run) = &result {
        if gate_run_merged(&store, gate_run).await? {
            leave_environment_for_gate_members(ctx, &gate_run.world.members).await?;
        } else {
            reconcile_status_for_keys(
                ctx,
                gate_run
                    .world
                    .members
                    .iter()
                    .map(|member| member.key.clone())
                    .collect(),
            )
            .await?;
        }
    }
    Ok(result)
}

async fn gate_run_merged(store: &PgQueueStore, gate_run: &RecordedGateRun) -> anyhow::Result<bool> {
    let Some(member) = gate_run.world.members.first() else {
        return Ok(false);
    };
    Ok(store
        .latest_gate_status(&member.key)
        .await?
        .map(|status| status.status == MERGED_STATUS)
        .unwrap_or(false))
}

async fn leave_environment_for_gate_members(
    ctx: &BorsContext,
    members: &[QueuePullRequest],
) -> anyhow::Result<()> {
    for member in members {
        let repo_name: GithubRepoName = member.key.repo.parse().map_err(|error| {
            anyhow::anyhow!("Invalid GitHub repo `{}`: {error}", member.key.repo)
        })?;
        let repo = ctx
            .repositories
            .get(&repo_name)
            .with_context(|| format!("Repository `{repo_name}` is not loaded"))?;
        let pr = repo
            .client
            .get_pull_request(PullRequestNumber(member.key.number))
            .await?;
        leave_environment(ctx, &repo_name, &pr).await?;
    }
    Ok(())
}

pub async fn run_advisory_gate_on_approval(
    ctx: &BorsContext,
    repo: &GithubRepoName,
    pr: &PullRequest,
) -> anyhow::Result<bool> {
    let Some(config) = ctx.henosis_config.as_ref() else {
        return Ok(false);
    };
    let Some(component) = config.component_for_repo(&repo.to_string()) else {
        return Ok(false);
    };
    if !config.is_chained_component_repo(&repo.to_string()) {
        return Ok(false);
    }

    let advisory_pr = QueuePullRequest::with_base_branch(
        repo.to_string(),
        pr.number.0,
        component.name,
        pr.head.sha.to_string(),
        pr.head.name.clone(),
        pr.head.sha.to_string(),
        pr.base.name.clone(),
    );
    let external_id = advisory_gate_external_id(&advisory_pr);
    let deploy_repo = deploy_repo(ctx, config)?;
    let dev = GithubDevManifestReader::new(
        &deploy_repo.client,
        &config.manifest_branch,
        &config.dev_manifest_path,
    );
    let manager = QueueManager::new(config.registered_components());
    let world = manager
        .candidate_world(&dev, vec![advisory_pr.clone()])
        .await?;
    let gate_run = RecordedGateRun {
        id: 0,
        external_id: external_id.clone(),
        status: crate::henosis::queue::RUNNING_STATUS.to_string(),
        world,
        merge_commit_sha: None,
        dev_bump_commit_sha: None,
    };

    let reporter =
        GithubGateCheckReporter::new(&ctx.repositories, &config.advisory_gate_check_run_name);
    reporter
        .create_in_progress_check(&advisory_pr, &external_id)
        .await?;
    let gate_dev = GithubDevManifestReader::new(
        &deploy_repo.client,
        &config.manifest_branch,
        &config.dev_manifest_path,
    );
    let gate_executor = CliGateExecutor::new(&config.gate_command, gate_dev);
    let mut store = PgQueueStore::new(ctx.db.pool().clone());
    let commenter = GithubPrCommenter::new(ctx.repositories.as_ref(), ctx.db.as_ref());
    match gate_executor.execute(&gate_run).await {
        Ok(report) if report.ok => {
            store
                .record_advisory_gate_status(
                    &advisory_pr,
                    &external_id,
                    ADVISORY_PASSED_STATUS,
                    None,
                )
                .await?;
            reporter
                .resolve_check_run(
                    &external_id,
                    CheckConclusion::Success,
                    &report.check_run_summary(),
                )
                .await?;
        }
        Ok(report) => {
            let diagnostic = report.status_diagnostic();
            let comment = report.pr_comment();
            store
                .record_advisory_gate_status(
                    &advisory_pr,
                    &external_id,
                    ADVISORY_FAILED_STATUS,
                    diagnostic.as_deref(),
                )
                .await?;
            reporter
                .resolve_check_run(
                    &external_id,
                    CheckConclusion::Failure,
                    &report.check_run_summary(),
                )
                .await?;
            commenter.post_comment(&advisory_pr, &comment).await?;
        }
        Err(error) => {
            store
                .record_advisory_gate_status(
                    &advisory_pr,
                    &external_id,
                    ADVISORY_FAILED_STATUS,
                    None,
                )
                .await?;
            reporter
                .resolve_check_run(
                    &external_id,
                    CheckConclusion::Failure,
                    &format!("Advisory merge gate could not run: {error:#}"),
                )
                .await?;
            return Err(error);
        }
    }

    reconcile_status_for_pr(ctx, repo, pr.number).await?;
    Ok(true)
}

/// On startup, invalidate any gate runs left in transient states (pending, running).
/// These belong to a previous bot process that died mid-execution; they cannot be
/// safely resumed because the gate CLI may have been killed partway through.
pub async fn cleanup_stale_gate_runs(ctx: &BorsContext) -> anyhow::Result<u64> {
    if ctx.henosis_config.is_none() {
        return Ok(0);
    }
    let affected = sqlx::query(
        r#"
UPDATE gate_run
SET status = 'invalidated', updated_at = NOW()
WHERE status IN ('pending', 'pending-executor', 'running')
"#,
    )
    .execute(ctx.db.pool())
    .await?
    .rows_affected();
    if affected > 0 {
        tracing::warn!(
            count = affected,
            "Invalidated stale gate run(s) left by previous bot process"
        );
    }
    Ok(affected)
}

pub async fn on_pr_push(
    ctx: &BorsContext,
    repo: &GithubRepoName,
    pr: &PullRequest,
) -> anyhow::Result<bool> {
    let Some(config) = ctx.henosis_config.as_ref() else {
        return Ok(false);
    };
    let Some(component) = config.component_for_repo(&repo.to_string()) else {
        return Ok(false);
    };

    let pushed = QueuePullRequest::new(
        repo.to_string(),
        pr.number.0,
        component.name,
        pr.head.sha.to_string(),
        pr.head.name.clone(),
        pr.head.sha.to_string(),
    );
    let manager = QueueManager::new(config.registered_components());
    let mut store = PgQueueStore::new(ctx.db.pool().clone());
    let reporter = GithubGateCheckReporter::new(&ctx.repositories, &config.gate_check_run_name);
    let invalidated = manager
        .invalidate_pr_push(&mut store, &reporter, &pushed)
        .await?;
    if invalidated {
        return Ok(true);
    }

    if config.is_chained_component_repo(&repo.to_string()) {
        store.reenqueue_pr(&pushed).await?;
        return Ok(true);
    }

    Ok(false)
}

pub async fn reconcile_status_for_pr(
    ctx: &BorsContext,
    repo: &GithubRepoName,
    pr_number: PullRequestNumber,
) -> anyhow::Result<bool> {
    reconcile_status_for_keys(
        ctx,
        vec![PullRequestKey::new(repo.to_string(), pr_number.0)],
    )
    .await
}

pub async fn handle_render_workflow_completed(
    ctx: &BorsContext,
    payload: &WorkflowRunCompleted,
) -> anyhow::Result<bool> {
    let Some(config) = ctx.henosis_config.as_ref() else {
        return Ok(false);
    };
    if config.deploy_repo != payload.repository.to_string()
        || config.render_workflow_name != payload.name
        || config.manifest_branch != payload.branch
    {
        return Ok(false);
    }

    let status = match payload.status {
        WorkflowStatus::Success => RenderStatus::Success,
        WorkflowStatus::Failure => RenderStatus::Failure,
        WorkflowStatus::Pending => return Ok(true),
    };
    let excerpt = if status == RenderStatus::Failure {
        Some(render_failure_diagnostic(ctx, payload).await)
    } else {
        None
    };

    let environment_store = PgEnvironmentStore::new(ctx.db.pool().clone());
    let environments = if config.core_api.is_some() {
        Vec::new()
    } else {
        environment_store
            .active_preview_environments_for_commit_sha(&payload.commit_sha.0)
            .await?
    };
    for environment in environments {
        let outcome = RenderOutcome {
            environment_id: environment.id.clone(),
            commit_sha: payload.commit_sha.0.clone(),
            status,
            run_url: payload.url.clone(),
            excerpt: excerpt.clone(),
            generation: None,
            publication: None,
        };
        let previous = environment_store
            .latest_render_outcome(&environment.id)
            .await?;
        environment_store.record_render_outcome(&outcome).await?;
        if should_post_render_failure(&previous, &outcome) {
            for member in environment_store.active_members(&environment.id).await? {
                post_render_failure_comment(
                    ctx,
                    &environment_store,
                    &member.key,
                    &outcome,
                    "environment",
                )
                .await?;
            }
        }
        reconcile_environment_status(ctx, &environment.id).await?;
    }

    let queue_store = PgQueueStore::new(ctx.db.pool().clone());
    let merged_prs = queue_store
        .pull_requests_for_dev_bump_commit_sha(&payload.commit_sha.0)
        .await?;
    if !merged_prs.is_empty() {
        let outcome = RenderOutcome {
            environment_id: "dev".to_string(),
            commit_sha: payload.commit_sha.0.clone(),
            status,
            run_url: payload.url.clone(),
            excerpt,
            generation: None,
            publication: None,
        };
        let previous = environment_store.latest_render_outcome("dev").await?;
        environment_store.record_render_outcome(&outcome).await?;
        if should_post_render_failure(&previous, &outcome) {
            for key in &merged_prs {
                post_render_failure_comment(ctx, &environment_store, key, &outcome, "environment")
                    .await?;
            }
        }
        reconcile_status_for_keys(ctx, merged_prs).await?;
    }

    Ok(true)
}

async fn reconcile_status_for_change(
    ctx: &BorsContext,
    change: &EnvironmentChange,
) -> anyhow::Result<()> {
    let mut environment_ids = BTreeSet::new();
    for write in &change.written {
        environment_ids.insert(write.id.clone());
    }
    for environment_id in environment_ids {
        reconcile_environment_status(ctx, &environment_id).await?;
    }
    if ctx
        .henosis_config
        .as_ref()
        .is_some_and(|config| config.core_api.is_some())
    {
        CORE_STATUS_WAKE.notify_one();
    }
    Ok(())
}

async fn reconcile_status_for_keys(
    ctx: &BorsContext,
    keys: Vec<PullRequestKey>,
) -> anyhow::Result<bool> {
    let Some(config) = ctx.henosis_config.as_ref() else {
        return Ok(false);
    };
    let store = PgEnvironmentStore::new(ctx.db.pool().clone());
    let mut environment_ids = BTreeSet::new();
    for key in keys {
        if !config.is_component_repo(&key.repo) {
            continue;
        }
        if let Some(environment) = store.environment_for_pr(&key).await? {
            environment_ids.insert(environment.id);
        }
    }
    for environment_id in environment_ids {
        reconcile_environment_status(ctx, &environment_id).await?;
    }
    Ok(true)
}

async fn clear_status_for_pr(
    ctx: &BorsContext,
    repo_name: &GithubRepoName,
    pr_number: PullRequestNumber,
) -> anyhow::Result<()> {
    let repo = ctx
        .repositories
        .get(repo_name)
        .with_context(|| format!("Repository `{repo_name}` is not loaded"))?;
    let pr = repo.client.get_pull_request(pr_number).await?;
    let body = remove_status_section(&pr.message);
    if body != pr.message {
        repo.client
            .update_pull_request_body(pr_number, &body)
            .await?;
    }
    Ok(())
}

async fn reconcile_environment_status(
    ctx: &BorsContext,
    environment_id: &str,
) -> anyhow::Result<()> {
    let Some(config) = ctx.henosis_config.as_ref() else {
        return Ok(());
    };
    let environment_store = PgEnvironmentStore::new(ctx.db.pool().clone());
    let Some(environment) = environment_store.active_environment(environment_id).await? else {
        return Ok(());
    };
    let members = environment_store.active_members(environment_id).await?;
    let render = if let Some(core_api) = config.core_api.as_ref() {
        let client = CoreClient::new(core_api)?;
        match client.get_graph(environment_id).await? {
            Some(state) => {
                let status = client.graph_status(&state)?;
                let failure_presentations = status.failure_presentations.clone();
                let outcome = RenderOutcome {
                    environment_id: environment_id.to_string(),
                    commit_sha: format!("generation:{}", status.generation),
                    status: status.status,
                    run_url: client.generation_url(environment_id, status.generation),
                    excerpt: status.diagnostic,
                    generation: Some(status.generation),
                    publication: status.publication,
                };
                let outcome = outcome_for_desired_state(&environment, outcome, &client);
                record_core_render_outcome(
                    ctx,
                    &environment_store,
                    &outcome,
                    &failure_presentations,
                )
                .await?;
                Some(outcome)
            }
            None => None,
        }
    } else {
        environment_store
            .latest_render_outcome(environment_id)
            .await?
    };
    let last_publication = environment_store
        .latest_published_outcome(environment_id)
        .await?;
    let queue_store = PgQueueStore::new(ctx.db.pool().clone());

    for member in &members {
        let repo_name: GithubRepoName = member.key.repo.parse().map_err(|error| {
            anyhow::anyhow!("Invalid GitHub repo `{}`: {error}", member.key.repo)
        })?;
        let repo = ctx
            .repositories
            .get(&repo_name)
            .with_context(|| format!("Repository `{repo_name}` is not loaded"))?;
        let pr_number = PullRequestNumber(member.key.number);
        let pr = repo.client.get_pull_request(pr_number).await?;
        let gate = queue_store.latest_gate_status(&member.key).await?;
        let advisory_gate = queue_store.latest_advisory_gate_status(&member.key).await?;
        let snapshot = StatusSnapshot {
            environment: EnvironmentStatus {
                environment: environment.clone(),
                branch: environment_branch(&environment.id),
                members: members.clone(),
            },
            current_pr: member.key.clone(),
            manifest_url: deploy_manifest_url(
                &config.deploy_repo,
                &config.manifest_branch,
                &environment.manifest_path,
            ),
            graph_url: config
                .core_api
                .as_ref()
                .map(CoreClient::new)
                .transpose()?
                .map(|client| client.graph_url(&environment.id)),
            advisory_gate,
            gate,
            render: render.clone(),
            last_publication: last_publication.clone(),
        };
        let section = render_status_section(&snapshot);
        let body = upsert_status_section(&pr.message, &section);
        if body != pr.message {
            repo.client
                .update_pull_request_body(pr_number, &body)
                .await?;
        }
    }

    Ok(())
}

pub async fn reconcile_core_graphs(ctx: &BorsContext) -> anyhow::Result<usize> {
    let Some(config) = ctx.henosis_config.as_ref() else {
        return Ok(0);
    };
    let Some(core_api) = config.core_api.as_ref() else {
        return Ok(0);
    };
    let client = CoreClient::new(core_api)?;
    let store = PgEnvironmentStore::new(ctx.db.pool().clone());
    let environments = store.active_preview_environments().await?;
    for environment in &environments {
        let state = match client.watch_graph(&environment.id).await {
            Ok(state) => state,
            Err(_) => match client.get_graph(&environment.id).await? {
                Some(state) => state,
                None if environment
                    .desired_render_key
                    .as_deref()
                    .is_some_and(|key| key == "creating" || key.starts_with("pending:")) =>
                {
                    continue;
                }
                None => anyhow::bail!("Core graph `{}` disappeared", environment.id),
            },
        };
        let status = client.graph_status(&state)?;
        let failure_presentations = status.failure_presentations.clone();
        let outcome = RenderOutcome {
            environment_id: environment.id.clone(),
            commit_sha: format!("generation:{}", status.generation),
            status: status.status,
            run_url: client.generation_url(&environment.id, status.generation),
            excerpt: status.diagnostic,
            generation: Some(status.generation),
            publication: status.publication,
        };
        let outcome = outcome_for_desired_state(environment, outcome, &client);
        record_core_render_outcome(ctx, &store, &outcome, &failure_presentations).await?;
        reconcile_environment_status(ctx, &environment.id).await?;
    }
    Ok(environments.len())
}

fn outcome_for_desired_state(
    environment: &crate::henosis::environment::EnvironmentState,
    observed: RenderOutcome,
    client: &CoreClient,
) -> RenderOutcome {
    let Some(desired) = environment.desired_render_key.as_deref() else {
        return observed;
    };
    if desired == observed.commit_sha {
        return observed;
    }
    let generation = desired
        .strip_prefix("generation:")
        .and_then(|generation| generation.parse::<u64>().ok());
    RenderOutcome {
        environment_id: environment.id.clone(),
        commit_sha: desired.to_string(),
        status: RenderStatus::Pending,
        run_url: generation
            .map(|generation| client.generation_url(&environment.id, generation))
            .unwrap_or_else(|| client.graph_url(&environment.id)),
        excerpt: None,
        generation,
        publication: None,
    }
}

async fn record_core_render_outcome(
    ctx: &BorsContext,
    store: &PgEnvironmentStore,
    outcome: &RenderOutcome,
    failure_presentations: &[CoreFailurePresentation],
) -> anyhow::Result<()> {
    let previous = store.latest_render_outcome(&outcome.environment_id).await?;
    if previous.as_ref() == Some(outcome) {
        if outcome.status == RenderStatus::Success
            && let Some(generation) = outcome.generation
        {
            resolve_render_failure_comments(ctx, store, &outcome.environment_id, generation)
                .await?;
        }
        return Ok(());
    }
    store.record_render_outcome(outcome).await?;
    if should_post_render_failure(&previous, outcome) {
        for member in store.active_members(&outcome.environment_id).await? {
            if failure_presentations.is_empty() {
                post_render_failure_comment(ctx, store, &member.key, outcome, "environment")
                    .await?;
            } else {
                for presentation in failure_presentations {
                    let mut diagnostic_outcome = outcome.clone();
                    diagnostic_outcome.excerpt = Some(presentation.body.clone());
                    post_render_failure_comment(
                        ctx,
                        store,
                        &member.key,
                        &diagnostic_outcome,
                        &presentation.consumer,
                    )
                    .await?;
                }
            }
        }
    }
    if outcome.status == RenderStatus::Success
        && let Some(generation) = outcome.generation
    {
        resolve_render_failure_comments(ctx, store, &outcome.environment_id, generation).await?;
    }
    Ok(())
}

fn should_post_render_failure(previous: &Option<RenderOutcome>, outcome: &RenderOutcome) -> bool {
    outcome.status == RenderStatus::Failure
        && previous
            .as_ref()
            .map(|previous| {
                previous.status != RenderStatus::Failure
                    || previous.commit_sha != outcome.commit_sha
            })
            .unwrap_or(true)
}

async fn render_failure_diagnostic(ctx: &BorsContext, payload: &WorkflowRunCompleted) -> String {
    let Some(repo) = ctx.repositories.get(&payload.repository) else {
        return fallback_render_failure_diagnostic(payload);
    };
    match generate_render_failure_diagnostic(&repo.client, payload).await {
        Ok(diagnostic) => diagnostic,
        Err(error) => {
            tracing::warn!(
                "Could not build render failure diagnostic for workflow run {}: {error:?}",
                payload.run_id
            );
            fallback_render_failure_diagnostic(payload)
        }
    }
}

async fn post_render_failure_comment(
    ctx: &BorsContext,
    store: &PgEnvironmentStore,
    key: &PullRequestKey,
    outcome: &RenderOutcome,
    consumer: &str,
) -> anyhow::Result<()> {
    let repo_name: GithubRepoName = key
        .repo
        .parse()
        .map_err(|error| anyhow::anyhow!("Invalid GitHub repo `{}`: {error}", key.repo))?;
    let repo = ctx
        .repositories
        .get(&repo_name)
        .with_context(|| format!("Repository `{repo_name}` is not loaded"))?;
    let body = render_failure_comment(outcome);
    let comment = repo
        .client
        .post_comment(
            PullRequestNumber(key.number),
            Comment::new(body.clone()),
            &ctx.db,
        )
        .await?;
    store
        .record_render_comment(outcome, consumer, key, &comment.node_id, &body)
        .await?;
    Ok(())
}

async fn resolve_render_failure_comments(
    ctx: &BorsContext,
    store: &PgEnvironmentStore,
    environment_id: &str,
    resolved_generation: u64,
) -> anyhow::Result<()> {
    for comment in store.unresolved_render_comments(environment_id).await? {
        let repo_name: GithubRepoName = comment
            .repo
            .parse()
            .map_err(|error| anyhow::anyhow!("Invalid GitHub repo `{}`: {error}", comment.repo))?;
        let repo = ctx
            .repositories
            .get(&repo_name)
            .with_context(|| format!("Repository `{repo_name}` is not loaded"))?;
        let body = format!(
            "✅ **Resolved in generation {resolved_generation}.**\n\n<details><summary>Earlier generation {} diagnostic for {}</summary>\n\n{}\n\n</details>",
            comment.generation, comment.consumer, comment.original_body
        );
        repo.client
            .update_comment_content(&comment.node_id, &body)
            .await?;
        store
            .mark_render_comment_resolved(environment_id, &comment.node_id, resolved_generation)
            .await?;
    }
    Ok(())
}

fn preview_pull_request(
    config: &HenosisConfig,
    repo: &GithubRepoName,
    pr: &PullRequest,
) -> Option<PreviewPullRequest> {
    let component = config.component_for_repo(&repo.to_string())?;
    Some(PreviewPullRequest::new(
        repo.to_string(),
        pr.number.0,
        component.name,
        pr.head.name.clone(),
        pr.head.sha.to_string(),
    ))
}

fn deploy_repo(
    ctx: &BorsContext,
    config: &HenosisConfig,
) -> anyhow::Result<std::sync::Arc<crate::bors::RepositoryState>> {
    let deploy_repo: GithubRepoName = config.deploy_repo.parse().map_err(|error| {
        anyhow::anyhow!("Invalid deploy repo `{}`: {error}", config.deploy_repo)
    })?;
    ctx.get_repo(&deploy_repo)
}

pub fn preview_branch_for_environment(environment_id: &str) -> String {
    environment_branch(environment_id)
}

#[cfg(test)]
mod desired_state_tests {
    use super::*;
    use crate::henosis::config::CoreApiConfig;
    use crate::henosis::environment::EnvironmentState;

    fn client() -> CoreClient {
        let config: CoreApiConfig = toml::from_str(
            r#"
endpoint = "http://core:8080"
presentation_endpoint = "https://henosis.example"
token = "test-token"
"#,
        )
        .unwrap();
        CoreClient::new(&config).unwrap()
    }

    #[test]
    fn membership_and_head_changes_never_reuse_old_green() {
        for change in ["join", "leave", "close", "simultaneous-push-and-leave"] {
            let environment = EnvironmentState {
                id: format!("preview_{change}"),
                manifest_path: format!("preview_{change}.toml"),
                is_preview: true,
                display_label: None,
                desired_render_key: Some("generation:5".to_string()),
            };
            let outcome = outcome_for_desired_state(
                &environment,
                RenderOutcome {
                    environment_id: environment.id.clone(),
                    commit_sha: "generation:4".to_string(),
                    status: RenderStatus::Success,
                    run_url: "https://henosis.example/old".to_string(),
                    excerpt: None,
                    generation: Some(4),
                    publication: None,
                },
                &client(),
            );
            assert_eq!(outcome.status, RenderStatus::Pending, "{change}");
            assert_eq!(outcome.generation, Some(5), "{change}");
            assert!(outcome.run_url.ends_with("/generations/5"), "{change}");
        }
    }
}
