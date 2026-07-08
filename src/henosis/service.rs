use crate::BorsContext;
use crate::bors::Comment;
use crate::bors::event::WorkflowRunCompleted;
use crate::database::WorkflowStatus;
use crate::github::{GithubRepoName, PullRequest, PullRequestNumber};
use crate::henosis::config::HenosisConfig;
use crate::henosis::db::{PgEnvironmentStore, PgQueueStore};
use crate::henosis::environment::{
    EnvironmentChange, EnvironmentManager, EnvironmentStatus, EnvironmentStore, PreviewPullRequest,
    PullRequestKey, RenderOutcome, RenderStatus, environment_branch,
};
use crate::henosis::gate::{CliGateExecutor, GateExecutor};
use crate::henosis::github::{
    GitHubMergeExecutor, GithubComponentPackageReader, GithubDeployRepoWriter,
    GithubDevManifestReader, GithubGateCheckReporter, GithubImageDigestResolver, GithubPrCommenter,
    GithubRepoValidationChecker, deploy_branch_url, deploy_manifest_url,
};
use crate::henosis::queue::{
    ADVISORY_FAILED_STATUS, ADVISORY_PASSED_STATUS, AdvisoryGateStore, CheckConclusion,
    GateCheckReporter, GateStatus, QueueManager, QueuePullRequest, QueueStore, RecordedGateRun,
    advisory_gate_external_id,
};
use crate::henosis::render_diagnostics::{
    fallback_render_failure_diagnostic, generate_render_failure_diagnostic, render_failure_comment,
};
use crate::henosis::status::{StatusSnapshot, render_status_section, upsert_status_section};
use anyhow::Context;
use std::collections::BTreeSet;

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
    let Some(pr) = preview_pull_request(config, repo, pr) else {
        return Ok(None);
    };

    let manager = EnvironmentManager::new(config.registered_components());
    let mut store = PgEnvironmentStore::new(ctx.db.pool().clone());
    let deploy_repo = deploy_repo(ctx, config)?;
    let mut writer = GithubDeployRepoWriter::new(&deploy_repo.client, &config.manifest_branch);
    let dev = GithubDevManifestReader::new(
        &deploy_repo.client,
        &config.manifest_branch,
        &config.dev_manifest_path,
    );
    let packages = GithubComponentPackageReader::new(&ctx.repositories);
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

    let manager = EnvironmentManager::new(config.registered_components());
    let mut store = PgEnvironmentStore::new(ctx.db.pool().clone());
    let deploy_repo = deploy_repo(ctx, config)?;
    let mut writer = GithubDeployRepoWriter::new(&deploy_repo.client, &config.manifest_branch);
    let dev = GithubDevManifestReader::new(
        &deploy_repo.client,
        &config.manifest_branch,
        &config.dev_manifest_path,
    );
    let packages = GithubComponentPackageReader::new(&ctx.repositories);
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

    let manager = EnvironmentManager::new(config.registered_components());
    let mut store = PgEnvironmentStore::new(ctx.db.pool().clone());
    let deploy_repo = deploy_repo(ctx, config)?;
    let mut writer = GithubDeployRepoWriter::new(&deploy_repo.client, &config.manifest_branch);
    let dev = GithubDevManifestReader::new(
        &deploy_repo.client,
        &config.manifest_branch,
        &config.dev_manifest_path,
    );
    let packages = GithubComponentPackageReader::new(&ctx.repositories);
    let digest = GithubImageDigestResolver::new(&ctx.repositories);

    let change = manager
        .refresh_pr(&mut store, &mut writer, &packages, &dev, &digest, pr)
        .await?;
    reconcile_status_for_change(ctx, &change).await?;
    Ok(Some(change))
}

pub async fn retire_preview_environment(
    ctx: &BorsContext,
    repo: &GithubRepoName,
    pr_number: PullRequestNumber,
) -> anyhow::Result<Option<EnvironmentChange>> {
    let Some(config) = ctx.henosis_config.as_ref() else {
        return Ok(None);
    };
    if !config.is_component_repo(&repo.to_string()) {
        return Ok(None);
    }

    let manager = EnvironmentManager::new(config.registered_components());
    let mut store = PgEnvironmentStore::new(ctx.db.pool().clone());
    let deploy_repo = deploy_repo(ctx, config)?;
    let mut writer = GithubDeployRepoWriter::new(&deploy_repo.client, &config.manifest_branch);
    let dev = GithubDevManifestReader::new(
        &deploy_repo.client,
        &config.manifest_branch,
        &config.dev_manifest_path,
    );
    let packages = GithubComponentPackageReader::new(&ctx.repositories);
    let digest = GithubImageDigestResolver::new(&ctx.repositories);
    let change = manager
        .retire_pr(
            &mut store,
            &mut writer,
            &packages,
            &dev,
            &digest,
            PullRequestKey::new(repo.to_string(), pr_number.0),
        )
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

    let manager = EnvironmentManager::new(config.registered_components());
    let mut store = PgEnvironmentStore::new(ctx.db.pool().clone());
    let deploy_repo = deploy_repo(ctx, config)?;
    let mut writer = GithubDeployRepoWriter::new(&deploy_repo.client, &config.manifest_branch);
    let dev = GithubDevManifestReader::new(
        &deploy_repo.client,
        &config.manifest_branch,
        &config.dev_manifest_path,
    );
    let packages = GithubComponentPackageReader::new(&ctx.repositories);
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
    let Some(pr) = preview_pull_request(config, repo, pr) else {
        return Ok(None);
    };

    let manager = EnvironmentManager::new(config.registered_components());
    let mut store = PgEnvironmentStore::new(ctx.db.pool().clone());
    let deploy_repo = deploy_repo(ctx, config)?;
    let mut writer = GithubDeployRepoWriter::new(&deploy_repo.client, &config.manifest_branch);
    let dev = GithubDevManifestReader::new(
        &deploy_repo.client,
        &config.manifest_branch,
        &config.dev_manifest_path,
    );
    let packages = GithubComponentPackageReader::new(&ctx.repositories);
    let digest = GithubImageDigestResolver::new(&ctx.repositories);

    let change = manager
        .leave(&mut store, &mut writer, &packages, &dev, &digest, pr)
        .await?;
    reconcile_status_for_change(ctx, &change).await?;
    Ok(Some(change))
}

pub async fn environment_status(
    ctx: &BorsContext,
    repo: &GithubRepoName,
    pr_number: PullRequestNumber,
) -> anyhow::Result<Option<EnvironmentStatus>> {
    let Some(config) = ctx.henosis_config.as_ref() else {
        return Ok(None);
    };
    if !config.is_component_repo(&repo.to_string()) {
        return Ok(None);
    }
    let manager = EnvironmentManager::new(config.registered_components());
    let store = PgEnvironmentStore::new(ctx.db.pool().clone());
    manager
        .status(&store, &PullRequestKey::new(repo.to_string(), pr_number.0))
        .await
}

pub async fn latest_gate_status(
    ctx: &BorsContext,
    repo: &GithubRepoName,
    pr_number: PullRequestNumber,
) -> anyhow::Result<Option<GateStatus>> {
    let Some(config) = ctx.henosis_config.as_ref() else {
        return Ok(None);
    };
    if !config.is_component_repo(&repo.to_string()) {
        return Ok(None);
    }
    let store = PgQueueStore::new(ctx.db.pool().clone());
    store
        .latest_gate_status(&PullRequestKey::new(repo.to_string(), pr_number.0))
        .await
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
    Ok(result)
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
                    &format!("Advisory gate could not run: {error:#}"),
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
    let environments = environment_store
        .active_preview_environments_for_commit_sha(&payload.commit_sha.0)
        .await?;
    for environment in environments {
        let outcome = RenderOutcome {
            environment_id: environment.id.clone(),
            commit_sha: payload.commit_sha.0.clone(),
            status,
            run_url: payload.url.clone(),
            excerpt: excerpt.clone(),
        };
        let previous = environment_store
            .latest_render_outcome(&environment.id)
            .await?;
        environment_store.record_render_outcome(&outcome).await?;
        if should_post_render_failure(&previous, &outcome) {
            for member in environment_store.active_members(&environment.id).await? {
                post_render_failure_comment(ctx, &member.key, &outcome).await?;
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
        };
        let previous = environment_store.latest_render_outcome("dev").await?;
        environment_store.record_render_outcome(&outcome).await?;
        if should_post_render_failure(&previous, &outcome) {
            for key in &merged_prs {
                post_render_failure_comment(ctx, key, &outcome).await?;
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
    let render = environment_store
        .latest_render_outcome(environment_id)
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
            manifest_url: deploy_manifest_url(
                &config.deploy_repo,
                &config.manifest_branch,
                &environment.manifest_path,
            ),
            branch_url: deploy_branch_url(
                &config.deploy_repo,
                &environment_branch(&environment.id),
            ),
            advisory_gate,
            gate,
            render: render.clone(),
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
    key: &PullRequestKey,
    outcome: &RenderOutcome,
) -> anyhow::Result<()> {
    let repo_name: GithubRepoName = key
        .repo
        .parse()
        .map_err(|error| anyhow::anyhow!("Invalid GitHub repo `{}`: {error}", key.repo))?;
    let repo = ctx
        .repositories
        .get(&repo_name)
        .with_context(|| format!("Repository `{repo_name}` is not loaded"))?;
    repo.client
        .post_comment(
            PullRequestNumber(key.number),
            Comment::new(render_failure_comment(outcome)),
            &ctx.db,
        )
        .await?;
    Ok(())
}

pub fn environment_change_comment(
    config: &HenosisConfig,
    change: &EnvironmentChange,
) -> Option<String> {
    let mut lines = Vec::new();
    for write in &change.written {
        lines.push(format!(
            "Preview environment `{}` is ready.\nManifest: <{}>\nBranch: <{}>\nMembers: {}",
            write.id,
            deploy_manifest_url(
                &config.deploy_repo,
                &config.manifest_branch,
                &write.manifest_path
            ),
            deploy_branch_url(&config.deploy_repo, &write.branch),
            member_list(&write.members),
        ));
    }
    for retired in &change.retired {
        lines.push(format!(
            "Preview environment `{}` was retired.\nManifest removed: `{}`\nBranch removed: `{}`",
            retired.id, retired.manifest_path, retired.branch
        ));
    }
    if lines.is_empty() {
        None
    } else {
        Some(lines.join("\n\n"))
    }
}

pub fn environment_status_comment(
    config: &HenosisConfig,
    status: Option<EnvironmentStatus>,
) -> String {
    match status {
        Some(status) => format!(
            "Current preview environment: `{}`\nManifest: <{}>\nBranch: <{}>\nMembers: {}",
            status.environment.id,
            deploy_manifest_url(
                &config.deploy_repo,
                &config.manifest_branch,
                &status.environment.manifest_path
            ),
            deploy_branch_url(&config.deploy_repo, &status.branch),
            member_list(&status.members),
        ),
        None => "This PR is not assigned to a Henosis preview environment.".to_string(),
    }
}

pub fn gate_status_comment(status: Option<GateStatus>) -> String {
    match status {
        Some(status) => {
            let mut body = format!(
                "Latest Henosis gate: `{}` is `{}`.",
                status.external_id, status.status
            );
            if let Some(diagnostic) = status.diagnostic {
                body.push_str("\n\n");
                body.push_str(&diagnostic);
            }
            body
        }
        None => "No Henosis gate run has been recorded for this PR.".to_string(),
    }
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

fn member_list(members: &[PreviewPullRequest]) -> String {
    if members.is_empty() {
        return "none".to_string();
    }
    members
        .iter()
        .map(|member| format!("{}#{}", member.key.repo, member.key.number))
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn preview_branch_for_environment(environment_id: &str) -> String {
    environment_branch(environment_id)
}
