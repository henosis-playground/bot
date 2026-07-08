use crate::BorsContext;
use crate::github::{GithubRepoName, PullRequest, PullRequestNumber};
use crate::henosis::config::HenosisConfig;
use crate::henosis::db::{PgEnvironmentStore, PgQueueStore};
use crate::henosis::environment::{
    EnvironmentChange, EnvironmentManager, EnvironmentStatus, PreviewPullRequest, PullRequestKey,
    environment_branch,
};
use crate::henosis::gate::CliGateExecutor;
use crate::henosis::github::{
    GitHubMergeExecutor, GithubComponentPackageReader, GithubDeployRepoWriter,
    GithubDevManifestReader, GithubGateCheckReporter, GithubImageDigestResolver, GithubPrCommenter,
    deploy_branch_url, deploy_manifest_url,
};
use crate::henosis::queue::{
    GateStatus, QueueManager, QueuePullRequest, QueueStore, RecordedGateRun,
};

pub fn is_henosis_component_repo(ctx: &BorsContext, repo: &GithubRepoName) -> bool {
    ctx.henosis_config
        .as_ref()
        .map(|config| config.is_component_repo(&repo.to_string()))
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
    let change = manager
        .retire_pr(
            &mut store,
            &mut writer,
            PullRequestKey::new(repo.to_string(), pr_number.0),
        )
        .await?;
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
    manager
        .tick(
            &mut store,
            &dev,
            &reporter,
            &gate_executor,
            &merge_executor,
            &commenter,
        )
        .await
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
    manager
        .invalidate_pr_push(&mut store, &reporter, &pushed)
        .await
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
        Some(status) => format!(
            "Latest Henosis gate: `{}` is `{}`.",
            status.external_id, status.status
        ),
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
