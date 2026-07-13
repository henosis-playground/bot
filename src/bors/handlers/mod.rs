use std::sync::Arc;

use crate::bors::command::{BorsCommand, CommandParseError};
use crate::bors::event::{BorsGlobalEvent, BorsRepositoryEvent, PullRequestComment};
use crate::bors::handlers::autobuild::{command_cancel, command_retry};
use crate::bors::handlers::help::command_help;
use crate::bors::handlers::info::command_info;
use crate::bors::handlers::ping::command_ping;
use crate::bors::handlers::pr_events::{
    handle_pull_request_assigned, handle_pull_request_unassigned,
};
use crate::bors::handlers::refresh::{
    reload_mergeability_status, reload_repository_config, reload_repository_permissions,
};
use crate::bors::handlers::review::{
    TreeCloseArguments, command_approve, command_close_tree, command_open_tree, command_unapprove,
};
use crate::bors::handlers::trybuild::{command_try_build, command_try_cancel};
use crate::bors::handlers::workflow::{
    AutoBuildCancelReason, handle_workflow_completed, handle_workflow_started,
    maybe_cancel_auto_build,
};
use crate::bors::labels::handle_label_trigger;
use crate::bors::mergeability_queue::set_pr_mergeability_based_on_user_action;
use crate::bors::process::QueueSenders;
use crate::bors::{
    AUTO_BRANCH_NAME, BorsContext, CommandPrefix, Comment, PullRequestStatus, RepositoryState,
    TRY_BRANCH_NAME,
};
use crate::database::{DelegatedPermission, DelegationStatus, PullRequestModel};
use crate::github::{
    CommitSha, GithubUser, LabelTrigger, PullRequest, PullRequestInfo, PullRequestNumber,
};
use crate::henosis::service::{
    handle_render_workflow_completed, reconcile_dev_pin_after_component_push,
};
use crate::permissions::PermissionType;
use crate::{PgDbClient, TeamApiClient, load_repositories};
use anyhow::Context;
use futures::TryFutureExt;
use octocrab::Octocrab;
use pr_events::{
    handle_pull_request_closed, handle_pull_request_converted_to_draft, handle_pull_request_edited,
    handle_pull_request_merged, handle_pull_request_opened, handle_pull_request_ready_for_review,
    handle_pull_request_reopened, handle_push_to_branch, handle_push_to_pull_request,
};
use refresh::sync_pull_requests_state;
use review::{command_delegate, command_set_priority, command_set_rollup, command_undelegate};
use tracing::{Instrument, debug_span};

mod autobuild;
mod help;
mod info;
mod ping;
mod pr_events;
mod refresh;
mod review;
mod squash;
mod trybuild;
mod workflow;

fn command_failure_comment(error: &anyhow::Error) -> String {
    format!(":x: **Command failed.**\n\n```text\nerror: {error:#}\n```")
}

/// This function executes a single bors repository event
pub async fn handle_bors_repository_event(
    event: BorsRepositoryEvent,
    ctx: Arc<BorsContext>,
    senders: QueueSenders,
) -> anyhow::Result<()> {
    let db = Arc::clone(&ctx.db);
    let Some(repo) = ctx.repositories.get(event.repository()) else {
        return Err(anyhow::anyhow!(
            "Repository {} not found in the bot state",
            event.repository()
        ));
    };

    match event {
        BorsRepositoryEvent::Comment(comment) => {
            // We want to ignore comments made by this bot
            if repo.client.is_comment_internal(&comment).await? {
                tracing::trace!("Ignoring comment {comment:?} because it was authored by this bot");
                return Ok(());
            }

            // Also ignore comments made by homu
            if comment.author.username == "bors" {
                tracing::trace!("Ignoring comment {comment:?} because it was authored by homu");
                return Ok(());
            }

            let span = tracing::info_span!(
                "Comment",
                pr = format!("{}#{}", comment.repository, comment.pr_number),
                author = comment.author.username
            );
            let pr_number = comment.pr_number;
            if let Err(error) =
                handle_comment(Arc::clone(&repo), db.clone(), ctx, comment, &senders)
                    .instrument(span.clone())
                    .await
            {
                repo.client
                    .post_comment(
                        pr_number,
                        Comment::new(command_failure_comment(&error)),
                        &db,
                    )
                    .await
                    .context("Cannot send comment reacting to an error")?;

                return Err(error.context("Cannot perform command"));
            }
        }
        BorsRepositoryEvent::WorkflowStarted(payload) => {
            let span = tracing::info_span!(
                "Workflow started",
                repo = payload.repository.to_string(),
                id = payload.run_id.into_inner()
            );
            handle_workflow_started(repo, db, payload)
                .instrument(span.clone())
                .await?;
        }
        BorsRepositoryEvent::WorkflowCompleted(payload) => {
            let span = tracing::info_span!(
                "Workflow completed",
                repo = payload.repository.to_string(),
                id = payload.run_id.into_inner()
            );
            if handle_render_workflow_completed(&ctx, &payload)
                .instrument(span.clone())
                .await?
            {
                return Ok(());
            }
            handle_workflow_completed(repo, db, payload, senders.build_queue())
                .instrument(span.clone())
                .await?;
        }
        BorsRepositoryEvent::PullRequestEdited(payload) => {
            let span =
                tracing::info_span!("Pull request edited", repo = payload.repository.to_string());

            handle_pull_request_edited(repo, db, senders.mergeability_queue(), payload)
                .instrument(span.clone())
                .await?;
        }
        BorsRepositoryEvent::PullRequestCommitPushed(payload) => {
            let span =
                tracing::info_span!("Pull request pushed", repo = payload.repository.to_string());

            handle_push_to_pull_request(repo, db, ctx, &senders, payload)
                .instrument(span.clone())
                .await?;
        }
        BorsRepositoryEvent::PullRequestOpened(payload) => {
            let span =
                tracing::info_span!("Pull request opened", repo = payload.repository.to_string());

            handle_pull_request_opened(repo, db, ctx, &senders, payload)
                .instrument(span.clone())
                .await?;
        }
        BorsRepositoryEvent::PullRequestClosed(payload) => {
            let span =
                tracing::info_span!("Pull request closed", repo = payload.repository.to_string());

            handle_pull_request_closed(repo, db, ctx, payload)
                .instrument(span.clone())
                .await?;
        }
        BorsRepositoryEvent::PullRequestMerged(payload) => {
            let span =
                tracing::info_span!("Pull request merged", repo = payload.repository.to_string());

            handle_pull_request_merged(repo, db, ctx, payload)
                .instrument(span.clone())
                .await?;
        }
        BorsRepositoryEvent::PullRequestReopened(payload) => {
            let span = tracing::info_span!(
                "Pull request reopened",
                repo = payload.repository.to_string()
            );

            handle_pull_request_reopened(repo, db, ctx, senders.mergeability_queue(), payload)
                .instrument(span.clone())
                .await?;
        }
        BorsRepositoryEvent::PullRequestConvertedToDraft(payload) => {
            let span = tracing::info_span!(
                "Pull request converted to draft",
                repo = payload.repository.to_string()
            );

            handle_pull_request_converted_to_draft(repo, db, payload)
                .instrument(span.clone())
                .await?;
        }
        BorsRepositoryEvent::PullRequestAssigned(payload) => {
            let span = tracing::info_span!(
                "Pull request assigned",
                repo = payload.repository.to_string()
            );

            handle_pull_request_assigned(repo, db, payload)
                .instrument(span.clone())
                .await?;
        }
        BorsRepositoryEvent::PullRequestUnassigned(payload) => {
            let span = tracing::info_span!(
                "Pull request unassigned",
                repo = payload.repository.to_string()
            );

            handle_pull_request_unassigned(repo, db, payload)
                .instrument(span.clone())
                .await?;
        }
        BorsRepositoryEvent::PullRequestReadyForReview(payload) => {
            let span = tracing::info_span!(
                "Pull request ready for review",
                repo = payload.repository.to_string()
            );

            handle_pull_request_ready_for_review(repo, db, payload)
                .instrument(span.clone())
                .await?;
        }
        BorsRepositoryEvent::PushToBranch(payload) => {
            let span =
                tracing::info_span!("Pushed to branch", repo = payload.repository.to_string());

            reconcile_dev_pin_after_component_push(&ctx, &payload)
                .instrument(span.clone())
                .await?;
            handle_push_to_branch(repo, db, senders.mergeability_queue(), payload)
                .instrument(span.clone())
                .await?;
        }
    }
    Ok(())
}

/// This function executes a single BORS global event
pub async fn handle_bors_global_event(
    event: BorsGlobalEvent,
    ctx: Arc<BorsContext>,
    gh_client: &Octocrab,
    team_api_client: &TeamApiClient,
    senders: QueueSenders,
) -> anyhow::Result<()> {
    let db = Arc::clone(&ctx.db);
    match event {
        BorsGlobalEvent::InstallationsChanged => {
            let span = tracing::info_span!("Installations changed");
            reload_repos(ctx, gh_client, team_api_client)
                .instrument(span)
                .await?;
        }
        BorsGlobalEvent::RefreshConfig => {
            let span = tracing::info_span!("Refresh config");
            for_each_repo(&ctx, |repo| {
                let span = tracing::info_span!("Repo", "{}", repo.repository());
                reload_repository_config(repo).instrument(span)
            })
            .instrument(span)
            .await?;
        }
        BorsGlobalEvent::RefreshPermissions => {
            let span = tracing::info_span!("Refresh permissions");
            for_each_repo(&ctx, |repo| {
                let span = tracing::info_span!("Repo", "{}", repo.repository());
                reload_repository_permissions(repo, team_api_client).instrument(span)
            })
            .instrument(span)
            .await?;
        }
        BorsGlobalEvent::RefreshPendingBuilds => {
            let span = tracing::info_span!("Refresh pending builds");
            for_each_repo(&ctx, |repo| {
                senders
                    .build_queue()
                    .refresh_pending_builds(repo.repository().clone())
                    .instrument(span.clone())
                    .map_err(|e| e.into())
            })
            .instrument(span.clone())
            .await?;
        }
        BorsGlobalEvent::RefreshPullRequestMergeability => {
            let span = tracing::info_span!("Refresh PR mergeability status");
            for_each_repo(&ctx, |repo| {
                let span = tracing::info_span!("Repo", "{}", repo.repository());
                reload_mergeability_status(repo, &db, senders.mergeability_queue().clone())
                    .instrument(span)
            })
            .instrument(span)
            .await?;

            #[cfg(test)]
            crate::bors::WAIT_FOR_MERGEABILITY_STATUS_REFRESH.mark();
        }
        BorsGlobalEvent::RefreshPullRequestState => {
            let span = tracing::info_span!("Refresh PR status");
            for_each_repo(&ctx, |repo| {
                let subspan = tracing::info_span!("Repo", "{}", repo.repository());
                sync_pull_requests_state(repo, Arc::clone(&db), senders.mergeability_queue())
                    .instrument(subspan)
            })
            .instrument(span)
            .await?;

            #[cfg(test)]
            crate::bors::WAIT_FOR_PR_STATUS_REFRESH.mark();
        }
        BorsGlobalEvent::ProcessMergeQueue => {
            senders.merge_queue().maybe_perform_tick().await?;
        }
    }
    Ok(())
}

/// Perform an asynchronous operation created by `make_fut` for each repository in parallel.
async fn for_each_repo<MakeFut, Fut>(ctx: &BorsContext, make_fut: MakeFut) -> anyhow::Result<()>
where
    MakeFut: Fn(Arc<RepositoryState>) -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    let repos: Vec<Arc<RepositoryState>> = ctx.repositories.repositories();
    futures::future::join_all(repos.into_iter().map(make_fut))
        .await
        .into_iter()
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(())
}

#[derive(Copy, Clone)]
pub struct PullRequestData<'a> {
    pub github: &'a PullRequest,
    pub db: &'a PullRequestModel,
}

impl PullRequestData<'_> {
    pub fn number(&self) -> PullRequestNumber {
        self.db.number
    }
}

async fn handle_comment(
    repo: Arc<RepositoryState>,
    database: Arc<PgDbClient>,
    ctx: Arc<BorsContext>,
    comment: PullRequestComment,
    senders: &QueueSenders,
) -> anyhow::Result<()> {
    use std::fmt::Write;

    let pr_number = comment.pr_number;
    let commands = ctx.parser.parse_commands(&comment.text);

    // Bail if no commands
    if commands.is_empty() {
        return Ok(());
    }

    tracing::debug!("Commands: {commands:?}");
    tracing::trace!("Text: {}", comment.text);

    let pr_github = repo
        .client
        .get_pull_request(pr_number)
        .await
        .with_context(|| format!("Cannot get information about PR {pr_number}"))?;

    let mut pr_db = database
        .upsert_pull_request(repo.repository(), pr_github.clone().into())
        .await
        .with_context(|| format!("Cannot upsert PR {pr_number} into the database"))?;
    set_pr_mergeability_based_on_user_action(
        &database,
        &pr_github,
        &pr_db,
        senders.mergeability_queue(),
    )
    .await?;

    for (index, command) in commands.into_iter().enumerate() {
        match command {
            Ok(command) => {
                // Reload the PR state from DB, because a previous command might have changed it.
                if index > 0
                    && let Some(pr) = database
                        .get_pull_request(repo.repository(), pr_number)
                        .await?
                {
                    pr_db = pr;
                }

                let pr = PullRequestData {
                    github: &pr_github,
                    db: &pr_db,
                };

                let repo = Arc::clone(&repo);
                let database = Arc::clone(&database);
                let result = match command {
                    BorsCommand::Approve {
                        approver,
                        priority,
                        rollup,
                    } => {
                        let span = tracing::info_span!("Approve");
                        command_approve(
                            ctx.clone(),
                            repo,
                            database,
                            pr,
                            &comment.author,
                            &approver,
                            priority,
                            rollup,
                            senders.merge_queue(),
                        )
                        .instrument(span)
                        .await
                    }
                    BorsCommand::OpenTree => {
                        let span = tracing::info_span!("TreeOpen");
                        command_open_tree(
                            repo,
                            database,
                            pr,
                            &comment.author,
                            senders.merge_queue(),
                        )
                        .instrument(span)
                        .await
                    }
                    BorsCommand::TreeClosed { priority, reason } => {
                        let span = tracing::info_span!("TreeClosed");
                        command_close_tree(
                            repo,
                            database,
                            pr,
                            &comment.author,
                            TreeCloseArguments {
                                priority,
                                reason,
                                comment_url: &comment.html_url,
                            },
                            senders.merge_queue(),
                        )
                        .instrument(span)
                        .await
                    }
                    BorsCommand::Unapprove => {
                        let span = tracing::info_span!("Unapprove");
                        command_unapprove(repo, database, pr, &comment.author, &comment.html_url)
                            .instrument(span)
                            .await
                    }
                    BorsCommand::SetPriority(priority) => {
                        let span = tracing::info_span!("Priority");
                        command_set_priority(repo, database, pr, &comment.author, priority)
                            .instrument(span)
                            .await
                    }
                    BorsCommand::Delegate(cmd) => {
                        let span = tracing::info_span!("Delegate");
                        command_delegate(
                            repo,
                            database,
                            pr,
                            &comment.author,
                            cmd,
                            ctx.parser.prefix(),
                        )
                        .instrument(span)
                        .await
                    }
                    BorsCommand::Undelegate => {
                        let span = tracing::info_span!("Undelegate");
                        command_undelegate(repo, database, pr, &comment.author)
                            .instrument(span)
                            .await
                    }
                    BorsCommand::Help => {
                        let span = tracing::info_span!("Help");
                        command_help(repo, &ctx.db, pr.number())
                            .instrument(span)
                            .await
                    }
                    BorsCommand::Ping => {
                        let span = tracing::info_span!("Ping");
                        command_ping(repo, &ctx.db, pr.number(), &ctx.service_name)
                            .instrument(span)
                            .await
                    }
                    BorsCommand::Try { parent, jobs } => {
                        let span = tracing::info_span!("Try");
                        // we hard code the command prefix instead of using `ctx.parser.prefix()`
                        // because we are using the new bors for try builds, so we don't want to
                        // suggest using the `@bors2` prefix.
                        let command_prefix: CommandPrefix = "@bors".to_string().into();
                        command_try_build(
                            repo,
                            database,
                            pr,
                            &comment.author,
                            parent,
                            jobs,
                            trybuild::TryBuildOptions {
                                bot_prefix: &command_prefix,
                                commit_author: &ctx.commit_author,
                                check_run_name: &ctx.try_build_check_run_name,
                                merge_commit_message_prefix: &ctx.merge_commit_message_prefix,
                            },
                        )
                        .instrument(span)
                        .await
                    }
                    BorsCommand::TryCancel => {
                        let span = tracing::info_span!("Cancel try");
                        command_try_cancel(repo, database, pr, &comment.author)
                            .instrument(span)
                            .await
                    }
                    BorsCommand::Info => {
                        let span = tracing::info_span!("Info");
                        command_info(repo, pr, database).instrument(span).await
                    }
                    BorsCommand::EnvCreate => {
                        let span = tracing::info_span!("HenosisEnvCreate");
                        command_henosis_env_create(ctx.clone(), repo, database, pr, &comment.author)
                            .instrument(span)
                            .await
                    }
                    BorsCommand::EnvJoin { name } => {
                        let span = tracing::info_span!("HenosisEnvJoin");
                        command_henosis_env_join(
                            ctx.clone(),
                            repo,
                            database,
                            pr,
                            &comment.author,
                            &name,
                        )
                        .instrument(span)
                        .await
                    }
                    BorsCommand::EnvLeave => {
                        let span = tracing::info_span!("HenosisEnvLeave");
                        command_henosis_env_leave(ctx.clone(), repo, database, pr, &comment.author)
                            .instrument(span)
                            .await
                    }
                    BorsCommand::SetRollupMode(rollup) => {
                        let span = tracing::info_span!("Rollup");
                        command_set_rollup(repo, database, pr, &comment.author, rollup)
                            .instrument(span)
                            .await
                    }
                    BorsCommand::Retry => {
                        let span = tracing::info_span!("Retry");
                        command_retry(
                            repo,
                            database,
                            pr,
                            &comment.author,
                            senders.merge_queue(),
                            ctx.parser.prefix(),
                        )
                        .instrument(span)
                        .await
                    }
                    BorsCommand::Cancel => {
                        let span = tracing::info_span!("Cancel");
                        command_cancel(
                            repo,
                            database,
                            pr,
                            &comment.author,
                            ctx.parser.prefix(),
                            senders.merge_queue(),
                        )
                        .instrument(span)
                        .await
                    }
                    BorsCommand::Squash { commit_message } => {
                        let span = tracing::info_span!("Squash");
                        if ctx.local_git_available() {
                            squash::command_squash(
                                repo,
                                database,
                                pr,
                                &comment.author,
                                commit_message,
                                ctx.parser.prefix(),
                                senders.gitops_queue(),
                            )
                            .instrument(span)
                            .await
                        } else {
                            repo.client
                                .post_comment(
                                    pr_number,
                                    Comment::new(
                                        "`@bors squash` is not enabled in this bors instance."
                                            .to_string(),
                                    ),
                                    &ctx.db,
                                )
                                .instrument(span)
                                .await?;
                            Ok(())
                        }
                    }
                };
                if result.is_err() {
                    return result.context("Cannot execute Bors command");
                }
            }
            Err(error) => {
                let mut message = match error {
                    CommandParseError::MissingCommand => "Missing command.".to_string(),
                    CommandParseError::UnknownCommand(command) => {
                        format!(r#"Unknown command "{command}"."#)
                    }
                    CommandParseError::MissingArgValue { arg } => {
                        format!(r#"Unknown value for argument "{arg}"."#)
                    }
                    CommandParseError::UnknownArg { arg, did_you_mean } => {
                        format!(
                            r#"Unknown argument "{arg}". Did you mean to use `{} {did_you_mean}`?"#,
                            ctx.parser.prefix()
                        )
                    }
                    CommandParseError::DuplicateArg(arg) => {
                        format!(r#"Argument "{arg}" found multiple times."#)
                    }
                    CommandParseError::ValidationError(error) => {
                        format!("Invalid command: {error}.")
                    }
                    CommandParseError::UnclosedQuote => "Unclosed quote in argument.".to_string(),
                };
                let help_url = ctx.get_web_url();
                writeln!(
                    message,
                    " Run `{} help` or go to <{help_url}{}help> to see available commands.",
                    ctx.parser.prefix(),
                    if help_url.ends_with('/') { "" } else { "/" },
                )?;
                tracing::warn!("{}", message);
                repo.client
                    .post_comment(pr_github.number, Comment::new(message), &database)
                    .await
                    .context("Could not reply to PR comment")?;
            }
        }
    }
    Ok(())
}

async fn command_henosis_env_join(
    ctx: Arc<BorsContext>,
    repo: Arc<RepositoryState>,
    db: Arc<PgDbClient>,
    pr: PullRequestData<'_>,
    author: &GithubUser,
    name: &str,
) -> anyhow::Result<()> {
    if !has_permission(&repo, author, pr, PermissionType::Review).await? {
        deny_request(&repo, &db, pr.number(), author, PermissionType::Review).await?;
        return Ok(());
    }

    let Some(config) = ctx.henosis_config.as_ref() else {
        return post_henosis_not_configured(&repo, &db, pr.number()).await;
    };
    if !config.is_component_repo(&repo.repository().to_string()) {
        return post_henosis_not_managed(&repo, &db, pr.number()).await;
    }
    let Some(change) =
        crate::henosis::service::join_environment(&ctx, repo.repository(), pr.github, name).await?
    else {
        return post_henosis_not_managed(&repo, &db, pr.number()).await;
    };
    let _ = change;
    Ok(())
}

async fn command_henosis_env_create(
    ctx: Arc<BorsContext>,
    repo: Arc<RepositoryState>,
    db: Arc<PgDbClient>,
    pr: PullRequestData<'_>,
    author: &GithubUser,
) -> anyhow::Result<()> {
    if !has_permission(&repo, author, pr, PermissionType::Review).await? {
        deny_request(&repo, &db, pr.number(), author, PermissionType::Review).await?;
        return Ok(());
    }

    let Some(config) = ctx.henosis_config.as_ref() else {
        return post_henosis_not_configured(&repo, &db, pr.number()).await;
    };
    if !config.is_component_repo(&repo.repository().to_string()) {
        return post_henosis_not_managed(&repo, &db, pr.number()).await;
    }
    let Some(change) =
        crate::henosis::service::create_preview_environment(&ctx, repo.repository(), pr.github)
            .await?
    else {
        return post_henosis_not_managed(&repo, &db, pr.number()).await;
    };
    let _ = change;
    Ok(())
}

async fn command_henosis_env_leave(
    ctx: Arc<BorsContext>,
    repo: Arc<RepositoryState>,
    db: Arc<PgDbClient>,
    pr: PullRequestData<'_>,
    author: &GithubUser,
) -> anyhow::Result<()> {
    if !has_permission(&repo, author, pr, PermissionType::Review).await? {
        deny_request(&repo, &db, pr.number(), author, PermissionType::Review).await?;
        return Ok(());
    }

    let Some(config) = ctx.henosis_config.as_ref() else {
        return post_henosis_not_configured(&repo, &db, pr.number()).await;
    };
    if !config.is_component_repo(&repo.repository().to_string()) {
        return post_henosis_not_managed(&repo, &db, pr.number()).await;
    }
    let Some(change) =
        crate::henosis::service::leave_environment(&ctx, repo.repository(), pr.github).await?
    else {
        return post_henosis_not_managed(&repo, &db, pr.number()).await;
    };
    let _ = change;
    Ok(())
}

async fn post_henosis_not_configured(
    repo: &RepositoryState,
    db: &PgDbClient,
    pr_number: PullRequestNumber,
) -> anyhow::Result<()> {
    repo.client
        .post_comment(
            pr_number,
            Comment::new("Henosis is not configured for this bot instance.".to_string()),
            db,
        )
        .await?;
    Ok(())
}

async fn post_henosis_not_managed(
    repo: &RepositoryState,
    db: &PgDbClient,
    pr_number: PullRequestNumber,
) -> anyhow::Result<()> {
    repo.client
        .post_comment(
            pr_number,
            Comment::new("This repository is not managed by Henosis.".to_string()),
            db,
        )
        .await?;
    Ok(())
}

async fn reload_repos(
    ctx: Arc<BorsContext>,
    gh_client: &Octocrab,
    team_api_client: &TeamApiClient,
) -> anyhow::Result<()> {
    let reloaded_repos = load_repositories(gh_client, team_api_client).await?;
    for repo in ctx.repositories.repositories() {
        if !reloaded_repos.contains_key(repo.repository()) {
            tracing::warn!("Repository {} was not reloaded", repo.repository());
        }
    }
    for (name, repo) in reloaded_repos {
        let repo = match repo {
            Ok(repo) => repo,
            Err(error) => {
                tracing::error!("Failed to reload repository {name}: {error:?}");
                continue;
            }
        };

        if ctx.repositories.insert(repo) {
            tracing::info!("Repository {name} was added");
        } else {
            tracing::info!("Repository {name} was reloaded");
        }
    }
    Ok(())
}

/// Deny permission for a request.
async fn deny_request(
    repo: &RepositoryState,
    db: &PgDbClient,
    pr_number: PullRequestNumber,
    author: &GithubUser,
    permission_type: PermissionType,
) -> anyhow::Result<()> {
    tracing::warn!(
        "Permission denied for request command by {}",
        author.username
    );
    repo.client
        .post_comment(
            pr_number,
            Comment::new(format!(
                "@{}: :key: Insufficient privileges: not in {} users",
                author.username, permission_type
            )),
            db,
        )
        .await?;
    Ok(())
}

/// Check if a user has specified permission or has been delegated.
async fn has_permission(
    repo_state: &RepositoryState,
    user: &GithubUser,
    pr: PullRequestData<'_>,
    permission: PermissionType,
) -> anyhow::Result<bool> {
    if repo_state
        .permissions
        .load()
        .has_permission(user.id, permission.clone())
    {
        return Ok(true);
    }

    let is_delegated = match &pr.db.delegation {
        DelegationStatus::NotDelegated => false,
        DelegationStatus::Delegated(delegation) => {
            if delegation.delegatee() == *user.id {
                match delegation.permission() {
                    DelegatedPermission::Review => {
                        matches!(permission, PermissionType::Review | PermissionType::Try)
                    }
                    DelegatedPermission::Try => {
                        matches!(permission, PermissionType::Try)
                    }
                }
            } else {
                false
            }
        }
    };

    Ok(is_delegated)
}

pub struct InvalidationInfo {
    reason: InvalidationReason,
    /// URL to a comment that provides mores context about the invalidation.
    comment_url: Option<String>,
}

impl InvalidationInfo {
    pub fn new(reason: InvalidationReason) -> Self {
        Self {
            reason,
            comment_url: None,
        }
    }

    pub fn with_comment_url<C: Into<Option<String>>>(self, comment_url: C) -> Self {
        Self {
            comment_url: comment_url.into(),
            ..self
        }
    }
}

/// Why was a pull request invalidated?
#[derive(Debug, Clone)]
pub enum InvalidationReason {
    /// A new commit was pushed to the pull request.
    /// If it was approved, it will be unapproved.
    /// If it was contained in any rollups, they will be closed.
    CommitShaChanged,
    /// The pull request was closed.
    /// If it was approved, it will be unapproved.
    /// If it was contained in any rollups, they will be closed.
    Close,
    /// The pull request was unapproved, but its contents (HEAD SHA) should still be the same.
    Unapproval {
        /// Base SHA of the PR at the time of unapproval.
        base_sha: CommitSha,
        /// SHA that was previously approved before `@bors r-` was issued.
        previously_approved_sha: CommitSha,
    },
    /// A member of a rollup was invalidated.
    RollupMemberInvalidated {
        member: PullRequestNumber,
        reason: Box<InvalidationReason>,
    },
}

pub async fn unapprove_pr(
    repo_state: &RepositoryState,
    db: &PgDbClient,
    pr_db: &PullRequestModel,
    pr_gh: &PullRequestInfo,
) -> anyhow::Result<()> {
    db.unapprove(pr_db).await?;
    handle_label_trigger(repo_state, pr_gh, LabelTrigger::Unapproved).await?;
    Ok(())
}

pub struct InvalidationComment {
    /// Start of the invalidation comment
    base_text: String,
    /// Should the comment be sent even if no unapproval happened and thus only `base_text`
    /// would be sent?
    post_always: bool,
}

impl InvalidationComment {
    pub fn new(base_text: String) -> Self {
        Self {
            base_text,
            post_always: false,
        }
    }

    pub fn post_always(self) -> Self {
        Self {
            post_always: true,
            ..self
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct InvalidationOutcome {
    unapproved: bool,
    closed: bool,
}

pub struct MaybeInvalidatedRollup {
    number: PullRequestNumber,
    outcome: InvalidationOutcome,
}

/// In several situations, we need to "invalidate" a pull request, which amounts to doing several
/// things:
///
/// 1. Unapprove the PR if it was approved, and apply the corresponding unapproval label trigger.
/// 2. Cancel a running auto build, if there was any.
/// 3. "Recursively" invalidate all rollups containing the given pull request.
///   - If a rollup is invalidated due to a member PR changing its commit SHA, it will be closed.
///
/// This function does all that.
///
/// `comment` may contain the start of a comment that will be posted on the PR if invalidation
/// actually happened.
/// If nothing else would be added to the `comment` text, no comment will be sent!
pub async fn invalidate_pr(
    repo_state: &RepositoryState,
    db: &PgDbClient,
    pr_db: &PullRequestModel,
    pr_gh: &PullRequest,
    info: InvalidationInfo,
    comment: Option<InvalidationComment>,
) -> anyhow::Result<InvalidationOutcome> {
    // Step 1: unapprove the pull request if it was approved
    // This happens everytime the PR is invalidated, if it was approved before
    let pr_unapproved = if pr_db.is_approved() {
        unapprove_pr(repo_state, db, pr_db, &pr_gh.clone().into()).await?;
        true
    } else {
        false
    };

    fn get_cancel_reason(reason: &InvalidationReason) -> AutoBuildCancelReason {
        match reason {
            InvalidationReason::CommitShaChanged => AutoBuildCancelReason::PushToPR,
            InvalidationReason::Close => AutoBuildCancelReason::Close,
            InvalidationReason::Unapproval { .. } => AutoBuildCancelReason::Unapproval,
            InvalidationReason::RollupMemberInvalidated { reason, .. } => get_cancel_reason(reason),
        }
    }

    // Step 2: if there was a running auto build on the PR, cancel it
    let auto_build_cancel_message = maybe_cancel_auto_build(
        &repo_state.client,
        db,
        pr_db,
        get_cancel_reason(&info.reason),
    )
    .await?;

    // Step 3: close the rollup if its member was closed or received a new push
    // Note that we don't do this on `InvalidationReason::Close` itself, because that happens after
    // the PR has been closed already.
    let pr_closed = if let InvalidationReason::RollupMemberInvalidated { reason, .. } = &info.reason
        && let InvalidationReason::Close | InvalidationReason::CommitShaChanged = &**reason
        && matches!(
            pr_gh.status,
            PullRequestStatus::Open | PullRequestStatus::Draft
        ) {
        repo_state.client.close_pr(pr_gh.number).await?;
        true
    } else {
        matches!(info.reason, InvalidationReason::Close)
    };

    let comment_url = info.comment_url;

    // Step 4: recursively invalidate all open rollups containing this PR
    let invalidate_rollups = match info.reason {
        InvalidationReason::CommitShaChanged
        | InvalidationReason::Close
        | InvalidationReason::Unapproval { .. } => true,
        // We do not assume that rollups contain other rollups
        InvalidationReason::RollupMemberInvalidated { .. } => false,
    };
    let invalidated_rollups = if invalidate_rollups {
        let rollups = db
            .find_rollups_for_member_pr(pr_db)
            .await?
            .into_iter()
            // We do not deal with
            .filter(|rollup| match rollup.status {
                PullRequestStatus::Closed | PullRequestStatus::Merged => false,
                PullRequestStatus::Draft | PullRequestStatus::Open => true,
            })
            // Just to avoid weird edge cases, shouldn't happen ~ever
            .take(10)
            .collect::<Vec<_>>();

        let invalidate_rollup_futs = rollups.iter().map(|rollup_db| {
            let span =
                debug_span!("Invalidating rollup", rollup = rollup_db.number.0, reason = ?info.reason);
            let db = db.clone();
            let comment_url = comment_url.clone();
            let reason = info.reason.clone();
            async move {
                let rollup_pr = repo_state.client.get_pull_request(rollup_db.number).await?;
                let outcome = invalidate_pr(
                    repo_state,
                    &db,
                    rollup_db,
                    &rollup_pr,
                    InvalidationInfo::new(InvalidationReason::RollupMemberInvalidated {
                        member: pr_gh.number,
                        reason: Box::new(reason),
                    })
                        .with_comment_url(comment_url),
                    None,
                )
                    .await?;
                anyhow::Ok(MaybeInvalidatedRollup {
                    number: rollup_pr.number,
                    outcome
                })
            }
                .instrument(span)
        });

        let mut invalidated_rollups = vec![];
        for res in futures::future::join_all(invalidate_rollup_futs).await {
            match res {
                Ok(rollup) => {
                    invalidated_rollups.push(rollup);
                }
                Err(error) => {
                    tracing::error!("It was not possible to invalidate rollup: {error:?}");
                }
            }
        }
        invalidated_rollups
    } else {
        vec![]
    };

    let outcome = InvalidationOutcome {
        unapproved: pr_unapproved,
        closed: pr_closed,
    };
    if let Some(comment) = invalidation_comment(
        pr_db,
        auto_build_cancel_message,
        comment_url,
        comment,
        info.reason,
        invalidated_rollups,
        outcome,
    ) {
        repo_state
            .client
            .post_comment(pr_gh.number, comment, db)
            .await?;
    }

    Ok(outcome)
}

pub fn invalidation_comment(
    pr_db: &PullRequestModel,
    build_cancelled_msg: Option<String>,
    invalidation_comment_url: Option<String>,
    comment: Option<InvalidationComment>,
    reason: InvalidationReason,
    invalidated_rollups: Vec<MaybeInvalidatedRollup>,
    outcome: InvalidationOutcome,
) -> Option<Comment> {
    use itertools::Itertools;
    use std::fmt::Write;

    let mut invalidated_rollups: Vec<MaybeInvalidatedRollup> = invalidated_rollups
        .into_iter()
        .filter(|rollup| rollup.outcome.unapproved || rollup.outcome.closed)
        .collect();

    let mut msg = comment
        .as_ref()
        .map(|c| c.base_text.clone())
        .unwrap_or_default();

    let mut append = |fmt| {
        if !msg.is_empty() {
            msg.push_str("\n\n");
        }
        write!(msg, "{fmt}").unwrap();
    };

    // Rollup was invalidated
    let is_rollup = if let InvalidationReason::RollupMemberInvalidated { reason, member } = &reason
    {
        let wrap = |text: &str| {
            if let Some(comment_url) = invalidation_comment_url {
                format!("[{text}]({comment_url})")
            } else {
                text.to_string()
            }
        };

        let action = match &**reason {
            InvalidationReason::CommitShaChanged => format!("{} its commit SHA", wrap("changed")),
            InvalidationReason::Close => format!("was {}", wrap("closed")),
            InvalidationReason::Unapproval { .. } => format!("was {}", wrap("unapproved")),
            InvalidationReason::RollupMemberInvalidated { .. } => {
                format!("was {}", wrap("invalidated"))
            }
        };
        append(format!(
            "PR #{member}, which is a member of this rollup, {action}.",
        ));
        true
    } else {
        false
    };

    let pr_label = if is_rollup { "rollup" } else { "pull request" };
    // If we had an approved PR with a failed build, there's not much point in sending this warning
    let had_failed_build = pr_db
        .auto_build
        .as_ref()
        .map(|b| b.status.is_failure_or_cancel())
        .unwrap_or(false);

    if outcome.unapproved && (!had_failed_build || outcome.closed) {
        append(format!(
            "This {pr_label} was{} unapproved{}.",
            if is_rollup { " thus" } else { "" },
            if outcome.closed {
                " due to being closed"
            } else {
                ""
            }
        ));
    } else if is_rollup && outcome.closed {
        append("This rollup was closed.".to_string());
    }

    // Rollup member was invalidated
    if !invalidated_rollups.is_empty() {
        let action = |outcome: InvalidationOutcome| {
            if outcome.closed {
                "closed"
            } else if outcome.unapproved {
                "unapproved"
            } else {
                "invalidated"
            }
        };

        if let [rollup] = invalidated_rollups.as_slice() {
            append(format!(
                "This PR was contained in a rollup (#{}), which was {}.",
                rollup.number,
                action(rollup.outcome)
            ));
        } else {
            invalidated_rollups.sort_by_key(|pr| pr.number);
            append(format!(
                "This PR was contained in the following rollups:\n{}",
                invalidated_rollups
                    .into_iter()
                    .map(|rollup| format!("- #{} was {}", rollup.number, action(rollup.outcome)))
                    .join("\n")
            ));
        }
    }

    // Auto build was cancelled
    if let Some(cancel_msg) = build_cancelled_msg {
        append(cancel_msg.to_string());
    }

    // Add triagebot "View changes since this review" link
    if let InvalidationReason::Unapproval {
        previously_approved_sha,
        base_sha,
    } = reason
        && outcome.unapproved
    {
        append(format!(
            "[View changes since this unapproval](https://triagebot.infra.rust-lang.org/gh-changes-since/{}/{}/{}/{base_sha}..{previously_approved_sha})",
            pr_db.repository.owner(),
            pr_db.repository.name(),
            pr_db.number,
        ));
    }

    let post_always = comment.as_ref().map(|c| c.post_always).unwrap_or(false);

    // If we have nothing to post or the comment equals the base comment and post_always is false,
    // do not send any comment.
    if msg.is_empty() || (msg == comment.map(|c| c.base_text).unwrap_or_default() && !post_always) {
        None
    } else {
        Some(Comment::new(msg))
    }
}

/// Is this branch interesting for the bot?
fn is_bors_observed_branch(branch: &str) -> bool {
    branch == TRY_BRANCH_NAME || branch == AUTO_BRANCH_NAME
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::sync::{Arc, Mutex};

    use serde_json::{Value, json};
    use sha2::{Digest, Sha256};
    use tempfile::TempDir;
    use wiremock::matchers::{method, path_regex};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    use crate::bors::PullRequestStatus;
    use crate::database::WorkflowStatus;
    use crate::github::GithubRepoName;
    use crate::henosis::config::HenosisConfig;
    use crate::henosis::github::GithubPrCommenter;
    use crate::henosis::queue::{PrCommenter, QueuePullRequest};
    use crate::tests::{
        BorsBuilder, BorsTester, Comment, Commit, GitHub, Repo, User, default_repo_name, run_test,
    };

    #[test]
    fn command_failure_comment_includes_full_diagnostic_chain() {
        let error = anyhow::anyhow!(
            "Component-spec inspector returned unknown dependency 'service-e' for 'service-f'"
        )
        .context("Cannot collect preview component specs");

        assert_eq!(
            super::command_failure_comment(&error),
            ":x: **Command failed.**\n\n```text\nerror: Cannot collect preview component specs: Component-spec inspector returned unknown dependency 'service-e' for 'service-f'\n```"
        );
    }

    fn henosis_config() -> HenosisConfig {
        toml::from_str(
            r#"
deploy_repo = "rust-lang/borstest"
render_workflow_name = "Workflow1"
preview_mode = "auto"

[[components]]
name = "borstest"
repo = "rust-lang/borstest"

[[environments]]
id = "dev"
manifest_path = "dev.toml"
"#,
        )
        .unwrap()
    }

    fn henosis_config_on_demand() -> HenosisConfig {
        toml::from_str(
            r#"
deploy_repo = "rust-lang/borstest"
render_workflow_name = "Workflow1"
preview_mode = "on-demand"

[[components]]
name = "borstest"
repo = "rust-lang/borstest"

[[environments]]
id = "dev"
manifest_path = "dev.toml"
"#,
        )
        .unwrap()
    }

    const CORE_DEV_SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const CORE_PR_SHA: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const CORE_PUSH_SHA: &str = "cccccccccccccccccccccccccccccccccccccccc";

    fn henosis_core_config(endpoint: &str) -> HenosisConfig {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap().keep();
        let inspector = directory.join("inspect.py");
        std::fs::write(
            &inspector,
            r#"#!/usr/bin/env python3
import base64
import json
import sys
import tomllib

manifest_path = sys.argv[1]
output_path = sys.argv[sys.argv.index("--output") + 1]
with open(manifest_path, "rb") as source:
    manifest = tomllib.load(source)
components = {}
for name, pin in manifest["components"].items():
    context = {
        "apiVersion": "henosis.dev/k8s-component-context/v1",
        "environment": {"id": manifest["environment"]["id"]},
        "source": {"repository": pin["repo"], "revision": pin["ref"]},
        "image": {"digest": pin["digest"]},
    }
    components[name] = {
        "connector": "k8s",
        "outputsSchema": base64.b64encode(b'{"kind":"object"}').decode(),
        "connectorContext": base64.b64encode(json.dumps(context, separators=(",", ":")).encode()).decode(),
    }
with open(output_path, "w") as destination:
    json.dump({"apiVersion": "henosis.dev/component-spec-inspection/v1", "components": components}, destination)
"#,
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&inspector).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&inspector, permissions).unwrap();

        toml::from_str(&format!(
            r#"
deploy_repo = "rust-lang/borstest"
render_workflow_name = "Workflow1"
preview_mode = "on-demand"

[core_api]
endpoint = "{endpoint}"
token = "test-core-token"
component_spec_command = {inspector:?}

[[components]]
name = "borstest"
repo = "rust-lang/borstest"

[[environments]]
id = "dev"
manifest_path = "dev.toml"
"#,
        ))
        .unwrap()
    }

    fn henosis_core_github() -> GitHub {
        let github = GitHub::default();
        {
            let repo = github.default_repo();
            let mut repo = repo.lock();
            repo.set_file(
                "dev.toml",
                &format!(
                    r#"
[environment]
id = "dev"

[components.borstest]
repo = "rust-lang/borstest"
ref = "{CORE_DEV_SHA}"
digest = "sha256:{}"
"#,
                    "d".repeat(64)
                ),
            );
            repo.set_file(
                "henosis/package.json",
                r#"{"name":"@henosis/borstest","dependencies":{},"henosis":{"component":"borstest"}}"#,
            );
        }
        github
    }

    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    enum MockCoreReport {
        #[default]
        Pending,
        Ready,
        Failed,
    }

    #[derive(Debug, Default)]
    struct MockCoreState {
        graph: Option<Value>,
        specs: BTreeMap<String, Value>,
        retired: bool,
        report: MockCoreReport,
        calls: Vec<String>,
        requests: Vec<Value>,
    }

    impl MockCoreState {
        fn graph_id(&self) -> Option<&str> {
            self.graph
                .as_ref()
                .and_then(|graph| graph.get("id"))
                .and_then(Value::as_str)
        }

        fn generation(&self) -> u64 {
            self.graph
                .as_ref()
                .and_then(|graph| graph.get("generation"))
                .and_then(Value::as_str)
                .and_then(|generation| generation.parse().ok())
                .expect("mock graph generation")
        }

        fn durable(&self) -> Value {
            json!({
                "graph": self.graph.clone().expect("mock graph exists"),
                "lifecycle": if self.retired {
                    "GRAPH_LIFECYCLE_RETIRED"
                } else {
                    "GRAPH_LIFECYCLE_ACTIVE"
                }
            })
        }

        fn reports(&self) -> Vec<Value> {
            if self.report == MockCoreReport::Pending || self.retired {
                return Vec::new();
            }
            let graph = self.graph.as_ref().expect("mock graph exists");
            let hashes = graph["componentSpecHashes"]
                .as_array()
                .expect("mock component spec hashes");
            let failed_hash = hashes.first().and_then(Value::as_str);
            let dispositions = hashes
                .iter()
                .map(|hash| {
                    let hash = hash.as_str().expect("component spec hash");
                    json!({
                        "componentSpecHash": hash,
                        "kind": if self.report == MockCoreReport::Failed && Some(hash) == failed_hash {
                            "COMPONENT_DISPOSITION_KIND_FAILED"
                        } else {
                            "COMPONENT_DISPOSITION_KIND_READY"
                        }
                    })
                })
                .collect::<Vec<_>>();
            let diagnostics = if self.report == MockCoreReport::Failed {
                vec![json!({
                    "code": "k8s.render_failed",
                    "message": "renderer rejected the desired world",
                    "pointer": "/components/borstest",
                    "help": "fix the component declaration",
                    "severity": "DIAGNOSTIC_SEVERITY_ERROR"
                })]
            } else {
                Vec::new()
            };
            let publication = (self.report == MockCoreReport::Ready).then(|| {
                json!({
                    "revision": "ready",
                    "uri": "https://example.test/ready"
                })
            });
            vec![json!({
                "graphId": graph["id"],
                "generation": graph["generation"],
                "connector": "k8s",
                "dispositions": dispositions,
                "diagnostics": diagnostics,
                "publication": publication,
                "sequence": graph["generation"]
            })]
        }

        fn get_response(&self) -> Value {
            json!({
                "state": {
                    "durable": self.durable(),
                    "reports": self.reports()
                }
            })
        }
    }

    async fn mock_core() -> (MockServer, Arc<Mutex<MockCoreState>>) {
        let server = MockServer::start().await;
        let state = Arc::new(Mutex::new(MockCoreState::default()));
        let responder_state = state.clone();
        Mock::given(method("POST"))
            .and(path_regex(r"^/henosis\.v1\.GraphService/[A-Za-z]+$"))
            .respond_with(move |request: &Request| {
                assert_eq!(
                    request
                        .headers
                        .get("authorization")
                        .and_then(|value| value.to_str().ok()),
                    Some("Bearer test-core-token")
                );
                let method = request
                    .url
                    .path()
                    .rsplit('/')
                    .next()
                    .expect("GraphService method")
                    .to_string();
                let streaming = method == "WatchGraph";
                let body: Value = if streaming {
                    let length = u32::from_be_bytes(
                        request.body[1..5].try_into().expect("watch frame header"),
                    ) as usize;
                    serde_json::from_slice(&request.body[5..5 + length]).expect("watch request")
                } else {
                    request.body_json().expect("core JSON request")
                };
                let mut state = responder_state.lock().unwrap();
                state.calls.push(method.clone());
                state.requests.push(body.clone());

                match method.as_str() {
                    "RegisterComponentSpec" => {
                        let spec = body["spec"].clone();
                        let hash = base64::Engine::encode(
                            &base64::engine::general_purpose::STANDARD,
                            Sha256::digest(serde_json::to_vec(&spec).unwrap()),
                        );
                        state.specs.insert(hash.clone(), spec.clone());
                        ResponseTemplate::new(200).set_body_json(json!({
                            "component": {
                                "hash": hash,
                                "spec": spec
                            }
                        }))
                    }
                    "GetGraph" => {
                        if state.retired
                            || state.graph.is_none()
                            || state.graph_id() != body["graphId"].as_str()
                        {
                            ResponseTemplate::new(404).set_body_json(json!({
                                "code": "not_found",
                                "message": "graph does not exist"
                            }))
                        } else {
                            ResponseTemplate::new(200).set_body_json(state.get_response())
                        }
                    }
                    "GetGraphGeneration" => match state.graph.as_ref() {
                        Some(graph)
                            if !state.retired
                                && state.graph_id() == body["graphId"].as_str()
                                && body["generation"].as_str()
                                    == Some(state.generation().to_string()).as_deref() =>
                        {
                            let components = graph["componentSpecHashes"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .filter_map(|hash| {
                                    let hash = hash.as_str()?;
                                    Some(json!({
                                        "hash": hash,
                                        "spec": state.specs.get(hash)?
                                    }))
                                })
                                .collect::<Vec<_>>();
                            ResponseTemplate::new(200).set_body_json(json!({
                                "state": {
                                    "durable": state.durable(),
                                    "reports": state.reports()
                                },
                                "components": components,
                                "currentLifecycle": "GRAPH_LIFECYCLE_ACTIVE"
                            }))
                        }
                        _ => ResponseTemplate::new(404).set_body_json(json!({
                            "code": "not_found",
                            "message": "generation does not exist"
                        })),
                    },
                    "CreateGraph" => {
                        state.retired = false;
                        state.report = MockCoreReport::Pending;
                        state.graph = Some(json!({
                            "id": body["graphId"],
                            "generation": "1",
                            "componentSpecHashes": body["componentSpecHashes"]
                        }));
                        ResponseTemplate::new(200).set_body_json(json!({
                            "graph": state.graph
                        }))
                    }
                    "AddComponents" => {
                        let generation = state.generation() + 1;
                        let graph = state.graph.as_mut().expect("mock graph exists");
                        graph["generation"] = Value::String(generation.to_string());
                        graph["componentSpecHashes"].as_array_mut().unwrap().extend(
                            body["componentSpecHashes"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .cloned(),
                        );
                        ResponseTemplate::new(200).set_body_json(json!({"graph": graph}))
                    }
                    "UpdateComponents" => {
                        let generation = state.generation() + 1;
                        let graph = state.graph.as_mut().expect("mock graph exists");
                        graph["generation"] = Value::String(generation.to_string());
                        let hashes = graph["componentSpecHashes"].as_array_mut().unwrap();
                        for replacement in body["replacements"].as_array().unwrap() {
                            let current = replacement["currentSpecHash"].as_str();
                            if let Some(existing) =
                                hashes.iter_mut().find(|hash| hash.as_str() == current)
                            {
                                *existing = replacement["replacementSpecHash"].clone();
                            }
                        }
                        ResponseTemplate::new(200).set_body_json(json!({"graph": graph}))
                    }
                    "RemoveComponents" => {
                        let generation = state.generation() + 1;
                        let graph = state.graph.as_mut().expect("mock graph exists");
                        graph["generation"] = Value::String(generation.to_string());
                        let removed = body["componentSpecHashes"].as_array().unwrap();
                        graph["componentSpecHashes"]
                            .as_array_mut()
                            .unwrap()
                            .retain(|hash| !removed.iter().any(|removed| removed == hash));
                        ResponseTemplate::new(200).set_body_json(json!({"graph": graph}))
                    }
                    "RetireGraph" => {
                        state.retired = true;
                        ResponseTemplate::new(200).set_body_json(json!({
                            "graphId": body["graphId"],
                            "lastGeneration": body["expectedGeneration"]
                        }))
                    }
                    "WatchGraph" => {
                        let sequence = state.generation();
                        let snapshot = json!({
                            "snapshot": {
                                "sequence": sequence.to_string(),
                                "state": state.durable()
                            }
                        });
                        let status = json!({
                            "volatileStatus": {
                                "deliveredSequence": sequence.to_string(),
                                "reports": state.reports()
                            }
                        });
                        let mut frames = connect_frame(&snapshot);
                        frames.extend(connect_frame(&status));
                        ResponseTemplate::new(200)
                            .insert_header("content-type", "application/connect+json")
                            .set_body_bytes(frames)
                    }
                    other => panic!("unexpected GraphService call {other}"),
                }
            })
            .mount(&server)
            .await;
        (server, state)
    }

    fn connect_frame(value: &Value) -> Vec<u8> {
        let payload = serde_json::to_vec(value).unwrap();
        let mut frame = Vec::with_capacity(payload.len() + 5);
        frame.push(0);
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend(payload);
        frame
    }

    fn henosis_config_with_gate(gate_command: &str) -> HenosisConfig {
        let gate_command = gate_command.replace('\\', "\\\\").replace('"', "\\\"");
        toml::from_str(&format!(
            r#"
deploy_repo = "rust-lang/borstest"
render_workflow_name = "Workflow1"
preview_mode = "auto"
gate_command = "{gate_command}"

[[components]]
name = "borstest"
repo = "rust-lang/borstest"

[[environments]]
id = "dev"
manifest_path = "dev.toml"
"#,
        ))
        .unwrap()
    }

    fn passing_gate_script() -> (TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gate.sh");
        fs::write(
            &path,
            r#"#!/bin/sh
set -eu
output=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --output)
      output="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
test -n "$output"
printf '{"ok":true,"failures":[]}\n' > "$output/report.json"
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mut permissions = fs::metadata(&path).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&path, permissions).unwrap();
        }
        let path = path.to_string_lossy().to_string();
        (dir, path)
    }

    fn henosis_github() -> GitHub {
        let github = GitHub::default();
        {
            let repo = github.default_repo();
            let mut repo = repo.lock();
            repo.set_file(
                "dev.toml",
                r#"
[environment]
id = "dev"

[components.borstest]
repo = "rust-lang/borstest"
ref = "main-sha"
digest = "sha256:dev"
"#,
            );
            repo.set_file(
                "henosis/package.json",
                r#"{"name":"@henosis/borstest","dependencies":{},"henosis":{"component":"borstest"}}"#,
            );
        }
        github
    }

    fn environment_id_from_body(body: &str) -> String {
        body.lines()
            .find(|line| line.starts_with("| Environment |"))
            .and_then(|line| line.split('`').nth(1))
            .map(str::to_string)
            .expect("status body should include an environment id")
    }

    fn assert_no_status_section(body: &str) {
        assert!(!body.contains("<!-- henosis:status -->"));
        assert!(!body.contains("<!-- /henosis:status -->"));
    }

    fn preview_manifest_path(environment_id: &str) -> String {
        format!("{environment_id}.toml")
    }

    fn preview_branch_name(environment_id: &str) -> String {
        format!("env/{environment_id}")
    }

    fn deploy_has_manifest(ctx: &mut BorsTester, path: &str) -> bool {
        ctx.modify_repo(default_repo_name(), |repo| repo.get_file(path).is_some())
    }

    fn deploy_has_branch(ctx: &mut BorsTester, branch: &str) -> bool {
        ctx.modify_repo(default_repo_name(), |repo| {
            repo.get_branch_by_name(branch).is_some()
        })
    }

    async fn active_environment_for_pr(
        ctx: &BorsTester,
        repo: &GithubRepoName,
        pr_number: u64,
    ) -> anyhow::Result<Option<String>> {
        sqlx::query_scalar(
            r#"
SELECT e.id
FROM environment_member AS m
JOIN environment AS e ON e.id = m.environment_id
WHERE m.repo = $1
  AND m.pr_number = $2
  AND m.retired_at IS NULL
  AND e.retired_at IS NULL
ORDER BY m.created_at DESC, m.id DESC
LIMIT 1
"#,
        )
        .bind(repo.to_string())
        .bind(pr_number as i64)
        .fetch_optional(ctx.db().pool())
        .await
        .map_err(Into::into)
    }

    async fn assert_pr_has_no_active_environment(
        ctx: &BorsTester,
        repo: &GithubRepoName,
        pr_number: u64,
    ) -> anyhow::Result<()> {
        assert_eq!(active_environment_for_pr(ctx, repo, pr_number).await?, None);
        Ok(())
    }

    async fn active_member_count(ctx: &BorsTester, environment_id: &str) -> anyhow::Result<i64> {
        sqlx::query_scalar(
            r#"
SELECT COUNT(*)
FROM environment_member
WHERE environment_id = $1
  AND retired_at IS NULL
"#,
        )
        .bind(environment_id)
        .fetch_one(ctx.db().pool())
        .await
        .map_err(Into::into)
    }

    async fn assert_preview_retired(
        ctx: &mut BorsTester,
        environment_id: &str,
    ) -> anyhow::Result<()> {
        let retired: Option<bool> = sqlx::query_scalar(
            r#"
SELECT retired_at IS NOT NULL
FROM environment
WHERE id = $1
"#,
        )
        .bind(environment_id)
        .fetch_optional(ctx.db().pool())
        .await?;
        assert_eq!(retired, Some(true));
        assert_eq!(active_member_count(ctx, environment_id).await?, 0);
        assert!(!deploy_has_manifest(
            ctx,
            &preview_manifest_path(environment_id)
        ));
        assert!(!deploy_has_branch(
            ctx,
            &preview_branch_name(environment_id)
        ));
        Ok(())
    }

    async fn assert_preview_active(
        ctx: &mut BorsTester,
        environment_id: &str,
    ) -> anyhow::Result<()> {
        let retired: Option<bool> = sqlx::query_scalar(
            r#"
SELECT retired_at IS NOT NULL
FROM environment
WHERE id = $1
"#,
        )
        .bind(environment_id)
        .fetch_optional(ctx.db().pool())
        .await?;
        assert_eq!(retired, Some(false));
        assert!(deploy_has_manifest(
            ctx,
            &preview_manifest_path(environment_id)
        ));
        assert!(deploy_has_branch(ctx, &preview_branch_name(environment_id)));
        Ok(())
    }

    fn multi_henosis_config() -> HenosisConfig {
        toml::from_str(
            r#"
deploy_repo = "rust-lang/borstest"
render_workflow_name = "Workflow1"
preview_mode = "auto"

[[components]]
name = "borstest"
repo = "rust-lang/borstest"

[[components]]
name = "service-b"
repo = "rust-lang/service-b"

[[environments]]
id = "dev"
manifest_path = "dev.toml"
"#,
        )
        .unwrap()
    }

    fn multi_henosis_github() -> GitHub {
        let mut github = GitHub::default();
        let permissions = {
            let repo = github.default_repo();
            let mut repo = repo.lock();
            repo.set_file(
                "dev.toml",
                r#"
[environment]
id = "dev"

[components.borstest]
repo = "rust-lang/borstest"
ref = "main-sha"
digest = "sha256:borstest"

[components.service-b]
repo = "rust-lang/service-b"
ref = "main-sha"
digest = "sha256:service-b"
"#,
            );
            repo.set_file(
                "henosis/package.json",
                r#"{"name":"@henosis/borstest","dependencies":{},"henosis":{"component":"borstest"}}"#,
            );
            repo.permissions.clone()
        };

        let org = github.get_user("rust-lang").unwrap().clone();
        let mut service_b = Repo::new(org, "service-b");
        service_b.permissions = permissions;
        service_b.set_file(
            "henosis/package.json",
            r#"{"name":"@henosis/service-b","dependencies":{},"henosis":{"component":"service-b"}}"#,
        );
        github.add_repo(service_b);
        github
    }

    fn render_commit_from_comment(comment: &str) -> String {
        comment
            .split_once("for commit `")
            .and_then(|(_, rest)| rest.split_once('`'))
            .map(|(sha, _)| sha.to_string())
            .expect("render failure comment should include a commit sha")
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn ignore_bot_comment(pool: sqlx::PgPool) {
        run_test(pool, async |ctx: &mut BorsTester| {
            ctx.post_comment(Comment::from("@bors ping").with_author(User::bors_bot()))
                .await?;
            // Returning here will make sure that no comments were received
            Ok(())
        })
        .await;
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn do_not_load_pr_on_unrelated_comment(pool: sqlx::PgPool) {
        run_test(pool, async |ctx: &mut BorsTester| {
            ctx.modify_repo((), |repo| repo.pull_request_error = true);
            ctx.post_comment("no command").await?;
            Ok(())
        })
        .await;
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn unknown_command(pool: sqlx::PgPool) {
        run_test(pool, async |ctx: &mut BorsTester| {
            ctx.post_comment(Comment::from("@bors foo")).await?;
            insta::assert_snapshot!(ctx.get_next_comment_text(()).await?, @r#"Unknown command "foo". Run `@bors help` or go to <https://bors-test.com/help> to see available commands."#);
            Ok(())
        })
            .await;
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn henosis_component_main_push_reconciles_dev_pin(pool: sqlx::PgPool) {
        BorsBuilder::new(pool)
            .github(henosis_github())
            .henosis_config(henosis_config())
            .run_test(async |ctx: &mut BorsTester| {
                ctx.push_to_branch("main", Commit::new("new-main-sha", "direct push"))
                    .await?;

                let manifest = ctx.modify_repo(default_repo_name(), |repo| {
                    crate::henosis::manifest::parse_toml(
                        &repo
                            .get_file("dev.toml")
                            .expect("dev manifest should exist"),
                    )
                    .unwrap()
                });
                assert!(matches!(
                    manifest.components.get("borstest"),
                    Some(crate::henosis::manifest::ComponentEntry::Pinned(pin))
                        if pin.r#ref == "new-main-sha"
                            && pin.digest
                                == crate::henosis::manifest::synthetic_digest_for_ref(
                                    "new-main-sha"
                                )
                ));
                Ok(())
            })
            .await;
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn henosis_preview_open_writes_status_section(pool: sqlx::PgPool) {
        BorsBuilder::new(pool)
            .github(henosis_github())
            .henosis_config(henosis_config())
            .run_test(async |ctx: &mut BorsTester| {
                let pr = ctx.open_pr((), |_| {}).await?;
                let body = ctx
                    .pr((default_repo_name(), pr.number().0))
                    .await
                    .get_gh_pr()
                    .description;

                assert!(body.contains("<!-- henosis:status -->"));
                assert!(body.contains("<!-- /henosis:status -->"));
                assert!(body.contains("| Environment | `preview-"));
                assert!(body.contains(
                    "[manifest](https://github.com/rust-lang/borstest/blob/main/preview-"
                ));
                assert!(!body.contains("[branch]("));
                assert!(body.contains(
                    "[rust-lang/borstest#2](https://github.com/rust-lang/borstest/pull/2) (this PR)"
                ));
                assert!(body.contains("| Merge gate | :grey_question: none |"));
                assert!(body.contains("| Render | :grey_question: none |"));
                Ok(())
            })
            .await;
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn henosis_on_demand_waits_for_preview_command(pool: sqlx::PgPool) {
        BorsBuilder::new(pool)
            .github(henosis_github())
            .henosis_config(henosis_config_on_demand())
            .run_test(async |ctx: &mut BorsTester| {
                let pr = ctx.open_pr((), |_| {}).await?;
                let pr_id = (default_repo_name(), pr.number().0);
                assert_eq!(
                    ctx.pr(pr_id.clone()).await.get_gh_pr().description,
                    "Description of PR 2"
                );

                ctx.post_comment(Comment::new(pr_id.clone(), "@bors p+"))
                    .await?;

                let body = ctx.pr(pr_id.clone()).await.get_gh_pr().description;
                assert!(body.contains("<!-- henosis:status -->"));
                assert!(body.contains("| Environment | `preview-"));
                assert!(body.contains("| Render | :grey_question: none |"));
                let environment_id = environment_id_from_body(&body);
                assert_preview_active(ctx, &environment_id).await?;

                ctx.post_comment(Comment::new(pr_id.clone(), "@bors p-"))
                    .await?;

                let body = ctx.pr(pr_id).await.get_gh_pr().description;
                assert_eq!(body, "Description of PR 2");
                assert_no_status_section(&body);
                assert_preview_retired(ctx, &environment_id).await?;
                Ok(())
            })
            .await;
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn henosis_core_preview_lifecycle_and_status_cutover(pool: sqlx::PgPool) {
        let (core, core_state) = mock_core().await;
        BorsBuilder::new(pool)
            .github(henosis_core_github())
            .henosis_config(henosis_core_config(&core.uri()))
            .run_test(async move |ctx: &mut BorsTester| {
                let pr = ctx
                    .open_pr((), |pr| {
                        pr.reset_to_single_commit(Commit::from_sha(CORE_PR_SHA));
                    })
                    .await?;
                let pr_id = (default_repo_name(), pr.number().0);

                ctx.post_comment(Comment::new(pr_id.clone(), "@bors p+ shared-demo"))
                    .await?;
                let body = ctx.pr(pr_id.clone()).await.get_gh_pr().description;
                let first_environment_id = environment_id_from_body(&body);
                assert!(first_environment_id.starts_with("preview_"));
                assert_eq!(first_environment_id.len(), "preview_".len() + 26);
                assert!(body.contains("[graph](http://"));
                assert!(body.contains("| Render | :hourglass_flowing_sand: running"));
                assert!(!deploy_has_manifest(
                    ctx,
                    &preview_manifest_path(&first_environment_id)
                ));
                assert!(!deploy_has_branch(
                    ctx,
                    &preview_branch_name(&first_environment_id)
                ));

                {
                    let state = core_state.lock().unwrap();
                    let registration = state
                        .calls
                        .iter()
                        .zip(&state.requests)
                        .find(|(method, _)| method.as_str() == "RegisterComponentSpec")
                        .map(|(_, request)| request)
                        .expect("RegisterComponentSpec request");
                    let spec = &registration["spec"];
                    let context = base64::Engine::decode(
                        &base64::engine::general_purpose::STANDARD,
                        spec["connectorContext"].as_str().unwrap(),
                    )?;
                    let context: Value = serde_json::from_slice(&context)?;
                    assert_eq!(
                        context["apiVersion"],
                        "henosis.dev/k8s-component-context/v1"
                    );
                    assert_eq!(context["environment"]["id"], first_environment_id);
                    assert_eq!(context["source"]["repository"], "rust-lang/borstest");
                    assert_eq!(context["source"]["revision"], CORE_PR_SHA);
                    assert_eq!(spec["connector"], "k8s");
                    assert!(spec.get("revision").is_none());

                    let create = state
                        .calls
                        .iter()
                        .zip(&state.requests)
                        .find(|(method, _)| method.as_str() == "CreateGraph")
                        .map(|(_, request)| request)
                        .expect("CreateGraph request");
                    assert_eq!(create["componentSpecHashes"].as_array().unwrap().len(), 1);
                    assert_eq!(
                        base64::Engine::decode(
                            &base64::engine::general_purpose::STANDARD,
                            create["requestId"].as_str().unwrap(),
                        )?
                        .len(),
                        16
                    );
                }

                ctx.push_to_pr(pr_id.clone(), Commit::from_sha(CORE_PUSH_SHA))
                    .await?;
                {
                    let state = core_state.lock().unwrap();
                    let update = state
                        .calls
                        .iter()
                        .zip(&state.requests)
                        .rev()
                        .find(|(method, _)| method.as_str() == "UpdateComponents")
                        .map(|(_, request)| request)
                        .expect("UpdateComponents request");
                    assert_eq!(update["replacements"].as_array().unwrap().len(), 1);
                    let registration = state
                        .calls
                        .iter()
                        .zip(&state.requests)
                        .rev()
                        .find(|(method, _)| method.as_str() == "RegisterComponentSpec")
                        .map(|(_, request)| request)
                        .expect("latest RegisterComponentSpec request");
                    let context = base64::Engine::decode(
                        &base64::engine::general_purpose::STANDARD,
                        registration["spec"]["connectorContext"].as_str().unwrap(),
                    )?;
                    let context: Value = serde_json::from_slice(&context)?;
                    assert_eq!(context["source"]["revision"], CORE_PUSH_SHA);
                }

                core_state.lock().unwrap().report = MockCoreReport::Ready;
                assert_eq!(ctx.reconcile_henosis_core_now().await?, 1);
                let body = ctx.pr(pr_id.clone()).await.get_gh_pr().description;
                assert!(body.contains("| Render | :white_check_mark: passed"));

                core_state.lock().unwrap().report = MockCoreReport::Failed;
                assert_eq!(ctx.reconcile_henosis_core_now().await?, 1);
                let comment = ctx.get_next_comment_text(pr_id.clone()).await?;
                assert!(comment.contains("at graph generation `2`"));
                assert!(comment.contains("k8s.render_failed: renderer rejected the desired world"));
                assert!(comment.contains("help: fix the component declaration"));
                let body = ctx.pr(pr_id.clone()).await.get_gh_pr().description;
                assert!(body.contains("| Render | :x: failed"));
                assert!(!body.contains("renderer rejected the desired world"));

                core_state.lock().unwrap().report = MockCoreReport::Ready;
                assert_eq!(ctx.reconcile_henosis_core_now().await?, 1);
                let render_before_workflow = ctx.pr(pr_id.clone()).await.get_gh_pr().description;
                let comments_before_workflow =
                    ctx.modify_repo((), |repo| repo.get_pr(pr.number().0).comment_history_len());
                let run_id = ctx.create_workflow(default_repo_name(), "main");
                ctx.workflow_full_failure(run_id).await?;
                assert_eq!(
                    ctx.pr(pr_id.clone()).await.get_gh_pr().description,
                    render_before_workflow
                );
                assert_eq!(
                    ctx.modify_repo((), |repo| repo.get_pr(pr.number().0).comment_history_len()),
                    comments_before_workflow
                );

                ctx.post_comment(Comment::new(pr_id.clone(), "@bors p-"))
                    .await?;
                assert_eq!(
                    ctx.pr(pr_id.clone()).await.get_gh_pr().description,
                    "Description of PR 2"
                );

                ctx.post_comment(Comment::new(pr_id.clone(), "@bors p+ shared-demo"))
                    .await?;
                let second_environment_id =
                    environment_id_from_body(&ctx.pr(pr_id.clone()).await.get_gh_pr().description);
                assert!(second_environment_id.starts_with("preview_"));
                assert_ne!(second_environment_id, first_environment_id);

                ctx.set_pr_status_closed(pr_id.clone()).await?;
                assert_eq!(
                    ctx.pr(pr_id).await.get_gh_pr().description,
                    "Description of PR 2"
                );

                let calls = core_state.lock().unwrap().calls.clone();
                assert!(calls.iter().any(|call| call == "GetGraphGeneration"));
                let calls = calls
                    .into_iter()
                    .filter(|call| call != "GetGraphGeneration")
                    .collect::<Vec<_>>();
                assert_eq!(
                    calls,
                    vec![
                        "RegisterComponentSpec",
                        "GetGraph",
                        "CreateGraph",
                        "GetGraph",
                        "RegisterComponentSpec",
                        "GetGraph",
                        "UpdateComponents",
                        "GetGraph",
                        "WatchGraph",
                        "GetGraph",
                        "WatchGraph",
                        "GetGraph",
                        "WatchGraph",
                        "GetGraph",
                        "GetGraph",
                        "RetireGraph",
                        "RegisterComponentSpec",
                        "GetGraph",
                        "CreateGraph",
                        "GetGraph",
                        "GetGraph",
                        "RetireGraph",
                    ]
                );
                Ok(())
            })
            .await;
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn henosis_close_leaves_and_retires_solo_preview(pool: sqlx::PgPool) {
        BorsBuilder::new(pool)
            .github(henosis_github())
            .henosis_config(henosis_config_on_demand())
            .run_test(async |ctx: &mut BorsTester| {
                let pr = ctx.open_pr((), |_| {}).await?;
                let pr_id = (default_repo_name(), pr.number().0);

                ctx.post_comment(Comment::new(pr_id.clone(), "@bors p+"))
                    .await?;
                let body = ctx.pr(pr_id.clone()).await.get_gh_pr().description;
                let environment_id = environment_id_from_body(&body);
                assert_preview_active(ctx, &environment_id).await?;

                ctx.set_pr_status_closed(pr_id.clone()).await?;

                let body = ctx.pr(pr_id.clone()).await.get_gh_pr().description;
                assert_eq!(body, "Description of PR 2");
                assert_no_status_section(&body);
                assert_preview_retired(ctx, &environment_id).await?;
                assert_pr_has_no_active_environment(ctx, &default_repo_name(), pr.number().0)
                    .await?;
                Ok(())
            })
            .await;
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn henosis_merge_gate_merge_leaves_and_retires_preview(pool: sqlx::PgPool) {
        let (_gate_dir, gate_command) = passing_gate_script();
        BorsBuilder::new(pool)
            .github(henosis_github())
            .henosis_config(henosis_config_with_gate(&gate_command))
            .run_test(async |ctx: &mut BorsTester| {
                let pr = ctx.open_pr((), |_| {}).await?;
                let pr_id = (default_repo_name(), pr.number().0);
                let body = ctx.pr(pr_id.clone()).await.get_gh_pr().description;
                let environment_id = environment_id_from_body(&body);
                assert_preview_active(ctx, &environment_id).await?;

                ctx.approve(pr_id.clone()).await?;
                ctx.run_henosis_queue_now().await?.unwrap();
                let landed = ctx.get_next_comment_text(pr_id.clone()).await?;
                assert!(landed.contains("Landed as squash-pr-"));

                let body = ctx.pr(pr_id.clone()).await.get_gh_pr().description;
                assert_eq!(body, "Description of PR 2");
                assert_no_status_section(&body);
                assert_preview_retired(ctx, &environment_id).await?;
                assert_pr_has_no_active_environment(ctx, &default_repo_name(), pr.number().0)
                    .await?;
                ctx.pr(pr_id).await.expect_status(PullRequestStatus::Merged);
                Ok(())
            })
            .await;
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn henosis_gate_failure_comment_dedups_identical_body(pool: sqlx::PgPool) {
        BorsBuilder::new(pool)
            .github(henosis_github())
            .henosis_config(henosis_config())
            .run_test(async |ctx: &mut BorsTester| {
                let pr = ctx.open_pr((), |_| {}).await?;
                let pr_id = (default_repo_name(), pr.number().0);
                let head_sha = ctx.pr(pr_id.clone()).await.get_gh_pr().head_sha();
                let gate_pr = QueuePullRequest::new(
                    default_repo_name().to_string(),
                    pr.number().0,
                    "borstest",
                    head_sha.clone(),
                    format!("pr/{}", pr.number().0),
                    head_sha,
                );
                let bors = ctx.context();
                let commenter =
                    GithubPrCommenter::new(bors.repositories.as_ref(), bors.db.as_ref());
                let body = "**Henosis merge gate failed — this change breaks `service-b`.**\n\n```text\nerror: service-b consumes outputs from service-a that are incompatible with the resolved producer version.\n--> service-a outputs consumed by service-b: api (removed)\nnote: you pinned service-a @ ab3cf04; this environment resolved service-a @ bbb9ebf\n```";
                let same_break_at_merge_sha = "**Henosis merge gate failed — this change breaks `service-b`.**\n\n```text\nerror: service-b consumes outputs from service-a that are incompatible with the resolved producer version.\n--> service-a outputs consumed by service-b: api (removed)\nnote: you pinned service-a @ ab3cf04; this environment resolved service-a @ 451c7b5\n```";

                commenter.post_comment(&gate_pr, body).await?;
                commenter.post_comment(&gate_pr, body).await?;
                commenter
                    .post_comment(&gate_pr, same_break_at_merge_sha)
                    .await?;

                let comment_count =
                    ctx.modify_repo((), |repo| repo.get_pr(pr.number().0).comment_history_len());
                assert_eq!(comment_count, 1);
                assert_eq!(ctx.get_next_comment_text(pr_id).await?, body);
                Ok(())
            })
            .await;
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn henosis_render_failure_comments_and_updates_status(pool: sqlx::PgPool) {
        BorsBuilder::new(pool)
            .github(henosis_github())
            .henosis_config(henosis_config())
            .run_test(async |ctx: &mut BorsTester| {
                let pr = ctx.open_pr((), |_| {}).await?;
                let pr_id = (default_repo_name(), pr.number().0);
                let run_id = ctx.create_workflow(default_repo_name(), "main");
                ctx.modify_workflow(run_id, |workflow| {
                    workflow.add_job_with_log(
                        WorkflowStatus::Failure,
                        "Render dev",
                        "2026-07-08T10:00:00.000Z setup ok\n2026-07-08T10:00:01.000Z \u{1b}[31mRender dev\u{1b}[0m\n2026-07-08T10:00:02.000Z rendering manifest\n2026-07-08T10:00:03.000Z ##[error]missing DATABASE_URL\n2026-07-08T10:00:04.000Z Post job cleanup.",
                    );
                });

                ctx.workflow_full_failure(run_id).await?;

                let comment = ctx.get_next_comment_text(pr_id.clone()).await?;
                assert!(comment.contains("couldn't materialise environment `preview-"));
                assert!(comment.contains("<details><summary>render log</summary>"));
                assert!(comment.contains("```text\nrendering manifest\n##[error]missing DATABASE_URL\n```"));
                assert!(!comment.contains("Post job cleanup."));
                assert!(
                    comment.contains(
                        "[render run](https://github.com/rust-lang/borstest/actions/runs/"
                    )
                );

                let body = ctx.pr(pr_id).await.get_gh_pr().description;
                assert!(body.contains("| Render | :x: failed ([run](https://github.com/rust-lang/borstest/actions/runs/"));
                assert!(!body.contains("##[error]missing DATABASE_URL"));
                Ok(())
            })
            .await;
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn henosis_render_failure_comments_for_each_new_failing_commit(pool: sqlx::PgPool) {
        BorsBuilder::new(pool)
            .github(henosis_github())
            .henosis_config(henosis_config())
            .run_test(async |ctx: &mut BorsTester| {
                let pr = ctx.open_pr((), |_| {}).await?;
                let pr_id = (default_repo_name(), pr.number().0);

                let first_run_id = ctx.create_workflow(default_repo_name(), "main");
                ctx.modify_workflow(first_run_id, |workflow| {
                    workflow.add_job_with_log(
                        WorkflowStatus::Failure,
                        "Render dev",
                        "2026-07-08T10:00:00.000Z Render dev\n2026-07-08T10:00:01.000Z first context\n2026-07-08T10:00:02.000Z ##[error]first render break",
                    );
                });
                ctx.workflow_full_failure(first_run_id).await?;
                let first_comment = ctx.get_next_comment_text(pr_id.clone()).await?;
                assert!(first_comment.contains("##[error]first render break"));
                let first_render_commit = render_commit_from_comment(&first_comment);

                ctx.push_to_pr(pr_id.clone(), Commit::from_sha("pr-2-new-sha"))
                    .await?;
                let second_run_id = ctx.create_workflow(default_repo_name(), "main");
                ctx.modify_workflow(second_run_id, |workflow| {
                    workflow.add_job_with_log(
                        WorkflowStatus::Failure,
                        "Render dev",
                        "2026-07-08T10:01:00.000Z Render dev\n2026-07-08T10:01:01.000Z second context\n2026-07-08T10:01:02.000Z ##[error]second render break",
                    );
                });
                ctx.workflow_full_failure(second_run_id).await?;
                let second_comment = ctx.get_next_comment_text(pr_id.clone()).await?;
                assert!(second_comment.contains("##[error]second render break"));
                let second_render_commit = render_commit_from_comment(&second_comment);

                assert_ne!(first_render_commit, second_render_commit);
                let body = ctx.pr(pr_id).await.get_gh_pr().description;
                assert!(body.contains("actions/runs/2"));
                assert!(!body.contains("##[error]second render break"));
                Ok(())
            })
            .await;
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn henosis_join_rewrites_status_on_all_members(pool: sqlx::PgPool) {
        let service_b = GithubRepoName::new("rust-lang", "service-b");
        BorsBuilder::new(pool)
            .github(multi_henosis_github())
            .henosis_config(multi_henosis_config())
            .run_test(async |ctx: &mut BorsTester| {
                let service_a_pr = ctx.open_pr(default_repo_name(), |_| {}).await?;
                let service_b_pr = ctx.open_pr(service_b.clone(), |_| {}).await?;
                let service_a_id = (default_repo_name(), service_a_pr.number().0);
                let service_b_id = (service_b.clone(), service_b_pr.number().0);
                let environment_id = "preview-shared-demo";

                ctx.post_comment(Comment::new(service_a_id.clone(), "@bors p+ shared-demo"))
                    .await?;
                ctx.post_comment(Comment::new(service_b_id.clone(), "@bors p+ shared-demo"))
                .await?;

                let service_a_body = ctx.pr(service_a_id).await.get_gh_pr().description;
                let service_b_body = ctx.pr(service_b_id).await.get_gh_pr().description;
                assert!(service_a_body.contains(&format!("| Environment | `{environment_id}`")));
                assert!(service_b_body.contains(&format!("| Environment | `{environment_id}`")));
                assert!(
                    service_a_body
                        .contains("[rust-lang/borstest#2](https://github.com/rust-lang/borstest/pull/2) (this PR), [rust-lang/service-b#1](https://github.com/rust-lang/service-b/pull/1)")
                );
                assert!(
                    service_b_body
                        .contains("[rust-lang/borstest#2](https://github.com/rust-lang/borstest/pull/2), [rust-lang/service-b#1](https://github.com/rust-lang/service-b/pull/1) (this PR)")
                );
                Ok(())
            })
            .await;
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn henosis_shared_close_removes_member_and_updates_remaining_status(pool: sqlx::PgPool) {
        let service_b = GithubRepoName::new("rust-lang", "service-b");
        BorsBuilder::new(pool)
            .github(multi_henosis_github())
            .henosis_config(multi_henosis_config())
            .run_test(async |ctx: &mut BorsTester| {
                let service_a_pr = ctx.open_pr(default_repo_name(), |_| {}).await?;
                let service_b_pr = ctx.open_pr(service_b.clone(), |_| {}).await?;
                let service_a_id = (default_repo_name(), service_a_pr.number().0);
                let service_b_id = (service_b.clone(), service_b_pr.number().0);
                let environment_id = "preview-shared-demo";

                ctx.post_comment(Comment::new(service_a_id.clone(), "@bors p+ shared-demo"))
                    .await?;
                ctx.post_comment(Comment::new(service_b_id.clone(), "@bors p+ shared-demo"))
                    .await?;
                assert_preview_active(ctx, environment_id).await?;
                assert_eq!(active_member_count(ctx, environment_id).await?, 2);

                ctx.set_pr_status_closed(service_a_id.clone()).await?;

                let service_a_body = ctx.pr(service_a_id.clone()).await.get_gh_pr().description;
                assert_eq!(service_a_body, "Description of PR 2");
                assert_no_status_section(&service_a_body);
                assert_pr_has_no_active_environment(
                    ctx,
                    &default_repo_name(),
                    service_a_pr.number().0,
                )
                .await?;

                assert_preview_active(ctx, environment_id).await?;
                assert_eq!(active_member_count(ctx, environment_id).await?, 1);
                let service_b_body = ctx.pr(service_b_id).await.get_gh_pr().description;
                assert!(service_b_body.contains(&format!("| Environment | `{environment_id}`")));
                assert!(service_b_body.contains(
                    "[rust-lang/service-b#1](https://github.com/rust-lang/service-b/pull/1) (this PR)"
                ));
                assert!(!service_b_body.contains("rust-lang/borstest#2"));
                Ok(())
            })
            .await;
    }
}
