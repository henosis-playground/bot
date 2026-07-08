use octocrab::models::workflows::{Conclusion, Job, Status, Step};
use regex::Regex;
use std::sync::LazyLock;

use crate::bors::event::WorkflowRunCompleted;
use crate::github::api::client::GithubRepositoryClient;
use crate::henosis::environment::RenderOutcome;

const LOG_EXCERPT_LINES: usize = 30;
const LOG_LINE_LIMIT: usize = 600;

static ANSI_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\x1b\[[0-?]*[ -/]*[@-~]").unwrap());
static TIMESTAMP_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\s*").unwrap());

pub async fn generate_render_failure_diagnostic(
    client: &GithubRepositoryClient,
    payload: &WorkflowRunCompleted,
) -> anyhow::Result<String> {
    let jobs = client.get_jobs_for_workflow_run(payload.run_id).await?;
    let Some(job) = jobs.iter().find(|job| job_failed(job)) else {
        return Ok(fallback_render_failure_diagnostic(payload));
    };
    let step = job.steps.iter().find(|step| step_failed(step));
    let logs = client.get_job_logs(job.id).await.ok();
    Ok(render_failure_diagnostic(
        payload,
        Some(job),
        step,
        logs.as_deref(),
    ))
}

pub fn fallback_render_failure_diagnostic(payload: &WorkflowRunCompleted) -> String {
    render_failure_diagnostic(payload, None, None, None)
}

pub fn render_failure_comment(outcome: &RenderOutcome) -> String {
    let diagnostic = outcome
        .excerpt
        .as_deref()
        .unwrap_or("Render workflow failed, but no diagnostic details were available.");
    format!(
        "couldn't materialise environment `{}` for commit `{}`.\n\n{}\n\n[render run]({})",
        outcome.environment_id, outcome.commit_sha, diagnostic, outcome.run_url
    )
}

fn render_failure_diagnostic(
    payload: &WorkflowRunCompleted,
    job: Option<&Job>,
    step: Option<&Step>,
    logs: Option<&str>,
) -> String {
    let mut diagnostic = format!(
        "{} failed for commit `{}` on `{}`.",
        payload.name, payload.commit_sha, payload.branch
    );

    if let Some(job) = job {
        diagnostic.push_str(&format!("\nFailed job: `{}`", job.name));
    }
    if let Some(step) = step {
        diagnostic.push_str(&format!("\nFailed step: `{}`", step.name));
    }

    match logs.and_then(|logs| log_excerpt(logs, step.map(|step| step.name.as_str()))) {
        Some(excerpt) => {
            diagnostic.push_str("\n\n```text\n");
            diagnostic.push_str(&excerpt);
            diagnostic.push_str("\n```");
        }
        None if job.is_some() => {
            diagnostic.push_str("\n\nNo log excerpt was available for the failed job.");
        }
        None => {}
    }

    diagnostic
}

fn job_failed(job: &Job) -> bool {
    job.status == Status::Failed
        || (job.status == Status::Completed && conclusion_failed(job.conclusion.as_ref()))
}

fn step_failed(step: &Step) -> bool {
    step.status == Status::Failed
        || (step.status == Status::Completed && conclusion_failed(step.conclusion.as_ref()))
}

fn conclusion_failed(conclusion: Option<&Conclusion>) -> bool {
    matches!(
        conclusion,
        Some(
            Conclusion::ActionRequired
                | Conclusion::Cancelled
                | Conclusion::Failure
                | Conclusion::TimedOut
        )
    )
}

fn log_excerpt(logs: &str, step_name: Option<&str>) -> Option<String> {
    let cleaned = logs
        .lines()
        .map(clean_log_line)
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>();
    if cleaned.is_empty() {
        return None;
    }

    let start = step_name
        .and_then(|step_name| cleaned.iter().rposition(|line| line.contains(step_name)))
        .unwrap_or(0);
    let relevant = if start < cleaned.len() {
        &cleaned[start..]
    } else {
        cleaned.as_slice()
    };
    let relevant = if relevant.is_empty() {
        cleaned.as_slice()
    } else {
        relevant
    };

    let from = relevant.len().saturating_sub(LOG_EXCERPT_LINES);
    Some(
        relevant[from..]
            .iter()
            .map(|line| line.replace("```", "'''"))
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

fn clean_log_line(line: &str) -> String {
    let line = ANSI_RE.replace_all(line, "");
    let line = TIMESTAMP_RE.replace(&line, "");
    let line = line.trim_end();
    if line.chars().count() > LOG_LINE_LIMIT {
        let mut truncated = line.chars().take(LOG_LINE_LIMIT).collect::<String>();
        truncated.push_str("...");
        truncated
    } else {
        line.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::log_excerpt;

    #[test]
    fn strips_timestamps_ansi_and_takes_failing_step_tail() {
        let logs = "\
2026-07-08T10:00:00.000Z setup ok
2026-07-08T10:00:01.000Z \u{1b}[31mRender dev\u{1b}[0m
2026-07-08T10:00:02.000Z line 1
2026-07-08T10:00:03.000Z line 2
";

        assert_eq!(
            log_excerpt(logs, Some("Render dev")).unwrap(),
            "Render dev\nline 1\nline 2"
        );
    }
}
