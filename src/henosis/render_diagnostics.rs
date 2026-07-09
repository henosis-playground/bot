use octocrab::models::workflows::{Conclusion, Job, Status, Step};
use regex::Regex;
use std::sync::LazyLock;

use crate::bors::event::WorkflowRunCompleted;
use crate::github::api::client::GithubRepositoryClient;
use crate::henosis::environment::RenderOutcome;

const LOG_CONTEXT_LINES: usize = 1;
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
        .unwrap_or("No render log excerpt was available.");
    format!(
        "couldn't materialise environment `{}` for commit `{}`.\n\n<details><summary>render log</summary>\n\n```text\n{}\n```\n\n</details>\n\n[render run]({})",
        outcome.environment_id, outcome.commit_sha, diagnostic, outcome.run_url
    )
}

fn render_failure_diagnostic(
    payload: &WorkflowRunCompleted,
    job: Option<&Job>,
    step: Option<&Step>,
    logs: Option<&str>,
) -> String {
    match logs.and_then(|logs| log_excerpt(logs, step.map(|step| step.name.as_str()))) {
        Some(excerpt) => excerpt,
        None if job.is_some() => {
            "No ##[error] lines were available for the failed job.".to_string()
        }
        None => format!(
            "{} failed for commit `{}` on `{}`, but no failed job logs were available.",
            payload.name, payload.commit_sha, payload.branch
        ),
    }
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

    let error_indices = relevant
        .iter()
        .enumerate()
        .filter_map(|(index, line)| diagnostic_log_line(line).then_some(index))
        .collect::<Vec<_>>();
    if error_indices.is_empty() {
        return None;
    }

    let mut selected = Vec::new();
    let mut last = None;
    for index in error_indices {
        let start = index.saturating_sub(LOG_CONTEXT_LINES);
        let end = (index + LOG_CONTEXT_LINES + 1).min(relevant.len());
        for line_index in start..end {
            if last != Some(line_index) {
                selected.push(relevant[line_index].replace("```", "'''"));
                last = Some(line_index);
            }
        }
    }
    Some(selected.join("\n"))
}

fn diagnostic_log_line(line: &str) -> bool {
    line.contains("##[error]")
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
    fn strips_timestamps_ansi_and_keeps_error_context() {
        let logs = "\
2026-07-08T10:00:00.000Z setup ok
2026-07-08T10:00:01.000Z \u{1b}[31mRender dev\u{1b}[0m
2026-07-08T10:00:02.000Z compiling manifest
2026-07-08T10:00:03.000Z ##[error]missing DATABASE_URL
2026-07-08T10:00:04.000Z Post job cleanup.
";

        assert_eq!(
            log_excerpt(logs, Some("Render dev")).unwrap(),
            "compiling manifest\n##[error]missing DATABASE_URL\nPost job cleanup."
        );
    }

    #[test]
    fn focuses_on_failure_before_post_job_cleanup() {
        let cleanup = (0..40)
            .map(|index| format!("2026-07-08T10:01:{index:02}.000Z cleanup line {index}"))
            .collect::<Vec<_>>()
            .join("\n");
        let logs = format!(
            "\
2026-07-08T10:00:00.000Z Render changed manifests
2026-07-08T10:00:01.000Z Rendering preview.toml
2026-07-08T10:00:02.000Z Failed to evaluate service-b: live render failure first 2026-07-08
2026-07-08T10:00:03.000Z [ERR_PNPM_RECURSIVE_EXEC_FIRST_FAIL] Command failed with exit code 1
2026-07-08T10:00:04.000Z ##[error]Process completed with exit code 1.
2026-07-08T10:00:05.000Z Post job cleanup.
{cleanup}
"
        );

        let excerpt = log_excerpt(&logs, Some("Render changed manifests")).unwrap();

        assert!(excerpt.contains("##[error]Process completed with exit code 1."));
        assert!(excerpt.contains("[ERR_PNPM_RECURSIVE_EXEC_FIRST_FAIL]"));
        assert!(excerpt.contains("Post job cleanup."));
        assert!(!excerpt.contains("live render failure first 2026-07-08"));
        assert!(!excerpt.contains("cleanup line 39"));
    }
}
