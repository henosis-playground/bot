use octocrab::models::workflows::{Conclusion, Job, Status, Step};
use regex::Regex;
use std::collections::BTreeSet;
use std::sync::LazyLock;

use crate::bors::event::WorkflowRunCompleted;
use crate::github::api::client::GithubRepositoryClient;
use crate::henosis::environment::RenderOutcome;
use crate::henosis::gate_report::GateReport;

const LOG_CONTEXT_BEFORE_LINES: usize = 1;
const LOG_LINE_LIMIT: usize = 600;
const STRUCTURED_RENDER_FAILURE_PREFIX: &str = "HENOSIS_GATE_REPORT:";

static ANSI_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\x1b\[[0-?]*[ -/]*[@-~]").unwrap());
static TIMESTAMP_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\s*").unwrap());

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticPresentation {
    Markdown,
    RawText,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderFailureDiagnostic {
    pub body: String,
    pub presentation: DiagnosticPresentation,
}

pub async fn generate_render_failure_diagnostic(
    client: &GithubRepositoryClient,
    payload: &WorkflowRunCompleted,
) -> anyhow::Result<RenderFailureDiagnostic> {
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

pub fn fallback_render_failure_diagnostic(
    payload: &WorkflowRunCompleted,
) -> RenderFailureDiagnostic {
    render_failure_diagnostic(payload, None, None, None)
}

pub fn render_failure_comment(
    environment: &str,
    outcome: &RenderOutcome,
    presentation: DiagnosticPresentation,
) -> String {
    let diagnostic = outcome
        .excerpt
        .as_deref()
        .unwrap_or("No render log excerpt was available.");
    let revision = outcome
        .commit_sha
        .strip_prefix("generation:")
        .map(|generation| format!("at graph generation `{generation}`"))
        .unwrap_or_else(|| format!("for commit `{}`", outcome.commit_sha));
    match presentation {
        DiagnosticPresentation::Markdown => format!(
            "couldn't materialise environment `{}` {revision}.\n\n{}\n\n[render run]({})",
            environment, diagnostic, outcome.run_url
        ),
        DiagnosticPresentation::RawText => format!(
            "couldn't materialise environment `{}` {revision}.\n\n<details><summary>render log</summary>\n\n```text\n{}\n```\n\n</details>\n\n[render run]({})",
            environment, diagnostic, outcome.run_url
        ),
    }
}

fn render_failure_diagnostic(
    payload: &WorkflowRunCompleted,
    job: Option<&Job>,
    step: Option<&Step>,
    logs: Option<&str>,
) -> RenderFailureDiagnostic {
    if let Some(report) = logs.and_then(structured_gate_report) {
        return RenderFailureDiagnostic {
            body: report.pr_comment(),
            presentation: DiagnosticPresentation::Markdown,
        };
    }

    let body = match logs.and_then(|logs| log_excerpt(logs, step.map(|step| step.name.as_str()))) {
        Some(excerpt) => excerpt,
        None if job.is_some() => {
            "No ##[error] lines were available for the failed job.".to_string()
        }
        None => format!(
            "{} failed for commit `{}` on `{}`, but no failed job logs were available.",
            payload.name, payload.commit_sha, payload.branch
        ),
    };
    RenderFailureDiagnostic {
        body,
        presentation: DiagnosticPresentation::RawText,
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

fn structured_gate_report(logs: &str) -> Option<GateReport> {
    logs.lines().find_map(|line| {
        let (_, json) = line.split_once(STRUCTURED_RENDER_FAILURE_PREFIX)?;
        GateReport::parse(json.trim()).ok()
    })
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

    let mut selected_indices = BTreeSet::new();
    for index in error_indices {
        let start = index.saturating_sub(LOG_CONTEXT_BEFORE_LINES);
        let end = index + 1;
        for line_index in start..end {
            selected_indices.insert(line_index);
        }
    }
    let selected = selected_indices
        .into_iter()
        .filter_map(|line_index| excerpt_line(&relevant[line_index]))
        .collect::<Vec<_>>();
    if selected.is_empty() {
        return None;
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

fn excerpt_line(line: &str) -> Option<String> {
    if runner_housekeeping_line(line) {
        return None;
    }
    Some(line.replace("```", "'''"))
}

fn runner_housekeeping_line(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed.starts_with("##[group]")
        || trimmed.starts_with("##[endgroup]")
        || trimmed.starts_with("(node:")
        || trimmed.contains("DeprecationWarning")
        || trimmed.contains("node --trace-deprecation")
}

#[cfg(test)]
mod tests {
    use super::{
        DiagnosticPresentation, log_excerpt, render_failure_comment, structured_gate_report,
    };
    use crate::henosis::environment::{RenderOutcome, RenderStatus};
    use crate::henosis::gate_report::{GateFailure, GateReport};

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
            "compiling manifest\n##[error]missing DATABASE_URL"
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
        assert!(!excerpt.contains("Post job cleanup."));
        assert!(!excerpt.contains("live render failure first 2026-07-08"));
        assert!(!excerpt.contains("cleanup line 39"));
    }

    #[test]
    fn drops_runner_housekeeping_around_errors() {
        let logs = "\
2026-07-08T10:00:00.000Z ##[group]Setup Node
2026-07-08T10:00:01.000Z (node:123) [DEP0040] DeprecationWarning: punycode is deprecated
2026-07-08T10:00:02.000Z Use `node --trace-deprecation ...` to show where the warning was created
2026-07-08T10:00:03.000Z ##[endgroup]
2026-07-08T10:00:04.000Z ##[group]Render preview
2026-07-08T10:00:05.000Z compiling preview
2026-07-08T10:00:06.000Z ##[error]render failed
2026-07-08T10:00:07.000Z ##[endgroup]
";

        assert_eq!(
            log_excerpt(logs, Some("Render preview")).unwrap(),
            "compiling preview\n##[error]render failed"
        );
    }

    #[test]
    fn parses_structured_gate_report_from_render_logs() {
        let report_json = serde_json::json!({
            "ok": false,
            "failures": [{
                "consumer": "service-a",
                "producer": "service-a",
                "pinnedSha": null,
                "resolvedSha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "outputsSchemaAtPinned": null,
                "outputsSchemaAtResolved": {
                    "kind": "object",
                    "shape": {
                        "test": { "kind": "string" }
                    }
                },
                "consumedPaths": ["test"],
                "kind": "validate",
                "message": "service-a.test expected string, got missing",
                "excerpt": "service-a.test expected string, got missing"
            }]
        })
        .to_string();
        let logs = format!("2026-07-09T00:00:00Z ##[error]HENOSIS_GATE_REPORT:{report_json}");

        let comment = structured_gate_report(&logs).unwrap().pr_comment();

        assert!(comment.contains(
            "**Henosis merge gate failed — `service-a` violates its own output contract.**"
        ));
        assert!(comment.contains("--> declared but not returned: test (string)"));
        assert!(!comment.contains("note:"));
    }

    #[test]
    fn rich_render_failure_comment_is_not_wrapped_as_raw_log() {
        let diagnostic = "**Henosis component validation failed — `service-a` could not compile.**\n\n```text\nerror: unsupported field\n```\n\n[source](https://example.com/source)\n\nhelp: fix it";
        let body = render_failure_comment(
            "dev (graph_01k00000000000000000000000)",
            &RenderOutcome {
                environment_id: "dev".to_string(),
                commit_sha: "aaaaaaaa".to_string(),
                status: RenderStatus::Failure,
                run_url: "https://github.com/henosis-playground/deploy/actions/runs/1".to_string(),
                excerpt: Some(diagnostic.to_string()),
                generation: None,
                publication: None,
            },
            DiagnosticPresentation::Markdown,
        );

        assert!(body.contains(diagnostic));
        assert!(!body.contains("<details><summary>render log</summary>"));
        assert_eq!(body.matches("```text").count(), 1);
    }

    #[tokio::test]
    #[ignore = "calls GitHub's Markdown rendering API"]
    async fn github_renders_component_failure_as_balanced_actionable_html() {
        let source_url = "https://github.com/henosis-playground/service-a/blob/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/henosis/src/index.ts#L29";
        let run_url = "https://henosis.skuld.systems/graphs/preview_test/generations/1";
        let diagnostic = GateReport {
            ok: false,
            failures: vec![GateFailure {
                consumer: "service-a".to_string(),
                producer: "unknown".to_string(),
                pinned_sha: None,
                resolved_sha: Some("a".repeat(40)),
                outputs_schema_at_pinned: None,
                outputs_schema_at_resolved: None,
                consumed_paths: Vec::new(),
                kind: "compile".to_string(),
                message: "service-a uses unsupported Resources field cpu; Resources is { requests?, limits? }".to_string(),
                excerpt: "src/index.ts:29:9 - error TS2353".to_string(),
                source_url: Some(source_url.to_string()),
            }],
        }
        .pr_comment();
        let original = render_failure_comment(
            "shared-demo (graph_01k00000000000000000000001)",
            &RenderOutcome {
                environment_id: "preview_test".to_string(),
                commit_sha: "generation:1".to_string(),
                status: RenderStatus::Failure,
                run_url: run_url.to_string(),
                excerpt: Some(diagnostic),
                generation: Some(1),
                publication: None,
            },
            DiagnosticPresentation::Markdown,
        );
        let markdown = format!(
            "✅ **Resolved in generation 2.**\n\n<details><summary>Earlier generation 1 diagnostic for service-a</summary>\n\n{original}\n\n</details>"
        );
        let token = std::env::var("GITHUB_TOKEN").unwrap_or_else(|_| {
            let path = std::env::var("HENOSIS_GITHUB_TOKEN_FILE")
                .expect("GITHUB_TOKEN or HENOSIS_GITHUB_TOKEN_FILE is required");
            std::fs::read_to_string(path)
                .expect("GitHub token file must be readable")
                .trim()
                .to_string()
        });
        let response = reqwest::Client::new()
            .post("https://api.github.com/markdown")
            .header(reqwest::header::USER_AGENT, "henosis-render-regression")
            .bearer_auth(token)
            .json(&serde_json::json!({
                "text": markdown,
                "mode": "gfm",
                "context": "henosis-playground/service-a"
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .text()
            .await
            .unwrap();

        assert_eq!(response.matches("<details").count(), 1);
        assert_eq!(response.matches("</details>").count(), 1);
        assert_eq!(response.matches("<pre").count(), 1);
        assert!(response.contains(&format!("href=\"{source_url}\"")));
        assert!(response.contains(&format!("href=\"{run_url}\"")));
        let heading = response
            .find("Henosis component validation failed")
            .unwrap();
        let code_block = response.find("<pre").unwrap();
        let code_block_end = response.find("</pre>").unwrap();
        let help = response.find("help:").unwrap();
        assert!(heading < code_block);
        assert!(code_block_end < help);
    }
}
