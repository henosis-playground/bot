use crate::henosis::environment::{
    EnvironmentStatus, PreviewPullRequest, PullRequestKey, RenderOutcome, RenderStatus,
};
use crate::henosis::queue::GateStatus;

pub const STATUS_START: &str = "<!-- henosis:status -->";
pub const STATUS_END: &str = "<!-- /henosis:status -->";

pub struct StatusSnapshot {
    pub environment: EnvironmentStatus,
    pub current_pr: PullRequestKey,
    pub manifest_url: String,
    pub branch_url: String,
    pub advisory_gate: Option<GateStatus>,
    pub gate: Option<GateStatus>,
    pub render: Option<RenderOutcome>,
}

pub fn render_status_section(snapshot: &StatusSnapshot) -> String {
    format!(
        "{STATUS_START}\n### Henosis status\n\n**Environment** `{}` · [manifest]({}) · [branch]({})\n**Members** {}\n**Merge gate** {}\n**Render** {}\n{STATUS_END}",
        snapshot.environment.environment.id,
        snapshot.manifest_url,
        snapshot.branch_url,
        member_list(&snapshot.environment.members, &snapshot.current_pr),
        merge_gate_row(
            snapshot.current_pr.repo.as_str(),
            snapshot.advisory_gate.as_ref(),
            snapshot.gate.as_ref(),
        ),
        render_row(snapshot.render.as_ref()),
    )
}

pub fn upsert_status_section(body: &str, section: &str) -> String {
    match (body.find(STATUS_START), body.find(STATUS_END)) {
        (Some(start), Some(end)) if start <= end => {
            let end = end + STATUS_END.len();
            let mut next = String::new();
            next.push_str(body[..start].trim_end());
            if !next.is_empty() {
                next.push_str("\n\n");
            }
            next.push_str(section);
            let tail = body[end..].trim_start();
            if !tail.is_empty() {
                next.push_str("\n\n");
                next.push_str(tail);
            }
            next
        }
        _ if body.trim().is_empty() => section.to_string(),
        _ => format!("{}\n\n{}", body.trim_end(), section),
    }
}

pub fn remove_status_section(body: &str) -> String {
    match (body.find(STATUS_START), body.find(STATUS_END)) {
        (Some(start), Some(end)) if start <= end => {
            let end = end + STATUS_END.len();
            let mut next = String::new();
            next.push_str(body[..start].trim_end());
            let tail = body[end..].trim_start();
            if !tail.is_empty() {
                if !next.is_empty() {
                    next.push_str("\n\n");
                }
                next.push_str(tail);
            }
            next
        }
        _ => body.to_string(),
    }
}

fn member_list(members: &[PreviewPullRequest], current_pr: &PullRequestKey) -> String {
    if members.is_empty() {
        return "none".to_string();
    }
    members
        .iter()
        .map(|member| {
            let mut link = format!(
                "[{}#{}]({})",
                member.key.repo,
                member.key.number,
                pr_url(&member.key)
            );
            if member.key == *current_pr {
                link.push_str(" (this PR)");
            }
            link
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn merge_gate_row(
    repo: &str,
    advisory: Option<&GateStatus>,
    final_gate: Option<&GateStatus>,
) -> String {
    match (advisory, final_gate) {
        (Some(advisory), Some(final_gate)) if gate_word(advisory) != gate_word(final_gate) => {
            format!(
                "final: {}<br>advisory: {}",
                gate_result(repo, final_gate),
                gate_result(repo, advisory)
            )
        }
        (_, Some(final_gate)) => gate_result(repo, final_gate),
        (Some(advisory), None) => gate_result(repo, advisory),
        (None, None) => ":grey_question: none".to_string(),
    }
}

fn gate_result(repo: &str, gate: &GateStatus) -> String {
    format!(
        "{} ([details]({}))",
        icon_word(gate_word(gate)),
        check_details_url(repo, &gate.head_sha),
    )
}

fn gate_word(gate: &GateStatus) -> &'static str {
    match gate.status.as_str() {
        "gate-passed" | "advisory-passed" | "merged" => "passed",
        "gate-failed" | "advisory-failed" => "failed",
        "pending" | "pending-executor" | "running" | "merging-pr" | "bumping-dev" => "running",
        "invalidated" => "cancelled",
        _ => "unknown",
    }
}

fn render_row(render: Option<&RenderOutcome>) -> String {
    match render {
        Some(render) => format!(
            "{} ([run]({}))",
            icon_word(match render.status {
                RenderStatus::Success => "passed",
                RenderStatus::Failure => "failed",
            }),
            render.run_url
        ),
        None => ":grey_question: none".to_string(),
    }
}

fn icon_word(word: &str) -> String {
    let icon = match word {
        "passed" => ":white_check_mark:",
        "failed" => ":x:",
        "running" => ":hourglass_flowing_sand:",
        "cancelled" => ":heavy_minus_sign:",
        _ => ":grey_question:",
    };
    format!("{icon} {word}")
}

fn pr_url(key: &PullRequestKey) -> String {
    format!("https://github.com/{}/pull/{}", key.repo, key.number)
}

fn check_details_url(repo: &str, head_sha: &str) -> String {
    format!("https://github.com/{repo}/commit/{head_sha}/checks")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::henosis::environment::{EnvironmentState, RenderStatus};

    #[test]
    fn upserts_marker_section() {
        let old = "hello\n\n<!-- henosis:status -->\nold\n<!-- /henosis:status -->\n\nbye";
        let new = upsert_status_section(old, "new-section");

        assert_eq!(new, "hello\n\nnew-section\n\nbye");
    }

    #[test]
    fn removes_marker_section() {
        let old = "hello\n\n<!-- henosis:status -->\nold\n<!-- /henosis:status -->\n\nbye";
        let new = remove_status_section(old);

        assert_eq!(new, "hello\n\nbye");
    }

    #[test]
    fn renders_ratified_status_block() {
        let section = render_status_section(&StatusSnapshot {
            environment: EnvironmentStatus {
                environment: EnvironmentState {
                    id: "preview-00000000-0000-4000-8000-000000000001".to_string(),
                    manifest_path: "preview-00000000-0000-4000-8000-000000000001.toml".to_string(),
                    is_preview: true,
                },
                branch: "env/preview-00000000-0000-4000-8000-000000000001".to_string(),
                members: vec![
                    PreviewPullRequest::new(
                        "henosis-playground/service-a",
                        12,
                        "service-a",
                        "pr/12",
                        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    ),
                    PreviewPullRequest::new(
                        "henosis-playground/service-b",
                        34,
                        "service-b",
                        "pr/34",
                        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    ),
                ],
            },
            current_pr: PullRequestKey::new("henosis-playground/service-a", 12),
            manifest_url: "https://github.com/henosis-playground/deploy/blob/main/preview.toml"
                .to_string(),
            branch_url: "https://github.com/henosis-playground/deploy/tree/env/preview".to_string(),
            advisory_gate: Some(GateStatus {
                external_id: "gate-advisory".to_string(),
                head_sha: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
                status: "advisory-passed".to_string(),
                diagnostic: Some("not rendered in status".to_string()),
            }),
            gate: Some(GateStatus {
                external_id: "gate-final".to_string(),
                head_sha: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
                status: "gate-failed".to_string(),
                diagnostic: Some("not rendered in status".to_string()),
            }),
            render: Some(RenderOutcome {
                environment_id: "preview-00000000-0000-4000-8000-000000000001".to_string(),
                commit_sha: "cccccccccccccccccccccccccccccccccccccccc".to_string(),
                status: RenderStatus::Failure,
                run_url: "https://github.com/henosis-playground/deploy/actions/runs/1".to_string(),
                excerpt: Some("not rendered in status".to_string()),
            }),
        });

        insta::assert_snapshot!(section, @r#"
<!-- henosis:status -->
### Henosis status

**Environment** `preview-00000000-0000-4000-8000-000000000001` · [manifest](https://github.com/henosis-playground/deploy/blob/main/preview.toml) · [branch](https://github.com/henosis-playground/deploy/tree/env/preview)
**Members** [henosis-playground/service-a#12](https://github.com/henosis-playground/service-a/pull/12) (this PR), [henosis-playground/service-b#34](https://github.com/henosis-playground/service-b/pull/34)
**Merge gate** final: :x: failed ([details](https://github.com/henosis-playground/service-a/commit/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/checks))<br>advisory: :white_check_mark: passed ([details](https://github.com/henosis-playground/service-a/commit/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/checks))
**Render** :x: failed ([run](https://github.com/henosis-playground/deploy/actions/runs/1))
<!-- /henosis:status -->
"#);
    }
}
