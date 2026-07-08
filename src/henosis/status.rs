use crate::henosis::environment::{EnvironmentStatus, PreviewPullRequest, RenderOutcome};
use crate::henosis::queue::GateStatus;

pub const STATUS_START: &str = "<!-- henosis:status -->";
pub const STATUS_END: &str = "<!-- /henosis:status -->";

pub struct StatusSnapshot {
    pub environment: EnvironmentStatus,
    pub manifest_url: String,
    pub branch_url: String,
    pub gate: Option<GateStatus>,
    pub render: Option<RenderOutcome>,
}

pub fn render_status_section(snapshot: &StatusSnapshot) -> String {
    let gate = match &snapshot.gate {
        Some(gate) => format!("Final gate: `{}` (`{}`)", gate.status, gate.external_id),
        None => "Final gate: no gate run recorded".to_string(),
    };
    let render = match &snapshot.render {
        Some(render) => {
            let mut line = format!(
                "Latest render: `{}` for `{}` ([run]({}))",
                render.status.as_str(),
                render.commit_sha,
                render.run_url
            );
            if let Some(excerpt) = &render.excerpt {
                line.push_str(&format!(" - {excerpt}"));
            }
            line
        }
        None => "Latest render: no render run recorded".to_string(),
    };

    format!(
        "{STATUS_START}\n### Henosis status\n\nEnvironment: `{}`\nManifest: [{}]({})\nBranch: [{}]({})\nMembers: {}\n{gate}\n{render}\n{STATUS_END}",
        snapshot.environment.environment.id,
        snapshot.environment.environment.manifest_path,
        snapshot.manifest_url,
        snapshot.environment.branch,
        snapshot.branch_url,
        member_list(&snapshot.environment.members),
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

fn member_list(members: &[PreviewPullRequest]) -> String {
    if members.is_empty() {
        return "none".to_string();
    }
    members
        .iter()
        .map(|member| format!("`{}#{}`", member.key.repo, member.key.number))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upserts_marker_section() {
        let old = "hello\n\n<!-- henosis:status -->\nold\n<!-- /henosis:status -->\n\nbye";
        let new = upsert_status_section(old, "new-section");

        assert_eq!(new, "hello\n\nnew-section\n\nbye");
    }
}
