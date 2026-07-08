use std::fmt::Write;

use serde::Deserialize;
use serde_json::Value;
use similar::{ChangeTag, TextDiff};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GateFailure {
    pub consumer: String,
    pub producer: String,
    pub pinned_sha: Option<String>,
    pub resolved_sha: Option<String>,
    pub outputs_schema_at_pinned: Option<Value>,
    pub outputs_schema_at_resolved: Option<Value>,
    #[serde(default)]
    pub consumed_paths: Vec<String>,
    pub kind: String,
    pub message: String,
    pub excerpt: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateReport {
    pub ok: bool,
    pub failures: Vec<GateFailure>,
}

impl GateReport {
    pub fn parse(json: &str) -> anyhow::Result<Self> {
        serde_json::from_str(json).map_err(Into::into)
    }

    /// Format a legible check-run summary naming the failing consumer + contract break.
    pub fn check_run_summary(&self) -> String {
        if self.ok {
            return "Henosis gate passed. The candidate world compiled and rendered.".to_string();
        }

        self.failure_presentation()
    }

    /// Format a PR comment body for gate failure.
    pub fn pr_comment(&self) -> String {
        if self.ok {
            return "Henosis gate passed.".to_string();
        }

        self.failure_presentation()
    }

    pub fn status_diagnostic(&self) -> Option<String> {
        if self.ok {
            None
        } else {
            Some(self.failure_presentation())
        }
    }

    fn failure_presentation(&self) -> String {
        if self.failures.is_empty() {
            return "Henosis gate failed, but the gate runner did not report a structured failure."
                .to_string();
        }

        let mut body = "Henosis gate failed.\n\nThe candidate cannot land because it breaks a consumer contract.".to_string();
        for failure in &self.failures {
            write!(body, "\n\n{}", render_failure(failure)).unwrap();
        }

        body
    }
}

fn render_failure(failure: &GateFailure) -> String {
    let analyses = analyze_consumed_paths(failure);
    let mut body = String::new();

    writeln!(
        body,
        "### `{}` consuming `{}`\n",
        failure.consumer, failure.producer
    )
    .unwrap();
    writeln!(body, "error: {}", error_line(failure, &analyses)).unwrap();
    writeln!(body, "note: {}", version_note(failure)).unwrap();

    if let Some(diff) = schema_diff(failure) {
        writeln!(body, "\n```diff\n{diff}```").unwrap();
    } else if !failure.excerpt.trim().is_empty() {
        writeln!(
            body,
            "\nnote: gate runner excerpt\n\n```text\n{}\n```",
            failure.excerpt.trim()
        )
        .unwrap();
    }

    for analysis in &analyses {
        writeln!(body, "note: {}", analysis.note(&failure.consumer)).unwrap();
    }

    writeln!(body, "help: {}", help_line(failure, &analyses)).unwrap();
    body.trim_end().to_string()
}

fn error_line(failure: &GateFailure, analyses: &[PathAnalysis]) -> String {
    if let Some(analysis) = analyses.iter().find(|analysis| analysis.is_breaking()) {
        return analysis.error(&failure.consumer, &failure.producer);
    }

    failure.message.clone()
}

fn version_note(failure: &GateFailure) -> String {
    match (&failure.pinned_sha, &failure.resolved_sha) {
        (Some(pinned), Some(resolved)) if pinned != resolved => format!(
            "you pinned {} @ {}; this environment resolved {} @ {}",
            failure.producer,
            short_sha(pinned),
            failure.producer,
            short_sha(resolved)
        ),
        (Some(pinned), Some(_)) => format!(
            "you pinned {} @ {}; this environment resolved the same producer version",
            failure.producer,
            short_sha(pinned)
        ),
        (Some(pinned), None) => format!(
            "you pinned {} @ {}; this environment did not report a resolved producer sha",
            failure.producer,
            short_sha(pinned)
        ),
        (None, Some(resolved)) => format!(
            "the consumer pin for {} was not available; this environment resolved {} @ {}",
            failure.producer,
            failure.producer,
            short_sha(resolved)
        ),
        (None, None) => "producer version context was not available".to_string(),
    }
}

fn help_line(failure: &GateFailure, analyses: &[PathAnalysis]) -> String {
    let changed_paths = analyses
        .iter()
        .filter(|analysis| analysis.is_breaking())
        .map(PathAnalysis::path)
        .collect::<Vec<_>>();
    let paths = if changed_paths.is_empty() {
        failure
            .consumed_paths
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
    } else {
        changed_paths
    };

    if paths.is_empty() {
        return format!(
            "update the consumer contract usage, or update your pin: pnpm update @henosis/{}",
            failure.producer
        );
    }

    format!(
        "you depended on outputs [{}] which no longer exist or changed type; update your usage, or update your pin: pnpm update @henosis/{}",
        paths
            .iter()
            .map(|path| format!("`{path}`"))
            .collect::<Vec<_>>()
            .join(", "),
        failure.producer
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PathAnalysis {
    Removed {
        path: String,
        pinned_type: String,
    },
    TypeChanged {
        path: String,
        pinned_type: String,
        resolved_type: String,
    },
    Unchanged {
        path: String,
        resolved_type: String,
    },
    Unknown {
        path: String,
    },
}

impl PathAnalysis {
    fn path(&self) -> &str {
        match self {
            Self::Removed { path, .. }
            | Self::TypeChanged { path, .. }
            | Self::Unchanged { path, .. }
            | Self::Unknown { path } => path,
        }
    }

    fn is_breaking(&self) -> bool {
        matches!(self, Self::Removed { .. } | Self::TypeChanged { .. })
    }

    fn error(&self, consumer: &str, producer: &str) -> String {
        match self {
            Self::Removed { path, .. } => {
                format!(
                    "{consumer} relies on {producer}.{path}, which no longer exists in the resolved version"
                )
            }
            Self::TypeChanged {
                path,
                pinned_type,
                resolved_type,
            } => format!(
                "{consumer} relies on {producer}.{path}, whose type changed: {pinned_type} → {resolved_type}"
            ),
            Self::Unchanged { .. } | Self::Unknown { .. } => unreachable!(),
        }
    }

    fn note(&self, consumer: &str) -> String {
        match self {
            Self::Removed { path, pinned_type } => format!(
                "{consumer} relies on `{path}` ({pinned_type}) - removed in the resolved version"
            ),
            Self::TypeChanged {
                path,
                pinned_type,
                resolved_type,
            } => format!("`{path}` changed type: {pinned_type} → {resolved_type}"),
            Self::Unchanged {
                path,
                resolved_type,
            } => format!("`{path}` still exists as {resolved_type}"),
            Self::Unknown { path } => {
                format!("`{path}` could not be compared in the reported output schemas")
            }
        }
    }
}

fn analyze_consumed_paths(failure: &GateFailure) -> Vec<PathAnalysis> {
    failure
        .consumed_paths
        .iter()
        .map(|path| {
            let pinned_type = schema_kind_at_path(failure.outputs_schema_at_pinned.as_ref(), path);
            let resolved_type =
                schema_kind_at_path(failure.outputs_schema_at_resolved.as_ref(), path);

            match (pinned_type, resolved_type) {
                (Some(pinned_type), None) => PathAnalysis::Removed {
                    path: path.clone(),
                    pinned_type,
                },
                (Some(pinned_type), Some(resolved_type)) if pinned_type != resolved_type => {
                    PathAnalysis::TypeChanged {
                        path: path.clone(),
                        pinned_type,
                        resolved_type,
                    }
                }
                (Some(_), Some(resolved_type)) => PathAnalysis::Unchanged {
                    path: path.clone(),
                    resolved_type,
                },
                _ => PathAnalysis::Unknown { path: path.clone() },
            }
        })
        .collect()
}

fn schema_kind_at_path(schema: Option<&Value>, path: &str) -> Option<String> {
    let mut current = schema?;
    for part in path.split('.') {
        current = current.get("shape")?.get(part)?;
    }
    current.get("kind")?.as_str().map(ToString::to_string)
}

fn schema_diff(failure: &GateFailure) -> Option<String> {
    let pinned = sorted_json_string(failure.outputs_schema_at_pinned.as_ref()?);
    let resolved = sorted_json_string(failure.outputs_schema_at_resolved.as_ref()?);
    if pinned == resolved {
        return None;
    }

    let diff = TextDiff::from_lines(&pinned, &resolved);
    let relevant = relevant_diff_lines(&diff, &failure.consumed_paths);
    if relevant.trim().is_empty() {
        None
    } else {
        Some(relevant)
    }
}

fn relevant_diff_lines(diff: &TextDiff<'_, '_, '_, str>, consumed_paths: &[String]) -> String {
    let fragments = consumed_paths
        .iter()
        .flat_map(|path| path.split('.').last())
        .map(|path| format!("\"{path}\""))
        .collect::<Vec<_>>();

    let mut selected_groups = Vec::new();
    let mut all_groups = Vec::new();
    for group in diff.grouped_ops(3) {
        let mut group_text = String::new();
        for op in group {
            for change in diff.iter_changes(&op) {
                let sign = match change.tag() {
                    ChangeTag::Delete => '-',
                    ChangeTag::Insert => '+',
                    ChangeTag::Equal => ' ',
                };
                write!(group_text, "{sign}{change}").unwrap();
                if !group_text.ends_with('\n') {
                    group_text.push('\n');
                }
            }
        }

        let relevant = fragments.is_empty()
            || fragments
                .iter()
                .any(|fragment| group_text.contains(fragment));
        if relevant {
            selected_groups.push(group_text.clone());
        }
        all_groups.push(group_text);
    }

    let groups = if selected_groups.is_empty() {
        all_groups
    } else {
        selected_groups
    };
    groups.join("@@\n")
}

fn sorted_json_string(value: &Value) -> String {
    serde_json::to_string_pretty(&sort_json(value)).unwrap_or_else(|_| value.to_string())
}

fn sort_json(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(sort_json).collect()),
        Value::Object(map) => {
            let mut sorted = serde_json::Map::new();
            let mut entries = map.iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            for (key, child) in entries {
                sorted.insert(key.clone(), sort_json(child));
            }
            Value::Object(sorted)
        }
        _ => value.clone(),
    }
}

fn short_sha(sha: &str) -> &str {
    sha.get(..7).unwrap_or(sha)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_success_report() {
        let report = GateReport::parse(r#"{"ok":true,"failures":[]}"#).unwrap();

        assert_eq!(
            report,
            GateReport {
                ok: true,
                failures: vec![]
            }
        );
    }

    #[test]
    fn parses_failure_report_with_multiple_failures() {
        let report = GateReport::parse(
            r#"
{
  "ok": false,
  "failures": [
    {
      "consumer": "service-b",
      "producer": "service-a",
      "pinnedSha": "1111111111111111111111111111111111111111",
      "resolvedSha": "2222222222222222222222222222222222222222",
      "outputsSchemaAtPinned": {"kind":"object","shape":{"databaseUrl":{"kind":"url"}}},
      "outputsSchemaAtResolved": {"kind":"object","shape":{"apiUrl":{"kind":"url"}}},
      "consumedPaths": ["databaseUrl"],
      "kind": "compile",
      "message": "service-b consumes service-a.databaseUrl which no longer exists",
      "excerpt": "src/index.ts:1:1 - error TS2339"
    },
    {
      "consumer": "renderer",
      "producer": "unknown",
      "pinnedSha": null,
      "resolvedSha": null,
      "outputsSchemaAtPinned": null,
      "outputsSchemaAtResolved": null,
      "consumedPaths": [],
      "kind": "render",
      "message": "render failed",
      "excerpt": "boom"
    }
  ]
}
"#,
        )
        .unwrap();

        assert!(!report.ok);
        assert_eq!(report.failures.len(), 2);
        assert_eq!(report.failures[0].producer, "service-a");
        assert_eq!(report.failures[1].kind, "render");
    }

    #[test]
    fn parses_failure_report_with_null_contract_context() {
        let report = GateReport::parse(
            r#"
{
  "ok": false,
  "failures": [
    {
      "consumer": "service-a",
      "producer": "unknown",
      "pinnedSha": null,
      "resolvedSha": null,
      "outputsSchemaAtPinned": null,
      "outputsSchemaAtResolved": null,
      "consumedPaths": [],
      "kind": "validate",
      "message": "service-a.api expected url, got string",
      "excerpt": "service-a.api expected url, got string"
    }
  ]
}
"#,
        )
        .unwrap();

        assert_eq!(report.failures[0].producer, "unknown");
        assert!(
            report
                .check_run_summary()
                .contains("producer version context")
        );
    }

    #[test]
    fn renders_rich_contract_break_diagnostic() {
        let report = GateReport {
            ok: false,
            failures: vec![GateFailure {
                consumer: "service-b".to_string(),
                producer: "service-a".to_string(),
                pinned_sha: Some("1111111111111111111111111111111111111111".to_string()),
                resolved_sha: Some("2222222222222222222222222222222222222222".to_string()),
                outputs_schema_at_pinned: Some(serde_json::json!({
                    "kind": "object",
                    "shape": {
                        "api": { "kind": "url" },
                        "port": { "kind": "number" }
                    }
                })),
                outputs_schema_at_resolved: Some(serde_json::json!({
                    "kind": "object",
                    "shape": {
                        "apiUrl": { "kind": "url" },
                        "port": { "kind": "string" }
                    }
                })),
                consumed_paths: vec!["api".to_string(), "port".to_string()],
                kind: "compile".to_string(),
                message: "service-b consumes service-a.api which no longer exists".to_string(),
                excerpt: "Property 'api' does not exist on type".to_string(),
            }],
        };

        let summary = report.check_run_summary();

        insta::assert_snapshot!(summary, @r###"
Henosis gate failed.

The candidate cannot land because it breaks a consumer contract.

### `service-b` consuming `service-a`

error: service-b relies on service-a.api, which no longer exists in the resolved version
note: you pinned service-a @ 1111111; this environment resolved service-a @ 2222222

```diff
 {
   "kind": "object",
   "shape": {
-    "api": {
+    "apiUrl": {
       "kind": "url"
     },
     "port": {
-      "kind": "number"
+      "kind": "string"
     }
   }
 }
```
note: service-b relies on `api` (url) - removed in the resolved version
note: `port` changed type: number → string
help: you depended on outputs [`api`, `port`] which no longer exist or changed type; update your usage, or update your pin: pnpm update @henosis/service-a
"###);
    }
}
