use std::collections::BTreeMap;
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
    #[serde(default)]
    pub source_url: Option<String>,
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
            return "Henosis merge gate passed. The candidate world compiled and rendered."
                .to_string();
        }

        self.failure_presentation()
    }

    /// Format a PR comment body for gate failure.
    pub fn pr_comment(&self) -> String {
        if self.ok {
            return "Henosis merge gate passed.".to_string();
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
            return "Henosis merge gate failed, but the gate runner did not report a structured failure."
                .to_string();
        }

        self.failures
            .iter()
            .map(render_failure)
            .collect::<Vec<_>>()
            .join("\n\n---\n\n")
    }
}

fn render_failure(failure: &GateFailure) -> String {
    if failure.producer == "unknown" {
        return render_component_failure(failure);
    }
    if is_self_mismatch(failure) {
        return render_self_mismatch(failure);
    }

    let analyses = analyze_consumed_paths(failure);
    let mut body = String::new();

    writeln!(
        body,
        "**Henosis merge gate failed — this change breaks `{}`.**\n",
        failure.consumer
    )
    .unwrap();
    writeln!(body, "```text").unwrap();
    writeln!(body, "error: {}", error_line(failure, &analyses)).unwrap();
    writeln!(body, "--> {}", consumed_span_line(failure, &analyses)).unwrap();
    writeln!(body, "note: {}", version_note(failure)).unwrap();
    writeln!(body, "```").unwrap();
    if let Some(source_url) = &failure.source_url {
        writeln!(body, "\n[source]({source_url})").unwrap();
    }

    if let Some(diff) = schema_diff(failure) {
        writeln!(body, "\n```diff\n{diff}```").unwrap();
    }

    writeln!(body, "\nhelp: {}", help_line(failure, &analyses)).unwrap();
    body.trim_end().to_string()
}

fn render_component_failure(failure: &GateFailure) -> String {
    let mut body = String::new();
    writeln!(
        body,
        "**Henosis component validation failed — `{}` could not compile.**\n",
        failure.consumer
    )
    .unwrap();
    writeln!(body, "```text").unwrap();
    writeln!(body, "error: {}", sentence(&failure.message)).unwrap();
    writeln!(body, "--> {} source", failure.consumer).unwrap();
    writeln!(
        body,
        "note: the @henosis/platform-k8s Resources capability accepts only requests and limits"
    )
    .unwrap();
    writeln!(body, "```").unwrap();
    if let Some(source_url) = &failure.source_url {
        writeln!(body, "\n[source]({source_url})").unwrap();
    }
    if failure.message.contains("Resources field") {
        writeln!(
            body,
            "\nhelp: use `resources: {{ requests: {{ cpu: \"100m\" }} }}`"
        )
        .unwrap();
    } else {
        writeln!(
            body,
            "\nhelp: fix the TypeScript error at the linked source line"
        )
        .unwrap();
    }
    body.trim_end().to_string()
}

fn is_self_mismatch(failure: &GateFailure) -> bool {
    failure.kind == "validate" && failure.consumer == failure.producer
}

fn render_self_mismatch(failure: &GateFailure) -> String {
    let analyses = analyze_self_paths(failure);
    let mut body = String::new();

    writeln!(
        body,
        "**Henosis merge gate failed — `{}` violates its own output contract.**\n",
        failure.consumer
    )
    .unwrap();
    writeln!(body, "```text").unwrap();
    writeln!(
        body,
        "error: {}'s build does not return what its outputs schema declares.",
        failure.consumer
    )
    .unwrap();
    writeln!(body, "--> {}", self_span_line(&analyses)).unwrap();
    writeln!(body, "```").unwrap();

    if analyses.iter().any(SelfPathAnalysis::is_type_mismatch)
        && let Some(diff) = schema_diff(failure)
    {
        writeln!(body, "\n```diff\n{diff}```").unwrap();
    }

    writeln!(body, "\nhelp: {}", self_help_line(&analyses)).unwrap();
    body.trim_end().to_string()
}

fn error_line(failure: &GateFailure, analyses: &[PathAnalysis]) -> String {
    if analyses.iter().any(PathAnalysis::is_breaking) {
        return format!(
            "{} consumes outputs from {} that are incompatible with the resolved producer version.",
            failure.consumer, failure.producer
        );
    }

    sentence(&failure.message)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SelfPathAnalysis {
    Missing {
        path: String,
        expected_type: String,
    },
    TypeMismatch {
        path: String,
        expected_type: String,
        actual_type: String,
    },
    Unknown {
        path: String,
        expected_type: String,
    },
}

impl SelfPathAnalysis {
    fn is_type_mismatch(&self) -> bool {
        matches!(self, Self::TypeMismatch { .. })
    }

    fn path(&self) -> &str {
        match self {
            Self::Missing { path, .. }
            | Self::TypeMismatch { path, .. }
            | Self::Unknown { path, .. } => path,
        }
    }

    fn expected_type(&self) -> &str {
        match self {
            Self::Missing { expected_type, .. }
            | Self::TypeMismatch { expected_type, .. }
            | Self::Unknown { expected_type, .. } => expected_type,
        }
    }
}

fn analyze_self_paths(failure: &GateFailure) -> Vec<SelfPathAnalysis> {
    failure
        .consumed_paths
        .iter()
        .map(|path| {
            let expected_type =
                schema_kind_at_path(failure.outputs_schema_at_resolved.as_ref(), path)
                    .unwrap_or_else(|| "unknown".to_string());
            match validation_actual_type(failure, path).as_deref() {
                Some("missing") => SelfPathAnalysis::Missing {
                    path: path.clone(),
                    expected_type,
                },
                Some(actual_type) => SelfPathAnalysis::TypeMismatch {
                    path: path.clone(),
                    expected_type,
                    actual_type: actual_type.to_string(),
                },
                None => SelfPathAnalysis::Unknown {
                    path: path.clone(),
                    expected_type,
                },
            }
        })
        .collect()
}

fn validation_actual_type(failure: &GateFailure, path: &str) -> Option<String> {
    let prefix = format!("{}.{} expected ", failure.consumer, path);
    let rest = failure.message.strip_prefix(&prefix)?;
    let (_, actual) = rest.rsplit_once(", got ")?;
    Some(actual.trim_end_matches('.').to_string())
}

fn self_span_line(analyses: &[SelfPathAnalysis]) -> String {
    if analyses.is_empty() {
        return "declared output contract was not satisfied".to_string();
    }

    let missing = analyses
        .iter()
        .filter_map(|analysis| match analysis {
            SelfPathAnalysis::Missing {
                path,
                expected_type,
            } => Some(format!("{path} ({expected_type})")),
            _ => None,
        })
        .collect::<Vec<_>>();
    let mismatched = analyses
        .iter()
        .filter_map(|analysis| match analysis {
            SelfPathAnalysis::TypeMismatch {
                path,
                expected_type,
                actual_type,
            } => Some(format!(
                "{path} (declared {expected_type}, returned {actual_type})"
            )),
            SelfPathAnalysis::Unknown {
                path,
                expected_type,
            } => Some(format!("{path} (declared {expected_type})")),
            _ => None,
        })
        .collect::<Vec<_>>();

    let mut parts = Vec::new();
    if !missing.is_empty() {
        parts.push(format!("declared but not returned: {}", missing.join(", ")));
    }
    if !mismatched.is_empty() {
        parts.push(format!(
            "returned with wrong type: {}",
            mismatched.join(", ")
        ));
    }
    parts.join("; ")
}

fn self_help_line(analyses: &[SelfPathAnalysis]) -> String {
    let paths = analyses
        .iter()
        .map(|analysis| format!("`{}`", analysis.path()))
        .collect::<Vec<_>>();
    if paths.is_empty() {
        return "return the declared outputs from build, or update `outputs`".to_string();
    }

    if analyses.iter().all(SelfPathAnalysis::is_type_mismatch) && analyses.len() == 1 {
        return format!(
            "return {} as `{}`, or change its `outputs` declaration",
            paths[0],
            analyses[0].expected_type()
        );
    }

    format!(
        "return {} from build, or remove {} from `outputs`",
        paths.join(", "),
        if analyses.len() == 1 { "it" } else { "them" }
    )
}

fn consumed_span_line(failure: &GateFailure, analyses: &[PathAnalysis]) -> String {
    if analyses.is_empty() {
        return format!(
            "{} outputs consumed by {}: no consumed paths were reported",
            failure.producer, failure.consumer
        );
    }

    format!(
        "{} outputs consumed by {}: {}",
        failure.producer,
        failure.consumer,
        analyses
            .iter()
            .map(PathAnalysis::fate)
            .collect::<Vec<_>>()
            .join(", ")
    )
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
            "update your usage, or update your pin: `pnpm update @henosis/{}`",
            failure.producer
        );
    }

    format!(
        "you depended on outputs [{}] which no longer exist or changed type; update your usage, or update your pin: `pnpm update @henosis/{}`",
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

    fn fate(&self) -> String {
        match self {
            Self::Removed { path, .. } => {
                format!("{path} (removed)")
            }
            Self::TypeChanged {
                path,
                pinned_type,
                resolved_type,
            } => format!("{path} ({pinned_type} → {resolved_type})"),
            Self::Unchanged {
                path,
                resolved_type,
            } => format!("{path} (unchanged {resolved_type})"),
            Self::Unknown { path } => {
                format!("{path} (unknown)")
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
    let pinned = human_schema_string(failure.outputs_schema_at_pinned.as_ref()?);
    let resolved = human_schema_string(failure.outputs_schema_at_resolved.as_ref()?);
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
        .flat_map(|path| path.split('.').next_back())
        .map(|path| path.to_string())
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

fn human_schema_string(value: &Value) -> String {
    let mut out = String::new();
    write_schema_object(&mut out, "outputs", value, 0);
    out
}

fn write_schema_object(out: &mut String, label: &str, value: &Value, indent: usize) {
    let prefix = "  ".repeat(indent);
    let Some(shape) = value.get("shape").and_then(Value::as_object) else {
        let kind = value
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        writeln!(out, "{prefix}{label}: {kind}").unwrap();
        return;
    };

    writeln!(out, "{prefix}{label} {{").unwrap();
    let fields = shape.iter().collect::<BTreeMap<_, _>>();
    for (field, child) in fields {
        let kind = child
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        if kind == "object" {
            write_schema_object(out, field, child, indent + 1);
        } else {
            writeln!(out, "{}{}: {}", "  ".repeat(indent + 1), field, kind).unwrap();
        }
    }
    writeln!(out, "{prefix}}}").unwrap();
}

fn sentence(message: &str) -> String {
    let trimmed = message.trim();
    if trimmed.ends_with('.') {
        trimmed.to_string()
    } else {
        format!("{trimmed}.")
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
                .contains("Henosis component validation failed")
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
                source_url: Some("https://github.com/henosis-playground/service-b/blob/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/henosis/src/index.ts#L25".to_string()),
            }],
        };

        let comment = report.pr_comment();

        insta::assert_snapshot!(comment, @r###"
**Henosis merge gate failed — this change breaks `service-b`.**

```text
error: service-b consumes outputs from service-a that are incompatible with the resolved producer version.
--> service-a outputs consumed by service-b: api (removed), port (number → string)
note: you pinned service-a @ 1111111; this environment resolved service-a @ 2222222
```

[source](https://github.com/henosis-playground/service-b/blob/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/henosis/src/index.ts#L25)

```diff
 outputs {
-  api: url
-  port: number
+  apiUrl: url
+  port: string
 }
```

help: you depended on outputs [`api`, `port`] which no longer exist or changed type; update your usage, or update your pin: `pnpm update @henosis/service-a`
"###);
    }

    #[test]
    fn renders_self_mismatch_missing_output_diagnostic() {
        let report = GateReport {
            ok: false,
            failures: vec![GateFailure {
                consumer: "service-a".to_string(),
                producer: "service-a".to_string(),
                pinned_sha: None,
                resolved_sha: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()),
                outputs_schema_at_pinned: None,
                outputs_schema_at_resolved: Some(serde_json::json!({
                    "kind": "object",
                    "shape": {
                        "api": { "kind": "url" },
                        "test": { "kind": "string" }
                    }
                })),
                consumed_paths: vec!["test".to_string()],
                kind: "validate".to_string(),
                message: "service-a.test expected string, got missing".to_string(),
                excerpt: "service-a.test expected string, got missing".to_string(),
                source_url: None,
            }],
        };

        let comment = report.pr_comment();

        insta::assert_snapshot!(comment, @r###"
**Henosis merge gate failed — `service-a` violates its own output contract.**

```text
error: service-a's build does not return what its outputs schema declares.
--> declared but not returned: test (string)
```

help: return `test` from build, or remove it from `outputs`
"###);
    }

    #[test]
    fn renders_self_mismatch_type_change_diagnostic() {
        let report = GateReport {
            ok: false,
            failures: vec![GateFailure {
                consumer: "service-a".to_string(),
                producer: "service-a".to_string(),
                pinned_sha: None,
                resolved_sha: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()),
                outputs_schema_at_pinned: Some(serde_json::json!({
                    "kind": "object",
                    "shape": {
                        "port": { "kind": "number" }
                    }
                })),
                outputs_schema_at_resolved: Some(serde_json::json!({
                    "kind": "object",
                    "shape": {
                        "port": { "kind": "string" }
                    }
                })),
                consumed_paths: vec!["port".to_string()],
                kind: "validate".to_string(),
                message: "service-a.port expected string, got number".to_string(),
                excerpt: "service-a.port expected string, got number".to_string(),
                source_url: None,
            }],
        };

        let comment = report.pr_comment();

        insta::assert_snapshot!(comment, @r###"
**Henosis merge gate failed — `service-a` violates its own output contract.**

```text
error: service-a's build does not return what its outputs schema declares.
--> returned with wrong type: port (declared string, returned number)
```

```diff
 outputs {
-  port: number
+  port: string
 }
```

help: return `port` as `string`, or change its `outputs` declaration
"###);
    }
}
