use std::fmt::Write;

use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateFailure {
    pub component: String,
    #[serde(rename = "consumerOf")]
    pub consumer_of: Option<String>,
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

        if self.failures.is_empty() {
            return "Henosis gate failed, but the gate runner did not report a structured failure."
                .to_string();
        }

        let mut summary = "Henosis gate failed.\n\nContract breaks:\n".to_string();
        for failure in &self.failures {
            let consumer_of = failure.consumer_of.as_deref().unwrap_or("unknown");
            writeln!(
                summary,
                "- `{}` consuming `{}`: {}",
                failure.component, consumer_of, failure.message
            )
            .unwrap();
        }
        summary
    }

    /// Format a PR comment body for gate failure.
    pub fn pr_comment(&self) -> String {
        if self.ok {
            return "Henosis gate passed.".to_string();
        }

        let mut body = "Henosis gate failed.\n\nThe candidate cannot land because it breaks a consumer contract.".to_string();
        if self.failures.is_empty() {
            body.push_str("\n\nNo structured failures were reported by the gate runner.");
            return body;
        }

        for failure in &self.failures {
            let consumer_of = failure.consumer_of.as_deref().unwrap_or("unknown");
            writeln!(
                body,
                "\n\n### `{}` consuming `{}`\n\n{}\n\nKind: `{}`",
                failure.component, consumer_of, failure.message, failure.kind
            )
            .unwrap();

            if !failure.excerpt.trim().is_empty() {
                writeln!(body, "\n```text\n{}\n```", failure.excerpt.trim()).unwrap();
            }
        }

        body
    }
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
      "component": "service-b",
      "consumerOf": "service-a",
      "kind": "compile",
      "message": "service-b consumes service-a.databaseUrl which no longer exists",
      "excerpt": "src/index.ts:1:1 - error TS2339"
    },
    {
      "component": "renderer",
      "consumerOf": "unknown",
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
        assert_eq!(report.failures[0].consumer_of.as_deref(), Some("service-a"));
        assert_eq!(report.failures[1].kind, "render");
    }

    #[test]
    fn parses_failure_report_without_consumer_of() {
        let report = GateReport::parse(
            r#"
{
  "ok": false,
  "failures": [
    {
      "component": "service-a",
      "kind": "validate",
      "message": "service-a.api expected url, got string",
      "excerpt": "service-a.api expected url, got string"
    }
  ]
}
"#,
        )
        .unwrap();

        assert_eq!(report.failures[0].consumer_of, None);
        assert!(report.check_run_summary().contains("unknown"));
    }

    #[test]
    fn check_run_summary_names_consumer_and_contract_break() {
        let report = GateReport {
            ok: false,
            failures: vec![GateFailure {
                component: "service-b".to_string(),
                consumer_of: Some("service-a".to_string()),
                kind: "compile".to_string(),
                message: "service-b consumes service-a.databaseUrl which no longer exists"
                    .to_string(),
                excerpt: "error".to_string(),
            }],
        };

        let summary = report.check_run_summary();

        assert!(summary.contains("service-b"));
        assert!(summary.contains("service-a"));
        assert!(summary.contains("databaseUrl which no longer exists"));
    }
}
