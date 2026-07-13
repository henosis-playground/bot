use std::collections::{BTreeMap, BTreeSet};

use anyhow::ensure;
use henosis_proto::proto::henosis::v1::{
    ComponentDispositionKind, DiagnosticSeverity, SliceReport,
};

use crate::henosis::environment::RenderStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenerationState {
    Reconciling,
    PartiallyPublished,
    Failed,
    Converged,
}

impl GenerationState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reconciling => "reconciling",
            Self::PartiallyPublished => "partially published",
            Self::Failed => "failed",
            Self::Converged => "converged",
        }
    }

    pub fn render_status(self) -> RenderStatus {
        match self {
            Self::Reconciling | Self::PartiallyPublished => RenderStatus::Pending,
            Self::Failed => RenderStatus::Failure,
            Self::Converged => RenderStatus::Success,
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Failed | Self::Converged)
    }
}

pub struct GenerationPresentation<'a> {
    pub state: GenerationState,
    pub sequence: Option<u64>,
    pub reports: Vec<&'a SliceReport>,
}

pub fn derive_generation<'a>(
    generation: u64,
    expected_components: &BTreeSet<Vec<u8>>,
    reports: &'a [SliceReport],
) -> anyhow::Result<GenerationPresentation<'a>> {
    let current_reports = reports
        .iter()
        .filter(|report| report.generation == Some(generation))
        .collect::<Vec<_>>();
    ensure!(
        current_reports
            .iter()
            .all(|report| report.connector.is_some() && report.sequence.is_some()),
        "Current-generation report omitted connector or durable sequence"
    );

    let latest_sequences =
        current_reports
            .iter()
            .fold(BTreeMap::<&str, u64>::new(), |mut latest, report| {
                let connector = report.connector.as_deref().expect("checked above");
                let sequence = report.sequence.expect("checked above");
                latest
                    .entry(connector)
                    .and_modify(|current| *current = (*current).max(sequence))
                    .or_insert(sequence);
                latest
            });
    let reports = current_reports
        .into_iter()
        .filter(|report| {
            report.sequence
                == report
                    .connector
                    .as_deref()
                    .and_then(|connector| latest_sequences.get(connector).copied())
        })
        .collect::<Vec<_>>();
    let sequence = reports.iter().filter_map(|report| report.sequence).max();

    let failed = reports.iter().any(|report| {
        report.dispositions.iter().any(|disposition| {
            disposition.kind.as_ref().and_then(|kind| kind.as_known())
                == Some(ComponentDispositionKind::Failed)
        }) || report.diagnostics.iter().any(|diagnostic| {
            diagnostic
                .severity
                .as_ref()
                .and_then(|severity| severity.as_known())
                == Some(DiagnosticSeverity::Error)
        })
    });
    let reported_components = reports
        .iter()
        .flat_map(|report| &report.dispositions)
        .filter_map(|disposition| disposition.component_spec_hash.as_deref())
        .collect::<BTreeSet<_>>();
    let ready_components = reports
        .iter()
        .flat_map(|report| &report.dispositions)
        .filter(|disposition| {
            disposition.kind.as_ref().and_then(|kind| kind.as_known())
                == Some(ComponentDispositionKind::Ready)
        })
        .filter_map(|disposition| disposition.component_spec_hash.as_deref())
        .collect::<BTreeSet<_>>();
    let expected_components = expected_components
        .iter()
        .map(Vec::as_slice)
        .collect::<BTreeSet<_>>();
    let required_connectors = reports
        .iter()
        .filter(|report| {
            report.dispositions.iter().any(|disposition| {
                disposition
                    .component_spec_hash
                    .as_deref()
                    .is_some_and(|hash| expected_components.contains(hash))
            })
        })
        .filter_map(|report| report.connector.as_deref())
        .collect::<BTreeSet<_>>();
    let published_connectors = reports
        .iter()
        .filter(|report| report.publication.is_set())
        .filter_map(|report| report.connector.as_deref())
        .collect::<BTreeSet<_>>();
    let all_ready = !expected_components.is_empty()
        && reported_components == expected_components
        && ready_components == expected_components;
    let all_published = !required_connectors.is_empty()
        && required_connectors
            .iter()
            .all(|connector| published_connectors.contains(connector));

    let state = if failed {
        GenerationState::Failed
    } else if all_ready && all_published {
        GenerationState::Converged
    } else if !published_connectors.is_empty() {
        GenerationState::PartiallyPublished
    } else {
        GenerationState::Reconciling
    };

    Ok(GenerationPresentation {
        state,
        sequence,
        reports,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use henosis_proto::proto::henosis::v1::GraphState;
    use serde_json::json;

    fn presentation(reports: serde_json::Value) -> GenerationPresentation<'static> {
        let first = vec![1_u8; 32];
        let second = vec![2_u8; 32];
        let state: GraphState = serde_json::from_value(json!({
            "durable": {
                "graph": {
                    "generation": "4",
                    "componentSpecHashes": [
                        base64::engine::general_purpose::STANDARD.encode(&first),
                        base64::engine::general_purpose::STANDARD.encode(&second)
                    ]
                },
                "lifecycle": "GRAPH_LIFECYCLE_ACTIVE"
            },
            "reports": reports
        }))
        .unwrap();
        let reports = Box::leak(state.reports.into_boxed_slice());
        derive_generation(4, &BTreeSet::from([first, second]), reports).unwrap()
    }

    #[test]
    fn derives_mixed_generation_state_matrix() {
        let first = base64::engine::general_purpose::STANDARD.encode([1_u8; 32]);
        let second = base64::engine::general_purpose::STANDARD.encode([2_u8; 32]);
        let publication = |revision: &str| json!({"revision": revision, "uri": format!("https://example.test/{revision}")});

        let reconciling = presentation(json!([{
            "generation": "4",
            "connector": "k8s",
            "sequence": "7",
            "dispositions": [{
                "componentSpecHash": &first,
                "kind": "COMPONENT_DISPOSITION_KIND_READY"
            }]
        }]));
        assert_eq!(reconciling.state, GenerationState::Reconciling);

        let partial = presentation(json!([{
            "generation": "4",
            "connector": "k8s",
            "sequence": "7",
            "dispositions": [{
                "componentSpecHash": &first,
                "kind": "COMPONENT_DISPOSITION_KIND_READY"
            }],
            "publication": publication("k8s-ready")
        }]));
        assert_eq!(partial.state, GenerationState::PartiallyPublished);

        let failed = presentation(json!([
            {
                "generation": "4",
                "connector": "k8s",
                "sequence": "7",
                "dispositions": [{
                    "componentSpecHash": &first,
                    "kind": "COMPONENT_DISPOSITION_KIND_READY"
                }],
                "publication": publication("k8s-ready")
            },
            {
                "generation": "4",
                "connector": "supabase",
                "sequence": "9",
                "dispositions": [{
                    "componentSpecHash": &second,
                    "kind": "COMPONENT_DISPOSITION_KIND_FAILED"
                }]
            }
        ]));
        assert_eq!(failed.state, GenerationState::Failed);

        let converged = presentation(json!([
            {
                "generation": "4",
                "connector": "k8s",
                "sequence": "7",
                "dispositions": [{
                    "componentSpecHash": &first,
                    "kind": "COMPONENT_DISPOSITION_KIND_READY"
                }],
                "publication": publication("k8s-ready")
            },
            {
                "generation": "4",
                "connector": "supabase",
                "sequence": "9",
                "dispositions": [{
                    "componentSpecHash": &second,
                    "kind": "COMPONENT_DISPOSITION_KIND_READY"
                }],
                "publication": publication("supabase-ready")
            }
        ]));
        assert_eq!(converged.state, GenerationState::Converged);
    }

    #[test]
    fn selects_each_connectors_newest_durable_level() {
        let first = base64::engine::general_purpose::STANDARD.encode([1_u8; 32]);
        let second = base64::engine::general_purpose::STANDARD.encode([2_u8; 32]);
        let status = presentation(json!([
            {
                "generation": "4",
                "connector": "k8s",
                "sequence": "7",
                "dispositions": [{
                    "componentSpecHash": &first,
                    "kind": "COMPONENT_DISPOSITION_KIND_FAILED"
                }]
            },
            {
                "generation": "4",
                "connector": "k8s",
                "sequence": "8",
                "dispositions": [{
                    "componentSpecHash": &first,
                    "kind": "COMPONENT_DISPOSITION_KIND_READY"
                }],
                "publication": {"revision": "k8s", "uri": "https://example.test/k8s"}
            },
            {
                "generation": "4",
                "connector": "supabase",
                "sequence": "6",
                "dispositions": [{
                    "componentSpecHash": &second,
                    "kind": "COMPONENT_DISPOSITION_KIND_READY"
                }],
                "publication": {"revision": "supabase", "uri": "https://example.test/supabase"}
            }
        ]));

        assert_eq!(status.state, GenerationState::Converged);
        assert_eq!(status.sequence, Some(8));
        assert_eq!(status.reports.len(), 2);
    }
}
