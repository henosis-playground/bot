use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

use connectrpc::client::{ClientConfig, HttpClient};
use henosis_proto::connect::henosis::v1::GraphServiceClient;
use henosis_proto::proto::henosis::v1 as proto;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, watch};

// This module is the only core-facing contract in the bot workspace.
// TODO(d26-proto): replace the transport adapter, not these domain messages, once
// the d26-core ConnectRPC schema settles.

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundlePin {
    pub component: String,
    pub bundle_id: String,
    #[serde(default)]
    pub input_bindings: BTreeMap<String, serde_json::Value>,
    pub source: Option<SourceProvenance>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceProvenance {
    Local {
        repository: Option<String>,
        base_revision: Option<String>,
        dirty: bool,
    },
    Vcs {
        repository: String,
        revision: String,
        reference: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphSourcePolicy {
    #[default]
    AcceptLocal,
    RequireVcs,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphIntent {
    Create {
        graph: String,
        bundles: Vec<BundlePin>,
        source_policy: GraphSourcePolicy,
    },
    Update {
        graph: String,
        expected_generation: u64,
        bundles: Vec<BundlePin>,
    },
    Retire {
        graph: String,
    },
}

impl GraphIntent {
    pub fn graph(&self) -> &str {
        match self {
            Self::Create { graph, .. } | Self::Update { graph, .. } | Self::Retire { graph } => {
                graph
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphPhase {
    Planning,
    Blocked,
    Reconciling,
    Ready,
    Failed,
    Retired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockedOn {
    pub component: String,
    pub input: String,
    pub producer: Option<String>,
    pub output: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceDisposition {
    pub resource: String,
    pub state: String,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphOutput {
    pub reference: String,
    pub value: serde_json::Value,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphStatus {
    pub graph: String,
    pub generation: u64,
    pub phase: GraphPhase,
    pub blocked_on: Vec<BlockedOn>,
    pub outputs: Vec<GraphOutput>,
    pub observed_ready: usize,
    pub planned_resources: usize,
    pub diagnostic: Option<String>,
    pub bundles: Vec<BundlePin>,
    pub source_policy: GraphSourcePolicy,
    pub dispositions: Vec<ResourceDisposition>,
}

impl GraphStatus {
    pub fn planning(graph: impl Into<String>, generation: u64) -> Self {
        Self {
            graph: graph.into(),
            generation,
            phase: GraphPhase::Planning,
            blocked_on: Vec::new(),
            outputs: Vec::new(),
            observed_ready: 0,
            planned_resources: 0,
            diagnostic: None,
            bundles: Vec::new(),
            source_policy: GraphSourcePolicy::AcceptLocal,
            dispositions: Vec::new(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CoreBoundaryError {
    #[error("graph `{0}` does not exist")]
    GraphNotFound(String),
    #[error("core boundary rejected graph intent: {0}")]
    Rejected(String),
    #[error("cannot reach core GraphService: {0}")]
    Transport(String),
}

pub trait CoreBoundary: Send + Sync {
    fn apply(
        &self,
        intent: GraphIntent,
    ) -> impl Future<Output = Result<GraphStatus, CoreBoundaryError>> + Send;
    fn status(
        &self,
        graph: &str,
    ) -> impl Future<Output = Result<GraphStatus, CoreBoundaryError>> + Send;
    fn watch(
        &self,
        graph: &str,
    ) -> impl Future<Output = Result<watch::Receiver<GraphStatus>, CoreBoundaryError>> + Send;
}

#[derive(Debug, Clone)]
pub struct ConnectCoreBoundary {
    endpoint: String,
}

impl ConnectCoreBoundary {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
        }
    }

    fn client(&self) -> Result<GraphServiceClient<HttpClient>, CoreBoundaryError> {
        let endpoint = self
            .endpoint
            .parse()
            .map_err(|error| CoreBoundaryError::Transport(format!("invalid core URL: {error}")))?;
        Ok(GraphServiceClient::new(
            HttpClient::plaintext(),
            ClientConfig::new(endpoint),
        ))
    }

    fn component(pin: BundlePin) -> Result<proto::ComponentIntent, CoreBoundaryError> {
        let digest = hex::decode(&pin.bundle_id).map_err(|error| {
            CoreBoundaryError::Rejected(format!(
                "bundle {} has invalid hexadecimal identity: {error}",
                pin.bundle_id
            ))
        })?;
        let input_bindings = pin
            .input_bindings
            .into_iter()
            .map(|(name, value)| {
                serde_json::to_vec(&value)
                    .map(|value_json| proto::InputBinding {
                        name: Some(name),
                        value_json: Some(value_json),
                        ..Default::default()
                    })
                    .map_err(|error| CoreBoundaryError::Rejected(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(proto::ComponentIntent {
            name: Some(pin.component),
            bundle_digest: Some(digest),
            source: pin.source.map(source_to_proto).into(),
            input_bindings,
            ..Default::default()
        })
    }
}

fn source_to_proto(source: SourceProvenance) -> proto::SourceProvenance {
    use proto::__buffa::oneof::source_provenance::Source;

    let source = match source {
        SourceProvenance::Local {
            repository,
            base_revision,
            dirty,
        } => Source::Local(Box::new(proto::LocalSource {
            repository,
            base_revision,
            dirty: Some(dirty),
            ..Default::default()
        })),
        SourceProvenance::Vcs {
            repository,
            revision,
            reference,
        } => Source::Vcs(Box::new(proto::VcsSource {
            repository: Some(repository),
            revision: Some(revision),
            reference,
            ..Default::default()
        })),
    };
    proto::SourceProvenance {
        source: Some(source),
        ..Default::default()
    }
}

impl CoreBoundary for ConnectCoreBoundary {
    async fn apply(&self, intent: GraphIntent) -> Result<GraphStatus, CoreBoundaryError> {
        let client = self.client()?;
        match intent {
            GraphIntent::Create {
                graph,
                bundles,
                source_policy,
            } => {
                let response = client
                    .create_graph(proto::CreateGraphRequest {
                        graph_id: Some(graph.clone()),
                        name: Some(graph),
                        components: bundles
                            .into_iter()
                            .map(Self::component)
                            .collect::<Result<_, _>>()?,
                        source_policy: Some(
                            match source_policy {
                                GraphSourcePolicy::AcceptLocal => {
                                    proto::GraphSourcePolicy::AcceptLocal
                                }
                                GraphSourcePolicy::RequireVcs => {
                                    proto::GraphSourcePolicy::RequireVcs
                                }
                            }
                            .into(),
                        ),
                        ..Default::default()
                    })
                    .await
                    .map_err(transport)?
                    .into_owned();
                status_from_response(response.status.into_option())
            }
            GraphIntent::Update {
                graph,
                expected_generation,
                bundles,
            } => {
                let response = client
                    .update_graph(proto::UpdateGraphRequest {
                        graph_id: Some(graph),
                        expected_generation: Some(expected_generation),
                        components: bundles
                            .into_iter()
                            .map(Self::component)
                            .collect::<Result<_, _>>()?,
                        ..Default::default()
                    })
                    .await
                    .map_err(transport)?
                    .into_owned();
                status_from_response(response.status.into_option())
            }
            GraphIntent::Retire { graph } => {
                let current = self.status(&graph).await?;
                let response = client
                    .retire_graph(proto::RetireGraphRequest {
                        graph_id: Some(graph),
                        expected_generation: Some(current.generation),
                        ..Default::default()
                    })
                    .await
                    .map_err(transport)?
                    .into_owned();
                status_from_response(response.status.into_option())
            }
        }
    }

    async fn status(&self, graph: &str) -> Result<GraphStatus, CoreBoundaryError> {
        let response = self
            .client()?
            .get_graph(proto::GetGraphRequest {
                graph_id: Some(graph.to_string()),
                ..Default::default()
            })
            .await
            .map_err(|error| {
                if error.code == connectrpc::ErrorCode::NotFound {
                    CoreBoundaryError::GraphNotFound(graph.to_string())
                } else {
                    transport(error)
                }
            })?
            .into_owned();
        status_from_response(response.status.into_option())
    }

    async fn watch(&self, graph: &str) -> Result<watch::Receiver<GraphStatus>, CoreBoundaryError> {
        let mut stream = self
            .client()?
            .watch_graph(proto::WatchGraphRequest {
                graph_id: Some(graph.to_string()),
                after_sequence: Some(0),
                ..Default::default()
            })
            .await
            .map_err(transport)?;
        let first = stream
            .message::<proto::WatchGraphResponse>()
            .await
            .map_err(transport)?
            .ok_or_else(|| {
                CoreBoundaryError::Transport("core watch ended before its first status".into())
            })?
            .to_owned_message();
        let initial = status_from_response(first.status.into_option())?;
        let (sender, receiver) = watch::channel(initial);
        tokio::spawn(async move {
            loop {
                let message = match stream.message::<proto::WatchGraphResponse>().await {
                    Ok(Some(message)) => message.to_owned_message(),
                    Ok(None) | Err(_) => break,
                };
                let Ok(status) = status_from_response(message.status.into_option()) else {
                    break;
                };
                sender.send_replace(status);
            }
        });
        Ok(receiver)
    }
}

fn source_from_proto(source: Option<proto::SourceProvenance>) -> Option<SourceProvenance> {
    use proto::__buffa::oneof::source_provenance::Source;

    match source?.source? {
        Source::Local(local) => Some(SourceProvenance::Local {
            repository: local.repository.filter(|value| !value.is_empty()),
            base_revision: local.base_revision.filter(|value| !value.is_empty()),
            dirty: local.dirty.unwrap_or(false),
        }),
        Source::Vcs(vcs) => Some(SourceProvenance::Vcs {
            repository: vcs.repository.unwrap_or_default(),
            revision: vcs.revision.unwrap_or_default(),
            reference: vcs.reference.filter(|value| !value.is_empty()),
        }),
    }
}

fn status_from_response(
    status: Option<proto::GraphStatus>,
) -> Result<GraphStatus, CoreBoundaryError> {
    let status = status.ok_or_else(|| {
        CoreBoundaryError::Rejected("GraphService response omitted status".into())
    })?;
    let graph = status.graph_id.ok_or_else(|| {
        CoreBoundaryError::Rejected("GraphService status omitted graph_id".into())
    })?;
    let generation = status.generation.unwrap_or_default();
    let input_sources = status
        .components
        .iter()
        .flat_map(|component| {
            let consumer = component.name.clone().unwrap_or_default();
            component.inputs.iter().map(move |input| {
                (
                    (consumer.clone(), input.name.clone().unwrap_or_default()),
                    (
                        input.source_component.clone().unwrap_or_default(),
                        input.source_output.clone().unwrap_or_default(),
                    ),
                )
            })
        })
        .collect::<BTreeMap<_, _>>();
    let plan = status.plan.into_option();
    let blocked_on = plan
        .as_ref()
        .into_iter()
        .flat_map(|plan| &plan.blocked)
        .flat_map(|blocked| {
            blocked.inputs.iter().map(|input| {
                let component = blocked.component.clone().unwrap_or_default();
                let source = input_sources.get(&(component.clone(), input.clone()));
                BlockedOn {
                    component,
                    input: input.clone(),
                    producer: source.map(|(producer, _)| producer.clone()),
                    output: source.map(|(_, output)| output.clone()),
                }
            })
        })
        .collect::<Vec<_>>();
    let planned_resources = plan.as_ref().map_or(0, |plan| plan.resources.len());
    let dispositions = status
        .dispositions
        .into_iter()
        .map(|disposition| ResourceDisposition {
            resource: disposition.resource_id.unwrap_or_default(),
            state: disposition.state.unwrap_or_default(),
            message: disposition.message,
        })
        .collect::<Vec<_>>();
    let failed = dispositions.iter().any(|item| item.state == "failed");
    let all_ready = dispositions.len() >= planned_resources
        && dispositions.iter().all(|item| item.state == "ready");
    let outputs = status
        .outputs
        .into_iter()
        .map(|output| {
            let reference = output.reference.unwrap_or_default();
            let value =
                serde_json::from_slice(output.canonical_value_json.as_deref().unwrap_or_default())
                    .map_err(|error| {
                        CoreBoundaryError::Rejected(format!(
                            "GraphService output {reference:?} contains invalid JSON: {error}"
                        ))
                    })?;
            Ok(GraphOutput {
                reference,
                value,
                source: output.source.unwrap_or_default(),
            })
        })
        .collect::<Result<Vec<_>, CoreBoundaryError>>()?;
    let diagnostic = status.diagnostic.or_else(|| {
        (!status.stall_cycle.is_empty())
            .then(|| format!("stall: {}", status.stall_cycle.join(" -> ")))
    });
    let phase = if status.retired.unwrap_or(false) {
        GraphPhase::Retired
    } else if failed || diagnostic.is_some() {
        GraphPhase::Failed
    } else if plan.is_none() {
        GraphPhase::Planning
    } else if !blocked_on.is_empty() {
        GraphPhase::Blocked
    } else if planned_resources == 0 || all_ready {
        GraphPhase::Ready
    } else {
        GraphPhase::Reconciling
    };
    let bundles = status
        .components
        .into_iter()
        .map(|component| BundlePin {
            component: component.name.unwrap_or_default(),
            bundle_id: hex::encode(component.bundle_digest.unwrap_or_default()),
            input_bindings: component
                .input_bindings
                .into_iter()
                .filter_map(|binding| {
                    let name = binding.name?;
                    let value = serde_json::from_slice(binding.value_json.as_deref()?).ok()?;
                    Some((name, value))
                })
                .collect(),
            source: source_from_proto(component.source.into_option()),
        })
        .collect();
    let source_policy = match status
        .source_policy
        .as_ref()
        .and_then(buffa::EnumValue::as_known)
    {
        Some(proto::GraphSourcePolicy::RequireVcs) => GraphSourcePolicy::RequireVcs,
        _ => GraphSourcePolicy::AcceptLocal,
    };
    Ok(GraphStatus {
        graph,
        generation,
        phase,
        blocked_on,
        observed_ready: outputs.len(),
        outputs,
        planned_resources,
        diagnostic,
        bundles,
        source_policy,
        dispositions,
    })
}

fn transport(error: connectrpc::ConnectError) -> CoreBoundaryError {
    if error.code == connectrpc::ErrorCode::InvalidArgument {
        CoreBoundaryError::Rejected(error.message.clone().unwrap_or_else(|| error.to_string()))
    } else {
        CoreBoundaryError::Transport(error.to_string())
    }
}

#[derive(Debug, Default)]
struct FakeState {
    statuses: BTreeMap<String, watch::Sender<GraphStatus>>,
    intents: Vec<GraphIntent>,
}

#[derive(Debug, Clone, Default)]
pub struct FakeCoreBoundary {
    state: Arc<Mutex<FakeState>>,
}

impl FakeCoreBoundary {
    pub async fn intents(&self) -> Vec<GraphIntent> {
        self.state.lock().await.intents.clone()
    }

    pub async fn publish(&self, status: GraphStatus) {
        let mut state = self.state.lock().await;
        if let Some(sender) = state.statuses.get(&status.graph) {
            sender.send_replace(status);
        } else {
            let (sender, _) = watch::channel(status.clone());
            state.statuses.insert(status.graph.clone(), sender);
        }
    }
}

impl CoreBoundary for FakeCoreBoundary {
    async fn apply(&self, intent: GraphIntent) -> Result<GraphStatus, CoreBoundaryError> {
        let graph = intent.graph().to_string();
        let mut state = self.state.lock().await;
        let generation = state
            .statuses
            .get(&graph)
            .map(|sender| sender.borrow().generation + 1)
            .unwrap_or(1);
        let mut status = GraphStatus::planning(graph.clone(), generation);
        if matches!(intent, GraphIntent::Retire { .. }) {
            status.phase = GraphPhase::Retired;
        }
        state.intents.push(intent);
        if let Some(sender) = state.statuses.get(&graph) {
            sender.send_replace(status.clone());
        } else {
            let (sender, _) = watch::channel(status.clone());
            state.statuses.insert(graph, sender);
        }
        Ok(status)
    }

    async fn status(&self, graph: &str) -> Result<GraphStatus, CoreBoundaryError> {
        self.state
            .lock()
            .await
            .statuses
            .get(graph)
            .map(|sender| sender.borrow().clone())
            .ok_or_else(|| CoreBoundaryError::GraphNotFound(graph.to_string()))
    }

    async fn watch(&self, graph: &str) -> Result<watch::Receiver<GraphStatus>, CoreBoundaryError> {
        self.state
            .lock()
            .await
            .statuses
            .get(graph)
            .map(watch::Sender::subscribe)
            .ok_or_else(|| CoreBoundaryError::GraphNotFound(graph.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fake_records_intent_and_publishes_status() {
        let core = FakeCoreBoundary::default();
        let status = core
            .apply(GraphIntent::Create {
                graph: "preview_test".to_string(),
                bundles: vec![BundlePin {
                    component: "web".to_string(),
                    bundle_id: "abc".to_string(),
                    input_bindings: BTreeMap::new(),
                    source: None,
                }],
                source_policy: GraphSourcePolicy::AcceptLocal,
            })
            .await
            .unwrap();
        assert_eq!(status.generation, 1);
        assert_eq!(core.intents().await.len(), 1);
        assert_eq!(core.status("preview_test").await.unwrap(), status);
    }
}
