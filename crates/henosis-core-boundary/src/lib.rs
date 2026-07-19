use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

use connectrpc::client::{ClientConfig, HttpClient};
use henosis_proto::connect::henosis::v1::GraphServiceClient;
use henosis_proto::proto::henosis::v1 as proto;
use tokio::sync::{Mutex, watch};

// This module is the only core-facing contract in the bot workspace.
// TODO(d26-proto): replace the transport adapter, not these domain messages, once
// the d26-core ConnectRPC schema settles.

pub use henosis_app::{
    BlockedOn, BundlePin, GraphIntent, GraphOutput, GraphPhase, GraphSourcePolicy, GraphStatus,
    GraphSummary, ResourceDisposition, SourceProvenance,
};

#[derive(Debug, thiserror::Error)]
pub enum CoreBoundaryError {
    #[error("graph `{0}` does not exist")]
    GraphNotFound(String),
    #[error("{0}")]
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
    fn list(
        &self,
        include_retired: bool,
    ) -> impl Future<Output = Result<Vec<GraphSummary>, CoreBoundaryError>> + Send;
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
                        graph_id: Some(graph),
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

    async fn list(&self, include_retired: bool) -> Result<Vec<GraphSummary>, CoreBoundaryError> {
        let response = self
            .client()?
            .list_graphs(proto::ListGraphsRequest {
                include_retired: Some(include_retired),
                ..Default::default()
            })
            .await
            .map_err(transport)?
            .into_owned();
        response
            .graphs
            .into_iter()
            .map(|summary| {
                let graph = required_nonempty(summary.graph_id, "GraphSummary.graph_id")?;
                let generation = required(
                    summary.current_generation,
                    "GraphSummary.current_generation",
                )?;
                if generation == 0 {
                    return Err(protocol(
                        "GraphSummary.current_generation must be greater than zero",
                    ));
                }
                Ok(GraphSummary {
                    graph,
                    generation,
                    phase: graph_phase_from_proto(summary.phase.as_ref())?,
                    created: required(summary.created, "GraphSummary.created")?,
                    retired: required(summary.retired, "GraphSummary.retired")?,
                })
            })
            .collect()
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

impl henosis_app::CoreClient for ConnectCoreBoundary {
    type Error = CoreBoundaryError;

    async fn status(&self, graph: &str) -> Result<Option<GraphStatus>, Self::Error> {
        match CoreBoundary::status(self, graph).await {
            Ok(status) => Ok(Some(status)),
            Err(CoreBoundaryError::GraphNotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn apply(&self, intent: GraphIntent) -> Result<GraphStatus, Self::Error> {
        CoreBoundary::apply(self, intent).await
    }
}

fn source_from_proto(
    source: Option<proto::SourceProvenance>,
) -> Result<Option<SourceProvenance>, CoreBoundaryError> {
    use proto::__buffa::oneof::source_provenance::Source;

    let Some(source) = source else {
        return Ok(None);
    };
    match source.source {
        Some(Source::Local(local)) => Ok(Some(SourceProvenance::Local {
            repository: nonempty(local.repository),
            base_revision: nonempty(local.base_revision),
            dirty: required(local.dirty, "LocalSource.dirty")?,
        })),
        Some(Source::Vcs(vcs)) => Ok(Some(SourceProvenance::Vcs {
            repository: required_nonempty(vcs.repository, "VcsSource.repository")?,
            revision: required_nonempty(vcs.revision, "VcsSource.revision")?,
            reference: nonempty(vcs.reference),
        })),
        None => Err(protocol("SourceProvenance omitted its source value")),
    }
}

fn status_from_response(
    status: Option<proto::GraphStatus>,
) -> Result<GraphStatus, CoreBoundaryError> {
    let status = required(status, "GraphService response status")?;
    let graph = required_nonempty(status.graph_id, "GraphStatus.graph_id")?;
    let generation = required(status.generation, "GraphStatus.generation")?;
    if generation == 0 {
        return Err(protocol("GraphStatus.generation must be greater than zero"));
    }

    let mut input_sources = BTreeMap::new();
    for component in &status.components {
        let consumer = required_nonempty(component.name.clone(), "ComponentStatus.name")?;
        for input in &component.inputs {
            let name = required_nonempty(input.name.clone(), "ComponentInputStatus.name")?;
            match (&input.source_component, &input.source_output) {
                (Some(producer), Some(output)) if !producer.is_empty() && !output.is_empty() => {
                    input_sources
                        .insert((consumer.clone(), name), (producer.clone(), output.clone()));
                }
                (None, None) => {}
                _ => {
                    return Err(protocol(format!(
                        "ComponentInputStatus {consumer}.{name} has an incomplete output source"
                    )));
                }
            }
        }
    }

    let plan = status.plan.into_option();
    let mut blocked_on = Vec::new();
    if let Some(plan) = &plan {
        for blocked in &plan.blocked {
            let component =
                required_nonempty(blocked.component.clone(), "BlockedComponent.component")?;
            for input in &blocked.inputs {
                if input.is_empty() {
                    return Err(protocol("BlockedComponent input name is empty"));
                }
                let source = input_sources.get(&(component.clone(), input.clone()));
                blocked_on.push(BlockedOn {
                    component: component.clone(),
                    input: input.clone(),
                    producer: source.map(|(producer, _)| producer.clone()),
                    output: source.map(|(_, output)| output.clone()),
                });
            }
        }
    }
    let planned_resources = plan.as_ref().map_or(0, |plan| plan.resources.len());

    let dispositions = status
        .dispositions
        .into_iter()
        .map(|disposition| {
            Ok(ResourceDisposition {
                resource: required_nonempty(
                    disposition.resource_id,
                    "ResourceDisposition.resource_id",
                )?,
                state: required_nonempty(disposition.state, "ResourceDisposition.state")?,
                message: nonempty(disposition.message),
            })
        })
        .collect::<Result<Vec<_>, CoreBoundaryError>>()?;
    let failed = dispositions.iter().any(|item| item.state == "failed");
    let all_ready = dispositions.len() >= planned_resources
        && dispositions.iter().all(|item| item.state == "ready");

    let outputs = status
        .outputs
        .into_iter()
        .map(|output| {
            let reference = required_nonempty(output.reference, "GraphOutput.reference")?;
            let bytes = required(
                output.canonical_value_json,
                "GraphOutput.canonical_value_json",
            )?;
            let value = serde_json::from_slice(&bytes).map_err(|error| {
                protocol(format!(
                    "GraphService output {reference:?} contains invalid JSON: {error}"
                ))
            })?;
            Ok(GraphOutput {
                reference,
                value,
                source: required_nonempty(output.source, "GraphOutput.source")?,
            })
        })
        .collect::<Result<Vec<_>, CoreBoundaryError>>()?;
    let diagnostic = nonempty(status.diagnostic).or_else(|| {
        (!status.stall_cycle.is_empty())
            .then(|| format!("stall: {}", status.stall_cycle.join(" -> ")))
    });
    let retired = required(status.retired, "GraphStatus.retired")?;
    let phase = if retired {
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
        .map(|component| {
            let component_name = required_nonempty(component.name, "ComponentStatus.name")?;
            let digest = required(component.bundle_digest, "ComponentStatus.bundle_digest")?;
            if digest.len() != 32 {
                return Err(protocol(format!(
                    "ComponentStatus {component_name} bundle_digest must contain exactly 32 bytes"
                )));
            }
            let input_bindings = component
                .input_bindings
                .into_iter()
                .map(|binding| {
                    let name = required_nonempty(binding.name, "InputBinding.name")?;
                    let bytes = required(binding.value_json, "InputBinding.value_json")?;
                    let value = serde_json::from_slice(&bytes).map_err(|error| {
                        protocol(format!(
                            "InputBinding {component_name}.{name} contains invalid JSON: {error}"
                        ))
                    })?;
                    Ok((name, value))
                })
                .collect::<Result<BTreeMap<_, _>, CoreBoundaryError>>()?;
            Ok(BundlePin {
                component: component_name,
                bundle_id: hex::encode(digest),
                input_bindings,
                source: source_from_proto(component.source.into_option())?,
            })
        })
        .collect::<Result<Vec<_>, CoreBoundaryError>>()?;
    let source_policy = match status
        .source_policy
        .as_ref()
        .and_then(buffa::EnumValue::as_known)
    {
        Some(proto::GraphSourcePolicy::AcceptLocal) => GraphSourcePolicy::AcceptLocal,
        Some(proto::GraphSourcePolicy::RequireVcs) => GraphSourcePolicy::RequireVcs,
        other => {
            return Err(protocol(format!(
                "GraphStatus has unknown source policy {other:?}"
            )));
        }
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

fn required<T>(value: Option<T>, field: &str) -> Result<T, CoreBoundaryError> {
    value.ok_or_else(|| protocol(format!("GraphService omitted required {field}")))
}

fn required_nonempty(value: Option<String>, field: &str) -> Result<String, CoreBoundaryError> {
    let value = required(value, field)?;
    if value.is_empty() {
        Err(protocol(format!(
            "GraphService returned empty required {field}"
        )))
    } else {
        Ok(value)
    }
}

fn nonempty(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.is_empty())
}

fn protocol(message: impl Into<String>) -> CoreBoundaryError {
    CoreBoundaryError::Rejected(format!("core protocol error: {}", message.into()))
}

fn graph_phase_from_proto(
    value: Option<&buffa::EnumValue<proto::GraphPhase>>,
) -> Result<GraphPhase, CoreBoundaryError> {
    match value.and_then(buffa::EnumValue::as_known) {
        Some(proto::GraphPhase::Planning) => Ok(GraphPhase::Planning),
        Some(proto::GraphPhase::Blocked) => Ok(GraphPhase::Blocked),
        Some(proto::GraphPhase::Reconciling) => Ok(GraphPhase::Reconciling),
        Some(proto::GraphPhase::Ready) => Ok(GraphPhase::Ready),
        Some(proto::GraphPhase::Failed) => Ok(GraphPhase::Failed),
        Some(proto::GraphPhase::Retired) => Ok(GraphPhase::Retired),
        other => Err(CoreBoundaryError::Rejected(format!(
            "ListGraphs returned unknown phase {other:?}"
        ))),
    }
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
        match &intent {
            GraphIntent::Create {
                bundles,
                source_policy,
                ..
            } => {
                status.bundles = bundles.clone();
                status.source_policy = *source_policy;
            }
            GraphIntent::Update { bundles, .. } => {
                status.bundles = bundles.clone();
            }
            GraphIntent::Retire { .. } => status.phase = GraphPhase::Retired,
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

    async fn list(&self, include_retired: bool) -> Result<Vec<GraphSummary>, CoreBoundaryError> {
        Ok(self
            .state
            .lock()
            .await
            .statuses
            .values()
            .map(|sender| sender.borrow().clone())
            .filter(|status| include_retired || status.phase != GraphPhase::Retired)
            .map(|status| GraphSummary {
                graph: status.graph,
                generation: status.generation,
                created: true,
                retired: status.phase == GraphPhase::Retired,
                phase: status.phase,
            })
            .collect())
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

impl henosis_app::CoreClient for FakeCoreBoundary {
    type Error = CoreBoundaryError;

    async fn status(&self, graph: &str) -> Result<Option<GraphStatus>, Self::Error> {
        match CoreBoundary::status(self, graph).await {
            Ok(status) => Ok(Some(status)),
            Err(CoreBoundaryError::GraphNotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn apply(&self, intent: GraphIntent) -> Result<GraphStatus, Self::Error> {
        CoreBoundary::apply(self, intent).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejection_diagnostic_is_rendered_verbatim() {
        let diagnostic = "error[HENOSIS_CONTRACT_SKEW]: consumer -> producer.api";
        assert_eq!(
            CoreBoundaryError::Rejected(diagnostic.to_owned()).to_string(),
            diagnostic
        );
    }

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
