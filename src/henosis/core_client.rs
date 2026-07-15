use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

pub use henosis_core_boundary::{
    BundlePin, ConnectCoreBoundary, CoreBoundary, CoreBoundaryError, FakeCoreBoundary, GraphIntent,
    GraphPhase, GraphStatus, SourceProvenance,
};

use anyhow::Context;
use base64::Engine;
use futures::StreamExt;
use henosis_proto::proto::henosis::v1::__buffa::oneof::watch_graph_response::Item as WatchItem;
use henosis_proto::proto::henosis::v1::{
    ContractFailureKind, Diagnostic, GetGraphGenerationRequest, GetGraphGenerationResponse,
    GetGraphRequest, GetGraphResponse, Graph, GraphLifecycle, GraphState, RetireGraphRequest,
    RetireGraphResponse, SliceReport, WatchGraphRequest, WatchGraphResponse,
};
use newtype_uuid::{GenericUuid, TypedUuid, TypedUuidKind, TypedUuidTag};
use reqwest::{Client, StatusCode};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::henosis::config::CoreApiConfig;
use crate::henosis::environment::{EnvironmentIdGenerator, PublicationLink, RenderStatus};
use crate::henosis::gate_report::{GateFailure, GateReport};
use crate::henosis::generation::{GenerationState, derive_generation};
use crate::henosis::render_diagnostics::DiagnosticPresentation;

const WATCH_INITIAL_TIMEOUT: Duration = Duration::from_secs(30);

pub enum PreviewEnvironmentKind {}

impl TypedUuidKind for PreviewEnvironmentKind {
    fn tag() -> TypedUuidTag {
        const TAG: TypedUuidTag = TypedUuidTag::new("preview");
        TAG
    }
}

#[derive(Default)]
pub struct CoreEnvironmentIdGenerator;

impl EnvironmentIdGenerator for CoreEnvironmentIdGenerator {
    fn new_preview_environment_id(&self) -> String {
        TypedUuid::<PreviewEnvironmentKind>::from_untyped_uuid(uuid::Uuid::now_v7()).to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreGraphStatus {
    pub generation: u64,
    pub sequence: Option<u64>,
    pub lifecycle: GraphLifecycle,
    pub status: RenderStatus,
    pub diagnostic: Option<String>,
    pub publication: Option<PublicationLink>,
    pub failure_presentations: Vec<CoreFailurePresentation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreFailurePresentation {
    pub consumer: String,
    pub body: String,
    pub presentation: DiagnosticPresentation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UiLink {
    pub label: String,
    pub url: String,
}

pub fn ui_links_from_generation(
    record: &GetGraphGenerationResponse,
    materialized_components: &BTreeSet<String>,
) -> Vec<UiLink> {
    let value = serde_json::to_value(record).unwrap_or_default();
    ui_links_from_generation_json(&value, materialized_components)
}

pub fn borrowed_components_from_generation(record: &GetGraphGenerationResponse) -> Vec<String> {
    let value = serde_json::to_value(record).unwrap_or_default();
    let mut borrowed = value
        .get("components")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|component| {
            let name = component.pointer("/spec/name")?.as_str()?;
            let encoded = component.pointer("/spec/connectorContext")?.as_str()?;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .ok()?;
            let context: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
            let from = context.pointer("/borrow/from")?.as_str()?;
            Some(format!("{name} from `{from}`"))
        })
        .collect::<Vec<_>>();
    borrowed.sort();
    borrowed
}

fn ui_links_from_generation_json(
    record: &serde_json::Value,
    changed_components: &BTreeSet<String>,
) -> Vec<UiLink> {
    let generation = json_u64(record.pointer("/state/durable/graph/generation"));
    let components = record
        .get("components")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let names_by_hash = components
        .iter()
        .filter_map(|component| {
            Some((
                component.get("hash")?.as_str()?.to_string(),
                component.pointer("/spec/name")?.as_str()?.to_string(),
            ))
        })
        .collect::<BTreeMap<_, _>>();
    let mut materialized = names_by_hash
        .iter()
        .filter(|(_, name)| changed_components.contains(*name))
        .map(|(hash, _)| hash.clone())
        .collect::<BTreeSet<_>>();
    loop {
        let before = materialized.len();
        for component in &components {
            let Some(hash) = component.get("hash").and_then(serde_json::Value::as_str) else {
                continue;
            };
            if component
                .pointer("/spec/dependsOn")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(serde_json::Value::as_str)
                .any(|dependency| materialized.contains(dependency))
            {
                materialized.insert(hash.to_string());
            }
        }
        if materialized.len() == before {
            break;
        }
    }

    let output_levels = record
        .pointer("/state/durable/publishedOutputs")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|level| json_u64(level.get("generation")) == generation)
        .collect::<Vec<_>>();
    let latest_sequences =
        output_levels
            .iter()
            .fold(BTreeMap::<&str, u64>::new(), |mut latest, level| {
                if let (Some(connector), Some(sequence)) = (
                    level.get("connector").and_then(serde_json::Value::as_str),
                    json_u64(level.get("publicationSequence")),
                ) {
                    latest
                        .entry(connector)
                        .and_modify(|current| *current = (*current).max(sequence))
                        .or_insert(sequence);
                }
                latest
            });
    let values_by_hash = output_levels
        .into_iter()
        .filter(|level| {
            level
                .get("connector")
                .and_then(serde_json::Value::as_str)
                .and_then(|connector| latest_sequences.get(connector))
                .copied()
                == json_u64(level.get("publicationSequence"))
        })
        .flat_map(|level| {
            level
                .get("outputs")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
        })
        .filter_map(|output| {
            let hash = output.get("componentSpecHash")?.as_str()?.to_string();
            let encoded = output.get("valuesJson")?.as_str()?;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .ok()?;
            Some((
                hash,
                serde_json::from_slice::<serde_json::Value>(&bytes).ok()?,
            ))
        })
        .collect::<BTreeMap<_, _>>();

    let mut links = Vec::new();
    for component in &components {
        let Some(hash) = component.get("hash").and_then(serde_json::Value::as_str) else {
            continue;
        };
        if !materialized.contains(hash) {
            continue;
        }
        let Some(name) = names_by_hash.get(hash) else {
            continue;
        };
        let Some(schema) = component
            .pointer("/spec/outputsSchema")
            .and_then(serde_json::Value::as_str)
            .and_then(|encoded| {
                base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .ok()
            })
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
        else {
            continue;
        };
        let Some(values) = values_by_hash.get(hash) else {
            continue;
        };
        collect_ui_links(name, &schema, values, &mut Vec::new(), &mut links);
    }
    links.sort_by(|left, right| left.label.cmp(&right.label).then(left.url.cmp(&right.url)));
    links
}

fn collect_ui_links(
    component: &str,
    schema: &serde_json::Value,
    value: &serde_json::Value,
    path: &mut Vec<String>,
    links: &mut Vec<UiLink>,
) {
    if schema.get("role").and_then(serde_json::Value::as_str) == Some("ui") {
        if let Some(url) = value.as_str()
            && url::Url::parse(url).is_ok_and(|url| matches!(url.scheme(), "http" | "https"))
        {
            links.push(UiLink {
                label: format!("{component}.{}", path.join(".")),
                url: url.to_string(),
            });
        }
        return;
    }
    let Some(shape) = schema.get("shape").and_then(serde_json::Value::as_object) else {
        return;
    };
    for (name, child_schema) in shape {
        let Some(child_value) = value.get(name) else {
            continue;
        };
        path.push(name.clone());
        collect_ui_links(component, child_schema, child_value, path, links);
        path.pop();
    }
}

fn json_u64(value: Option<&serde_json::Value>) -> Option<u64> {
    value.and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
}

#[derive(Clone)]
pub struct CoreClient {
    endpoint: String,
    presentation_endpoint: String,
    token: String,
    http: Client,
}

impl CoreClient {
    pub fn new(config: &CoreApiConfig) -> anyhow::Result<Self> {
        let endpoint = config.endpoint.trim_end_matches('/').to_string();
        anyhow::ensure!(
            !endpoint.is_empty(),
            "Henosis core endpoint cannot be empty"
        );
        reqwest::Url::parse(&endpoint)
            .with_context(|| format!("Invalid Henosis core endpoint `{endpoint}`"))?;
        let presentation_endpoint = config
            .presentation_endpoint
            .as_deref()
            .unwrap_or(&endpoint)
            .trim_end_matches('/')
            .to_string();
        anyhow::ensure!(
            !presentation_endpoint.is_empty(),
            "Henosis core presentation endpoint cannot be empty"
        );
        reqwest::Url::parse(&presentation_endpoint).with_context(|| {
            format!("Invalid Henosis core presentation endpoint `{presentation_endpoint}`")
        })?;
        anyhow::ensure!(
            !config.token.expose().is_empty(),
            "Henosis core token cannot be empty"
        );
        Ok(Self {
            endpoint,
            presentation_endpoint,
            token: config.token.expose().to_string(),
            http: Client::new(),
        })
    }

    pub fn graph_url(&self, environment_id: &str) -> String {
        format!("{}/graphs/{environment_id}", self.presentation_endpoint)
    }

    pub fn generation_url(&self, environment_id: &str, generation: u64) -> String {
        format!(
            "{}/graphs/{environment_id}/generations/{generation}",
            self.presentation_endpoint
        )
    }

    pub async fn retire_graph(&self, environment_id: &str) -> anyhow::Result<()> {
        let graph_id = graph_id_bytes(environment_id)?;
        let Some(state) = self.get_graph_by_id(&graph_id).await? else {
            return Ok(());
        };
        let generation = current_graph(&state)?
            .generation
            .context("GetGraph response omitted graph generation")?;
        let request = RetireGraphRequest {
            graph_id: Some(graph_id),
            expected_generation: Some(generation),
            request_id: Some(new_request_id()),
            ..Default::default()
        };
        let _: RetireGraphResponse = self
            .unary("RetireGraph", &request)
            .await
            .context("Cannot retire Henosis core graph")?;
        Ok(())
    }

    pub async fn get_graph(&self, environment_id: &str) -> anyhow::Result<Option<GraphState>> {
        self.get_graph_by_id(&graph_id_bytes(environment_id)?).await
    }

    pub async fn get_graph_generation(
        &self,
        environment_id: &str,
        generation: u64,
    ) -> anyhow::Result<Option<GetGraphGenerationResponse>> {
        let request = GetGraphGenerationRequest {
            graph_id: Some(graph_id_bytes(environment_id)?),
            generation: Some(generation),
            ..Default::default()
        };
        match self
            .unary::<_, GetGraphGenerationResponse>("GetGraphGeneration", &request)
            .await
        {
            Ok(response) => Ok(Some(response)),
            Err(error)
                if error
                    .downcast_ref::<CoreApiError>()
                    .is_some_and(CoreApiError::not_found) =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    pub async fn watch_graph(&self, environment_id: &str) -> anyhow::Result<GraphState> {
        let request = WatchGraphRequest {
            graph_id: Some(graph_id_bytes(environment_id)?),
            ..Default::default()
        };
        let payload = serde_json::to_vec(&request).context("Cannot encode WatchGraph request")?;
        let mut framed = Vec::with_capacity(payload.len() + 5);
        framed.push(0);
        framed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        framed.extend_from_slice(&payload);

        let response = self
            .http
            .post(self.rpc_url("WatchGraph"))
            .bearer_auth(&self.token)
            .header("connect-protocol-version", "1")
            .header("content-type", "application/connect+json")
            .header("accept", "application/connect+json")
            .body(framed)
            .send()
            .await
            .context("Cannot call Henosis core WatchGraph")?;
        if !response.status().is_success() {
            return Err(core_http_error(response).await);
        }

        let mut stream = response.bytes_stream();
        let mut buffered = Vec::new();
        let mut durable = None;
        let deadline = tokio::time::Instant::now() + WATCH_INITIAL_TIMEOUT;
        loop {
            while let Some((flags, payload)) = take_connect_envelope(&mut buffered)? {
                if flags & 0x02 != 0 {
                    return watched_state(durable, Vec::new())
                        .context("WatchGraph ended before returning graph state");
                }
                let response: WatchGraphResponse = serde_json::from_slice(&payload)
                    .context("Cannot decode WatchGraph response envelope")?;
                match response.item {
                    Some(WatchItem::Snapshot(snapshot)) => {
                        durable = Some((
                            snapshot
                                .sequence
                                .context("WatchGraph snapshot omitted durable sequence")?,
                            snapshot
                                .state
                                .into_option()
                                .context("WatchGraph snapshot omitted durable state")?,
                        ));
                    }
                    Some(WatchItem::Change(change)) => {
                        durable = Some((
                            change
                                .sequence
                                .context("WatchGraph change omitted durable sequence")?,
                            change
                                .state
                                .into_option()
                                .context("WatchGraph change omitted durable state")?,
                        ));
                    }
                    Some(WatchItem::VolatileStatus(status)) => {
                        let delivered_sequence = status.delivered_sequence.context(
                            "WatchGraph volatile status omitted delivered durable sequence",
                        )?;
                        let (durable_sequence, durable_state) = durable
                            .as_ref()
                            .context("WatchGraph returned volatile status before durable state")?;
                        anyhow::ensure!(
                            delivered_sequence <= *durable_sequence,
                            "WatchGraph volatile status is ahead of its durable state"
                        );
                        if delivered_sequence == *durable_sequence {
                            let state = watched_state(
                                Some((*durable_sequence, durable_state.clone())),
                                status.reports,
                            )
                            .expect("durable state is present");
                            if graph_status(&state)?.status != RenderStatus::Pending {
                                return Ok(state);
                            }
                        }
                    }
                    Some(WatchItem::Progress(_)) | None => {}
                }
            }

            let next = tokio::time::timeout_at(deadline, stream.next())
                .await
                .context("Timed out waiting for terminal WatchGraph state")?;
            match next {
                Some(Ok(chunk)) => buffered.extend_from_slice(&chunk),
                Some(Err(error)) => return Err(error).context("Cannot read WatchGraph stream"),
                None => {
                    return watched_state(durable, Vec::new())
                        .context("WatchGraph ended before returning graph state");
                }
            }
        }
    }

    pub fn graph_status(&self, state: &GraphState) -> anyhow::Result<CoreGraphStatus> {
        graph_status(state)
    }

    async fn get_graph_by_id(&self, graph_id: &[u8]) -> anyhow::Result<Option<GraphState>> {
        let request = GetGraphRequest {
            graph_id: Some(graph_id.to_vec()),
            ..Default::default()
        };
        match self
            .unary::<_, GetGraphResponse>("GetGraph", &request)
            .await
        {
            Ok(response) => {
                anyhow::ensure!(
                    response.state.is_set(),
                    "GetGraph response omitted graph state"
                );
                Ok(Some(response.state.into_option().expect("checked above")))
            }
            Err(error)
                if error
                    .downcast_ref::<CoreApiError>()
                    .is_some_and(CoreApiError::not_found) =>
            {
                Ok(None)
            }
            Err(error) => Err(error).context("Cannot get Henosis core graph"),
        }
    }

    async fn unary<Request, Response>(
        &self,
        method: &str,
        request: &Request,
    ) -> anyhow::Result<Response>
    where
        Request: Serialize + ?Sized,
        Response: DeserializeOwned,
    {
        let response = self
            .http
            .post(self.rpc_url(method))
            .bearer_auth(&self.token)
            .header("connect-protocol-version", "1")
            .header("content-type", "application/json")
            .json(request)
            .send()
            .await
            .with_context(|| format!("Cannot call Henosis core {method}"))?;
        if !response.status().is_success() {
            return Err(core_http_error(response).await);
        }
        response
            .json()
            .await
            .with_context(|| format!("Cannot decode Henosis core {method} response"))
    }

    fn rpc_url(&self, method: &str) -> String {
        format!("{}/henosis.v1.GraphService/{method}", self.endpoint)
    }
}

fn graph_id_bytes(environment_id: &str) -> anyhow::Result<Vec<u8>> {
    let id: TypedUuid<PreviewEnvironmentKind> = environment_id
        .parse()
        .with_context(|| format!("Invalid core preview environment id `{environment_id}`"))?;
    Ok(id.into_bytes().to_vec())
}

fn new_request_id() -> Vec<u8> {
    uuid::Uuid::new_v4().into_bytes().to_vec()
}

fn current_graph(state: &GraphState) -> anyhow::Result<&Graph> {
    anyhow::ensure!(state.durable.is_set(), "Graph state omitted durable state");
    let lifecycle = state
        .durable
        .lifecycle
        .as_ref()
        .and_then(|lifecycle| lifecycle.as_known())
        .context("Graph state omitted a known lifecycle")?;
    anyhow::ensure!(
        lifecycle == GraphLifecycle::Active,
        "Graph state is not active"
    );
    anyhow::ensure!(state.durable.graph.is_set(), "Graph state omitted graph");
    Ok(&state.durable.graph)
}

fn graph_status(state: &GraphState) -> anyhow::Result<CoreGraphStatus> {
    let graph = current_graph(state)?;
    let lifecycle = state
        .durable
        .lifecycle
        .as_ref()
        .and_then(|lifecycle| lifecycle.as_known())
        .expect("current_graph checked lifecycle");
    let generation = graph
        .generation
        .context("Graph state omitted graph generation")?;
    let expected_components = graph
        .component_spec_hashes
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let presentation = derive_generation(generation, &expected_components, &state.reports)?;
    let (diagnostic, failure_presentations) = format_diagnostics(&presentation.reports);
    let publication = if presentation.state == GenerationState::Converged {
        let mut publications = presentation.reports.iter().filter_map(|report| {
            report.publication.as_option().and_then(|publication| {
                Some(PublicationLink {
                    revision: publication.revision.as_deref()?.to_string(),
                    url: publication.uri.as_deref()?.to_string(),
                })
            })
        });
        let publication = publications.next();
        publication.filter(|_| publications.next().is_none())
    } else {
        None
    };
    let status = presentation.state.render_status();
    let sequence = presentation.sequence;
    Ok(CoreGraphStatus {
        generation,
        sequence,
        lifecycle,
        status,
        diagnostic,
        publication,
        failure_presentations,
    })
}

fn format_diagnostics(reports: &[&SliceReport]) -> (Option<String>, Vec<CoreFailurePresentation>) {
    let diagnostics = reports
        .iter()
        .flat_map(|report| &report.diagnostics)
        .collect::<Vec<_>>();
    let contract_failures = diagnostics
        .iter()
        .filter_map(|diagnostic| gate_failure(diagnostic))
        .collect::<Vec<_>>();
    let mut failures = contract_failures
        .into_iter()
        .map(|failure| CoreFailurePresentation {
            consumer: failure.consumer.clone(),
            body: GateReport {
                ok: false,
                failures: vec![failure],
            }
            .pr_comment(),
            presentation: DiagnosticPresentation::Markdown,
        })
        .collect::<Vec<_>>();
    failures.extend(
        diagnostics
            .into_iter()
            .filter(|diagnostic| !diagnostic.contract_failure.is_set())
            .map(|diagnostic| CoreFailurePresentation {
                consumer: "environment".to_string(),
                body: format_diagnostic(diagnostic),
                presentation: DiagnosticPresentation::RawText,
            }),
    );
    let combined = (!failures.is_empty()).then(|| {
        failures
            .iter()
            .map(|failure| failure.body.as_str())
            .collect::<Vec<_>>()
            .join("\n\n---\n\n")
    });
    (combined, failures)
}

fn gate_failure(diagnostic: &Diagnostic) -> Option<GateFailure> {
    let detail = diagnostic.contract_failure.as_option()?;
    let kind = match detail.kind.as_ref()?.as_known()? {
        ContractFailureKind::Compile => "compile",
        ContractFailureKind::Render => "render",
        ContractFailureKind::Validate => "validate",
        ContractFailureKind::Resolve => "resolve",
        ContractFailureKind::CONTRACT_FAILURE_KIND_UNSPECIFIED => return None,
    };
    Some(GateFailure {
        consumer: detail.consumer.as_deref()?.to_string(),
        producer: detail.producer.as_deref()?.to_string(),
        pinned_sha: detail.pinned_sha.clone(),
        resolved_sha: detail.resolved_sha.clone(),
        outputs_schema_at_pinned: detail
            .outputs_schema_at_pinned_json
            .as_deref()
            .filter(|bytes| !bytes.is_empty())
            .and_then(|bytes| serde_json::from_slice(bytes).ok()),
        outputs_schema_at_resolved: detail
            .outputs_schema_at_resolved_json
            .as_deref()
            .filter(|bytes| !bytes.is_empty())
            .and_then(|bytes| serde_json::from_slice(bytes).ok()),
        consumed_paths: detail.consumed_paths.clone(),
        kind: kind.to_string(),
        message: diagnostic
            .message
            .as_deref()
            .unwrap_or_default()
            .to_string(),
        excerpt: detail.excerpt.clone().unwrap_or_default(),
        source_url: detail.source_url.clone(),
    })
}

fn format_diagnostic(diagnostic: &Diagnostic) -> String {
    let code = diagnostic.code.as_deref().unwrap_or("core.unknown");
    let message = diagnostic
        .message
        .as_deref()
        .unwrap_or("Henosis reconciliation failed");
    let mut rendered = format!("{code}: {message}");
    if let Some(pointer) = diagnostic
        .pointer
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        rendered.push_str(&format!("\n  at {pointer}"));
    }
    if let Some(help) = diagnostic.help.as_deref().filter(|value| !value.is_empty()) {
        rendered.push_str(&format!("\n  help: {help}"));
    }
    rendered
}

fn take_connect_envelope(buffer: &mut Vec<u8>) -> anyhow::Result<Option<(u8, Vec<u8>)>> {
    if buffer.len() < 5 {
        return Ok(None);
    }
    let flags = buffer[0];
    anyhow::ensure!(
        flags & !0x03 == 0,
        "WatchGraph returned unsupported envelope flags"
    );
    let length = u32::from_be_bytes(buffer[1..5].try_into().expect("fixed length")) as usize;
    if buffer.len() < length + 5 {
        return Ok(None);
    }
    let payload = buffer[5..length + 5].to_vec();
    buffer.drain(..length + 5);
    Ok(Some((flags, payload)))
}

fn watched_state(
    durable: Option<(u64, henosis_proto::proto::henosis::v1::DurableGraphState)>,
    reports: Vec<SliceReport>,
) -> Option<GraphState> {
    durable.map(|(sequence, durable)| GraphState {
        durable: durable.into(),
        reports: reports
            .into_iter()
            .filter(|report| report.sequence == Some(sequence))
            .collect(),
        ..Default::default()
    })
}

#[derive(Debug, serde::Deserialize)]
struct ConnectErrorBody {
    code: Option<String>,
    message: Option<String>,
}

#[derive(Debug, thiserror::Error)]
#[error("Henosis core returned {status} ({code}): {message}")]
struct CoreApiError {
    status: StatusCode,
    code: String,
    message: String,
}

impl CoreApiError {
    fn not_found(&self) -> bool {
        self.code == "not_found" || self.status == StatusCode::NOT_FOUND
    }
}

async fn core_http_error(response: reqwest::Response) -> anyhow::Error {
    let status = response.status();
    let body = response.json::<ConnectErrorBody>().await.ok();
    CoreApiError {
        status,
        code: body
            .as_ref()
            .and_then(|body| body.code.clone())
            .unwrap_or_else(|| "unknown".to_string()),
        message: body
            .and_then(|body| body.message)
            .unwrap_or_else(|| "no error message".to_string()),
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    #[test]
    fn core_preview_ids_are_canonical_uuid_v7_typeids() {
        let id = CoreEnvironmentIdGenerator.new_preview_environment_id();
        let parsed: TypedUuid<PreviewEnvironmentKind> = id.parse().unwrap();
        assert_eq!(parsed.get_version_num(), 7);
        assert_eq!(parsed.to_string(), id);
    }

    #[test]
    fn mutation_request_ids_are_unique_raw_uuid_v4_values() {
        let first = new_request_id();
        let second = new_request_id();
        assert_ne!(first, second);
        assert_eq!(uuid::Uuid::from_slice(&first).unwrap().get_version_num(), 4);
        assert_eq!(
            uuid::Uuid::from_slice(&second).unwrap().get_version_num(),
            4
        );
    }

    #[test]
    fn status_uses_only_the_newest_durable_report_level() {
        let first = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [1_u8; 32]);
        let second = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [2_u8; 32]);
        let state: GraphState = serde_json::from_value(json!({
            "durable": {
                "graph": {
                    "id": base64::Engine::encode(
                        &base64::engine::general_purpose::STANDARD,
                        [3_u8; 16]
                    ),
                    "generation": "4",
                    "componentSpecHashes": [&first, &second]
                },
                "lifecycle": "GRAPH_LIFECYCLE_ACTIVE"
            },
            "reports": [
                {
                    "generation": "4",
                    "connector": "k8s",
                    "sequence": "7",
                    "dispositions": [{
                        "componentSpecHash": &first,
                        "kind": "COMPONENT_DISPOSITION_KIND_FAILED"
                    }],
                    "diagnostics": [{
                        "code": "stale.failure",
                        "severity": "DIAGNOSTIC_SEVERITY_ERROR"
                    }]
                },
                {
                    "generation": "4",
                    "connector": "k8s",
                    "sequence": "8",
                    "dispositions": [
                        {
                            "componentSpecHash": &first,
                            "kind": "COMPONENT_DISPOSITION_KIND_READY"
                        },
                        {
                            "componentSpecHash": &second,
                            "kind": "COMPONENT_DISPOSITION_KIND_READY"
                        }
                    ],
                    "publication": {
                        "revision": "ready",
                        "uri": "https://example.test/ready"
                    }
                }
            ]
        }))
        .unwrap();

        let status = graph_status(&state).unwrap();
        assert_eq!(status.generation, 4);
        assert_eq!(status.sequence, Some(8));
        assert_eq!(status.lifecycle, GraphLifecycle::Active);
        assert_eq!(status.status, RenderStatus::Success);
        assert_eq!(status.diagnostic, None);
    }

    #[test]
    fn status_aggregates_failures_and_diagnostics_across_connectors() {
        let first = base64::engine::general_purpose::STANDARD.encode([1_u8; 32]);
        let second = base64::engine::general_purpose::STANDARD.encode([2_u8; 32]);
        let state: GraphState = serde_json::from_value(json!({
            "durable": {
                "graph": {
                    "generation": "2",
                    "componentSpecHashes": [&first, &second]
                },
                "lifecycle": "GRAPH_LIFECYCLE_ACTIVE"
            },
            "reports": [
                {
                    "generation": "2",
                    "connector": "k8s",
                    "sequence": "7",
                    "dispositions": [{
                        "componentSpecHash": &first,
                        "kind": "COMPONENT_DISPOSITION_KIND_READY"
                    }],
                    "publication": {
                        "revision": "k8s-ready",
                        "uri": "https://example.test/k8s-ready"
                    }
                },
                {
                    "generation": "2",
                    "connector": "supabase",
                    "sequence": "9",
                    "dispositions": [{
                        "componentSpecHash": &second,
                        "kind": "COMPONENT_DISPOSITION_KIND_FAILED"
                    }],
                    "diagnostics": [{
                        "code": "supabase.plan.migration-checksum",
                        "message": "migration checksum does not match",
                        "severity": "DIAGNOSTIC_SEVERITY_ERROR"
                    }]
                }
            ]
        }))
        .unwrap();

        let status = graph_status(&state).unwrap();
        assert_eq!(status.status, RenderStatus::Failure);
        assert_eq!(status.sequence, Some(9));
        assert_eq!(status.publication, None);
        assert_eq!(
            status.diagnostic.as_deref(),
            Some("supabase.plan.migration-checksum: migration checksum does not match")
        );
        assert_eq!(status.failure_presentations.len(), 1);
    }

    #[test]
    fn status_rejects_retired_graph_lifecycle() {
        let state: GraphState = serde_json::from_value(json!({
            "durable": {
                "graph": {
                    "id": base64::Engine::encode(
                        &base64::engine::general_purpose::STANDARD,
                        [4_u8; 16]
                    ),
                    "generation": "1"
                },
                "lifecycle": "GRAPH_LIFECYCLE_RETIRED"
            }
        }))
        .unwrap();

        assert_eq!(
            graph_status(&state).unwrap_err().to_string(),
            "Graph state is not active"
        );
    }

    #[test]
    fn borrowed_components_are_exposed_for_status_presentation() {
        let context = base64::engine::general_purpose::STANDARD.encode(
            serde_json::to_vec(&json!({
                "borrow": {"from": "dev", "effectiveEnvironment": {"id": "dev"}}
            }))
            .unwrap(),
        );
        let record: GetGraphGenerationResponse = serde_json::from_value(json!({
            "components": [
                {"spec": {"name": "service-b", "connectorContext": context}},
                {"spec": {"name": "service-a", "connectorContext": context}}
            ]
        }))
        .unwrap();

        assert_eq!(
            borrowed_components_from_generation(&record),
            ["service-a from `dev`", "service-b from `dev`"]
        );
    }

    #[test]
    fn ui_links_use_published_outputs_from_materialized_preview_closure_only() {
        let first = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [1_u8; 32]);
        let second = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [2_u8; 32]);
        let schema = |name: &str| {
            base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                serde_json::to_vec(&json!({
                    "kind": "object",
                    "shape": {name: {"kind": "url", "role": "ui"}}
                }))
                .unwrap(),
            )
        };
        let values = |value: Value| {
            base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                serde_json::to_vec(&value).unwrap(),
            )
        };
        let record = json!({
            "state": {
                "durable": {
                    "graph": {"generation": "3"},
                    "publishedOutputs": [
                        {
                            "generation": "3",
                            "connector": "k8s",
                            "publicationSequence": "9",
                            "outputs": [
                                {"componentSpecHash": first, "valuesJson": values(json!({"admin": "https://a.example/admin"}))}
                            ]
                        },
                        {
                            "generation": "3",
                            "connector": "supabase",
                            "publicationSequence": "11",
                            "outputs": [
                                {"componentSpecHash": second, "valuesJson": values(json!({"app": "https://b.example/app"}))}
                            ]
                        }
                    ]
                }
            },
            "components": [
                {"hash": first, "spec": {"name": "service-a", "outputsSchema": schema("admin")}},
                {"hash": second, "spec": {"name": "service-b", "dependsOn": [first], "outputsSchema": schema("app")}}
            ]
        });

        assert_eq!(
            ui_links_from_generation_json(&record, &BTreeSet::from(["service-b".to_string()])),
            vec![UiLink {
                label: "service-b.app".to_string(),
                url: "https://b.example/app".to_string(),
            }]
        );
        assert_eq!(
            ui_links_from_generation_json(&record, &BTreeSet::from(["service-a".to_string()])),
            vec![
                UiLink {
                    label: "service-a.admin".to_string(),
                    url: "https://a.example/admin".to_string(),
                },
                UiLink {
                    label: "service-b.app".to_string(),
                    url: "https://b.example/app".to_string(),
                },
            ]
        );
    }
}
