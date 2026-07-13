use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use anyhow::{Context, anyhow};
use base64::Engine;
use futures::StreamExt;
use henosis_proto::proto::henosis::v1::__buffa::oneof::watch_graph_response::Item as WatchItem;
use henosis_proto::proto::henosis::v1::{
    AddComponentsRequest, AddComponentsResponse, ComponentReplacement, ComponentSpec,
    ContractFailureKind, CreateGraphRequest, CreateGraphResponse, Diagnostic,
    GetGraphGenerationRequest, GetGraphGenerationResponse, GetGraphRequest, GetGraphResponse,
    Graph, GraphLifecycle, GraphState, RegisterComponentSpecRequest, RegisterComponentSpecResponse,
    RemoveComponentsRequest, RemoveComponentsResponse, RetireGraphRequest, RetireGraphResponse,
    SliceReport, UpdateComponentsRequest, UpdateComponentsResponse, WatchGraphRequest,
    WatchGraphResponse,
};
use newtype_uuid::{GenericUuid, TypedUuid, TypedUuidKind, TypedUuidTag};
use reqwest::{Client, StatusCode};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::henosis::config::{CoreApiConfig, RegisteredComponent};
use crate::henosis::environment::{
    DeployRepoWriter, DeployWriteResult, EnvironmentIdGenerator, PublicationLink, RenderStatus,
};
use crate::henosis::gate_report::{GateFailure, GateReport};
use crate::henosis::generation::{GenerationState, derive_generation};
use crate::henosis::graph::ComponentPackageReader;
use crate::henosis::manifest::{self, ComponentEntry, Manifest, PinnedEntry};
use crate::henosis::render_diagnostics::DiagnosticPresentation;

const COMPONENT_SPEC_INSPECTION_API_VERSION: &str = "henosis.dev/component-spec-inspection/v1";
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlannedComponentSpec {
    name: String,
    connector: String,
    dependencies: Vec<String>,
    outputs_schema: Vec<u8>,
    connector_context: Vec<u8>,
    dependency_spec_hash_slots: Vec<DependencySpecHashSlot>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ComponentSpecInspection {
    api_version: String,
    components: BTreeMap<String, InspectedComponentSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InspectedComponentSpec {
    connector: String,
    #[serde(default)]
    dependencies: Vec<String>,
    outputs_schema: String,
    connector_context: String,
    #[serde(default)]
    dependency_spec_hash_slots: Vec<DependencySpecHashSlot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DependencySpecHashSlot {
    component: String,
    pointer: String,
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

    async fn apply_graph(
        &self,
        environment_id: &str,
        specs: Vec<PlannedComponentSpec>,
    ) -> anyhow::Result<u64> {
        let graph_id = graph_id_bytes(environment_id)?;
        let component_spec_hashes = self.register_component_specs(specs).await?;
        let Some(state) = self.get_graph_by_id(&graph_id).await? else {
            let request = CreateGraphRequest {
                graph_id: Some(graph_id),
                component_spec_hashes,
                request_id: Some(new_request_id()),
                ..Default::default()
            };
            let response: CreateGraphResponse = self
                .unary("CreateGraph", &request)
                .await
                .context("Cannot create Henosis core graph")?;
            return graph_generation(&response.graph)
                .context("CreateGraph response omitted the accepted graph generation");
        };

        self.edit_graph(graph_id, state, component_spec_hashes)
            .await
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

    async fn edit_graph(
        &self,
        graph_id: Vec<u8>,
        state: GraphState,
        desired: Vec<Vec<u8>>,
    ) -> anyhow::Result<u64> {
        let graph = current_graph(&state)?;
        let mut generation = graph
            .generation
            .context("GetGraph response omitted graph generation")?;
        let existing = graph
            .component_spec_hashes
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let desired = desired.into_iter().collect::<BTreeSet<_>>();
        let additions = desired.difference(&existing).cloned().collect::<Vec<_>>();
        let removals = existing.difference(&desired).cloned().collect::<Vec<_>>();

        if additions.len() == removals.len() && !additions.is_empty() {
            let replacements = removals
                .into_iter()
                .zip(additions)
                .map(
                    |(current_spec_hash, replacement_spec_hash)| ComponentReplacement {
                        current_spec_hash: Some(current_spec_hash),
                        replacement_spec_hash: Some(replacement_spec_hash),
                        ..Default::default()
                    },
                )
                .collect();
            let request = UpdateComponentsRequest {
                graph_id: Some(graph_id),
                expected_generation: Some(generation),
                replacements,
                request_id: Some(new_request_id()),
                ..Default::default()
            };
            let _: UpdateComponentsResponse = self
                .unary("UpdateComponents", &request)
                .await
                .context("Cannot replace Henosis core graph component specs")?;
            return Ok(generation + 1);
        }

        if !additions.is_empty() {
            let request = AddComponentsRequest {
                graph_id: Some(graph_id.clone()),
                expected_generation: Some(generation),
                component_spec_hashes: additions,
                request_id: Some(new_request_id()),
                ..Default::default()
            };
            let _: AddComponentsResponse = self
                .unary("AddComponents", &request)
                .await
                .context("Cannot add Henosis core graph components")?;
            generation += 1;
        }
        if !removals.is_empty() {
            let request = RemoveComponentsRequest {
                graph_id: Some(graph_id),
                expected_generation: Some(generation),
                component_spec_hashes: removals,
                request_id: Some(new_request_id()),
                ..Default::default()
            };
            let _: RemoveComponentsResponse = self
                .unary("RemoveComponents", &request)
                .await
                .context("Cannot remove Henosis core graph components")?;
            generation += 1;
        }

        Ok(generation)
    }

    async fn register_component_specs(
        &self,
        mut pending: Vec<PlannedComponentSpec>,
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        let mut hashes = BTreeMap::new();
        while !pending.is_empty() {
            let index = pending
                .iter()
                .position(|spec| {
                    spec.dependencies
                        .iter()
                        .all(|dependency| hashes.contains_key(dependency))
                })
                .context("Component specs could not be ordered by their dependencies")?;
            let planned = pending.remove(index);
            let mut depends_on = planned
                .dependencies
                .iter()
                .map(|dependency| {
                    hashes.get(dependency).cloned().with_context(|| {
                        format!("Component spec dependency `{dependency}` was not registered")
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            depends_on.sort_unstable();
            let connector_context = materialize_connector_context(
                &planned.name,
                planned.connector_context,
                &planned.dependency_spec_hash_slots,
                &planned.dependencies,
                &hashes,
            )?;
            let spec = ComponentSpec {
                name: Some(planned.name.clone()),
                connector: Some(planned.connector.clone()),
                outputs_schema: Some(planned.outputs_schema),
                depends_on,
                connector_context: Some(connector_context),
                ..Default::default()
            };
            let request = RegisterComponentSpecRequest {
                spec: spec.clone().into(),
                ..Default::default()
            };
            let response: RegisterComponentSpecResponse = self
                .unary("RegisterComponentSpec", &request)
                .await
                .with_context(|| {
                    format!(
                        "Cannot register Henosis core component spec `{}`",
                        planned.name
                    )
                })?;
            let registered = response
                .component
                .into_option()
                .context("RegisterComponentSpec response omitted registered component")?;
            let returned_spec = registered
                .spec
                .into_option()
                .context("RegisterComponentSpec response omitted immutable spec body")?;
            anyhow::ensure!(
                returned_spec == spec,
                "RegisterComponentSpec returned a different immutable spec body"
            );
            let hash = registered
                .hash
                .context("RegisterComponentSpec response omitted content hash")?;
            anyhow::ensure!(
                hash.len() == 32,
                "RegisterComponentSpec returned a non-BLAKE3 content hash"
            );
            anyhow::ensure!(
                hashes.insert(planned.name.clone(), hash).is_none(),
                "Component spec name `{}` was planned more than once",
                planned.name
            );
        }
        Ok(hashes.into_values().collect())
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

fn materialize_connector_context(
    component: &str,
    context: Vec<u8>,
    slots: &[DependencySpecHashSlot],
    dependencies: &[String],
    hashes: &BTreeMap<String, Vec<u8>>,
) -> anyhow::Result<Vec<u8>> {
    if slots.is_empty() {
        return Ok(context);
    }
    let mut context: serde_json::Value = serde_json::from_slice(&context).with_context(|| {
        format!("Connector context for `{component}` has dependency slots but is not JSON")
    })?;
    for slot in slots {
        anyhow::ensure!(
            dependencies.contains(&slot.component),
            "Connector context for `{component}` references undeclared dependency `{}`",
            slot.component
        );
        let hash = hashes.get(&slot.component).with_context(|| {
            format!(
                "Connector context for `{component}` references unregistered dependency `{}`",
                slot.component
            )
        })?;
        let target = context.pointer_mut(&slot.pointer).with_context(|| {
            format!(
                "Connector context for `{component}` has no dependency hash slot at `{}`",
                slot.pointer
            )
        })?;
        *target = serde_json::Value::Array(
            hash.iter()
                .map(|byte| serde_json::Value::from(*byte))
                .collect(),
        );
    }
    serde_json::to_vec(&context)
        .with_context(|| format!("Cannot encode materialized connector context for `{component}`"))
}

pub struct CoreGraphWriter<'a, R> {
    client: CoreClient,
    components: Vec<RegisteredComponent>,
    package_reader: &'a R,
    component_spec_command: Option<String>,
}

impl<'a, R> CoreGraphWriter<'a, R> {
    pub fn new(
        config: &CoreApiConfig,
        components: Vec<RegisteredComponent>,
        package_reader: &'a R,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            client: CoreClient::new(config)?,
            components,
            package_reader,
            component_spec_command: config.component_spec_command.clone(),
        })
    }
}

impl<R: ComponentPackageReader> DeployRepoWriter for CoreGraphWriter<'_, R> {
    async fn write_manifest(
        &mut self,
        _path: &str,
        contents: &str,
    ) -> anyhow::Result<DeployWriteResult> {
        let manifest = manifest::parse_toml(contents)
            .context("Cannot parse resolved preview world at core boundary")?;
        let command = self.component_spec_command.as_deref().context(
            "Core preview collection requires a TypeScript component-spec inspector; components without the TS authoring pattern are unsupported",
        )?;
        let inspected_specs = collect_component_specs(command, contents).await?;
        let specs = graph_component_specs(
            &manifest,
            &self.components,
            self.package_reader,
            inspected_specs,
        )
        .await?;
        let generation = self
            .client
            .apply_graph(&manifest.environment.id, specs)
            .await?;
        Ok(DeployWriteResult {
            commit_sha: format!("generation:{generation}"),
        })
    }

    async fn delete_manifest(&mut self, path: &str) -> anyhow::Result<()> {
        let environment_id = path
            .strip_suffix(".toml")
            .with_context(|| format!("Invalid preview environment path `{path}`"))?;
        self.client.retire_graph(environment_id).await
    }

    async fn create_branch(&mut self, _branch: &str) -> anyhow::Result<()> {
        Ok(())
    }

    async fn delete_branch(&mut self, _branch: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

async fn graph_component_specs<R: ComponentPackageReader>(
    manifest: &Manifest,
    registered: &[RegisteredComponent],
    _package_reader: &R,
    mut inspected_specs: BTreeMap<String, InspectedComponentSpec>,
) -> anyhow::Result<Vec<PlannedComponentSpec>> {
    let pins = resolved_pins(manifest)?;
    let registered_names = registered
        .iter()
        .map(|component| component.name.as_str())
        .collect::<BTreeSet<_>>();

    let specs = registered
        .iter()
        .map(|registered| {
            validate_component_name(&registered.name)?;
            pins.get(&registered.name).with_context(|| {
                format!("Core preview world omitted component `{}`", registered.name)
            })?;
            let inspected = inspected_specs.remove(&registered.name).with_context(|| {
                format!(
                    "Component-spec inspector omitted registered component `{}`",
                    registered.name
                )
            })?;
            anyhow::ensure!(
                !inspected.connector.is_empty(),
                "Component-spec inspector returned an empty connector for `{}`",
                registered.name
            );
            for dependency in &inspected.dependencies {
                anyhow::ensure!(
                    registered_names.contains(dependency.as_str()),
                    "Component-spec inspector returned unknown dependency `{dependency}` for `{}`",
                    registered.name
                );
                anyhow::ensure!(
                    dependency != &registered.name,
                    "Component-spec inspector returned a self-dependency for `{}`",
                    registered.name
                );
            }
            Ok(PlannedComponentSpec {
                name: registered.name.clone(),
                connector: inspected.connector,
                dependencies: inspected.dependencies,
                outputs_schema: decode_inspected_bytes(
                    &registered.name,
                    "outputsSchema",
                    &inspected.outputs_schema,
                )?,
                connector_context: decode_inspected_bytes(
                    &registered.name,
                    "connectorContext",
                    &inspected.connector_context,
                )?,
                dependency_spec_hash_slots: inspected.dependency_spec_hash_slots,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    anyhow::ensure!(
        inspected_specs.is_empty(),
        "Component-spec inspector returned unknown components: {}",
        inspected_specs
            .keys()
            .cloned()
            .collect::<Vec<_>>()
            .join(", ")
    );
    Ok(specs)
}

async fn collect_component_specs(
    command: &str,
    manifest: &str,
) -> anyhow::Result<BTreeMap<String, InspectedComponentSpec>> {
    let directory = tempfile::tempdir().context("Cannot create component-spec workspace")?;
    let manifest_path = directory.path().join("world.toml");
    let output_path = directory.path().join("component-specs.json");
    tokio::fs::write(&manifest_path, manifest)
        .await
        .context("Cannot write component-spec manifest")?;
    let output = tokio::process::Command::new(command)
        .arg(&manifest_path)
        .arg("--output")
        .arg(&output_path)
        .output()
        .await
        .with_context(|| format!("Cannot run component-spec inspector `{command}`"))?;
    anyhow::ensure!(
        output.status.success(),
        "Component-spec inspector `{command}` failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    let inspection: ComponentSpecInspection = serde_json::from_slice(
        &tokio::fs::read(&output_path)
            .await
            .context("Component-spec inspector omitted component-specs.json")?,
    )
    .context("Cannot decode component-spec inspection")?;
    anyhow::ensure!(
        inspection.api_version == COMPONENT_SPEC_INSPECTION_API_VERSION,
        "Unsupported component-spec inspection API version `{}`",
        inspection.api_version
    );
    Ok(inspection.components)
}

fn decode_inspected_bytes(component: &str, field: &str, encoded: &str) -> anyhow::Result<Vec<u8>> {
    base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .with_context(|| {
            format!("Component-spec inspector returned invalid {field} bytes for `{component}`")
        })
}

fn resolved_pins(manifest: &Manifest) -> anyhow::Result<BTreeMap<String, &PinnedEntry>> {
    manifest
        .components
        .iter()
        .map(|(name, entry)| match entry {
            ComponentEntry::Pinned(pin) => Ok((name.clone(), pin)),
            ComponentEntry::Follower(_) => Err(anyhow!(
                "Core preview world contains unresolved follower `{name}`"
            )),
        })
        .collect()
}

fn validate_component_name(name: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !name.is_empty()
            && name.len() <= 63
            && name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            && name.as_bytes()[0].is_ascii_alphanumeric()
            && name.as_bytes()[name.len() - 1].is_ascii_alphanumeric(),
        "component name `{name}` is not a lowercase DNS label"
    );
    Ok(())
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

fn graph_generation(graph: &impl std::ops::Deref<Target = Graph>) -> Option<u64> {
    graph.generation
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
    use crate::henosis::config::ComponentMode;
    use crate::henosis::graph::{PackageHenosis, PackageJson};
    use crate::henosis::manifest::{EnvironmentSection, pinned};
    use indexmap::IndexMap;
    use serde_json::{Value, json};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    struct PackageReader {
        packages: BTreeMap<(String, String), PackageJson>,
    }

    impl ComponentPackageReader for PackageReader {
        async fn fetch_package_json(&self, repo: &str, sha: &str) -> anyhow::Result<PackageJson> {
            self.packages
                .get(&(repo.to_string(), sha.to_string()))
                .cloned()
                .with_context(|| format!("missing package {repo}@{sha}"))
        }
    }

    fn package(name: &str, component: &str, dependencies: &[&str]) -> PackageJson {
        PackageJson {
            name: name.to_string(),
            dependencies: dependencies
                .iter()
                .map(|dependency| (dependency.to_string(), "workspace:*".to_string()))
                .collect(),
            henosis: PackageHenosis {
                component: Some(component.to_string()),
            },
        }
    }

    fn inspected_spec(
        connector: &str,
        dependencies: &[&str],
        outputs_schema: &[u8],
        connector_context: &[u8],
    ) -> InspectedComponentSpec {
        InspectedComponentSpec {
            connector: connector.to_string(),
            dependencies: dependencies
                .iter()
                .map(|dependency| dependency.to_string())
                .collect(),
            outputs_schema: base64::engine::general_purpose::STANDARD.encode(outputs_schema),
            connector_context: base64::engine::general_purpose::STANDARD.encode(connector_context),
            dependency_spec_hash_slots: Vec::new(),
        }
    }

    #[tokio::test]
    async fn boundary_uses_inspector_context_and_dependency_edges() {
        let environment_id = TypedUuid::<PreviewEnvironmentKind>::from_untyped_uuid(
            uuid::Uuid::from_u128(0x018f_1234_5678_7abc_8def_0123_4567_89ab),
        )
        .to_string();
        let a_sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let b_sha = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let manifest = Manifest {
            environment: EnvironmentSection {
                id: environment_id.clone(),
            },
            components: IndexMap::from([
                (
                    "service-a".to_string(),
                    pinned(
                        "henosis-playground/service-a",
                        a_sha,
                        format!("sha256:{}", "a".repeat(64)),
                    ),
                ),
                (
                    "service-b".to_string(),
                    pinned(
                        "henosis-playground/service-b",
                        b_sha,
                        format!("sha256:{}", "b".repeat(64)),
                    ),
                ),
            ]),
        };
        let registered = vec![
            RegisteredComponent {
                name: "service-a".to_string(),
                repo: "henosis-playground/service-a".to_string(),
                main_branch: "main".to_string(),
                mode: ComponentMode::GateOnly,
            },
            RegisteredComponent {
                name: "service-b".to_string(),
                repo: "henosis-playground/service-b".to_string(),
                main_branch: "main".to_string(),
                mode: ComponentMode::GateOnly,
            },
        ];
        let reader = PackageReader {
            packages: BTreeMap::from([
                (
                    (
                        "henosis-playground/service-a".to_string(),
                        a_sha.to_string(),
                    ),
                    package("@henosis/service-a", "service-a", &[]),
                ),
                (
                    (
                        "henosis-playground/service-b".to_string(),
                        b_sha.to_string(),
                    ),
                    package("@henosis/service-b", "service-b", &["@henosis/service-a"]),
                ),
            ]),
        };

        let context = format!(
            r#"{{"apiVersion":"henosis.dev/k8s-component-context/v1","environment":{{"id":"{environment_id}"}},"source":{{"repository":"henosis-playground/service-b","revision":"{b_sha}"}},"image":{{"digest":"sha256:{}"}}}}"#,
            "b".repeat(64)
        )
        .into_bytes();
        let inspected = BTreeMap::from([
            (
                "service-a".to_string(),
                inspected_spec("k8s", &[], br#"{"kind":"object"}"#, b"service-a"),
            ),
            (
                "service-b".to_string(),
                inspected_spec("k8s", &["service-a"], br#"{"kind":"object"}"#, &context),
            ),
        ]);

        let specs = graph_component_specs(&manifest, &registered, &reader, inspected)
            .await
            .unwrap();
        assert_eq!(specs[1].connector, "k8s");
        assert_eq!(specs[1].dependencies, vec!["service-a"]);
        assert_eq!(specs[1].connector_context, context);
    }

    #[tokio::test]
    async fn registers_dependency_ordered_specs_before_graph_creation() {
        let server = MockServer::start().await;
        let first_hash =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [1_u8; 32]);
        let second_hash =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [2_u8; 32]);
        let registration_first_hash = first_hash.clone();
        let registration_second_hash = second_hash.clone();
        Mock::given(method("POST"))
            .and(path("/henosis.v1.GraphService/RegisterComponentSpec"))
            .respond_with(move |request: &Request| {
                let body: Value = request.body_json().unwrap();
                let spec = body["spec"].clone();
                assert_eq!(
                    base64::Engine::decode(
                        &base64::engine::general_purpose::STANDARD,
                        spec["outputsSchema"].as_str().unwrap()
                    )
                    .unwrap(),
                    br#"{"kind":"object"}"#
                );
                let hash = match spec["name"].as_str().unwrap() {
                    "service-a" => {
                        assert_eq!(spec["connector"], "k8s");
                        assert!(spec.get("dependsOn").is_none());
                        &registration_first_hash
                    }
                    "service-b" => {
                        assert_eq!(spec["connector"], "supabase");
                        assert_eq!(spec["dependsOn"], json!([registration_first_hash.clone()]));
                        let context = base64::engine::general_purpose::STANDARD
                            .decode(spec["connectorContext"].as_str().unwrap())
                            .unwrap();
                        let context: Value = serde_json::from_slice(&context).unwrap();
                        assert_eq!(context["producerSpecHash"], json!(vec![1_u8; 32]));
                        &registration_second_hash
                    }
                    name => panic!("unexpected component spec `{name}`"),
                };
                ResponseTemplate::new(200).set_body_json(json!({
                    "component": {
                        "hash": hash,
                        "spec": spec
                    }
                }))
            })
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/henosis.v1.GraphService/GetGraph"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({
                "code": "not_found",
                "message": "graph does not exist"
            })))
            .mount(&server)
            .await;
        let create_first_hash = first_hash.clone();
        let create_second_hash = second_hash.clone();
        Mock::given(method("POST"))
            .and(path("/henosis.v1.GraphService/CreateGraph"))
            .respond_with(move |request: &Request| {
                let body: Value = request.body_json().unwrap();
                assert_eq!(
                    body["componentSpecHashes"],
                    json!([create_first_hash.clone(), create_second_hash.clone()])
                );
                ResponseTemplate::new(200).set_body_json(json!({
                    "graph": {
                        "id": body["graphId"],
                        "generation": "1",
                        "componentSpecHashes": body["componentSpecHashes"]
                    }
                }))
            })
            .mount(&server)
            .await;
        let config: CoreApiConfig = toml::from_str(&format!(
            "endpoint = {:?}\ntoken = \"test-token\"",
            server.uri()
        ))
        .unwrap();
        let environment_id = TypedUuid::<PreviewEnvironmentKind>::from_untyped_uuid(
            uuid::Uuid::from_u128(0x018f_1234_5678_7abc_8def_0123_4567_89ac),
        )
        .to_string();
        let client = CoreClient::new(&config).unwrap();

        let generation = client
            .apply_graph(
                &environment_id,
                vec![
                    PlannedComponentSpec {
                        name: "service-b".to_string(),
                        connector: "supabase".to_string(),
                        dependencies: vec!["service-a".to_string()],
                        outputs_schema: br#"{"kind":"object"}"#.to_vec(),
                        connector_context: br#"{"producerSpecHash":null}"#.to_vec(),
                        dependency_spec_hash_slots: vec![DependencySpecHashSlot {
                            component: "service-a".to_string(),
                            pointer: "/producerSpecHash".to_string(),
                        }],
                    },
                    PlannedComponentSpec {
                        name: "service-a".to_string(),
                        connector: "k8s".to_string(),
                        dependencies: Vec::new(),
                        outputs_schema: br#"{"kind":"object"}"#.to_vec(),
                        connector_context: b"service-a".to_vec(),
                        dependency_spec_hash_slots: Vec::new(),
                    },
                ],
            )
            .await
            .unwrap();

        assert_eq!(generation, 1);
        let requests = server.received_requests().await.unwrap();
        let methods = requests
            .iter()
            .map(|request| request.url.path().rsplit('/').next().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            methods,
            vec![
                "RegisterComponentSpec",
                "RegisterComponentSpec",
                "GetGraph",
                "CreateGraph"
            ]
        );
    }

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
