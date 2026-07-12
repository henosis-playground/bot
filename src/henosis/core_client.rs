use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use anyhow::{Context, anyhow};
use futures::StreamExt;
use henosis_proto::proto::henosis::v1::__buffa::oneof::watch_graph_response::Item as WatchItem;
use henosis_proto::proto::henosis::v1::{
    AddComponentsRequest, AddComponentsResponse, Component, ComponentDispositionKind,
    ComponentRevision, ComponentUpdate, CreateGraphRequest, CreateGraphResponse, Diagnostic,
    DiagnosticSeverity, GetGraphRequest, GetGraphResponse, Graph, GraphState,
    RemoveComponentsRequest, RemoveComponentsResponse, RetireGraphRequest, RetireGraphResponse,
    SliceReport, UpdateComponentsRequest, UpdateComponentsResponse, WatchGraphRequest,
    WatchGraphResponse,
};
use newtype_uuid::{GenericUuid, TypedUuid, TypedUuidKind, TypedUuidTag};
use reqwest::{Client, StatusCode};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};

use crate::henosis::config::{CoreApiConfig, RegisteredComponent};
use crate::henosis::environment::{
    DeployRepoWriter, DeployWriteResult, EnvironmentIdGenerator, RenderStatus,
};
use crate::henosis::graph::{ComponentGraph, ComponentPackageReader, ComponentRef};
use crate::henosis::manifest::{self, ComponentEntry, Manifest, PinnedEntry};

const K8S_CONNECTOR: &str = "k8s";
const COMPONENT_CONTEXT_API_VERSION: &str = "henosis.dev/k8s-component-context/v1";
const WATCH_INITIAL_TIMEOUT: Duration = Duration::from_secs(5);

pub enum PreviewEnvironmentKind {}

impl TypedUuidKind for PreviewEnvironmentKind {
    fn tag() -> TypedUuidTag {
        const TAG: TypedUuidTag = TypedUuidTag::new("preview");
        TAG
    }
}

type PreviewEnvironmentId = TypedUuid<PreviewEnvironmentKind>;

pub enum RequestKind {}

impl TypedUuidKind for RequestKind {
    fn tag() -> TypedUuidTag {
        const TAG: TypedUuidTag = TypedUuidTag::new("request");
        TAG
    }
}

type RequestId = TypedUuid<RequestKind>;

#[derive(Default)]
pub struct CoreEnvironmentIdGenerator;

impl EnvironmentIdGenerator for CoreEnvironmentIdGenerator {
    fn new_preview_environment_id(&self) -> String {
        PreviewEnvironmentId::from_untyped_uuid(uuid::Uuid::now_v7()).to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreGraphStatus {
    pub generation: u64,
    pub status: RenderStatus,
    pub diagnostic: Option<String>,
}

#[derive(Clone)]
pub struct CoreClient {
    endpoint: String,
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
        anyhow::ensure!(
            !config.token.expose().is_empty(),
            "Henosis core token cannot be empty"
        );
        Ok(Self {
            endpoint,
            token: config.token.expose().to_string(),
            http: Client::new(),
        })
    }

    pub fn graph_url(&self, environment_id: &str) -> String {
        format!("{}/graphs/{environment_id}", self.endpoint)
    }

    pub async fn apply_graph(
        &self,
        environment_id: &str,
        components: Vec<Component>,
    ) -> anyhow::Result<u64> {
        let graph_id = graph_id_bytes(environment_id)?;
        let Some(state) = self.get_graph_by_id(&graph_id).await? else {
            let request = CreateGraphRequest {
                graph_id: Some(graph_id),
                components,
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

        self.edit_graph(graph_id, state, components).await
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
        loop {
            while let Some((flags, payload)) = take_connect_envelope(&mut buffered)? {
                if flags & 0x02 != 0 {
                    return watched_state(durable, Vec::new())
                        .context("WatchGraph ended before returning graph state");
                }
                let response: WatchGraphResponse = serde_json::from_slice(&payload)
                    .context("Cannot decode WatchGraph response envelope")?;
                match response.item {
                    Some(WatchItem::Snapshot(snapshot)) => durable = snapshot.state.into_option(),
                    Some(WatchItem::Change(change)) => durable = change.state.into_option(),
                    Some(WatchItem::VolatileStatus(status)) => {
                        let durable = durable
                            .context("WatchGraph returned volatile status before durable state")?;
                        return Ok(watched_state(Some(durable), status.reports)
                            .expect("durable state is present"));
                    }
                    Some(WatchItem::Progress(_)) | None => {}
                }
            }

            let next = tokio::time::timeout(WATCH_INITIAL_TIMEOUT, stream.next())
                .await
                .context("Timed out waiting for initial WatchGraph state")?;
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
        desired: Vec<Component>,
    ) -> anyhow::Result<u64> {
        let graph = current_graph(&state)?;
        let mut generation = graph
            .generation
            .context("GetGraph response omitted graph generation")?;
        let existing_by_id = graph
            .components
            .iter()
            .filter_map(|component| component.id.clone().map(|id| (id, component)))
            .collect::<BTreeMap<_, _>>();
        let desired_ids = desired
            .iter()
            .filter_map(|component| component.id.clone())
            .collect::<BTreeSet<_>>();

        let additions = desired
            .iter()
            .filter(|component| {
                component
                    .id
                    .as_ref()
                    .is_some_and(|id| !existing_by_id.contains_key(id))
            })
            .map(|component| {
                let mut component = component.clone();
                component.depends_on.clear();
                component
            })
            .collect::<Vec<_>>();
        if !additions.is_empty() {
            let request = AddComponentsRequest {
                graph_id: Some(graph_id.clone()),
                expected_generation: Some(generation),
                components: additions,
                request_id: Some(new_request_id()),
                ..Default::default()
            };
            let _: AddComponentsResponse = self
                .unary("AddComponents", &request)
                .await
                .context("Cannot add Henosis core graph components")?;
            generation += 1;
        }

        let updates = desired
            .into_iter()
            .map(|component| {
                let mut update = ComponentUpdate {
                    component: component.into(),
                    ..Default::default()
                };
                update.update_mask.get_or_insert_default().paths = vec![
                    "name".to_string(),
                    "revision".to_string(),
                    "connector".to_string(),
                    "outputs_schema".to_string(),
                    "depends_on".to_string(),
                    "context".to_string(),
                ];
                update
            })
            .collect::<Vec<_>>();
        if !updates.is_empty() {
            let request = UpdateComponentsRequest {
                graph_id: Some(graph_id.clone()),
                expected_generation: Some(generation),
                updates,
                request_id: Some(new_request_id()),
                ..Default::default()
            };
            let _: UpdateComponentsResponse = self
                .unary("UpdateComponents", &request)
                .await
                .context("Cannot update Henosis core graph components")?;
            generation += 1;
        }

        let removals = existing_by_id
            .keys()
            .filter(|id| !desired_ids.contains(*id))
            .cloned()
            .collect::<Vec<_>>();
        if !removals.is_empty() {
            let request = RemoveComponentsRequest {
                graph_id: Some(graph_id),
                expected_generation: Some(generation),
                component_ids: removals,
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

pub struct CoreGraphWriter<'a, R> {
    client: CoreClient,
    components: Vec<RegisteredComponent>,
    package_reader: &'a R,
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
        let components = graph_components(&manifest, &self.components, self.package_reader).await?;
        let generation = self
            .client
            .apply_graph(&manifest.environment.id, components)
            .await?;
        Ok(DeployWriteResult {
            commit_sha: generation.to_string(),
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

pub async fn graph_components<R: ComponentPackageReader>(
    manifest: &Manifest,
    registered: &[RegisteredComponent],
    package_reader: &R,
) -> anyhow::Result<Vec<Component>> {
    let graph_id = graph_id_bytes(&manifest.environment.id)?;
    let pins = resolved_pins(manifest)?;
    let refs = registered
        .iter()
        .map(|component| {
            let pin = pins.get(&component.name).with_context(|| {
                format!("Core preview world omitted component `{}`", component.name)
            })?;
            Ok(ComponentRef::new(
                component.name.clone(),
                pin.repo.clone(),
                pin.r#ref.clone(),
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let package_graph = ComponentGraph::read(&refs, package_reader).await?;
    let ids = registered
        .iter()
        .map(|component| {
            (
                component.name.clone(),
                component_id_bytes(&graph_id, &component.name),
            )
        })
        .collect::<BTreeMap<_, _>>();

    registered
        .iter()
        .map(|registered| {
            validate_component_name(&registered.name)?;
            let pin = pins.get(&registered.name).expect("checked above");
            validate_pin(pin)?;
            let node = package_graph.node(&registered.name).with_context(|| {
                format!("Package graph omitted component `{}`", registered.name)
            })?;
            let context = component_context(&manifest.environment.id, pin)?;
            Ok(Component {
                id: Some(ids[&registered.name].clone()),
                name: Some(registered.name.clone()),
                revision: ComponentRevision {
                    source: Some(pin.repo.clone()),
                    revision: Some(pin.r#ref.clone()),
                    ..Default::default()
                }
                .into(),
                connector: Some(K8S_CONNECTOR.to_string()),
                outputs_schema: Some(Vec::new()),
                depends_on: node
                    .dependencies
                    .iter()
                    .map(|dependency| {
                        ids.get(dependency).cloned().with_context(|| {
                            format!("Unknown package-graph dependency `{dependency}`")
                        })
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?,
                context: Some(context),
                ..Default::default()
            })
        })
        .collect()
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

#[derive(Serialize)]
struct ComponentContext<'a> {
    #[serde(rename = "apiVersion")]
    api_version: &'static str,
    environment: ContextEnvironment<'a>,
    source: ContextSource<'a>,
    image: ContextImage<'a>,
}

#[derive(Serialize)]
struct ContextEnvironment<'a> {
    id: &'a str,
}

#[derive(Serialize)]
struct ContextSource<'a> {
    repository: &'a str,
    revision: &'a str,
}

#[derive(Serialize)]
struct ContextImage<'a> {
    digest: &'a str,
}

fn component_context(environment_id: &str, pin: &PinnedEntry) -> anyhow::Result<Vec<u8>> {
    serde_json::to_vec(&ComponentContext {
        api_version: COMPONENT_CONTEXT_API_VERSION,
        environment: ContextEnvironment { id: environment_id },
        source: ContextSource {
            repository: &pin.repo,
            revision: &pin.r#ref,
        },
        image: ContextImage {
            digest: &pin.digest,
        },
    })
    .context("Cannot serialize Kubernetes component context v1")
}

fn validate_pin(pin: &PinnedEntry) -> anyhow::Result<()> {
    let repository_parts = pin.repo.split('/').collect::<Vec<_>>();
    anyhow::ensure!(
        repository_parts.len() == 2
            && repository_parts.iter().all(|part| !part.is_empty())
            && !pin.repo.ends_with(".git"),
        "component repository `{}` is not a GitHub owner/name",
        pin.repo
    );
    anyhow::ensure!(
        pin.r#ref.len() == 40
            && pin
                .r#ref
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')),
        "component revision `{}` is not a full lowercase Git commit SHA",
        pin.r#ref
    );
    let digest = pin.digest.strip_prefix("sha256:").unwrap_or_default();
    anyhow::ensure!(
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')),
        "image digest `{}` is not a lowercase sha256 OCI digest",
        pin.digest
    );
    Ok(())
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
    let id: PreviewEnvironmentId = environment_id
        .parse()
        .with_context(|| format!("Invalid core preview environment id `{environment_id}`"))?;
    Ok(id.into_bytes().to_vec())
}

fn component_id_bytes(graph_id: &[u8], component_name: &str) -> Vec<u8> {
    let mut digest = Sha256::new();
    digest.update(b"henosis-component-v1\0");
    digest.update(graph_id);
    digest.update(b"\0");
    digest.update(component_name.as_bytes());
    let mut bytes: [u8; 16] = digest.finalize()[..16].try_into().expect("fixed length");
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    bytes.to_vec()
}

fn new_request_id() -> Vec<u8> {
    RequestId::from_untyped_uuid(uuid::Uuid::new_v4())
        .into_bytes()
        .to_vec()
}

fn current_graph(state: &GraphState) -> anyhow::Result<&Graph> {
    anyhow::ensure!(state.durable.is_set(), "Graph state omitted durable state");
    anyhow::ensure!(state.durable.graph.is_set(), "Graph state omitted graph");
    Ok(&state.durable.graph)
}

fn graph_generation(graph: &impl std::ops::Deref<Target = Graph>) -> Option<u64> {
    graph.generation
}

fn graph_status(state: &GraphState) -> anyhow::Result<CoreGraphStatus> {
    let graph = current_graph(state)?;
    let generation = graph
        .generation
        .context("Graph state omitted graph generation")?;
    let reports = state
        .reports
        .iter()
        .filter(|report| {
            report.connector.as_deref() == Some(K8S_CONNECTOR)
                && report.generation == Some(generation)
        })
        .collect::<Vec<_>>();
    let diagnostic = format_diagnostics(&reports);
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
    let reported_ids = reports
        .iter()
        .flat_map(|report| report.dispositions.iter())
        .filter_map(|disposition| disposition.component_id.as_deref())
        .collect::<BTreeSet<_>>();
    let ready_ids = reports
        .iter()
        .flat_map(|report| report.dispositions.iter())
        .filter(|disposition| {
            disposition.kind.as_ref().and_then(|kind| kind.as_known())
                == Some(ComponentDispositionKind::Ready)
        })
        .filter_map(|disposition| disposition.component_id.as_deref())
        .collect::<BTreeSet<_>>();
    let expected_ids = graph
        .components
        .iter()
        .filter_map(|component| component.id.as_deref())
        .collect::<BTreeSet<_>>();

    let status = if failed {
        RenderStatus::Failure
    } else if !expected_ids.is_empty() && reported_ids == expected_ids && ready_ids == expected_ids
    {
        RenderStatus::Success
    } else {
        RenderStatus::Pending
    };
    Ok(CoreGraphStatus {
        generation,
        status,
        diagnostic,
    })
}

fn format_diagnostics(reports: &[&SliceReport]) -> Option<String> {
    let lines = reports
        .iter()
        .flat_map(|report| &report.diagnostics)
        .map(format_diagnostic)
        .collect::<Vec<_>>();
    (!lines.is_empty()).then(|| lines.join("\n"))
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
    durable: Option<henosis_proto::proto::henosis::v1::DurableGraphState>,
    reports: Vec<SliceReport>,
) -> Option<GraphState> {
    durable.map(|durable| GraphState {
        durable: durable.into(),
        reports,
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

    #[tokio::test]
    async fn boundary_emits_context_v1_and_package_graph_edges() {
        let environment_id = PreviewEnvironmentId::from_untyped_uuid(uuid::Uuid::from_u128(
            0x018f_1234_5678_7abc_8def_0123_4567_89ab,
        ))
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

        let components = graph_components(&manifest, &registered, &reader)
            .await
            .unwrap();
        assert_eq!(
            components[1].depends_on,
            vec![components[0].id.clone().unwrap()]
        );
        let context: Value =
            serde_json::from_slice(components[1].context.as_deref().unwrap()).unwrap();
        assert_eq!(context["apiVersion"], COMPONENT_CONTEXT_API_VERSION);
        assert_eq!(context["environment"]["id"], environment_id);
        assert_eq!(
            context["source"],
            json!({
                "repository": "henosis-playground/service-b",
                "revision": b_sha
            })
        );
        assert_eq!(
            context["image"]["digest"],
            format!("sha256:{}", "b".repeat(64))
        );
    }

    #[test]
    fn core_preview_ids_are_canonical_uuid_v7_typeids() {
        let id = CoreEnvironmentIdGenerator.new_preview_environment_id();
        let parsed: PreviewEnvironmentId = id.parse().unwrap();
        assert_eq!(parsed.get_version_num(), 7);
        assert_eq!(parsed.to_string(), id);
    }
}
