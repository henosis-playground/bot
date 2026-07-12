use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use anyhow::{Context, anyhow};
use futures::StreamExt;
use henosis_proto::proto::henosis::v1::__buffa::oneof::watch_graph_response::Item as WatchItem;
use henosis_proto::proto::henosis::v1::{
    AddComponentsRequest, AddComponentsResponse, ComponentDispositionKind, ComponentReplacement,
    ComponentSpec, ContractFailureKind, CreateGraphRequest, CreateGraphResponse, Diagnostic,
    DiagnosticSeverity, GetGraphGenerationRequest, GetGraphGenerationResponse, GetGraphRequest,
    GetGraphResponse, Graph, GraphLifecycle, GraphState, RegisterComponentSpecRequest,
    RegisterComponentSpecResponse, RemoveComponentsRequest, RemoveComponentsResponse,
    RetireGraphRequest, RetireGraphResponse, SliceReport, UpdateComponentsRequest,
    UpdateComponentsResponse, WatchGraphRequest, WatchGraphResponse,
};
use newtype_uuid::{GenericUuid, TypedUuid, TypedUuidKind, TypedUuidTag};
use reqwest::{Client, StatusCode};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::henosis::config::{CoreApiConfig, RegisteredComponent};
use crate::henosis::environment::{
    DeployRepoWriter, DeployWriteResult, EnvironmentIdGenerator, PublicationLink, RenderStatus,
};
use crate::henosis::gate_report::{GateFailure, GateReport};
use crate::henosis::graph::{ComponentGraph, ComponentPackageReader, ComponentRef};
use crate::henosis::manifest::{self, ComponentEntry, Manifest, PinnedEntry};

const K8S_CONNECTOR: &str = "k8s";
const COMPONENT_CONTEXT_API_VERSION: &str = "henosis.dev/k8s-component-context/v1";
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlannedComponentSpec {
    name: String,
    dependencies: Vec<String>,
    connector_context: Vec<u8>,
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
            let spec = ComponentSpec {
                name: Some(planned.name.clone()),
                connector: Some(K8S_CONNECTOR.to_string()),
                outputs_schema: Some(Vec::new()),
                depends_on,
                connector_context: Some(planned.connector_context),
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
        let specs = graph_component_specs(&manifest, &self.components, self.package_reader).await?;
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
    package_reader: &R,
) -> anyhow::Result<Vec<PlannedComponentSpec>> {
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
            Ok(PlannedComponentSpec {
                name: registered.name.clone(),
                dependencies: node.dependencies.iter().cloned().collect(),
                connector_context: context,
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
    let current_reports = state
        .reports
        .iter()
        .filter(|report| {
            report.connector.as_deref() == Some(K8S_CONNECTOR)
                && report.generation == Some(generation)
        })
        .collect::<Vec<_>>();
    anyhow::ensure!(
        current_reports
            .iter()
            .all(|report| report.sequence.is_some()),
        "Current-generation k8s report omitted durable sequence"
    );
    let sequence = current_reports
        .iter()
        .filter_map(|report| report.sequence)
        .max();
    let reports = current_reports
        .into_iter()
        .filter(|report| report.sequence == sequence)
        .collect::<Vec<_>>();
    let (diagnostic, failure_presentations) = format_diagnostics(&reports);
    let publication = reports.iter().find_map(|report| {
        report.publication.as_option().and_then(|publication| {
            Some(PublicationLink {
                revision: publication.revision.as_deref()?.to_string(),
                url: publication.uri.as_deref()?.to_string(),
            })
        })
    });
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
        .filter_map(|disposition| disposition.component_spec_hash.as_deref())
        .collect::<BTreeSet<_>>();
    let ready_ids = reports
        .iter()
        .flat_map(|report| report.dispositions.iter())
        .filter(|disposition| {
            disposition.kind.as_ref().and_then(|kind| kind.as_known())
                == Some(ComponentDispositionKind::Ready)
        })
        .filter_map(|disposition| disposition.component_spec_hash.as_deref())
        .collect::<BTreeSet<_>>();
    let expected_ids = graph
        .component_spec_hashes
        .iter()
        .map(Vec::as_slice)
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
        })
        .collect::<Vec<_>>();
    failures.extend(
        diagnostics
            .into_iter()
            .filter(|diagnostic| !diagnostic.contract_failure.is_set())
            .map(|diagnostic| CoreFailurePresentation {
                consumer: "environment".to_string(),
                body: format_diagnostic(diagnostic),
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

    #[tokio::test]
    async fn boundary_emits_context_v1_and_package_graph_edges() {
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

        let specs = graph_component_specs(&manifest, &registered, &reader)
            .await
            .unwrap();
        assert_eq!(specs[1].dependencies, vec!["service-a"]);
        let context: Value = serde_json::from_slice(&specs[1].connector_context).unwrap();
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
                let hash = match spec["name"].as_str().unwrap() {
                    "service-a" => {
                        assert!(spec.get("dependsOn").is_none());
                        &registration_first_hash
                    }
                    "service-b" => {
                        assert_eq!(spec["dependsOn"], json!([registration_first_hash.clone()]));
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
                        dependencies: vec!["service-a".to_string()],
                        connector_context: b"service-b".to_vec(),
                    },
                    PlannedComponentSpec {
                        name: "service-a".to_string(),
                        dependencies: Vec::new(),
                        connector_context: b"service-a".to_vec(),
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
                    ]
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
}
