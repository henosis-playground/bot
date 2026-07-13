use crate::bors::event::BorsEvent;
use crate::bors::{CommandPrefix, RepositoryState, format_help};
use crate::database::{ApprovalStatus, QueueStatus};
use crate::github::{GithubRepoName, PullRequestNumber, rollup};
use crate::henosis::core_client::CoreClient;
use crate::templates::{
    HelpTemplate, HtmlTemplate, NotFoundTemplate, PullRequestStats, QueueTemplate, RepositoryView,
    RollupsInfo,
};
use crate::utils::sort_queue::sort_queue_prs;
use crate::{
    AppError, BorsContext, BorsGlobalEvent, BorsRepositoryEvent, OAuthClient, PgDbClient,
    WebhookSecret, bors, database,
};
use axum::extract::{FromRef, Path, Query, State};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use axum_embed::ServeEmbed;
use base64::Engine as _;
use chrono::Utc;
use http::{Request, StatusCode};
use pulldown_cmark::Parser;
use rust_embed::Embed;
use serde::de::Error;
use serde::{Deserialize, Deserializer};
use sqlx::Row as _;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::CompressionLevel;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::compression::CompressionLayer;
use tower_http::trace::TraceLayer;
use tracing::Span;
use webhook::GitHubWebhook;

pub mod webhook;

/// Shared server state for all axum handlers.
pub struct ServerState {
    repository_event_queue: mpsc::Sender<BorsRepositoryEvent>,
    global_event_queue: mpsc::Sender<BorsGlobalEvent>,
    webhook_secret: WebhookSecret,
    oauth: Option<OAuthClient>,
    ctx: Arc<BorsContext>,
}

impl ServerState {
    pub fn new(
        repository_event_queue: mpsc::Sender<BorsRepositoryEvent>,
        global_event_queue: mpsc::Sender<BorsGlobalEvent>,
        webhook_secret: WebhookSecret,
        oauth: Option<OAuthClient>,
        ctx: Arc<BorsContext>,
    ) -> Self {
        Self {
            repository_event_queue,
            global_event_queue,
            webhook_secret,
            oauth,
            ctx,
        }
    }

    pub fn get_webhook_secret(&self) -> &WebhookSecret {
        &self.webhook_secret
    }

    pub fn get_cmd_prefix(&self) -> &CommandPrefix {
        self.ctx.parser.prefix()
    }

    pub fn get_web_url(&self) -> &str {
        self.ctx.get_web_url()
    }

    pub fn get_repo(&self, repo: &GithubRepoName) -> Option<Arc<RepositoryState>> {
        self.ctx.repositories.get(repo)
    }
}

impl FromRef<ServerStateRef> for Option<OAuthClient> {
    fn from_ref(state: &ServerStateRef) -> Self {
        state.0.oauth.clone()
    }
}

impl FromRef<ServerStateRef> for Arc<PgDbClient> {
    fn from_ref(state: &ServerStateRef) -> Self {
        state.0.ctx.db.clone()
    }
}

#[derive(Clone)]
pub struct ServerStateRef(pub Arc<ServerState>);

pub fn create_app(state: ServerState) -> Router {
    let compression_layer = CompressionLayer::new()
        .br(true)
        .gzip(true)
        // The production bors machine is relatively weak, prefer faster compression, rather than
        // minimum file sizes
        .quality(CompressionLevel::Fastest);
    let trace_layer = TraceLayer::new_for_http()
        .make_span_with(|request: &Request<_>| {
            tracing::debug_span!("request", "{} {}", request.method(), request.uri().path())
        })
        .on_request(())
        .on_body_chunk(())
        .on_eos(())
        .on_failure(())
        .on_response(
            |response: &http::Response<_>, latency: Duration, _span: &Span| {
                tracing::debug!(
                    "response: {} ({}ms)",
                    response.status().as_u16(),
                    latency.as_millis()
                )
            },
        );

    #[derive(Embed, Clone)]
    #[folder = "web/assets/"]
    struct Assets;

    let serve_assets = ServeEmbed::<Assets>::new();

    let api = create_api_router();
    Router::new()
        .route("/", get(index_handler))
        .route("/help", get(help_handler))
        .route(
            "/queue/{repo_name}",
            get(queue_handler).layer(compression_layer),
        )
        .route("/github", post(github_webhook_handler))
        .route("/health", get(health_handler))
        .route("/graphs/{environment_id}", get(graph_handler))
        .route(
            "/graphs/{environment_id}/latest.json",
            get(graph_json_handler),
        )
        .route(
            "/graphs/{environment_id}/generations/{generation}",
            get(graph_generation_handler),
        )
        .route(
            "/graphs/{environment_id}/generations/{generation}/raw.json",
            get(graph_generation_json_handler),
        )
        .route("/oauth/callback", get(rollup::oauth_callback_handler))
        .nest("/api", api)
        .nest_service("/assets", serve_assets)
        .layer(ConcurrencyLimitLayer::new(100))
        .layer(CatchPanicLayer::custom(handle_panic))
        .layer(trace_layer)
        .with_state(ServerStateRef(Arc::new(state)))
        .fallback(not_found_handler)
}

async fn graph_handler(
    Path(environment_id): Path<String>,
    State(ServerStateRef(state)): State<ServerStateRef>,
) -> Response {
    let Some(core_api) = state
        .ctx
        .henosis_config
        .as_ref()
        .and_then(|config| config.core_api.as_ref())
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(client) = CoreClient::new(core_api) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    match client.get_graph(&environment_id).await {
        Ok(Some(graph)) => {
            let value = serde_json::to_value(&graph).unwrap_or_default();
            let Some(generation) = json_u64(value.pointer("/durable/graph/generation")) else {
                return StatusCode::BAD_GATEWAY.into_response();
            };
            graph_generation_response(
                &state,
                &client,
                &environment_id,
                generation,
                GraphPageView::Latest {
                    lifecycle: graph_lifecycle(&value),
                },
            )
            .await
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::BAD_GATEWAY.into_response(),
    }
}

async fn graph_json_handler(
    Path(environment_id): Path<String>,
    State(ServerStateRef(state)): State<ServerStateRef>,
) -> Response {
    let Some(core_api) = state
        .ctx
        .henosis_config
        .as_ref()
        .and_then(|config| config.core_api.as_ref())
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(client) = CoreClient::new(core_api) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    match client.get_graph(&environment_id).await {
        Ok(Some(graph)) => Json(graph).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::BAD_GATEWAY.into_response(),
    }
}

async fn graph_generation_handler(
    Path((environment_id, generation)): Path<(String, u64)>,
    State(ServerStateRef(state)): State<ServerStateRef>,
) -> Response {
    let Some(core_api) = state
        .ctx
        .henosis_config
        .as_ref()
        .and_then(|config| config.core_api.as_ref())
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(client) = CoreClient::new(core_api) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let latest_generation = match client.get_graph(&environment_id).await {
        Ok(Some(graph)) => serde_json::to_value(graph)
            .ok()
            .and_then(|value| json_u64(value.pointer("/durable/graph/generation"))),
        Ok(None) | Err(_) => None,
    };
    graph_generation_response(
        &state,
        &client,
        &environment_id,
        generation,
        GraphPageView::Generation { latest_generation },
    )
    .await
}

enum GraphPageView {
    Latest { lifecycle: String },
    Generation { latest_generation: Option<u64> },
}

async fn graph_generation_response(
    state: &ServerState,
    client: &CoreClient,
    environment_id: &str,
    generation: u64,
    view: GraphPageView,
) -> Response {
    match client
        .get_graph_generation(environment_id, generation)
        .await
    {
        Ok(Some(record)) => {
            let label = sqlx::query_scalar::<_, Option<String>>(
                "SELECT display_label FROM environment WHERE id = $1",
            )
            .bind(environment_id)
            .fetch_optional(state.ctx.db.pool())
            .await
            .ok()
            .flatten()
            .flatten();
            let value = serde_json::to_value(record).unwrap_or_default();
            let members = sqlx::query(
                "SELECT component, repo, pr_number, head_sha FROM environment_member WHERE environment_id = $1",
            )
            .bind(environment_id)
            .fetch_all(state.ctx.db.pool())
            .await
            .unwrap_or_default()
            .into_iter()
            .filter_map(|row| {
                Some((
                    (row.try_get::<Option<String>, _>("component").ok()??, row.try_get::<Option<String>, _>("head_sha").ok()??),
                    (row.try_get::<String, _>("repo").ok()?, row.try_get::<i64, _>("pr_number").ok()? as u64),
                ))
            })
            .collect::<HashMap<_, _>>();
            Html(render_generation_page(
                environment_id,
                label.as_deref(),
                &value,
                &members,
                &view,
            ))
            .into_response()
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::BAD_GATEWAY.into_response(),
    }
}

async fn graph_generation_json_handler(
    Path((environment_id, generation)): Path<(String, u64)>,
    State(ServerStateRef(state)): State<ServerStateRef>,
) -> Response {
    let Some(core_api) = state
        .ctx
        .henosis_config
        .as_ref()
        .and_then(|config| config.core_api.as_ref())
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(client) = CoreClient::new(core_api) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    match client
        .get_graph_generation(&environment_id, generation)
        .await
    {
        Ok(Some(record)) => Json(record).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::BAD_GATEWAY.into_response(),
    }
}

fn render_generation_page(
    environment_id: &str,
    label: Option<&str>,
    record: &serde_json::Value,
    members: &HashMap<(String, String), (String, u64)>,
    view: &GraphPageView,
) -> String {
    let generation = json_u64(record.pointer("/state/durable/graph/generation")).unwrap_or(0);
    let as_of_lifecycle = record
        .get("currentLifecycle")
        .and_then(serde_json::Value::as_str)
        .map(display_lifecycle)
        .unwrap_or_else(|| "unknown".to_string());
    let lifecycle = match view {
        GraphPageView::Latest { lifecycle } => lifecycle.clone(),
        GraphPageView::Generation { .. } => as_of_lifecycle,
    };
    let superseded_by = match view {
        GraphPageView::Latest { .. } => None,
        GraphPageView::Generation { latest_generation } => {
            superseded_generation(record, *latest_generation)
        }
    };
    let last_published = json_u64(record.get("lastPublishedGeneration"));
    let title = label
        .map(|label| {
            format!(
                "{} · <code>{}</code>",
                escape_html(label),
                escape_html(environment_id)
            )
        })
        .unwrap_or_else(|| format!("<code>{}</code>", escape_html(environment_id)));
    let mut component_rows = String::new();
    for component in record
        .get("components")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
    {
        let hash = component
            .get("hash")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        let spec = component.get("spec").unwrap_or(&serde_json::Value::Null);
        let name = spec
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let context = spec
            .get("connectorContext")
            .and_then(serde_json::Value::as_str)
            .and_then(|encoded| {
                base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .ok()
            })
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
            .unwrap_or_default();
        let repo = context
            .pointer("/source/repository")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let revision = context
            .pointer("/source/revision")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let pull_request = members
            .get(&(name.to_string(), revision.to_string()))
            .map(|(repo, number)| {
                format!(
                    " · <a href=\"https://github.com/{}/pull/{}\">PR #{}</a>",
                    escape_html(repo),
                    number,
                    number
                )
            })
            .unwrap_or_default();
        component_rows.push_str(&format!(
            "<tr><td><strong>{}</strong><br><small>{}</small></td><td><a href=\"https://github.com/{}/tree/{}\"><code>{}</code></a>{}</td><td>{}</td><td>{}</td></tr>",
            escape_html(name), escape_html(hash), escape_html(repo), escape_html(revision), escape_html(short_revision(revision)),
            pull_request, disposition_for(record, hash, superseded_by.is_some()), outputs_for(record, hash)
        ));
    }
    let reports = record
        .pointer("/state/reports")
        .and_then(serde_json::Value::as_array);
    let publication = reports
        .into_iter()
        .flatten()
        .find_map(|report| report.get("publication"));
    let publication_html = publication
        .and_then(|publication| {
            Some(format!(
                "<a href=\"{}\"><code>{}</code></a>",
                escape_html(publication.get("uri")?.as_str()?),
                escape_html(short_revision(publication.get("revision")?.as_str()?))
            ))
        })
        .unwrap_or_else(|| "not published".to_string());
    let diagnostics = reports
        .into_iter()
        .flatten()
        .flat_map(|report| {
            report
                .get("diagnostics")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
        })
        .map(|diagnostic| {
            format!(
                "<details><summary>{}: {}</summary><pre>{}</pre></details>",
                escape_html(
                    diagnostic
                        .get("code")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("diagnostic")
                ),
                escape_html(
                    diagnostic
                        .get("message")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("")
                ),
                escape_html(&serde_json::to_string_pretty(diagnostic).unwrap_or_default())
            )
        })
        .collect::<String>();
    let audit_context = match view {
        GraphPageView::Latest { .. } => format!(
            "<a href=\"/graphs/{environment_id}/generations/{generation}\">last generation {generation}</a> (immutable as-of evidence) · <a href=\"/graphs/{environment_id}/latest.json\">current raw JSON</a>",
            environment_id = escape_html(environment_id),
        ),
        GraphPageView::Generation { .. } => format!(
            "<a href=\"/graphs/{environment_id}/generations/{generation}/raw.json\">raw JSON</a> · <a href=\"/graphs/{environment_id}\">current graph</a>",
            environment_id = escape_html(environment_id),
        ),
    };
    let superseded_notice = superseded_by
        .map(|latest| {
            format!(
                "<aside><strong>Superseded by <a href=\"/graphs/{environment_id}/generations/{latest}\">generation {latest}</a></strong> before a terminal report was received.<br><small>This is current audit context; the component rows retain the last observation recorded for generation {generation}.</small></aside>",
                environment_id = escape_html(environment_id),
            )
        })
        .unwrap_or_default();
    format!(
        r#"<!doctype html><html><head><meta charset="utf-8"><title>Henosis generation {generation}</title><style>body{{font:16px system-ui;max-width:1100px;margin:3rem auto;padding:0 1rem;color:#202124}}table{{border-collapse:collapse;width:100%}}th,td{{text-align:left;vertical-align:top;border-bottom:1px solid #ddd;padding:.65rem}}code,pre{{font-family:ui-monospace,monospace}}pre{{white-space:pre-wrap;background:#f6f8fa;padding:1rem}}small{{color:#666}}aside{{margin:1rem 0;padding:1rem;border-left:4px solid #bf8700;background:#fff8c5}}</style></head><body><h1>{title}</h1>{superseded_notice}<table><tr><th>Lifecycle</th><td>{lifecycle}</td></tr><tr><th>Requested generation</th><td>{generation}</td></tr><tr><th>Last published generation</th><td>{last_published}</td></tr><tr><th>Publication</th><td>{publication_html}</td></tr><tr><th>Audit data</th><td>{audit_context}</td></tr></table><h2>Components</h2><table><thead><tr><th>Component</th><th>Source SHA</th><th>Disposition</th><th>Decoded outputs</th></tr></thead><tbody>{component_rows}</tbody></table><h2>Diagnostics</h2>{diagnostics}</body></html>"#,
        last_published = last_published
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string()),
    )
}

fn disposition_for(record: &serde_json::Value, hash: &str, superseded: bool) -> String {
    let observed = record
        .pointer("/state/reports")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .flat_map(|report| {
            report
                .get("dispositions")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
        })
        .find(|disposition| {
            disposition
                .get("componentSpecHash")
                .and_then(serde_json::Value::as_str)
                == Some(hash)
        })
        .and_then(|disposition| disposition.get("kind").and_then(serde_json::Value::as_str))
        .unwrap_or("PENDING")
        .trim_start_matches("COMPONENT_DISPOSITION_KIND_")
        .to_ascii_lowercase();
    if superseded {
        format!(
            "superseded<br><small>last report: {}</small>",
            escape_html(&observed)
        )
    } else {
        observed
    }
}

fn graph_lifecycle(graph: &serde_json::Value) -> String {
    graph
        .pointer("/durable/lifecycle")
        .and_then(serde_json::Value::as_str)
        .map(display_lifecycle)
        .unwrap_or_else(|| "unknown".to_string())
}

fn display_lifecycle(lifecycle: &str) -> String {
    lifecycle
        .trim_start_matches("GRAPH_LIFECYCLE_")
        .to_ascii_lowercase()
}

fn superseded_generation(
    record: &serde_json::Value,
    latest_generation: Option<u64>,
) -> Option<u64> {
    let generation = json_u64(record.pointer("/state/durable/graph/generation"))?;
    latest_generation.filter(|latest| *latest > generation)?;
    (!generation_is_terminal(record))
        .then(|| generation.checked_add(1))
        .flatten()
}

fn generation_is_terminal(record: &serde_json::Value) -> bool {
    record
        .pointer("/state/reports")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|reports| {
            reports.iter().any(|report| {
                report.get("publication").is_some()
                    || report
                        .get("diagnostics")
                        .and_then(serde_json::Value::as_array)
                        .is_some_and(|diagnostics| !diagnostics.is_empty())
                    || report
                        .get("dispositions")
                        .and_then(serde_json::Value::as_array)
                        .into_iter()
                        .flatten()
                        .any(|disposition| {
                            disposition.get("kind").and_then(serde_json::Value::as_str)
                                == Some("COMPONENT_DISPOSITION_KIND_FAILED")
                        })
            })
        })
}

fn outputs_for(record: &serde_json::Value, hash: &str) -> String {
    record
        .pointer("/state/reports")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .flat_map(|report| {
            report
                .get("outputs")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
        })
        .find(|output| {
            output
                .get("componentSpecHash")
                .and_then(serde_json::Value::as_str)
                == Some(hash)
        })
        .and_then(|output| output.get("valuesJson").and_then(serde_json::Value::as_str))
        .and_then(|encoded| {
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .ok()
        })
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
        .map(|value| {
            format!(
                "<pre>{}</pre>",
                escape_html(&serde_json::to_string_pretty(&value).unwrap_or_default())
            )
        })
        .unwrap_or_else(|| "none".to_string())
}

fn json_u64(value: Option<&serde_json::Value>) -> Option<u64> {
    value.and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
}

fn short_revision(revision: &str) -> &str {
    revision.get(..revision.len().min(8)).unwrap_or(revision)
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn create_api_router() -> Router<ServerStateRef> {
    let router = Router::new();
    router.route("/queue/{repo_name}", get(api_merge_queue))
}

async fn api_merge_queue(
    Path(repo_name): Path<String>,
    State(db): State<Arc<PgDbClient>>,
) -> Result<impl IntoResponse, AppError> {
    let repo = match db.repo_by_name(&repo_name).await? {
        Some(repo) => repo,
        None => {
            return Ok((
                StatusCode::NOT_FOUND,
                Json(format!("Repository {repo_name} not found")),
            )
                .into_response());
        }
    };

    #[derive(serde::Serialize)]
    #[serde(rename_all = "kebab-case")]
    enum PullRequestStatus {
        Closed,
        Draft,
        Merged,
        Open,
    }

    #[derive(serde::Serialize)]
    #[serde(rename_all = "kebab-case")]
    pub enum BuildStatus {
        Pending,
        Success,
        Failure,
        Cancelled,
        Timeouted,
    }

    #[derive(serde::Serialize)]
    struct PullRequest {
        number: u64,
        title: String,
        author: String,
        status: PullRequestStatus,
        head_branch: String,
        base_branch: String,
        priority: Option<u64>,
        approver: Option<String>,
        try_build: Option<BuildStatus>,
        auto_build: Option<BuildStatus>,
    }

    fn convert_status(status: database::BuildStatus) -> BuildStatus {
        match status {
            database::BuildStatus::Pending => BuildStatus::Pending,
            database::BuildStatus::Success => BuildStatus::Success,
            database::BuildStatus::Failure => BuildStatus::Failure,
            database::BuildStatus::Cancelled => BuildStatus::Cancelled,
            database::BuildStatus::Timeouted => BuildStatus::Timeouted,
        }
    }

    let prs = db.get_nonclosed_pull_requests(&repo.name).await?;
    let prs = sort_queue_prs(prs);
    let prs = prs
        .into_iter()
        .map(|pr| PullRequest {
            number: pr.number.0,
            title: pr.title,
            author: pr.author,
            status: match pr.status {
                bors::PullRequestStatus::Closed => PullRequestStatus::Closed,
                bors::PullRequestStatus::Draft => PullRequestStatus::Draft,
                bors::PullRequestStatus::Merged => PullRequestStatus::Merged,
                bors::PullRequestStatus::Open => PullRequestStatus::Open,
            },
            head_branch: pr.head_branch,
            base_branch: pr.base_branch,
            priority: pr.priority.map(|p| p as u64),
            approver: match pr.approval_status {
                ApprovalStatus::NotApproved => None,
                ApprovalStatus::Approved(info) => Some(info.approver),
            },
            try_build: pr.try_build.map(|b| convert_status(b.status)),
            auto_build: pr.auto_build.map(|b| convert_status(b.status)),
        })
        .collect::<Vec<_>>();
    Ok(Json(prs).into_response())
}

fn handle_panic(_err: Box<dyn Any + Send + 'static>) -> Response {
    tracing::error!("Router panicked");
    (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error").into_response()
}

async fn not_found_handler() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, HtmlTemplate(NotFoundTemplate {}))
}

async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, "")
}

async fn index_handler(State(ServerStateRef(state)): State<ServerStateRef>) -> impl IntoResponse {
    // If we manage exactly one repo, redirect to its queue page directly
    if let Some(repo_name) = state.ctx.repositories.repository_names().pop()
        && state.ctx.repositories.repo_count() == 1
    {
        return Redirect::temporary(&format!("/queue/{}", repo_name.name())).into_response();
    };
    help_handler(State(ServerStateRef(state)))
        .await
        .into_response()
}

async fn help_handler(State(ServerStateRef(state)): State<ServerStateRef>) -> impl IntoResponse {
    let mut repos = Vec::with_capacity(state.ctx.repositories.repo_count());
    for repo in state.ctx.repositories.repository_names() {
        let treeclosed = state
            .ctx
            .db
            .repo_db(&repo)
            .await
            .ok()
            .flatten()
            .is_some_and(|repo| repo.tree_state.is_closed());
        repos.push(RepositoryView {
            name: repo.name().to_string(),
            treeclosed,
        });
    }

    let help_md = format_help();
    let markdown = Parser::new(help_md);

    let mut help_html = String::new();
    pulldown_cmark::html::push_html(&mut help_html, markdown);

    HtmlTemplate(HelpTemplate {
        repos,
        help: help_html,
        cmd_prefix: state.get_cmd_prefix().as_ref().to_string(),
        service_name: state.ctx.service_name.clone(),
    })
}

#[derive(serde::Deserialize)]
pub struct QueueParams {
    #[serde(rename = "prs")]
    pull_requests: Option<PullRequestList>,
}

pub struct PullRequestList(Vec<u32>);

impl<'de> Deserialize<'de> for PullRequestList {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let prs = <&str>::deserialize(deserializer)?;
        let prs = prs
            .split(",")
            .map(|pr| {
                pr.parse::<u32>()
                    .map_err(|e| D::Error::custom(e.to_string()))
            })
            .collect::<Result<Vec<u32>, D::Error>>()?;
        Ok(Self(prs))
    }
}

pub async fn queue_handler(
    Path(repo_name): Path<String>,
    State(ServerStateRef(state)): State<ServerStateRef>,
    Query(params): Query<QueueParams>,
) -> Result<impl IntoResponse, AppError> {
    let db = &state.ctx.db;
    let repo = match db.repo_by_name(&repo_name).await? {
        Some(repo) => repo,
        None => {
            return Ok((
                StatusCode::NOT_FOUND,
                format!("Repository {repo_name} not found"),
            )
                .into_response());
        }
    };

    // Perform the queries concurrently to save a bit of time
    let (prs, rollups, last_ten_builds) = futures::future::join3(
        db.get_nonclosed_pull_requests(&repo.name),
        db.get_nonclosed_rollups(&repo.name),
        db.get_last_n_successful_auto_builds(&repo.name, 10),
    )
    .await;
    let prs = prs?;
    let rollups = rollups?;
    let last_ten_builds = last_ten_builds?;

    let prs = sort_queue_prs(prs);

    // Note: this assumed that there is ever at most a single pending build
    let pending_build = prs.iter().find_map(|pr| match pr.queue_status() {
        QueueStatus::Pending(_, build) => Some(build),
        _ => None,
    });
    let pending_workflow = match pending_build {
        Some(build) => db.get_workflows_for_build(build).await?.into_iter().next(),
        None => None,
    };

    let average_build_duration = {
        let total_duration = last_ten_builds
            .iter()
            .filter_map(|build| build.duration)
            .map(|d| d.0)
            .sum::<Duration>();
        let count = last_ten_builds.len() as u32;
        if total_duration.is_zero() {
            // Default guess of 3 hours per build
            Duration::from_secs(3600 * 3)
        } else if count > 1 {
            total_duration / count
        } else {
            total_duration
        }
    };

    let mut in_queue_count = 0;
    let mut failed_count = 0;

    // PR number -> expected remaining duration
    let mut in_queue: HashMap<PullRequestNumber, Duration> = HashMap::new();
    for pr in &prs {
        let status = pr.queue_status();
        let (in_queue_inc, failed_inc) = match &status {
            QueueStatus::Approved(..) => (1, 0),
            QueueStatus::ReadyForMerge(..) => (1, 0),
            QueueStatus::Pending(..) => (1, 0),
            QueueStatus::Failed(..) => (0, 1),
            QueueStatus::NotApproved | QueueStatus::NotOpen => (0, 0),
        };
        in_queue_count += in_queue_inc;
        failed_count += failed_inc;

        match &status {
            QueueStatus::Pending(_, _) => {
                // Try to guess already elapsed time of the pending workflow
                let elapsed = if let Some(workflow) = &pending_workflow {
                    (Utc::now() - workflow.created_at)
                        .to_std()
                        .unwrap_or_default()
                } else {
                    Duration::ZERO
                };
                in_queue.insert(pr.number, average_build_duration.saturating_sub(elapsed));
            }
            // For an approved PR, assume that it will take the average auto build duration
            QueueStatus::Approved(_) => {
                in_queue.insert(pr.number, average_build_duration);
            }
            QueueStatus::Failed(_, _)
            | QueueStatus::ReadyForMerge(_, _)
            | QueueStatus::NotOpen
            | QueueStatus::NotApproved => {}
        }
    }

    let mut expected_remaining_duration: Option<Duration> = None;

    // Rollup members whose rollup is in the queue, and thus its duration will be counted
    let rollup_members: HashSet<PullRequestNumber> = rollups
        .iter()
        .filter(|(rollup, _)| in_queue.contains_key(*rollup))
        .flat_map(|(_, member)| member)
        .copied()
        .collect();

    for (pr, remaining_duration) in in_queue {
        // For a rollup member, we will count its rollup instead
        if rollup_members.contains(&pr) {
            continue;
        }
        expected_remaining_duration =
            Some(expected_remaining_duration.unwrap_or_default() + remaining_duration);
    }

    Ok(HtmlTemplate(QueueTemplate {
        service_name: state.ctx.service_name.clone(),
        oauth_client_id: state
            .oauth
            .as_ref()
            .map(|client| client.config().client_id().to_string()),
        repo_name: repo.name.name().to_string(),
        repo_owner: repo.name.owner().to_string(),
        repo_url: format!("https://github.com/{}", repo.name),
        tree_state: repo.tree_state,
        stats: PullRequestStats {
            total_count: prs.len(),
            in_queue_count,
            failed_count,
        },
        prs,
        pending_workflow,
        selected_rollup_prs: params.pull_requests.map(|prs| prs.0).unwrap_or_default(),
        rollups_info: RollupsInfo::from(rollups),
        expected_remaining_duration,
        average_build_duration,
    })
    .into_response())
}

/// Axum handler that receives a webhook and sends it to a webhook channel.
pub async fn github_webhook_handler(
    State(ServerStateRef(state)): State<ServerStateRef>,
    GitHubWebhook(event): GitHubWebhook,
) -> impl IntoResponse {
    match event {
        BorsEvent::Global(e) => match state.global_event_queue.send(e).await {
            Ok(_) => (StatusCode::OK, ""),
            Err(err) => {
                tracing::error!("Could not send webhook global event: {err:?}");
                (StatusCode::INTERNAL_SERVER_ERROR, "")
            }
        },
        BorsEvent::Repository(e) => match state.repository_event_queue.send(e).await {
            Ok(_) => (StatusCode::OK, ""),
            Err(err) => {
                tracing::error!("Could not send webhook repository event: {err:?}");
                (StatusCode::INTERNAL_SERVER_ERROR, "")
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use crate::henosis::config::HenosisConfig;
    use crate::tests::{ApiRequest, BorsBuilder, BorsTester, default_repo_name, run_test};
    use serde_json::{Value, json};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn api_queue_page(pool: sqlx::PgPool) {
        run_test(pool, async |ctx: &mut BorsTester| {
            ctx.approve(()).await?;
            let response = ctx
                .api_request(ApiRequest::get(&format!("/api/queue/{}", default_repo_name().name())))
                .await?
                .assert_ok()
                .into_body();
            insta::assert_snapshot!(response, @r#"[{"number":1,"title":"Title of PR 1","author":"default-user","status":"open","head_branch":"pr/1","base_branch":"main","priority":null,"approver":"default-user","try_build":null,"auto_build":null}]"#);
            Ok(())
        })
        .await;
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn current_retired_page_differs_from_as_of_generation_and_marks_supersession(
        pool: sqlx::PgPool,
    ) {
        let core = MockServer::start().await;
        let environment_id = "preview_01kxc714rpftsts2nbgqax3hw2";
        let graph_id = "AZ9YcJMWfrOsiquF1dHHgg==";
        Mock::given(method("POST"))
            .and(path("/henosis.v1.GraphService/GetGraph"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "state": {
                    "durable": {
                        "graph": {
                            "id": graph_id,
                            "generation": "3",
                            "componentSpecHashes": []
                        },
                        "lifecycle": "GRAPH_LIFECYCLE_RETIRED"
                    },
                    "reports": []
                }
            })))
            .mount(&core)
            .await;
        Mock::given(method("POST"))
            .and(path("/henosis.v1.GraphService/GetGraphGeneration"))
            .respond_with(move |request: &Request| {
                let body: Value = request.body_json().unwrap();
                let generation = body["generation"].as_str().unwrap();
                ResponseTemplate::new(200).set_body_json(json!({
                    "state": {
                        "durable": {
                            "graph": {
                                "id": graph_id,
                                "generation": generation,
                                "componentSpecHashes": []
                            },
                            "lifecycle": "GRAPH_LIFECYCLE_ACTIVE"
                        },
                        "reports": [{
                            "graphId": graph_id,
                            "generation": generation,
                            "connector": "k8s",
                            "dispositions": [],
                            "sequence": "1"
                        }]
                    },
                    "components": [],
                    "currentLifecycle": "GRAPH_LIFECYCLE_ACTIVE",
                    "lastPublishedGeneration": "1"
                }))
            })
            .mount(&core)
            .await;
        let config: HenosisConfig = toml::from_str(&format!(
            r#"
deploy_repo = "rust-lang/borstest"
preview_mode = "on-demand"

[core_api]
endpoint = "{}"
token = "test-token"

[[components]]
name = "borstest"
repo = "rust-lang/borstest"

[[environments]]
id = "dev"
manifest_path = "dev.toml"
"#,
            core.uri()
        ))
        .unwrap();

        BorsBuilder::new(pool)
            .henosis_config(config)
            .run_test(async |ctx| {
                let latest = ctx
                    .api_request(ApiRequest::get(&format!("/graphs/{environment_id}")))
                    .await?
                    .assert_ok()
                    .into_body();
                assert!(latest.contains("<th>Lifecycle</th><td>retired</td>"));
                assert!(latest.contains("last generation 3"));
                assert!(latest.contains("immutable as-of evidence"));

                let immutable = ctx
                    .api_request(ApiRequest::get(&format!(
                        "/graphs/{environment_id}/generations/3"
                    )))
                    .await?
                    .assert_ok()
                    .into_body();
                assert!(immutable.contains("<th>Lifecycle</th><td>active</td>"));
                assert!(!immutable.contains("immutable as-of evidence"));

                let superseded = ctx
                    .api_request(ApiRequest::get(&format!(
                        "/graphs/{environment_id}/generations/2"
                    )))
                    .await?
                    .assert_ok()
                    .into_body();
                assert!(superseded.contains("Superseded by"));
                assert!(superseded.contains("generation 3"));
                assert!(superseded.contains("current audit context"));
                assert!(!superseded.contains("failed"));
                Ok(())
            })
            .await;
    }
}
