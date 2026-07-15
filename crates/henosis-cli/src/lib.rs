use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::{Parser, Subcommand};
use henosis_bundle::{BundleError, BundleRequest, BundleSetManifest, Bundler, EsbuildBundler};
use henosis_core_boundary::{
    BundlePin, CoreBoundary, CoreBoundaryError, GraphIntent, GraphPhase, GraphStatus,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, watch};

#[derive(Debug, Parser)]
#[command(
    name = "henosis",
    about = "Package and submit Henosis component graphs"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Discover and package every component in a repository.
    Bundle {
        #[arg(default_value = ".")]
        repository: PathBuf,
        #[arg(short, long, default_value = ".henosis/bundles")]
        output: PathBuf,
    },
    /// Submit a bundle manifest as graph intent.
    Submit {
        graph: String,
        #[arg(short, long, default_value = ".henosis/bundles/manifest.json")]
        manifest: PathBuf,
        #[arg(long, default_value = ".henosis/fake-core.json")]
        state: PathBuf,
    },
    /// Print the latest graph status.
    Status {
        graph: String,
        #[arg(long, default_value = ".henosis/fake-core.json")]
        state: PathBuf,
    },
    /// Watch graph status. The local fake emits its current value once.
    Watch {
        graph: String,
        #[arg(long, default_value = ".henosis/fake-core.json")]
        state: PathBuf,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error(transparent)]
    Bundle(#[from] BundleError),
    #[error("cannot read `{path}`: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("cannot decode `{path}`: {source}")]
    Decode {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error(transparent)]
    Core(#[from] CoreBoundaryError),
}

impl CliError {
    pub fn diagnostic(&self) -> String {
        let (code, summary, help) = match self {
            Self::Bundle(BundleError::NoComponents(path)) => (
                "HENOSIS_BUNDLE_NO_COMPONENTS",
                format!("no components were discovered under `{}`", path.display()),
                "Export components with `export default defineComponent({ ... })` from TypeScript source files.",
            ),
            Self::Bundle(BundleError::Esbuild { component, stderr }) => (
                "HENOSIS_BUNDLE_FAILED",
                format!("component `{component}` could not be bundled\n\n{stderr}"),
                "Fix the first esbuild diagnostic. Dependencies must resolve from the repository and no runtime npm imports may remain.",
            ),
            Self::Core(CoreBoundaryError::GraphNotFound(graph)) => (
                "HENOSIS_GRAPH_NOT_FOUND",
                format!("graph `{graph}` does not exist in the configured core boundary"),
                "Run `henosis submit <graph>` first, or point the command at the state created by submit.",
            ),
            error => (
                "HENOSIS_COMMAND_FAILED",
                error.to_string(),
                "Correct the reported input and run the command again.",
            ),
        };
        format!("error[{code}]: {summary}\n  |\n  = help: {help}")
    }
}

pub async fn run(cli: Cli) -> Result<String, CliError> {
    match cli.command {
        Command::Bundle { repository, output } => {
            let bundles = EsbuildBundler.bundle(&BundleRequest { repository, output })?;
            Ok(render_bundle_result(&bundles))
        }
        Command::Submit {
            graph,
            manifest,
            state,
        } => {
            let bundles = read_bundle_manifest(&manifest).await?;
            let core = FileFakeCore::open(state).await?;
            let existing = core.status(&graph).await.ok();
            let pins = bundles
                .bundles
                .into_iter()
                .map(|bundle| BundlePin {
                    component: bundle.component,
                    bundle_id: bundle.bundle_id,
                })
                .collect();
            let intent = if existing.is_some() {
                GraphIntent::Update {
                    graph: graph.clone(),
                    bundles: pins,
                }
            } else {
                GraphIntent::Create {
                    graph: graph.clone(),
                    bundles: pins,
                }
            };
            let status = core.apply(intent).await?;
            Ok(render_status(&status))
        }
        Command::Status { graph, state } => {
            let core = FileFakeCore::open(state).await?;
            Ok(render_status(&core.status(&graph).await?))
        }
        Command::Watch { graph, state } => {
            let core = FileFakeCore::open(state).await?;
            let receiver = core.watch(&graph).await?;
            Ok(render_status(&receiver.borrow().clone()))
        }
    }
}

fn render_bundle_result(bundles: &BundleSetManifest) -> String {
    let mut rendered = format!("bundled {} component(s)\n", bundles.bundles.len());
    for bundle in &bundles.bundles {
        rendered.push_str(&format!(
            "  {}  {}  {}\n",
            bundle.component,
            bundle.bundle_id,
            bundle.module.display()
        ));
    }
    rendered
}

fn render_status(status: &GraphStatus) -> String {
    let phase = match status.phase {
        GraphPhase::Planning => "planning",
        GraphPhase::Blocked => "blocked",
        GraphPhase::Reconciling => "reconciling",
        GraphPhase::Ready => "ready",
        GraphPhase::Failed => "failed",
        GraphPhase::Retired => "retired",
    };
    let blocked = if status.blocked_on.is_empty() {
        "none".to_string()
    } else {
        status
            .blocked_on
            .iter()
            .map(|blocked| format!("{}.{}", blocked.component, blocked.input))
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!(
        "graph: {}\ngeneration: {}\nplan: {} resource(s)\nblocked-on: {}\nobserved-ready: {}\nstatus: {}\n",
        status.graph,
        status.generation,
        status.planned_resources,
        blocked,
        status.observed_ready,
        phase
    )
}

async fn read_bundle_manifest(path: &Path) -> Result<BundleSetManifest, CliError> {
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|source| CliError::Read {
            path: path.to_path_buf(),
            source,
        })?;
    serde_json::from_slice(&bytes).map_err(|source| CliError::Decode {
        path: path.to_path_buf(),
        source,
    })
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct PersistedFakeCore {
    statuses: BTreeMap<String, GraphStatus>,
    intents: Vec<GraphIntent>,
}

#[derive(Debug, Clone)]
struct FileFakeCore {
    path: PathBuf,
    state: Arc<Mutex<PersistedFakeCore>>,
}

impl FileFakeCore {
    async fn open(path: PathBuf) -> Result<Self, CliError> {
        let state = match tokio::fs::read(&path).await {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|source| CliError::Decode {
                path: path.clone(),
                source,
            })?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                PersistedFakeCore::default()
            }
            Err(source) => {
                return Err(CliError::Read {
                    path: path.clone(),
                    source,
                });
            }
        };
        Ok(Self {
            path,
            state: Arc::new(Mutex::new(state)),
        })
    }

    async fn persist(&self, state: &PersistedFakeCore) -> Result<(), CoreBoundaryError> {
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|error| CoreBoundaryError::Rejected(error.to_string()))?;
        }
        let mut bytes = serde_json::to_vec_pretty(state)
            .map_err(|error| CoreBoundaryError::Rejected(error.to_string()))?;
        bytes.push(b'\n');
        tokio::fs::write(&self.path, bytes)
            .await
            .map_err(|error| CoreBoundaryError::Rejected(error.to_string()))
    }
}

impl CoreBoundary for FileFakeCore {
    async fn apply(&self, intent: GraphIntent) -> Result<GraphStatus, CoreBoundaryError> {
        let graph = intent.graph().to_string();
        let mut state = self.state.lock().await;
        let generation = state
            .statuses
            .get(&graph)
            .map(|status| status.generation + 1)
            .unwrap_or(1);
        let mut status = GraphStatus::planning(graph.clone(), generation);
        if matches!(intent, GraphIntent::Retire { .. }) {
            status.phase = GraphPhase::Retired;
        }
        state.intents.push(intent);
        state.statuses.insert(graph, status.clone());
        self.persist(&state).await?;
        Ok(status)
    }

    async fn status(&self, graph: &str) -> Result<GraphStatus, CoreBoundaryError> {
        self.state
            .lock()
            .await
            .statuses
            .get(graph)
            .cloned()
            .ok_or_else(|| CoreBoundaryError::GraphNotFound(graph.to_string()))
    }

    async fn watch(&self, graph: &str) -> Result<watch::Receiver<GraphStatus>, CoreBoundaryError> {
        let status = self.status(graph).await?;
        let (_, receiver) = watch::channel(status);
        Ok(receiver)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshots_missing_graph_diagnostic() {
        insta::assert_snapshot!(CliError::Core(CoreBoundaryError::GraphNotFound("preview_demo".to_string())).diagnostic(), @r#"
error[HENOSIS_GRAPH_NOT_FOUND]: graph `preview_demo` does not exist in the configured core boundary
  |
  = help: Run `henosis submit <graph>` first, or point the command at the state created by submit.
"#);
    }

    #[test]
    fn snapshots_discovery_diagnostic() {
        insta::assert_snapshot!(CliError::Bundle(BundleError::NoComponents(PathBuf::from("/work/repo"))).diagnostic(), @r#"
error[HENOSIS_BUNDLE_NO_COMPONENTS]: no components were discovered under `/work/repo`
  |
  = help: Export components with `export default defineComponent({ ... })` from TypeScript source files.
"#);
    }
}
