use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use henosis_bundle::{BundleError, BundleRequest, BundleSetManifest, Bundler, EsbuildBundler};
use henosis_core_boundary::{
    BundlePin, ConnectCoreBoundary, CoreBoundary, CoreBoundaryError, GraphIntent, GraphPhase,
    GraphStatus,
};

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
        #[arg(long, default_value = "http://127.0.0.1:4481")]
        core: String,
        /// Label the local recorded/fake controller targets in status output.
        #[arg(long)]
        demo_targets: bool,
    },
    /// Print the latest graph status.
    Status {
        graph: String,
        #[arg(long, default_value = "http://127.0.0.1:4481")]
        core: String,
        /// Label the local recorded/fake controller targets in status output.
        #[arg(long)]
        demo_targets: bool,
    },
    /// Watch graph status until it is complete, failed, or retired.
    Watch {
        graph: String,
        #[arg(long, default_value = "http://127.0.0.1:4481")]
        core: String,
        /// Label the local recorded/fake controller targets in status output.
        #[arg(long)]
        demo_targets: bool,
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
                "Run `henosis submit <graph>` first, or point `--core` at the server that accepted it.",
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
            core,
            demo_targets,
        } => {
            let bundles = read_bundle_manifest(&manifest).await?;
            let core = ConnectCoreBoundary::new(core);
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
            Ok(render_status(&status, demo_targets))
        }
        Command::Status {
            graph,
            core,
            demo_targets,
        } => {
            let core = ConnectCoreBoundary::new(core);
            Ok(render_status(&core.status(&graph).await?, demo_targets))
        }
        Command::Watch {
            graph,
            core,
            demo_targets,
        } => {
            let core = ConnectCoreBoundary::new(core);
            let mut receiver = core.watch(&graph).await?;
            let mut rendered = String::new();
            loop {
                let status = receiver.borrow_and_update().clone();
                rendered.push_str(&render_status(&status, demo_targets));
                if matches!(
                    status.phase,
                    GraphPhase::Ready | GraphPhase::Failed | GraphPhase::Retired
                ) {
                    break;
                }
                rendered.push_str("---\n");
                receiver.changed().await.map_err(|_| {
                    CoreBoundaryError::Transport(
                        "core watch closed before a terminal status".into(),
                    )
                })?;
            }
            Ok(rendered)
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

fn render_status(status: &GraphStatus, demo_targets: bool) -> String {
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
    let mut rendered = format!(
        "graph: {}\ngeneration: {}\nplan: {} resource(s)\nblocked-on: {}\nobserved-ready: {}\nstatus: {}\n",
        status.graph,
        status.generation,
        status.planned_resources,
        blocked,
        status.observed_ready,
        phase
    );
    if demo_targets {
        rendered.push_str(
            "targets: k8s=file:// Git; supabase=fake; cloudflare=recorded/fake (no live credentials)\n",
        );
    }
    rendered
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshots_missing_graph_diagnostic() {
        insta::assert_snapshot!(CliError::Core(CoreBoundaryError::GraphNotFound("preview_demo".to_string())).diagnostic(), @r#"
error[HENOSIS_GRAPH_NOT_FOUND]: graph `preview_demo` does not exist in the configured core boundary
  |
  = help: Run `henosis submit <graph>` first, or point `--core` at the server that accepted it.
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
