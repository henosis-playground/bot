use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::io::{IsTerminal as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::time::{Duration, SystemTime};

use clap::{Args, CommandFactory as _, Parser, Subcommand};
use henosis_app::{
    ApplyGraph, ArtifactBinding, BundleError, BundleRequest, BundleSetManifest, Bundler,
    CheckoutService, EsbuildBundler, GraphOperation, PreparedSource, SourceRequest,
};
use henosis_artifacts::DirectoryArtifactService;
#[cfg(test)]
use henosis_core_boundary::BundlePin;
use henosis_core_boundary::{
    ConnectCoreBoundary, CoreBoundary, CoreBoundaryError, GraphIntent, GraphPhase,
    GraphSourcePolicy, GraphStatus, GraphSummary, SourceProvenance,
};
use serde::{Deserialize, Serialize};

const DEFAULT_CORE: &str = "http://127.0.0.1:4481";
const CONTEXT_PATH: &str = ".henosis/context";
const BUNDLE_PATH: &str = ".henosis/bundles";
const ARTIFACT_PATH: &str = ".henosis/artifacts";
const TRANSIENT_RETRIES: usize = 3;

#[derive(Debug, Parser)]
#[command(
    name = "henosis",
    about = "Develop and deploy Henosis component graphs",
    long_about = "Develop and deploy Henosis component graphs.\n\nHenosis keeps the selected graph and core target in .henosis/context. Graph creation is always explicit."
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Continuously typecheck, build artifacts, bundle configuration, deploy, and follow status.
    Dev(PipelineArgs),
    /// Typecheck, build artifacts, bundle configuration, deploy once, and wait for status.
    Deploy(PipelineArgs),
    /// Print the selected graph's current status.
    Status(TargetArgs),
    /// Retire the selected graph.
    Retire(TargetArgs),
    /// Low-level packaging and troubleshooting commands.
    Debug {
        #[command(subcommand)]
        command: DebugCommand,
    },
}

#[derive(Debug, Subcommand)]
pub enum DebugCommand {
    /// Discover and package every component without deploying it.
    Bundle {
        #[arg(default_value = ".")]
        repository: PathBuf,
        #[arg(short, long, default_value = BUNDLE_PATH)]
        output: PathBuf,
    },
}

#[derive(Debug, Clone, Args)]
pub struct PipelineArgs {
    /// Repository containing Henosis TypeScript components.
    #[arg(default_value = ".")]
    pub repository: PathBuf,
    /// Select this graph and save it in .henosis/context.
    #[arg(long)]
    pub graph: Option<String>,
    /// Frontend-owned environment name to present with --graph (saved in context).
    #[arg(long, requires = "graph")]
    pub name: Option<String>,
    /// Core GraphService URL (saved with --graph).
    #[arg(long)]
    pub core: Option<String>,
    /// Explicitly create --graph when it does not exist.
    #[arg(long, requires = "graph")]
    pub create: bool,
    /// Require every submitted component to have verified VCS provenance.
    #[arg(long, requires = "create")]
    pub require_vcs: bool,
    /// Label the local recorded/fake controller targets in status output.
    #[arg(long, hide = true)]
    pub demo_targets: bool,
}

#[derive(Debug, Clone, Args)]
pub struct TargetArgs {
    /// Select this graph and save it in .henosis/context.
    #[arg(long)]
    pub graph: Option<String>,
    /// Frontend-owned environment name to present with --graph (saved in context).
    #[arg(long, requires = "graph")]
    pub name: Option<String>,
    /// Core GraphService URL (saved with --graph).
    #[arg(long)]
    pub core: Option<String>,
    /// Label the local recorded/fake controller targets in status output.
    #[arg(long, hide = true)]
    pub demo_targets: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    FixSource,
    Transient,
    FatalSetup,
}

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error(transparent)]
    Bundle(#[from] BundleError),
    #[error(transparent)]
    Operation(#[from] henosis_app::OperationError),
    #[error("TypeScript typecheck failed\n\n{0}")]
    Typecheck(String),
    #[error("cannot run TypeScript typecheck: {0}")]
    TypecheckSetup(String),
    #[error("cannot read `{path}`: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("cannot write `{path}`: {source}")]
    Write {
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
    #[error("no graph is selected")]
    ContextMissing {
        core: String,
        graphs: Vec<GraphSummary>,
    },
    #[error("the selected graph `{graph}` no longer exists at {core}")]
    ContextStale {
        graph: String,
        core: String,
        graphs: Vec<GraphSummary>,
    },
    #[error("graph `{graph}` does not exist at {core}")]
    GraphMissing { graph: String, core: String },
    #[error("deployment generation {generation} failed")]
    DeploymentFailed { generation: u64 },
}

impl CliError {
    pub fn classify(&self) -> ErrorClass {
        match self {
            Self::Typecheck(_)
            | Self::DeploymentFailed { .. }
            | Self::Bundle(BundleError::NoComponents(_))
            | Self::Bundle(BundleError::Esbuild { .. })
            | Self::Bundle(BundleError::ExternalImport { .. })
            | Self::Bundle(BundleError::ReadSource { .. })
            | Self::Operation(henosis_app::OperationError::Bundle(
                BundleError::NoComponents(_)
                | BundleError::Esbuild { .. }
                | BundleError::ExternalImport { .. }
                | BundleError::ReadSource { .. },
            ))
            | Self::Operation(henosis_app::OperationError::Artifact(_)) => ErrorClass::FixSource,
            Self::Core(CoreBoundaryError::Transport(_))
            | Self::Operation(henosis_app::OperationError::Core(_)) => ErrorClass::Transient,
            Self::Bundle(_)
            | Self::Operation(_)
            | Self::TypecheckSetup(_)
            | Self::Read { .. }
            | Self::Write { .. }
            | Self::Decode { .. }
            | Self::Core(_)
            | Self::ContextMissing { .. }
            | Self::ContextStale { .. }
            | Self::GraphMissing { .. } => ErrorClass::FatalSetup,
        }
    }

    pub fn exit_code(&self) -> u8 {
        match self {
            Self::Typecheck(_)
            | Self::Bundle(_)
            | Self::Operation(henosis_app::OperationError::Bundle(_))
            | Self::Operation(henosis_app::OperationError::Artifact(_)) => 2,
            Self::DeploymentFailed { .. } => 1,
            _ => 3,
        }
    }

    pub fn diagnostic(&self) -> String {
        let (code, summary, help) = match self {
            Self::ContextMissing { core, graphs } => (
                "HENOSIS_CONTEXT_MISSING",
                format!(
                    "no graph is selected for this repository{}",
                    render_graph_discovery(core, graphs)
                ),
                "Select an existing graph with `henosis deploy --graph <graph-id>`, or explicitly create one with `henosis deploy --graph <graph-id> --create`.",
            ),
            Self::ContextStale {
                graph,
                core,
                graphs,
            } => (
                "HENOSIS_CONTEXT_STALE",
                format!(
                    "selected graph `{graph}` no longer exists at {core}{}",
                    render_graph_discovery(core, graphs)
                ),
                "Select another existing graph with `henosis deploy --graph <graph-id>`. Add `--create` only when you intend to create a new graph.",
            ),
            Self::GraphMissing { graph, core } => (
                "HENOSIS_GRAPH_NOT_FOUND",
                format!("graph `{graph}` does not exist at {core}"),
                "Create it explicitly with `henosis deploy --graph <graph-id> --create`.",
            ),
            Self::Bundle(BundleError::NoComponents(path)) => (
                "HENOSIS_BUNDLE_NO_COMPONENTS",
                format!("no components were discovered under `{}`", path.display()),
                "Export components with `export default defineComponent({ ... })` from TypeScript source files.",
            ),
            Self::Bundle(BundleError::Esbuild { component, stderr }) => (
                "HENOSIS_BUNDLE_FAILED",
                format!("component `{component}` could not be bundled\n\n{stderr}"),
                "Fix the first bundler diagnostic. Dependencies must resolve from the repository and no runtime npm imports may remain.",
            ),
            Self::Typecheck(output) => (
                "HENOSIS_TYPECHECK_FAILED",
                output.clone(),
                "Fix the TypeScript diagnostics. The active generation was not changed.",
            ),
            Self::Core(CoreBoundaryError::Rejected(message))
                if message.contains("expected generation") =>
            {
                (
                    "HENOSIS_GENERATION_CONFLICT",
                    message.clone(),
                    "Another deploy won the generation race. Run the command again against the new active generation.",
                )
            }
            Self::Core(CoreBoundaryError::GraphNotFound(graph)) => (
                "HENOSIS_GRAPH_NOT_FOUND",
                format!("graph `{graph}` does not exist in the configured core boundary"),
                "Select an existing graph with `henosis deploy --graph <graph-id>`, or use `--create` explicitly.",
            ),
            error => (
                "HENOSIS_COMMAND_FAILED",
                error.to_string(),
                "Correct the reported input and run the command again.",
            ),
        };
        format!("error[{code}]: {summary}\n  |\n  = help: {help}")
    }

    pub fn rendered_diagnostic(&self) -> String {
        let diagnostic = self.diagnostic();
        if color_enabled() {
            diagnostic.replacen("error[", "\u{1b}[1;31merror\u{1b}[0m[", 1)
        } else {
            diagnostic
        }
    }
}

fn render_graph_discovery(core: &str, graphs: &[GraphSummary]) -> String {
    if graphs.is_empty() {
        return format!("; no live graphs were found at {core}");
    }
    let discovered = graphs
        .iter()
        .map(|graph| {
            format!(
                "  {} (generation {}, {})",
                graph.graph,
                graph.generation,
                match graph.phase {
                    GraphPhase::Planning => "planning",
                    GraphPhase::Blocked => "blocked",
                    GraphPhase::Reconciling => "reconciling",
                    GraphPhase::Ready => "ready",
                    GraphPhase::Failed => "failed",
                    GraphPhase::Retired => "retired",
                }
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!("\n\nLive graphs at {core}:\n{discovered}")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LocalContext {
    graph: String,
    #[serde(default)]
    name: Option<String>,
    core: String,
}

#[derive(Debug, Clone)]
struct SelectedTarget {
    graph: String,
    name: Option<String>,
    core: String,
    current: Option<GraphStatus>,
    create: bool,
    source_policy: GraphSourcePolicy,
}

#[derive(Debug)]
struct CycleResult {
    output: String,
    dependencies: Vec<PathBuf>,
    generation: u64,
}

pub async fn run(cli: Cli) -> Result<String, CliError> {
    match cli.command {
        Command::Debug {
            command: DebugCommand::Bundle { repository, output },
        } => {
            let bundles = EsbuildBundler.bundle(&BundleRequest { repository, output })?;
            Ok(render_bundle_result(&bundles))
        }
        Command::Deploy(args) => deploy(args).await,
        Command::Dev(args) => dev(args).await,
        Command::Status(args) => {
            let target = select_target(
                Path::new("."),
                &args.graph,
                &args.name,
                &args.core,
                false,
                false,
            )
            .await?;
            Ok(render_status(
                target.name.as_deref(),
                target
                    .current
                    .as_ref()
                    .expect("status selection requires graph"),
                args.demo_targets,
            ))
        }
        Command::Retire(args) => {
            let target = select_target(
                Path::new("."),
                &args.graph,
                &args.name,
                &args.core,
                false,
                false,
            )
            .await?;
            let current = target.current.expect("retire selection requires graph");
            let core = ConnectCoreBoundary::new(&target.core);
            let status = core
                .apply(GraphIntent::Retire {
                    graph: target.graph,
                })
                .await?;
            Ok(format!(
                "retired environment {} at generation {} (previous generation {})\n{}",
                environment_identity(target.name.as_deref(), &status.graph),
                status.generation,
                current.generation,
                render_status(target.name.as_deref(), &status, args.demo_targets)
            ))
        }
    }
}

async fn deploy(args: PipelineArgs) -> Result<String, CliError> {
    let target = select_target(
        &args.repository,
        &args.graph,
        &args.name,
        &args.core,
        args.create,
        args.require_vcs,
    )
    .await?;
    Ok(run_cycle(&args, target).await?.output)
}

async fn dev(args: PipelineArgs) -> Result<String, CliError> {
    let mut target = select_target(
        &args.repository,
        &args.graph,
        &args.name,
        &args.core,
        args.create,
        args.require_vcs,
    )
    .await?;
    let mut dependencies = fallback_watch_set(&args.repository)?;
    let mut active_generation = target
        .current
        .as_ref()
        .map_or(0, |status| status.generation);
    let mut transient_attempt = 0;

    loop {
        match run_cycle(&args, target.clone()).await {
            Ok(cycle) => {
                print!("{}", cycle.output);
                std::io::stdout().flush().ok();
                dependencies = cycle.dependencies;
                active_generation = cycle.generation;
                transient_attempt = 0;
                target = select_target(&args.repository, &None, &None, &None, false, false).await?;
            }
            Err(error) => match error.classify() {
                ErrorClass::FixSource => {
                    eprintln!("{}", error.rendered_diagnostic());
                    eprintln!("generation {active_generation} remains active");
                    transient_attempt = 0;
                }
                ErrorClass::Transient if transient_attempt < TRANSIENT_RETRIES => {
                    transient_attempt += 1;
                    eprintln!(
                        "transient core error (retry {transient_attempt}/{TRANSIENT_RETRIES}): {error}"
                    );
                    tokio::time::sleep(Duration::from_millis(250 * transient_attempt as u64)).await;
                    continue;
                }
                ErrorClass::Transient => {
                    eprintln!("{}", error.rendered_diagnostic());
                    eprintln!(
                        "generation {active_generation} remains active; waiting before retry"
                    );
                    transient_attempt = 0;
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
                ErrorClass::FatalSetup => return Err(error),
            },
        }

        tokio::select! {
            result = wait_for_change(&dependencies) => result?,
            signal = tokio::signal::ctrl_c() => {
                signal.map_err(|error| CliError::TypecheckSetup(error.to_string()))?;
                return Ok(String::new());
            }
        }
    }
}

#[derive(Clone)]
struct LocalCheckout {
    source: PreparedSource,
}

impl CheckoutService for LocalCheckout {
    type Error = std::convert::Infallible;

    async fn checkout(&self, _request: &SourceRequest) -> Result<PreparedSource, Self::Error> {
        Ok(self.source.clone())
    }
}

async fn run_cycle(args: &PipelineArgs, target: SelectedTarget) -> Result<CycleResult, CliError> {
    let repository = args
        .repository
        .canonicalize()
        .map_err(|source| CliError::Read {
            path: args.repository.clone(),
            source,
        })?;
    let initial_source = repository_provenance(&repository, &fallback_watch_set(&repository)?);
    emit_stdout(&render_target_banner(&target, &initial_source));

    run_typecheck(&repository).await?;
    let checkout = LocalCheckout {
        source: PreparedSource {
            repository: repository.clone(),
            provenance: repository_provenance(&repository, &fallback_watch_set(&repository)?),
            component: None,
            lease: None,
        },
    };
    let core = ConnectCoreBoundary::new(&target.core);
    let operation = GraphOperation::new(
        core.clone(),
        EsbuildBundler,
        DirectoryArtifactService::new(repository.join(ARTIFACT_PATH)),
        checkout,
        repository.join(BUNDLE_PATH),
    );
    let outcome = operation
        .apply(ApplyGraph {
            graph: target.graph.clone(),
            sources: vec![SourceRequest {
                repository: repository.display().to_string(),
                revision: None,
                reference: None,
                component: None,
            }],
            create: target.create,
            source_policy: target.source_policy,
            preserve_unmentioned: false,
        })
        .await?;
    emit_stdout(&render_artifact_result(&outcome.artifacts));
    emit_stdout(&format!(
        "changed components: {}\n",
        if outcome.changed_components.is_empty() {
            "none".to_string()
        } else {
            outcome.changed_components.join(", ")
        }
    ));

    let dependencies =
        observed_dependencies(&repository, &outcome.dependencies, &outcome.artifacts);
    if !outcome.changed {
        let generation = outcome.status.generation;
        emit_stdout(&format!(
            "no deployable changes — generation {generation} remains active\n"
        ));
        return Ok(CycleResult {
            output: String::new(),
            dependencies,
            generation,
        });
    }

    let accepted = outcome.status;
    write_context(
        &repository,
        &LocalContext {
            graph: target.graph.clone(),
            name: target.name.clone(),
            core: target.core.clone(),
        },
    )?;
    emit_stdout(&format!("accepted generation {}\n", accepted.generation));
    let terminal = stream_generation(
        &core,
        target.name.as_deref(),
        &target.graph,
        accepted.generation,
        args.demo_targets,
    )
    .await?;
    if terminal.phase == GraphPhase::Failed {
        return Err(CliError::DeploymentFailed {
            generation: terminal.generation,
        });
    }
    Ok(CycleResult {
        output: String::new(),
        dependencies,
        generation: terminal.generation,
    })
}

async fn select_target(
    repository: &Path,
    graph: &Option<String>,
    name: &Option<String>,
    core: &Option<String>,
    create: bool,
    require_vcs: bool,
) -> Result<SelectedTarget, CliError> {
    let repository = repository.canonicalize().map_err(|source| CliError::Read {
        path: repository.to_path_buf(),
        source,
    })?;
    let explicit = graph.as_ref().map(|graph| LocalContext {
        graph: graph.clone(),
        name: name.clone(),
        core: core.clone().unwrap_or_else(|| DEFAULT_CORE.to_string()),
    });
    let (context, from_file) = match explicit {
        Some(context) => (context, false),
        None => match read_context(&repository) {
            Ok(context) => (context, true),
            Err(CliError::ContextMissing { .. }) => {
                let core = DEFAULT_CORE.to_string();
                let graphs = ConnectCoreBoundary::new(&core).list(false).await?;
                return Err(CliError::ContextMissing { core, graphs });
            }
            Err(error) => return Err(error),
        },
    };
    let boundary = ConnectCoreBoundary::new(&context.core);
    let current = match boundary.status(&context.graph).await {
        Ok(status) => Some(status),
        Err(CoreBoundaryError::GraphNotFound(_)) if create && !from_file => None,
        Err(CoreBoundaryError::GraphNotFound(_)) if from_file => {
            let graphs = boundary.list(false).await?;
            return Err(CliError::ContextStale {
                graph: context.graph,
                core: context.core,
                graphs,
            });
        }
        Err(CoreBoundaryError::GraphNotFound(_)) => {
            return Err(CliError::GraphMissing {
                graph: context.graph,
                core: context.core,
            });
        }
        Err(error) => return Err(error.into()),
    };
    if !from_file && current.is_some() {
        write_context(&repository, &context)?;
    }
    Ok(SelectedTarget {
        graph: context.graph,
        name: context.name,
        core: context.core,
        current,
        create,
        source_policy: if require_vcs {
            GraphSourcePolicy::RequireVcs
        } else {
            GraphSourcePolicy::AcceptLocal
        },
    })
}

fn read_context(repository: &Path) -> Result<LocalContext, CliError> {
    let path = repository.join(CONTEXT_PATH);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(CliError::ContextMissing {
                core: DEFAULT_CORE.to_string(),
                graphs: Vec::new(),
            });
        }
        Err(source) => return Err(CliError::Read { path, source }),
    };
    serde_json::from_slice(&bytes).map_err(|source| CliError::Decode { path, source })
}

fn write_context(repository: &Path, context: &LocalContext) -> Result<(), CliError> {
    let path = repository.join(CONTEXT_PATH);
    let parent = path.parent().expect("context path has parent");
    std::fs::create_dir_all(parent).map_err(|source| CliError::Write {
        path: parent.to_path_buf(),
        source,
    })?;
    let mut bytes = serde_json::to_vec_pretty(context).expect("context is JSON encodable");
    bytes.push(b'\n');
    std::fs::write(&path, bytes).map_err(|source| CliError::Write { path, source })
}

async fn run_typecheck(repository: &Path) -> Result<(), CliError> {
    let executable = repository
        .ancestors()
        .map(|directory| directory.join("node_modules/.bin/tsc"))
        .find(|candidate| candidate.is_file())
        .unwrap_or_else(|| PathBuf::from("tsc"));
    let output = tokio::process::Command::new(&executable)
        .current_dir(repository)
        .arg("--noEmit")
        .output()
        .await
        .map_err(|error| {
            CliError::TypecheckSetup(format!(
                "could not execute `{}`: {error}. Install TypeScript in this repository.",
                executable.display()
            ))
        })?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    Err(CliError::Typecheck(if stderr.is_empty() {
        stdout
    } else if stdout.is_empty() {
        stderr
    } else {
        format!("{stdout}\n{stderr}")
    }))
}

fn observed_dependencies(
    repository: &Path,
    bundle_dependencies: &[PathBuf],
    artifacts: &[ArtifactBinding],
) -> Vec<PathBuf> {
    let mut dependencies = bundle_dependencies
        .iter()
        .cloned()
        .chain(artifacts.iter().map(|artifact| artifact.source.clone()))
        .collect::<Vec<_>>();
    for name in [
        "package.json",
        "pnpm-lock.yaml",
        "package-lock.json",
        "yarn.lock",
        "bun.lockb",
        "tsconfig.json",
    ] {
        let path = repository.join(name);
        if path.is_file() {
            dependencies.push(path);
        }
    }
    dependencies.sort();
    dependencies.dedup();
    dependencies
}

fn repository_provenance(repository: &Path, dependencies: &[PathBuf]) -> SourceProvenance {
    let root = git_output(repository, ["rev-parse", "--show-toplevel"])
        .ok()
        .map(PathBuf::from);
    let Some(root) = root else {
        return SourceProvenance::Local {
            repository: None,
            base_revision: None,
            dirty: true,
        };
    };
    let revision = git_output(&root, ["rev-parse", "HEAD"]).ok();
    let remote_url = git_output(&root, ["remote", "get-url", "origin"])
        .ok()
        .map(|value| sanitize_repository(&value));
    let relative = dependencies
        .iter()
        .filter_map(|path| path.strip_prefix(&root).ok())
        .map(|path| path.as_os_str())
        .collect::<Vec<_>>();
    let all_inside = relative.len() == dependencies.len();
    let mut command = ProcessCommand::new("git");
    command
        .current_dir(&root)
        .args(["status", "--porcelain", "--untracked-files=normal", "--"]);
    command.args(&relative);
    let clean = all_inside
        && command
            .output()
            .ok()
            .filter(|output| output.status.success())
            .is_some_and(|output| output.stdout.is_empty());
    if clean
        && let (Some(revision), Some((reference, repository_url))) =
            (revision.clone(), verified_remote_reference(&root))
    {
        return SourceProvenance::Vcs {
            repository: repository_url,
            revision,
            reference: Some(reference),
        };
    }
    SourceProvenance::Local {
        repository: remote_url,
        base_revision: revision,
        dirty: !clean,
    }
}

fn verified_remote_reference(repository: &Path) -> Option<(String, String)> {
    let revision = git_output(repository, ["rev-parse", "HEAD"]).ok()?;
    let references = git_output(
        repository,
        [
            "for-each-ref",
            "--format=%(refname)",
            "--points-at",
            "HEAD",
            "refs/remotes",
        ],
    )
    .ok()?;
    for reference in references
        .lines()
        .filter(|reference| !reference.ends_with("/HEAD"))
    {
        let rest = reference.strip_prefix("refs/remotes/")?;
        let (remote, branch) = rest.split_once('/')?;
        let advertised = format!("refs/heads/{branch}");
        let output = ProcessCommand::new("git")
            .current_dir(repository)
            .args(["ls-remote", "--refs", remote, &advertised])
            .output()
            .ok()?;
        if !output.status.success() {
            continue;
        }
        let advertised_revision = String::from_utf8_lossy(&output.stdout)
            .split_whitespace()
            .next()
            .map(str::to_string);
        if advertised_revision.as_deref() != Some(&revision) {
            continue;
        }
        let url = git_output(repository, ["remote", "get-url", remote]).ok()?;
        return Some((format!("{remote}/{branch}"), sanitize_repository(&url)));
    }
    None
}

fn sanitize_repository(value: &str) -> String {
    if let Ok(mut url) = url::Url::parse(value) {
        let _ = url.set_username("");
        let _ = url.set_password(None);
        url.set_query(None);
        url.set_fragment(None);
        return url.to_string().trim_end_matches('/').to_string();
    }
    if let Some((user_host, path)) = value.split_once(':')
        && user_host.contains('@')
        && !path.starts_with('/')
    {
        let host = user_host.rsplit('@').next().unwrap_or(user_host);
        return format!("ssh://{host}/{}", path.trim_start_matches('/'));
    }
    value.to_string()
}

fn git_output<I, S>(repository: &Path, args: I) -> Result<String, ()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = ProcessCommand::new("git")
        .current_dir(repository)
        .args(args)
        .output()
        .map_err(|_| ())?;
    if !output.status.success() {
        return Err(());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

#[cfg(test)]
fn changed_components(current: Option<&GraphStatus>, pins: &[BundlePin]) -> Vec<String> {
    let desired = pins
        .iter()
        .map(|pin| {
            (
                pin.component.as_str(),
                (pin.bundle_id.as_str(), &pin.input_bindings),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let existing = current
        .into_iter()
        .flat_map(|status| status.bundles.iter())
        .map(|pin| {
            (
                pin.component.as_str(),
                (pin.bundle_id.as_str(), &pin.input_bindings),
            )
        })
        .collect::<BTreeMap<_, _>>();
    desired
        .keys()
        .chain(existing.keys())
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|component| desired.get(component) != existing.get(component))
        .map(str::to_string)
        .collect()
}

async fn stream_generation(
    core: &ConnectCoreBoundary,
    name: Option<&str>,
    graph: &str,
    generation: u64,
    demo_targets: bool,
) -> Result<GraphStatus, CliError> {
    let mut receiver = core.watch(graph).await?;
    let mut previous = None;
    loop {
        let status = receiver.borrow_and_update().clone();
        if status.generation < generation {
            receiver.changed().await.map_err(|_| {
                CoreBoundaryError::Transport("core watch closed before submitted generation".into())
            })?;
            continue;
        }
        let event = render_status_event(name, &status, demo_targets);
        if previous.as_ref() != Some(&event) {
            emit_stdout(&event);
            previous = Some(event);
        }
        if matches!(
            status.phase,
            GraphPhase::Ready | GraphPhase::Failed | GraphPhase::Retired
        ) {
            return Ok(status);
        }
        receiver.changed().await.map_err(|_| {
            CoreBoundaryError::Transport("core watch closed before a terminal status".into())
        })?;
    }
}

fn emit_stdout(text: &str) {
    print!("{text}");
    std::io::stdout().flush().ok();
}

fn environment_identity(name: Option<&str>, graph: &str) -> String {
    format!("{} ({graph})", name.unwrap_or("unnamed"))
}

fn render_target_banner(target: &SelectedTarget, source: &SourceProvenance) -> String {
    format!(
        "environment: {}\nsource: {}\ncore: {}\n\n",
        environment_identity(target.name.as_deref(), &target.graph),
        render_provenance(source),
        target.core
    )
}

fn render_provenance(source: &SourceProvenance) -> String {
    match source {
        SourceProvenance::Vcs {
            repository,
            revision,
            reference,
        } => format!(
            "{} @ {}{}",
            repository,
            short_revision(revision),
            reference
                .as_ref()
                .map(|reference| format!(" ({reference})"))
                .unwrap_or_default()
        ),
        SourceProvenance::Local {
            repository,
            base_revision,
            dirty,
        } => {
            let state = if *dirty {
                "local changes"
            } else {
                "local checkout"
            };
            let mut details = Vec::new();
            if let Some(revision) = base_revision {
                details.push(format!("base {}", short_revision(revision)));
            }
            if let Some(repository) = repository {
                details.push(repository.clone());
            }
            if details.is_empty() {
                state.to_string()
            } else {
                format!("{state} ({})", details.join("; "))
            }
        }
    }
}

fn short_revision(revision: &str) -> &str {
    revision.get(..12).unwrap_or(revision)
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

fn render_artifact_result(artifacts: &[ArtifactBinding]) -> String {
    if artifacts.is_empty() {
        return "built 0 workload artifacts\n".to_owned();
    }
    let mut rendered = format!("built {} workload artifact(s)\n", artifacts.len());
    for artifact in artifacts {
        rendered.push_str(&format!(
            "  {}.{}  {}  {}\n",
            artifact.component,
            artifact.input,
            artifact.kind.as_str(),
            artifact.digest
        ));
    }
    rendered
}

fn render_status_event(name: Option<&str>, status: &GraphStatus, demo_targets: bool) -> String {
    let mut rendered = match status.phase {
        GraphPhase::Planning => format!("planning generation {}\n", status.generation),
        GraphPhase::Blocked => {
            let blocked = status
                .blocked_on
                .iter()
                .map(|blocked| match (&blocked.producer, &blocked.output) {
                    (Some(producer), Some(output))
                        if !producer.is_empty() && !output.is_empty() =>
                    {
                        format!("{} waits for {}.{}", blocked.component, producer, output)
                    }
                    _ => format!("{} waits for input {}", blocked.component, blocked.input),
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("blocked: {blocked}\n")
        }
        GraphPhase::Reconciling => {
            let detail = status
                .dispositions
                .iter()
                .filter(|item| item.state != "ready")
                .map(|item| {
                    item.message
                        .as_ref()
                        .map(|message| format!("{}: {message}", item.resource))
                        .unwrap_or_else(|| item.resource.clone())
                })
                .collect::<Vec<_>>()
                .join(", ");
            let progress = if status.observed_ready == 0 {
                String::new()
            } else {
                format!(
                    "observed {} output(s); re-evaluated; ",
                    status.observed_ready
                )
            };
            if detail.is_empty() {
                format!(
                    "{progress}reconciling {} resource(s)\n",
                    status.planned_resources
                )
            } else {
                format!("{progress}reconciling: {detail}\n")
            }
        }
        GraphPhase::Ready => format!("ready: generation {}\n", status.generation),
        GraphPhase::Failed => {
            let failures = status
                .dispositions
                .iter()
                .filter(|item| item.state == "failed")
                .map(|item| {
                    item.message
                        .as_ref()
                        .map(|message| format!("{}: {message}", item.resource))
                        .unwrap_or_else(|| item.resource.clone())
                })
                .collect::<Vec<_>>()
                .join(", ");
            if failures.is_empty() {
                format!("failed: generation {}\n", status.generation)
            } else {
                format!("failed: {failures}\n")
            }
        }
        GraphPhase::Retired => format!("retired: generation {}\n", status.generation),
    };
    rendered = format!("{}: {rendered}", environment_identity(name, &status.graph));
    if status.phase == GraphPhase::Ready {
        for output in &status.outputs {
            let value = output
                .value
                .as_str()
                .map_or_else(|| output.value.to_string(), str::to_owned);
            rendered.push_str(&format!("  output {} = {value}\n", output.reference));
        }
    }
    if let Some(diagnostic) = &status.diagnostic {
        if color_enabled() {
            eprintln!("\u{1b}[1;31mdiagnostic\u{1b}[0m: {diagnostic}");
        } else {
            eprintln!("diagnostic: {diagnostic}");
        }
    }
    if demo_targets {
        rendered.push_str("targets: k8s=file:// Git; supabase=fake; cloudflare=recorded/fake (no live credentials)\n");
    }
    rendered
}

fn render_status(name: Option<&str>, status: &GraphStatus, demo_targets: bool) -> String {
    let mut rendered = format!(
        "environment: {}\ngeneration: {}\nplan: {} resource(s)\nobserved-ready: {}\n",
        environment_identity(name, &status.graph),
        status.generation,
        status.planned_resources,
        status.observed_ready
    );
    rendered.push_str(&render_status_event(name, status, demo_targets));
    rendered
}

fn fallback_watch_set(repository: &Path) -> Result<Vec<PathBuf>, CliError> {
    let repository = repository.canonicalize().map_err(|source| CliError::Read {
        path: repository.to_path_buf(),
        source,
    })?;
    let mut paths = Vec::new();
    collect_watch_files(&repository, &mut paths)?;
    paths.sort();
    Ok(paths)
}

fn collect_watch_files(directory: &Path, paths: &mut Vec<PathBuf>) -> Result<(), CliError> {
    for entry in std::fs::read_dir(directory).map_err(|source| CliError::Read {
        path: directory.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| CliError::Read {
            path: directory.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if path.is_dir() {
            if matches!(
                path.file_name().and_then(OsStr::to_str),
                Some(".git" | ".henosis" | "node_modules" | "dist" | "target")
            ) {
                continue;
            }
            collect_watch_files(&path, paths)?;
        } else if matches!(
            path.extension().and_then(OsStr::to_str),
            Some("ts" | "tsx" | "json" | "yaml" | "yml" | "lock")
        ) {
            paths.push(path);
        }
    }
    Ok(())
}

async fn wait_for_change(paths: &[PathBuf]) -> Result<(), CliError> {
    let baseline = file_snapshot(paths);
    loop {
        tokio::time::sleep(Duration::from_millis(250)).await;
        if file_snapshot(paths) != baseline {
            tokio::time::sleep(Duration::from_millis(350)).await;
            return Ok(());
        }
    }
}

fn file_snapshot(paths: &[PathBuf]) -> BTreeMap<PathBuf, Option<(u64, SystemTime)>> {
    paths
        .iter()
        .map(|path| {
            let state = std::fs::metadata(path)
                .ok()
                .and_then(|metadata| Some((metadata.len(), metadata.modified().ok()?)));
            (path.clone(), state)
        })
        .collect()
}

pub fn color_enabled() -> bool {
    std::env::var_os("NO_COLOR").is_none() && std::io::stderr().is_terminal()
}

pub fn command_help() -> String {
    Cli::command().render_long_help().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshots_public_command_help() {
        insta::assert_snapshot!(command_help());
    }

    #[test]
    fn snapshots_context_stale_diagnostic() {
        insta::assert_snapshot!(
            CliError::ContextStale {
                graph: "graph_070w3ge1r70w3ge1r70w3ge1r7".to_string(),
                core: "http://127.0.0.1:4481".to_string(),
                graphs: vec![GraphSummary {
                    graph: "graph_01k00000000000000000000000".to_string(),
                    generation: 3,
                    phase: GraphPhase::Ready,
                    created: true,
                    retired: false,
                }],
            }
            .diagnostic()
        );
    }

    #[test]
    fn snapshots_dirty_tree_provenance_line() {
        insta::assert_snapshot!(render_provenance(&SourceProvenance::Local {
            repository: Some("https://github.com/henosis-playground/example.git".to_string()),
            base_revision: Some("0123456789abcdef".to_string()),
            dirty: true,
        }));
    }

    #[test]
    fn snapshots_no_deployable_changes() {
        let current = GraphStatus {
            graph: "graph_demo".to_string(),
            generation: 7,
            phase: GraphPhase::Ready,
            blocked_on: Vec::new(),
            outputs: Vec::new(),
            observed_ready: 0,
            planned_resources: 0,
            diagnostic: None,
            bundles: vec![BundlePin {
                component: "web".to_string(),
                bundle_id: "a".repeat(64),
                input_bindings: BTreeMap::new(),
                source: None,
            }],
            source_policy: GraphSourcePolicy::AcceptLocal,
            dispositions: Vec::new(),
        };
        let pins = current.bundles.clone();
        let changed = changed_components(Some(&current), &pins);
        insta::assert_snapshot!(format!(
            "changed components: {}\nno deployable changes — generation {} remains active\n",
            if changed.is_empty() {
                "none"
            } else {
                "unexpected"
            },
            current.generation,
        ));
    }

    #[test]
    fn classifies_dev_loop_errors() {
        assert_eq!(
            CliError::Typecheck("broken".to_string()).classify(),
            ErrorClass::FixSource
        );
        assert_eq!(
            CliError::Core(CoreBoundaryError::Transport("offline".to_string())).classify(),
            ErrorClass::Transient
        );
        assert_eq!(
            CliError::ContextMissing {
                core: DEFAULT_CORE.to_string(),
                graphs: Vec::new(),
            }
            .classify(),
            ErrorClass::FatalSetup
        );
    }

    #[test]
    fn component_changes_ignore_provenance_only_changes() {
        let status = GraphStatus {
            graph: "graph_demo".to_string(),
            generation: 1,
            phase: GraphPhase::Ready,
            blocked_on: Vec::new(),
            outputs: Vec::new(),
            observed_ready: 0,
            planned_resources: 0,
            diagnostic: None,
            bundles: vec![BundlePin {
                component: "web".to_string(),
                bundle_id: "a".repeat(64),
                input_bindings: BTreeMap::new(),
                source: None,
            }],
            source_policy: GraphSourcePolicy::AcceptLocal,
            dispositions: Vec::new(),
        };
        let desired = vec![BundlePin {
            component: "web".to_string(),
            bundle_id: "a".repeat(64),
            input_bindings: BTreeMap::new(),
            source: Some(SourceProvenance::Local {
                repository: None,
                base_revision: None,
                dirty: true,
            }),
        }];
        assert!(changed_components(Some(&status), &desired).is_empty());
    }

    #[test]
    fn artifact_binding_change_is_deployable_without_bundle_change() {
        let mut current = GraphStatus::planning("graph_demo", 1);
        current.bundles = vec![BundlePin {
            component: "web".to_owned(),
            bundle_id: "a".repeat(64),
            input_bindings: BTreeMap::from([(
                "workerArtifact".to_owned(),
                serde_json::json!(format!("sha256:{}", "11".repeat(32))),
            )]),
            source: None,
        }];
        let desired = vec![BundlePin {
            component: "web".to_owned(),
            bundle_id: "a".repeat(64),
            input_bindings: BTreeMap::from([(
                "workerArtifact".to_owned(),
                serde_json::json!(format!("sha256:{}", "22".repeat(32))),
            )]),
            source: None,
        }];
        assert_eq!(changed_components(Some(&current), &desired), ["web"]);
    }
}
