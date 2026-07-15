//! Deploy-repository files as one hand of the GitHub workflow frontend.
//!
//! The bot owns both directions: edits on deploy `main` become graph intent through the
//! ordinary core boundary, and accepted long-lived graph state is written back to `main`.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;

use serde::{Deserialize, Serialize};

use crate::henosis::core_client::{
    BundlePin, CoreBoundary, CoreBoundaryError, GraphIntent, GraphStatus, SourceProvenance,
};
use henosis_core_boundary::GraphSourcePolicy;

pub const GRAPH_DIRECTORY: &str = "henosis/graphs";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphIntentFile {
    pub schema: u32,
    pub graph: String,
    pub name: String,
    pub generation: u64,
    pub components: Vec<GraphComponentFile>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphComponentFile {
    pub name: String,
    #[serde(rename = "bundleDigest", with = "base64_bytes")]
    pub bundle_digest: Vec<u8>,
    #[serde(default, rename = "inputBindings")]
    pub input_bindings: Vec<InputBindingFile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceFile>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputBindingFile {
    pub name: String,
    #[serde(rename = "valueJson", with = "base64_bytes")]
    pub value_json: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum SourceFile {
    Local {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        repository: Option<String>,
        #[serde(
            default,
            rename = "baseRevision",
            skip_serializing_if = "Option::is_none"
        )]
        base_revision: Option<String>,
        dirty: bool,
    },
    Vcs {
        repository: String,
        revision: String,
        #[serde(default, rename = "ref", skip_serializing_if = "Option::is_none")]
        reference: Option<String>,
    },
}

mod base64_bytes {
    use base64::Engine as _;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&base64::engine::general_purpose::STANDARD.encode(bytes))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(serde::de::Error::custom)
    }
}

pub trait GraphFileRepository: Send + Sync {
    fn read_graph_files(
        &self,
    ) -> impl Future<Output = anyhow::Result<BTreeMap<String, Vec<u8>>>> + Send;

    fn write_graph_file(
        &self,
        path: &str,
        contents: &str,
        message: &str,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
}

pub struct GitSyncFrontend<R, C> {
    repository: R,
    core: C,
    seen: BTreeMap<String, GraphIntentFile>,
}

impl<R, C> GitSyncFrontend<R, C>
where
    R: GraphFileRepository,
    C: CoreBoundary,
{
    pub fn new(repository: R, core: C) -> Self {
        Self {
            repository,
            core,
            seen: BTreeMap::new(),
        }
    }

    /// Reconcile deploy-main pin files into graph intent.
    ///
    /// The first pass compares acknowledged files with core before deciding that an update is
    /// needed, so a bot restart does not manufacture a new graph generation.
    pub async fn sync_from_main(&mut self) -> Result<usize, GitSyncError> {
        let files = self.repository.read_graph_files().await?;
        let mut incoming = BTreeMap::new();
        for (path, bytes) in files {
            if !path.ends_with(".toml") {
                continue;
            }
            let text = std::str::from_utf8(&bytes).map_err(|error| {
                GitSyncError::Decode(format!("{path}: file is not UTF-8: {error}"))
            })?;
            let intent: GraphIntentFile = toml::from_str(text)
                .map_err(|error| GitSyncError::Decode(format!("{path}: {error}")))?;
            validate(&path, &intent)?;
            if incoming.insert(intent.graph.clone(), intent).is_some() {
                return Err(GitSyncError::Decode(format!(
                    "duplicate graph identity in {path}"
                )));
            }
        }

        let mut changes = 0;
        for intent in incoming.values_mut() {
            if self.seen.get(&intent.graph) == Some(intent) {
                continue;
            }
            let bundles = pins_from_file(intent)?;
            let status = if intent.generation == 0 {
                self.core
                    .apply(GraphIntent::Create {
                        graph: intent.graph.clone(),
                        bundles,
                        source_policy: GraphSourcePolicy::AcceptLocal,
                    })
                    .await?
            } else {
                match self.core.status(&intent.graph).await {
                    Ok(status)
                        if status.generation == intent.generation && status.bundles == bundles =>
                    {
                        status
                    }
                    Ok(_) => {
                        self.core
                            .apply(GraphIntent::Update {
                                graph: intent.graph.clone(),
                                expected_generation: intent.generation,
                                bundles,
                            })
                            .await?
                    }
                    Err(CoreBoundaryError::GraphNotFound(_)) => {
                        return Err(GitSyncError::Decode(format!(
                            "graph {} has generation {} but does not exist in core",
                            intent.graph, intent.generation
                        )));
                    }
                    Err(error) => return Err(error.into()),
                }
            };
            acknowledge(intent, &status)?;
            self.write_intent_file(intent).await?;
            changes += 1;
        }

        let incoming_graphs = incoming.keys().cloned().collect::<BTreeSet<_>>();
        let retired = self
            .seen
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            .difference(&incoming_graphs)
            .cloned()
            .collect::<Vec<_>>();
        for graph in retired {
            self.core.apply(GraphIntent::Retire { graph }).await?;
            changes += 1;
        }

        self.seen = incoming;
        Ok(changes)
    }

    /// Present accepted long-lived graph state on deploy `main`.
    pub async fn publish_graph(
        &mut self,
        display_name: impl Into<String>,
        status: &GraphStatus,
    ) -> Result<(), GitSyncError> {
        let intent = file_from_status(display_name.into(), status)?;
        self.write_intent_file(&intent).await?;
        self.seen.insert(intent.graph.clone(), intent);
        Ok(())
    }

    async fn write_intent_file(&self, intent: &GraphIntentFile) -> Result<(), GitSyncError> {
        let contents = toml::to_string_pretty(intent).map_err(GitSyncError::Encode)?;
        self.repository
            .write_graph_file(
                &graph_path(&intent.graph),
                &contents,
                "Update Henosis graph intent",
            )
            .await?;
        Ok(())
    }
}

pub fn graph_path(graph: &str) -> String {
    format!("{GRAPH_DIRECTORY}/{graph}.toml")
}

fn acknowledge(intent: &mut GraphIntentFile, status: &GraphStatus) -> Result<(), GitSyncError> {
    if status.graph != intent.graph {
        return Err(GitSyncError::Core(format!(
            "core returned status for {}, expected {}",
            status.graph, intent.graph
        )));
    }
    if status.generation == 0 {
        return Err(GitSyncError::Core(format!(
            "core returned generation zero for {}",
            intent.graph
        )));
    }
    intent.generation = status.generation;
    intent.components = components_from_pins(&status.bundles)?;
    Ok(())
}

fn file_from_status(name: String, status: &GraphStatus) -> Result<GraphIntentFile, GitSyncError> {
    if status.generation == 0 {
        return Err(GitSyncError::Core(format!(
            "cannot publish unaccepted graph {}",
            status.graph
        )));
    }
    Ok(GraphIntentFile {
        schema: 1,
        graph: status.graph.clone(),
        name,
        generation: status.generation,
        components: components_from_pins(&status.bundles)?,
    })
}

fn pins_from_file(intent: &GraphIntentFile) -> Result<Vec<BundlePin>, GitSyncError> {
    intent
        .components
        .iter()
        .map(|component| {
            if component.name.is_empty() || component.bundle_digest.is_empty() {
                return Err(GitSyncError::Decode(format!(
                    "graph {} components require name and bundleDigest",
                    intent.graph
                )));
            }
            let input_bindings = component
                .input_bindings
                .iter()
                .map(|binding| {
                    let value = serde_json::from_slice(&binding.value_json).map_err(|error| {
                        GitSyncError::Decode(format!(
                            "graph {} component {} input {} contains invalid JSON: {error}",
                            intent.graph, component.name, binding.name
                        ))
                    })?;
                    Ok((binding.name.clone(), value))
                })
                .collect::<Result<_, GitSyncError>>()?;
            Ok(BundlePin {
                component: component.name.clone(),
                bundle_id: hex::encode(&component.bundle_digest),
                input_bindings,
                source: component.source.clone().map(SourceProvenance::from),
            })
        })
        .collect()
}

fn components_from_pins(pins: &[BundlePin]) -> Result<Vec<GraphComponentFile>, GitSyncError> {
    pins.iter()
        .map(|pin| {
            let bundle_digest = hex::decode(&pin.bundle_id).map_err(|error| {
                GitSyncError::Core(format!(
                    "bundle {} has invalid hexadecimal identity: {error}",
                    pin.bundle_id
                ))
            })?;
            let input_bindings = pin
                .input_bindings
                .iter()
                .map(|(name, value)| {
                    serde_json::to_vec(value)
                        .map(|value_json| InputBindingFile {
                            name: name.clone(),
                            value_json,
                        })
                        .map_err(|error| GitSyncError::Core(error.to_string()))
                })
                .collect::<Result<_, _>>()?;
            Ok(GraphComponentFile {
                name: pin.component.clone(),
                bundle_digest,
                input_bindings,
                source: pin.source.clone().map(SourceFile::from),
            })
        })
        .collect()
}

impl From<SourceFile> for SourceProvenance {
    fn from(source: SourceFile) -> Self {
        match source {
            SourceFile::Local {
                repository,
                base_revision,
                dirty,
            } => Self::Local {
                repository,
                base_revision,
                dirty,
            },
            SourceFile::Vcs {
                repository,
                revision,
                reference,
            } => Self::Vcs {
                repository,
                revision,
                reference,
            },
        }
    }
}

impl From<SourceProvenance> for SourceFile {
    fn from(source: SourceProvenance) -> Self {
        match source {
            SourceProvenance::Local {
                repository,
                base_revision,
                dirty,
            } => Self::Local {
                repository,
                base_revision,
                dirty,
            },
            SourceProvenance::Vcs {
                repository,
                revision,
                reference,
            } => Self::Vcs {
                repository,
                revision,
                reference,
            },
        }
    }
}

fn validate(path: &str, intent: &GraphIntentFile) -> Result<(), GitSyncError> {
    if intent.schema != 1 {
        return Err(GitSyncError::Decode(format!(
            "graph {} uses unsupported schema {}; expected 1",
            intent.graph, intent.schema
        )));
    }
    if intent.graph.is_empty() || intent.name.is_empty() {
        return Err(GitSyncError::Decode(format!(
            "{path}: graph and name are required"
        )));
    }
    let expected = graph_path(&intent.graph);
    if path != expected {
        return Err(GitSyncError::Decode(format!(
            "{path}: graph identity requires filename {expected}"
        )));
    }
    pins_from_file(intent)?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum GitSyncError {
    #[error("core graph service failed: {0}")]
    Core(String),
    #[error("invalid graph intent file: {0}")]
    Decode(String),
    #[error("cannot encode graph intent file: {0}")]
    Encode(#[source] toml::ser::Error),
    #[error(transparent)]
    Repository(#[from] anyhow::Error),
}

impl From<CoreBoundaryError> for GitSyncError {
    fn from(error: CoreBoundaryError) -> Self {
        Self::Core(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::process::Command;

    use super::*;
    use crate::henosis::core_client::{FakeCoreBoundary, GraphPhase};

    struct BareRepository {
        remote: tempfile::TempDir,
    }

    impl BareRepository {
        fn new() -> Self {
            let remote = tempfile::tempdir().unwrap();
            git(remote.path(), ["init", "--bare", "--quiet"]);
            let checkout = tempfile::tempdir().unwrap();
            git(checkout.path(), ["init", "--quiet", "-b", "main"]);
            configure(checkout.path());
            std::fs::write(checkout.path().join("README"), "deploy\n").unwrap();
            git(checkout.path(), ["add", "README"]);
            git(checkout.path(), ["commit", "--quiet", "-m", "initial"]);
            git(
                checkout.path(),
                ["remote", "add", "origin", remote.path().to_str().unwrap()],
            );
            git(checkout.path(), ["push", "--quiet", "origin", "main"]);
            Self { remote }
        }

        fn edit(&self, graph: &str, file: Option<&GraphIntentFile>) {
            let checkout = self.checkout();
            let path = checkout.path().join(graph_path(graph));
            if let Some(file) = file {
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(path, toml::to_string_pretty(file).unwrap()).unwrap();
            } else {
                std::fs::remove_file(path).unwrap();
            }
            git(checkout.path(), ["add", "-A"]);
            git(checkout.path(), ["commit", "--quiet", "-m", "promotion"]);
            git(checkout.path(), ["push", "--quiet", "origin", "main"]);
        }

        fn read(&self, graph: &str) -> GraphIntentFile {
            let checkout = self.checkout();
            toml::from_str(
                &std::fs::read_to_string(checkout.path().join(graph_path(graph))).unwrap(),
            )
            .unwrap()
        }

        fn checkout(&self) -> tempfile::TempDir {
            let checkout = tempfile::tempdir().unwrap();
            git(
                checkout.path(),
                [
                    "clone",
                    "--quiet",
                    "--branch",
                    "main",
                    self.remote.path().to_str().unwrap(),
                    ".",
                ],
            );
            configure(checkout.path());
            checkout
        }
    }

    impl GraphFileRepository for BareRepository {
        async fn read_graph_files(&self) -> anyhow::Result<BTreeMap<String, Vec<u8>>> {
            let checkout = self.checkout();
            let root = checkout.path().join(GRAPH_DIRECTORY);
            if !root.exists() {
                return Ok(BTreeMap::new());
            }
            std::fs::read_dir(root)?
                .map(|entry| {
                    let entry = entry?;
                    Ok((
                        format!("{GRAPH_DIRECTORY}/{}", entry.file_name().to_string_lossy()),
                        std::fs::read(entry.path())?,
                    ))
                })
                .collect()
        }

        async fn write_graph_file(
            &self,
            path: &str,
            contents: &str,
            message: &str,
        ) -> anyhow::Result<()> {
            let checkout = self.checkout();
            let path = checkout.path().join(path);
            std::fs::create_dir_all(path.parent().unwrap())?;
            std::fs::write(path, contents)?;
            git(checkout.path(), ["add", "-A"]);
            git(checkout.path(), ["commit", "--quiet", "-m", message]);
            git(checkout.path(), ["push", "--quiet", "origin", "main"]);
            Ok(())
        }
    }

    #[tokio::test]
    async fn bare_repo_round_trips_pin_edit_and_retirement_through_core_boundary() {
        let repository = BareRepository::new();
        let graph = "graph_01k00000000000000000000000";
        repository.edit(graph, Some(&intent(graph, 0, 1)));
        let core = FakeCoreBoundary::default();
        let mut frontend = GitSyncFrontend::new(repository, core.clone());

        assert_eq!(frontend.sync_from_main().await.unwrap(), 1);
        assert_eq!(frontend.repository.read(graph).generation, 1);

        let mut edited = frontend.repository.read(graph);
        edited.components[0].bundle_digest = vec![2];
        frontend.repository.edit(graph, Some(&edited));
        assert_eq!(frontend.sync_from_main().await.unwrap(), 1);
        assert!(matches!(
            core.intents().await.last(),
            Some(GraphIntent::Update { expected_generation: 1, bundles, .. })
                if bundles[0].bundle_id == "02"
        ));

        frontend.repository.edit(graph, None);
        assert_eq!(frontend.sync_from_main().await.unwrap(), 1);
        assert!(matches!(
            core.intents().await.last(),
            Some(GraphIntent::Retire { graph: retired }) if retired == graph
        ));
    }

    #[tokio::test]
    async fn graph_state_is_written_to_deploy_main() {
        let repository = BareRepository::new();
        let core = FakeCoreBoundary::default();
        let mut frontend = GitSyncFrontend::new(repository, core);
        let mut status = GraphStatus::planning("graph_01k00000000000000000000001", 4);
        status.phase = GraphPhase::Ready;
        status.bundles = vec![pin(7)];

        frontend.publish_graph("dev", &status).await.unwrap();

        let file = frontend.repository.read(&status.graph);
        assert_eq!(file.name, "dev");
        assert_eq!(file.generation, 4);
        assert_eq!(file.components[0].bundle_digest, vec![7]);
    }

    #[tokio::test]
    async fn head_advance_updates_existing_graph_intent() {
        let repository = BareRepository::new();
        let graph = "graph_01k00000000000000000000002";
        repository.edit(graph, Some(&intent(graph, 0, 3)));
        let core = FakeCoreBoundary::default();
        let mut frontend = GitSyncFrontend::new(repository, core.clone());
        frontend.sync_from_main().await.unwrap();

        let status = core.status(graph).await.unwrap();
        let advanced = core
            .apply(GraphIntent::Update {
                graph: graph.to_string(),
                expected_generation: status.generation,
                bundles: vec![pin(9)],
            })
            .await
            .unwrap();
        frontend.publish_graph("dev", &advanced).await.unwrap();

        assert_eq!(frontend.repository.read(graph).generation, 2);
        assert_eq!(
            frontend.repository.read(graph).components[0].bundle_digest,
            vec![9]
        );
    }

    fn intent(graph: &str, generation: u64, digest: u8) -> GraphIntentFile {
        GraphIntentFile {
            schema: 1,
            graph: graph.to_string(),
            name: "dev".to_string(),
            generation,
            components: vec![GraphComponentFile {
                name: "api".to_string(),
                bundle_digest: vec![digest],
                input_bindings: Vec::new(),
                source: None,
            }],
        }
    }

    fn pin(digest: u8) -> BundlePin {
        BundlePin {
            component: "api".to_string(),
            bundle_id: format!("{digest:02x}"),
            input_bindings: BTreeMap::new(),
            source: None,
        }
    }

    fn configure(path: &Path) {
        git(path, ["config", "user.name", "Test"]);
        git(path, ["config", "user.email", "test@example.com"]);
    }

    fn git<'a>(path: &Path, args: impl IntoIterator<Item = &'a str>) {
        assert!(
            Command::new("git")
                .current_dir(path)
                .args(args)
                .status()
                .unwrap()
                .success()
        );
    }
}
