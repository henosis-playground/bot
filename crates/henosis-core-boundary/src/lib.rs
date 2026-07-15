use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, watch};

// This module is the only core-facing contract in the bot workspace.
// TODO(d26-proto): replace the transport adapter, not these domain messages, once
// the d26-core ConnectRPC schema settles.

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundlePin {
    pub component: String,
    pub bundle_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphIntent {
    Create {
        graph: String,
        bundles: Vec<BundlePin>,
    },
    Update {
        graph: String,
        bundles: Vec<BundlePin>,
    },
    Retire {
        graph: String,
    },
}

impl GraphIntent {
    pub fn graph(&self) -> &str {
        match self {
            Self::Create { graph, .. } | Self::Update { graph, .. } | Self::Retire { graph } => {
                graph
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphPhase {
    Planning,
    Blocked,
    Reconciling,
    Ready,
    Failed,
    Retired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockedOn {
    pub component: String,
    pub input: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphStatus {
    pub graph: String,
    pub generation: u64,
    pub phase: GraphPhase,
    pub blocked_on: Vec<BlockedOn>,
    pub observed_ready: usize,
    pub planned_resources: usize,
    pub diagnostic: Option<String>,
}

impl GraphStatus {
    pub fn planning(graph: impl Into<String>, generation: u64) -> Self {
        Self {
            graph: graph.into(),
            generation,
            phase: GraphPhase::Planning,
            blocked_on: Vec::new(),
            observed_ready: 0,
            planned_resources: 0,
            diagnostic: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CoreBoundaryError {
    #[error("graph `{0}` does not exist")]
    GraphNotFound(String),
    #[error("core boundary rejected graph intent: {0}")]
    Rejected(String),
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
    fn watch(
        &self,
        graph: &str,
    ) -> impl Future<Output = Result<watch::Receiver<GraphStatus>, CoreBoundaryError>> + Send;
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
        if matches!(intent, GraphIntent::Retire { .. }) {
            status.phase = GraphPhase::Retired;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fake_records_intent_and_publishes_status() {
        let core = FakeCoreBoundary::default();
        let status = core
            .apply(GraphIntent::Create {
                graph: "preview_test".to_string(),
                bundles: vec![BundlePin {
                    component: "web".to_string(),
                    bundle_id: "abc".to_string(),
                }],
            })
            .await
            .unwrap();
        assert_eq!(status.generation, 1);
        assert_eq!(core.intents().await.len(), 1);
        assert_eq!(core.status("preview_test").await.unwrap(), status);
    }
}
