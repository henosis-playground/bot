pub use henosis_core_boundary::{
    BundlePin, ConnectCoreBoundary, CoreBoundary, CoreBoundaryError, FakeCoreBoundary, GraphIntent,
    GraphPhase, GraphStatus, SourceProvenance,
};

use newtype_uuid::{GenericUuid, TypedUuid, TypedUuidKind, TypedUuidTag};

use crate::henosis::environment::EnvironmentIdGenerator;
use crate::henosis::render_diagnostics::DiagnosticPresentation;

pub enum PreviewEnvironmentKind {}

impl TypedUuidKind for PreviewEnvironmentKind {
    fn tag() -> TypedUuidTag {
        const TAG: TypedUuidTag = TypedUuidTag::new("graph");
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_preview_graph_ids_are_canonical_uuid_v7_typeids() {
        let id = CoreEnvironmentIdGenerator.new_preview_environment_id();
        let parsed: TypedUuid<PreviewEnvironmentKind> = id.parse().unwrap();
        assert_eq!(parsed.get_version_num(), 7);
        assert_eq!(parsed.to_string(), id);
    }
}
