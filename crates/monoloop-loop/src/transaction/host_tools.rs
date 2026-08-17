//! Immutable host tool registry shell (handlers land in WP-06).

use monoloop_contracts::{ToolId, ToolName, ToolSpec};
use std::collections::HashMap;

/// Immutable host tool definitions available to admission.
///
/// WP-03 shell stores specs only; linked handlers arrive in WP-06.
#[derive(Clone, Debug, Default)]
pub struct HostToolRegistry {
    by_id: HashMap<ToolId, ToolSpec>,
    by_name: HashMap<ToolName, ToolId>,
}

impl HostToolRegistry {
    /// Empty tool registry (required empty-tool path remains valid).
    pub fn empty() -> Self {
        Self::default()
    }

    /// Build from specs; rejects duplicate ids/names and empty invalid specs.
    pub fn build(specs: Vec<ToolSpec>) -> Result<Self, super::StartupError> {
        let mut by_id = HashMap::with_capacity(specs.len());
        let mut by_name = HashMap::with_capacity(specs.len());
        for spec in specs {
            if by_id.contains_key(&spec.id) {
                return Err(super::StartupError::ToolRegistry("duplicate ToolId"));
            }
            if by_name.contains_key(&spec.name) {
                return Err(super::StartupError::ToolRegistry("duplicate ToolName"));
            }
            by_name.insert(spec.name.clone(), spec.id.clone());
            by_id.insert(spec.id.clone(), spec);
        }
        Ok(Self { by_id, by_name })
    }

    /// Number of registered tools.
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// Lookup by id.
    pub fn get(&self, id: &ToolId) -> Option<&ToolSpec> {
        self.by_id.get(id)
    }

    /// Resolve name to id.
    pub fn id_for_name(&self, name: &ToolName) -> Option<&ToolId> {
        self.by_name.get(name)
    }

    /// Ordered specs for encoder projection (stable by insertion iteration order of map — not stable).
    /// Prefer collecting ids sorted for determinism.
    pub fn specs_sorted(&self) -> Vec<&ToolSpec> {
        let mut ids: Vec<_> = self.by_id.keys().collect();
        ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        ids.into_iter()
            .filter_map(|id| self.by_id.get(id))
            .collect()
    }
}
