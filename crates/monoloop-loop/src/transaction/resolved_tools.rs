//! Request-scoped immutable resolved tool set (empty until WP-06 handlers).

use monoloop_contracts::{ToolId, ToolName, ToolSpec};
use std::collections::HashMap;

/// Immutable tool set resolved at admission from the host registry.
#[derive(Clone, Debug, Default)]
pub struct ResolvedToolSet {
    by_id: HashMap<ToolId, ToolSpec>,
    by_name: HashMap<ToolName, ToolId>,
    ordered: Vec<ToolId>,
}

impl ResolvedToolSet {
    /// Empty set (required empty-tool path).
    pub fn empty() -> Self {
        Self::default()
    }

    /// Build from host specs selected by request tool ids (already validated).
    pub fn from_specs(specs: Vec<ToolSpec>) -> Self {
        let mut by_id = HashMap::new();
        let mut by_name = HashMap::new();
        let mut ordered = Vec::with_capacity(specs.len());
        for spec in specs {
            by_name.insert(spec.name.clone(), spec.id.clone());
            ordered.push(spec.id.clone());
            by_id.insert(spec.id.clone(), spec);
        }
        Self {
            by_id,
            by_name,
            ordered,
        }
    }

    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// Number of tools.
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// Specs in request order.
    pub fn specs(&self) -> Vec<&ToolSpec> {
        self.ordered
            .iter()
            .filter_map(|id| self.by_id.get(id))
            .collect()
    }

    /// Lookup by id.
    pub fn get(&self, id: &ToolId) -> Option<&ToolSpec> {
        self.by_id.get(id)
    }

    /// Lookup by name.
    pub fn get_by_name(&self, name: &ToolName) -> Option<&ToolSpec> {
        self.by_name
            .get(name)
            .and_then(|id| self.by_id.get(id))
    }
}

