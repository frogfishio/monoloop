//! Immutable host tool registry with linked handlers.

use super::tool_handler::ToolHandler;
use monoloop_contracts::{ToolId, ToolName, ToolSpec};
use std::collections::HashMap;
use std::sync::Arc;

/// Spec + linked handler pair registered at runtime startup.
#[derive(Clone)]
pub struct RegisteredTool {
    /// Canonical specification.
    pub spec: ToolSpec,
    /// Linked implementation.
    pub handler: Arc<dyn ToolHandler>,
}

impl std::fmt::Debug for RegisteredTool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredTool")
            .field("spec", &self.spec)
            .field("handler", &"<dyn ToolHandler>")
            .finish()
    }
}

impl RegisteredTool {
    /// Construct a registered tool.
    pub fn new(spec: ToolSpec, handler: Arc<dyn ToolHandler>) -> Self {
        Self { spec, handler }
    }
}

/// Immutable host tool definitions available to admission.
#[derive(Clone, Debug, Default)]
pub struct HostToolRegistry {
    by_id: HashMap<ToolId, RegisteredTool>,
    by_name: HashMap<ToolName, ToolId>,
}

impl HostToolRegistry {
    /// Empty tool registry (required empty-tool path remains valid).
    pub fn empty() -> Self {
        Self::default()
    }

    /// Build from registered tools; rejects duplicate ids/names.
    ///
    /// Every entry already carries a [`ToolCancellationPolicy`] on its spec
    /// (validated by [`ToolSpec::try_new`]); unstoppable handlers are rejected
    /// by not offering that policy.
    pub fn build(tools: Vec<RegisteredTool>) -> Result<Self, super::StartupError> {
        let mut by_id = HashMap::with_capacity(tools.len());
        let mut by_name = HashMap::with_capacity(tools.len());
        for tool in tools {
            // Schema root object already enforced by JsonSchema::try_new.
            let schema_bytes = serde_json::to_vec(tool.spec.input_schema.as_value())
                .map(|b| b.len())
                .unwrap_or(0);
            if schema_bytes > 64 * 1024 {
                return Err(super::StartupError::ToolRegistry("tool schema too large"));
            }
            if by_id.contains_key(&tool.spec.id) {
                return Err(super::StartupError::ToolRegistry("duplicate ToolId"));
            }
            if by_name.contains_key(&tool.spec.name) {
                return Err(super::StartupError::ToolRegistry("duplicate ToolName"));
            }
            by_name.insert(tool.spec.name.clone(), tool.spec.id.clone());
            by_id.insert(tool.spec.id.clone(), tool);
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

    /// Lookup registered tool by id.
    pub fn get(&self, id: &ToolId) -> Option<&RegisteredTool> {
        self.by_id.get(id)
    }

    /// Lookup spec by id.
    pub fn get_spec(&self, id: &ToolId) -> Option<&ToolSpec> {
        self.by_id.get(id).map(|t| &t.spec)
    }

    /// Resolve name to id.
    pub fn id_for_name(&self, name: &ToolName) -> Option<&ToolId> {
        self.by_name.get(name)
    }

    /// Specs sorted by tool id (deterministic projection).
    pub fn specs_sorted(&self) -> Vec<&ToolSpec> {
        let mut ids: Vec<_> = self.by_id.keys().collect();
        ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        ids.into_iter()
            .filter_map(|id| self.by_id.get(id).map(|t| &t.spec))
            .collect()
    }
}
