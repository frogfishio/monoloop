//! Request-scoped immutable resolved tool set with linked handlers.

use super::host_tools::RegisteredTool;
use super::tool_handler::ToolHandler;
use monoloop_contracts::{ToolId, ToolName, ToolSpec};
use std::collections::HashMap;
use std::sync::Arc;

/// One tool admitted into a transaction.
#[derive(Clone)]
pub struct ResolvedTool {
    /// Spec (encoder/MCP projection).
    pub spec: ToolSpec,
    /// Handler (same Arc as host registry).
    pub handler: Arc<dyn ToolHandler>,
}

impl std::fmt::Debug for ResolvedTool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedTool")
            .field("spec", &self.spec)
            .field("handler", &"<dyn ToolHandler>")
            .finish()
    }
}

/// Immutable tool set resolved at admission from the host registry.
#[derive(Clone, Debug, Default)]
pub struct ResolvedToolSet {
    by_id: HashMap<ToolId, ResolvedTool>,
    by_name: HashMap<ToolName, ToolId>,
    ordered: Vec<ToolId>,
}

impl ResolvedToolSet {
    /// Empty set (required empty-tool path).
    pub fn empty() -> Self {
        Self::default()
    }

    /// Build from registered tools selected by request tool ids (already validated).
    pub fn from_registered(tools: Vec<RegisteredTool>) -> Self {
        let mut by_id = HashMap::new();
        let mut by_name = HashMap::new();
        let mut ordered = Vec::with_capacity(tools.len());
        for tool in tools {
            by_name.insert(tool.spec.name.clone(), tool.spec.id.clone());
            ordered.push(tool.spec.id.clone());
            by_id.insert(
                tool.spec.id.clone(),
                ResolvedTool {
                    spec: tool.spec,
                    handler: tool.handler,
                },
            );
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
            .filter_map(|id| self.by_id.get(id).map(|t| &t.spec))
            .collect()
    }

    /// Ordered resolved tools.
    pub fn tools(&self) -> Vec<&ResolvedTool> {
        self.ordered
            .iter()
            .filter_map(|id| self.by_id.get(id))
            .collect()
    }

    /// Lookup by id.
    pub fn get(&self, id: &ToolId) -> Option<&ResolvedTool> {
        self.by_id.get(id)
    }

    /// Lookup by name.
    pub fn get_by_name(&self, name: &ToolName) -> Option<&ResolvedTool> {
        self.by_name.get(name).and_then(|id| self.by_id.get(id))
    }

    /// Whether name is allowlisted.
    pub fn contains_name(&self, name: &ToolName) -> bool {
        self.by_name.contains_key(name)
    }
}
