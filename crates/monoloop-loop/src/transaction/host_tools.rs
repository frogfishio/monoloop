//! Immutable host tool registry with linked handlers.

use super::tool_handler::ToolHandler;
use monoloop_contracts::{ToolId, ToolName, ToolSpec};
use std::collections::HashMap;
use std::sync::Arc;

/// Spec + linked handler pair registered at runtime startup.
///
/// Fields are private so callers cannot bypass [`Self::try_new`] /
/// [`Self::try_new_process_isolated`] via struct literals (V2 §14.3 / D-043).
#[derive(Clone)]
pub struct RegisteredTool {
    spec: ToolSpec,
    handler: Arc<dyn ToolHandler>,
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
    /// Canonical specification.
    pub fn spec(&self) -> &ToolSpec {
        &self.spec
    }

    /// Linked implementation.
    pub fn handler(&self) -> &Arc<dyn ToolHandler> {
        &self.handler
    }

    /// Construct a registered tool.
    ///
    /// Prefer [`Self::try_new`] so cancellation policy is checked against the handler.
    /// Panics if the class/handler pair is invalid — use [`Self::try_new`] in fallible paths.
    pub fn new(spec: ToolSpec, handler: Arc<dyn ToolHandler>) -> Self {
        Self::try_new(spec, handler).expect("handler supports declared ToolExecutionClass")
    }

    /// Construct a registered tool, rejecting unstoppable / mismatched class (D-024).
    ///
    /// [`ToolExecutionClass::ProcessIsolated`] MUST use
    /// [`Self::try_new_process_isolated`] with a concrete
    /// [`super::process_tool::ProcessIsolatedToolHandler`] (V2 §14.3 structural factory).
    pub fn try_new(
        spec: ToolSpec,
        handler: Arc<dyn ToolHandler>,
    ) -> Result<Self, super::StartupError> {
        use monoloop_contracts::ToolExecutionClass;
        match &spec.execution_class {
            ToolExecutionClass::AbortableAtYield { .. } => {
                if !handler.supports_abort() {
                    return Err(super::StartupError::ToolRegistry(
                        "AbortableAtYield tool requires supports_abort handler",
                    ));
                }
            }
            ToolExecutionClass::ProcessIsolated { .. } => {
                // Close the boolean-only gate: dyn handlers cannot self-assert
                // ProcessIsolated. Use try_new_process_isolated.
                return Err(super::StartupError::ToolRegistry(
                    "ProcessIsolated requires try_new_process_isolated(ProcessIsolatedToolHandler)",
                ));
            }
            ToolExecutionClass::CooperativeInProcess { .. } => {
                // Cooperative cancel is best-effort. Sync/immediate handlers may
                // omit supports_abort; cancel is vacuous once completion is already sent.
            }
        }
        Ok(Self { spec, handler })
    }

    /// Structural ProcessIsolated registration (V2 §14.3 / D-043).
    ///
    /// Only a concrete [`super::process_tool::ProcessIsolatedToolHandler`] may
    /// satisfy this class — capability booleans on `dyn ToolHandler` are rejected.
    pub fn try_new_process_isolated(
        spec: ToolSpec,
        handler: super::process_tool::ProcessIsolatedToolHandler,
    ) -> Result<Self, super::StartupError> {
        use monoloop_contracts::ToolExecutionClass;
        match &spec.execution_class {
            ToolExecutionClass::ProcessIsolated { .. } => {}
            _ => {
                return Err(super::StartupError::ToolRegistry(
                    "try_new_process_isolated requires ToolExecutionClass::ProcessIsolated",
                ));
            }
        }
        if !handler.os_process_isolated() || !handler.supports_isolated_kill() {
            return Err(super::StartupError::ToolRegistry(
                "ProcessIsolatedToolHandler must expose os_process_isolated + supports_isolated_kill",
            ));
        }
        Ok(Self {
            spec,
            handler: Arc::new(handler),
        })
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
    /// Re-validates ProcessIsolated entries so a forged `RegisteredTool` cannot
    /// enter the registry without an OS-process handler (D-043).
    pub fn build(tools: Vec<RegisteredTool>) -> Result<Self, super::StartupError> {
        use monoloop_contracts::ToolExecutionClass;
        let mut by_id = HashMap::with_capacity(tools.len());
        let mut by_name = HashMap::with_capacity(tools.len());
        for tool in tools {
            if matches!(
                tool.spec.execution_class,
                ToolExecutionClass::ProcessIsolated { .. }
            ) && !tool.handler.os_process_isolated()
            {
                return Err(super::StartupError::ToolRegistry(
                    "ProcessIsolated entry lacks os_process_isolated handler",
                ));
            }
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
