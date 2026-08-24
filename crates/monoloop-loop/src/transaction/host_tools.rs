//! Immutable host tool registry with linked handlers.

use super::tool_handler::{AbortableAtYieldHandler, ToolHandler};
use monoloop_contracts::{ToolId, ToolName, ToolSpec};
use std::collections::HashMap;
use std::sync::Arc;

/// Spec + linked handler pair registered at runtime startup.
///
/// Fields are private so callers cannot bypass [`Self::try_new`] /
/// [`Self::try_new_abortable`] / [`Self::try_new_process_isolated`] via struct
/// literals (V2 §14.2–14.3 / D-043 / D-050).
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
    /// Prefer [`Self::try_new`] / [`Self::try_new_abortable`] /
    /// [`Self::try_new_process_isolated`] so cancellation policy is checked.
    /// Panics if the class/handler pair is invalid.
    pub fn new(spec: ToolSpec, handler: Arc<dyn ToolHandler>) -> Self {
        Self::try_new(spec, handler).expect("handler supports declared ToolExecutionClass")
    }

    /// Construct a registered tool, rejecting unstoppable / mismatched class (D-024).
    ///
    /// Abortable and ProcessIsolated tools MUST use their structural factories —
    /// [`Self::try_new_abortable`] and [`Self::try_new_process_isolated`] — so a
    /// `dyn ToolHandler` cannot self-assert those classes via capability booleans.
    pub fn try_new(
        spec: ToolSpec,
        handler: Arc<dyn ToolHandler>,
    ) -> Result<Self, super::StartupError> {
        use monoloop_contracts::ToolExecutionClass;
        match &spec.execution_class {
            ToolExecutionClass::AbortableAtYield { .. } => {
                // Close the boolean-only gate: dyn handlers cannot self-assert
                // AbortableAtYield. Use try_new_abortable.
                return Err(super::StartupError::ToolRegistry(
                    "AbortableAtYield requires try_new_abortable(AbortableAtYieldHandler)",
                ));
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

    /// Structural AbortableAtYield registration (V2 §14.2 / D-050).
    ///
    /// Only a sealed [`AbortableAtYieldHandler`] (crate `AsyncToolHandler` /
    /// `IsolatedKillableToolHandler`) may satisfy this class — capability
    /// booleans on `dyn ToolHandler` are rejected.
    pub fn try_new_abortable<H>(spec: ToolSpec, handler: H) -> Result<Self, super::StartupError>
    where
        H: AbortableAtYieldHandler + 'static,
    {
        use monoloop_contracts::ToolExecutionClass;
        match &spec.execution_class {
            ToolExecutionClass::AbortableAtYield { .. } => {}
            _ => {
                return Err(super::StartupError::ToolRegistry(
                    "try_new_abortable requires ToolExecutionClass::AbortableAtYield",
                ));
            }
        }
        if !handler.runtime_owns_abortable_drive() || !handler.supports_abort() {
            return Err(super::StartupError::ToolRegistry(
                "AbortableAtYieldHandler must expose runtime_owns_abortable_drive + supports_abort",
            ));
        }
        Ok(Self {
            spec,
            handler: Arc::new(handler),
        })
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
    /// Re-validates ProcessIsolated and AbortableAtYield entries so a forged
    /// `RegisteredTool` cannot enter the registry without a structural handler
    /// (D-043 / D-050).
    pub fn build(tools: Vec<RegisteredTool>) -> Result<Self, super::StartupError> {
        use monoloop_contracts::ToolExecutionClass;
        let mut by_id = HashMap::with_capacity(tools.len());
        let mut by_name = HashMap::with_capacity(tools.len());
        for tool in tools {
            match &tool.spec.execution_class {
                ToolExecutionClass::ProcessIsolated { .. }
                    if !tool.handler.os_process_isolated() =>
                {
                    return Err(super::StartupError::ToolRegistry(
                        "ProcessIsolated entry lacks os_process_isolated handler",
                    ));
                }
                ToolExecutionClass::AbortableAtYield { .. }
                    if !tool.handler.runtime_owns_abortable_drive() =>
                {
                    return Err(super::StartupError::ToolRegistry(
                        "AbortableAtYield entry lacks runtime_owns_abortable_drive handler",
                    ));
                }
                _ => {}
            }
            // Schema root object already enforced by JsonSchema::try_new.
            // Byte ceiling uses TransactionLimits default here; StartedRuntime
            // re-checks against the runtime's max_tool_schema_bytes (§23 / D-056).
            let max_schema = monoloop_contracts::TransactionLimits::default().max_tool_schema_bytes;
            let schema_bytes = serde_json::to_vec(tool.spec.input_schema.as_value())
                .map(|b| b.len())
                .unwrap_or(0);
            if schema_bytes > max_schema {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::tool_handler::{AsyncToolHandler, ImmediateToolHandler};
    use monoloop_contracts::{
        CanonicalToolOutput, JsonSchema, ToolCompletion, ToolExecutionClass, ToolId, ToolLimits,
        ToolName, ToolOutputContract, ToolSuccessContract,
    };
    use std::time::Duration;

    fn abortable_spec() -> ToolSpec {
        let schema = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
        ToolSpec::try_new(
            ToolId::try_new("a").unwrap(),
            ToolName::try_new("a").unwrap(),
            "abortable",
            schema.clone(),
            ToolOutputContract {
                success: ToolSuccessContract::json(schema),
                error_data_schema: None,
            },
            ToolLimits::default(),
            ToolExecutionClass::AbortableAtYield {
                grace: Duration::from_secs(1),
            },
        )
        .unwrap()
    }

    #[test]
    fn abortable_rejects_dyn_handler_path() {
        let forged = Arc::new(ImmediateToolHandler::new(|_c, _x| {
            Ok(ToolCompletion::Succeeded(CanonicalToolOutput::Json(
                serde_json::json!({}),
            )))
        })) as Arc<dyn ToolHandler>;
        // Even a handler that lied about supports_abort cannot use try_new.
        let err = RegisteredTool::try_new(abortable_spec(), forged).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("try_new_abortable") || msg.contains("AbortableAtYield"),
            "got {msg}"
        );
    }

    #[test]
    fn abortable_rejects_boolean_self_assert() {
        struct Liar;
        impl ToolHandler for Liar {
            fn start(
                &self,
                _call: monoloop_contracts::ToolCall,
                _ctx: monoloop_contracts::ToolCallContext,
            ) -> Result<crate::LinkedToolExecutionHandle, monoloop_contracts::ToolStartError>
            {
                Err(monoloop_contracts::ToolStartError::Rejected("liar"))
            }
            fn supports_abort(&self) -> bool {
                true
            }
        }
        let err = RegisteredTool::try_new(abortable_spec(), Arc::new(Liar)).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("try_new_abortable"),
            "boolean self-assert must not register: {msg}"
        );
    }

    #[test]
    fn abortable_accepts_structural_handler() {
        RegisteredTool::try_new_abortable(
            abortable_spec(),
            AsyncToolHandler::new(|_c, _x, _ctl| {
                Box::pin(async {
                    ToolCompletion::Succeeded(CanonicalToolOutput::Json(serde_json::json!({})))
                })
            }),
        )
        .expect("structural AbortableAtYield ok");
    }

    #[test]
    fn abortable_typed_api_rejects_wrong_class() {
        let schema = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
        let cooperative = ToolSpec::try_new(
            ToolId::try_new("c").unwrap(),
            ToolName::try_new("c").unwrap(),
            "coop",
            schema.clone(),
            ToolOutputContract {
                success: ToolSuccessContract::json(schema),
                error_data_schema: None,
            },
            ToolLimits::default(),
            ToolExecutionClass::CooperativeInProcess {
                grace: Duration::from_secs(1),
            },
        )
        .unwrap();
        let err = RegisteredTool::try_new_abortable(
            cooperative,
            AsyncToolHandler::new(|_c, _x, _ctl| {
                Box::pin(async {
                    ToolCompletion::Succeeded(CanonicalToolOutput::Json(serde_json::json!({})))
                })
            }),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("AbortableAtYield"));
    }
}
