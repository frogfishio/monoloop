//! Per-transaction MCP ServerHandler delegating to TransactionToolDispatcher.

use crate::transaction::dispatcher::{DispatchOutcome, DispatchRequest, TransactionToolDispatcher};
use crate::transaction::resolved_tools::ResolvedToolSet;
use monoloop_contracts::{
    CanonicalToolResultOutcome, ExchangeId, ToolActionId, ToolName, TransactionId,
};
use rmcp::{
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, ErrorCode,
        ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerInfo, Tool,
    },
    service::RequestContext,
    ErrorData as McpError, RoleServer, ServerHandler,
};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

const STATE_ACTIVE: u8 = 1;

/// Project resolved tool specs into MCP `tools/list` definitions (parity with encoders).
pub fn tool_definitions_from_resolved(tools: &ResolvedToolSet) -> Vec<Tool> {
    tools
        .specs()
        .into_iter()
        .map(|spec| {
            let schema = spec.input_schema.as_value().clone();
            let obj = match schema {
                serde_json::Value::Object(m) => Arc::new(m),
                _ => Arc::new(serde_json::Map::new()),
            };
            Tool::new(
                spec.name.as_str().to_string(),
                spec.description.clone(),
                obj,
            )
        })
        .collect()
}

/// MCP handler bound to one transaction capability.
#[derive(Clone)]
pub struct TransactionMcpHandler {
    state: Arc<AtomicU8>,
    tools: ResolvedToolSet,
    dispatcher: Arc<TransactionToolDispatcher>,
    transaction_id: TransactionId,
    exchange_id: ExchangeId,
    tool_defs: Vec<Tool>,
}

impl TransactionMcpHandler {
    /// Construct for a binding (shares state atom with the route table entry).
    pub fn new(
        state: Arc<AtomicU8>,
        tools: ResolvedToolSet,
        dispatcher: Arc<TransactionToolDispatcher>,
        transaction_id: TransactionId,
        exchange_id: ExchangeId,
    ) -> Self {
        let tool_defs = tool_definitions_from_resolved(&tools);
        Self {
            state,
            tools,
            dispatcher,
            transaction_id,
            exchange_id,
            tool_defs,
        }
    }

    fn ensure_active(&self) -> Result<(), McpError> {
        if self.state.load(Ordering::SeqCst) == STATE_ACTIVE {
            Ok(())
        } else {
            Err(McpError::new(
                ErrorCode::INVALID_REQUEST,
                "MCP capability is not active",
                None,
            ))
        }
    }

    /// Direct list (unit tests without full MCP session).
    pub fn list_tool_defs(&self) -> Result<Vec<Tool>, McpError> {
        self.ensure_active()?;
        Ok(self.tool_defs.clone())
    }

    /// Direct call through the shared dispatcher.
    pub async fn call_tool_direct(
        &self,
        name: &str,
        arguments: Option<serde_json::Map<String, serde_json::Value>>,
    ) -> Result<CallToolResult, McpError> {
        self.ensure_active()?;
        let tool_name = ToolName::try_new(name)
            .map_err(|_| McpError::new(ErrorCode::INVALID_PARAMS, "invalid tool name", None))?;
        if !self.tools.contains_name(&tool_name) {
            return Err(McpError::new(
                ErrorCode::METHOD_NOT_FOUND,
                "tool not in resolved set",
                None,
            ));
        }
        let args = arguments.unwrap_or_default();
        let arguments_json = serde_json::Value::Object(args).to_string();
        let action = ToolActionId::new(format!("mcp:{}:{}", self.transaction_id.as_uuid(), name));
        let outcome = self
            .dispatcher
            .dispatch(DispatchRequest {
                exchange_id: self.exchange_id,
                tool_action_id: action,
                tool_name,
                provider_tool_call_id: format!("mcp-{}", uuid::Uuid::new_v4()),
                request_ordinal: 0,
                arguments_json,
            })
            .await;
        map_dispatch_to_call_result(outcome)
    }
}

impl ServerHandler for TransactionMcpHandler {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        self.ensure_active()?;
        // Bounded page: full resolved set is already admission-bounded.
        Ok(ListToolsResult::with_all_items(self.tool_defs.clone()))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, McpError> {
        let name = request.name.as_ref();
        let args = request.arguments;
        let result = self.call_tool_direct(name, args).await?;
        Ok(CallToolResponse::Complete(result))
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        if self.state.load(Ordering::SeqCst) != STATE_ACTIVE {
            return None;
        }
        self.tool_defs.iter().find(|t| t.name == name).cloned()
    }
}

fn map_dispatch_to_call_result(outcome: DispatchOutcome) -> Result<CallToolResult, McpError> {
    match outcome {
        DispatchOutcome::Canonical { result, .. } => match result.outcome {
            CanonicalToolResultOutcome::Succeeded(output) => {
                let text = match output {
                    monoloop_contracts::CanonicalToolOutput::Json(v) => {
                        serde_json::to_string(&v).unwrap_or_else(|_| "{}".into())
                    }
                    monoloop_contracts::CanonicalToolOutput::Text(t) => t,
                };
                Ok(CallToolResult::success(vec![ContentBlock::text(text)]))
            }
            CanonicalToolResultOutcome::DomainFailed(err) => {
                Ok(CallToolResult::error(vec![ContentBlock::text(format!(
                    "{}: {}",
                    err.code, err.message
                ))]))
            }
        },
        DispatchOutcome::Rejected { code, message, .. } => {
            // Invalid args: tool-level error visible to the agent.
            Ok(CallToolResult::error(vec![ContentBlock::text(format!(
                "{code}: {message}"
            ))]))
        }
        DispatchOutcome::RuntimeFailed { code, .. } => Err(McpError::new(
            ErrorCode::INTERNAL_ERROR,
            format!("tool runtime failure: {code}"),
            None,
        )),
    }
}
