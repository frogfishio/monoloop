//! WP-07: MCP capability lifecycle, list/call parity, isolation.

use monoloop_contracts::{
    CanonicalToolOutput, ChannelId, ExchangeId, JsonSchema, SessionId, SessionKey, ToolActionId,
    ToolCancellationPolicy, ToolCompletion, ToolId, ToolLimits, ToolName, ToolOutputContract,
    ToolSpec, ToolSuccessContract, TransactionId,
};
use monoloop_loop::{
    dispatch_ready_tool, tool_definitions_from_resolved, CapabilityToken, DispatchOutcome,
    HostToolRegistry, ImmediateToolHandler, McpBindingState, McpGateway, RegisteredTool,
    ResolvedToolSet, SharedToolCapacity, ToolHandler, TransactionToolDispatcher,
};
use std::sync::Arc;
use std::time::Duration;

fn object_schema() -> JsonSchema {
    JsonSchema::try_new(serde_json::json!({
        "type": "object",
        "properties": { "q": { "type": "string" } },
        "required": ["q"],
        "additionalProperties": false
    }))
    .unwrap()
}

fn success_schema() -> JsonSchema {
    JsonSchema::try_new(serde_json::json!({
        "type": "object",
        "properties": { "ok": { "type": "boolean" } },
        "required": ["ok"],
        "additionalProperties": false
    }))
    .unwrap()
}

fn make_spec(id: &str, name: &str) -> ToolSpec {
    ToolSpec::try_new(
        ToolId::try_new(id).unwrap(),
        ToolName::try_new(name).unwrap(),
        "echo tool",
        object_schema(),
        ToolOutputContract {
            success: ToolSuccessContract::json(success_schema()),
            error_data_schema: None,
        },
        ToolLimits {
            max_concurrent: 4,
            max_input_bytes: 1024,
            max_output_bytes: 1024,
            execution_deadline: Duration::from_secs(5),
        },
        ToolCancellationPolicy::Abortable,
    )
    .unwrap()
}

fn ok_handler() -> Arc<dyn ToolHandler> {
    Arc::new(ImmediateToolHandler::new(|_call, _ctx| {
        Ok(ToolCompletion::Succeeded(CanonicalToolOutput::Json(
            serde_json::json!({"ok": true}),
        )))
    }))
}

fn session_key() -> SessionKey {
    SessionKey::new(
        ChannelId::try_new("ch").unwrap(),
        SessionId::try_new("s1").unwrap(),
    )
}

fn build_dispatcher(tools: ResolvedToolSet) -> Arc<TransactionToolDispatcher> {
    TransactionToolDispatcher::new(
        TransactionId::generate(),
        session_key(),
        tools,
        SharedToolCapacity::unlimited(),
        8,
        16,
    )
}

fn resolved_echo() -> (HostToolRegistry, ResolvedToolSet) {
    let host = HostToolRegistry::build(vec![RegisteredTool::new(
        make_spec("echo", "echo"),
        ok_handler(),
    )])
    .unwrap();
    let tool = host.get(&ToolId::try_new("echo").unwrap()).unwrap().clone();
    (host, ResolvedToolSet::from_registered(vec![tool]))
}

#[tokio::test]
async fn bind_loopback_and_shutdown_revokes_routes() {
    let gw = McpGateway::bind_loopback(32).await.unwrap();
    assert!(gw.local_addr().ip().is_loopback());
    let (_, tools) = resolved_echo();
    let d = build_dispatcher(tools.clone());
    let pending = gw
        .install_pending(TransactionId::generate(), tools, d, ExchangeId::generate())
        .unwrap();
    assert!(gw.routes().get(&pending.token).is_some());
    gw.shutdown().await;
}

#[tokio::test]
async fn pending_rejects_list_and_call_until_active() {
    let gw = McpGateway::bind_loopback(32).await.unwrap();
    let (_, tools) = resolved_echo();
    let d = build_dispatcher(tools.clone());
    let pending = gw
        .install_pending(TransactionId::generate(), tools, d, ExchangeId::generate())
        .unwrap();
    let binding = gw.routes().get(&pending.token).unwrap();
    assert_eq!(binding.state(), McpBindingState::Pending);
    assert!(binding.handler.list_tool_defs().is_err());
    let mut args = serde_json::Map::new();
    args.insert("q".into(), serde_json::json!("hi"));
    assert!(binding
        .handler
        .call_tool_direct("echo", Some(args))
        .await
        .is_err());

    gw.activate(&pending.token).unwrap();
    assert_eq!(
        gw.routes().state_of(&pending.token),
        Some(McpBindingState::Active)
    );
    let listed = binding.handler.list_tool_defs().unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "echo");

    let mut args = serde_json::Map::new();
    args.insert("q".into(), serde_json::json!("hi"));
    let result = binding
        .handler
        .call_tool_direct("echo", Some(args))
        .await
        .unwrap();
    assert_eq!(result.is_error, Some(false));

    gw.shutdown().await;
}

#[tokio::test]
async fn empty_resolved_set_lists_no_tools() {
    let gw = McpGateway::bind_loopback(8).await.unwrap();
    let tools = ResolvedToolSet::empty();
    let d = build_dispatcher(tools.clone());
    let pending = gw
        .install_pending(TransactionId::generate(), tools, d, ExchangeId::generate())
        .unwrap();
    gw.activate(&pending.token).unwrap();
    let binding = gw.routes().get(&pending.token).unwrap();
    let listed = binding.handler.list_tool_defs().unwrap();
    assert!(listed.is_empty());
    gw.shutdown().await;
}

#[tokio::test]
async fn unknown_revoked_cross_transaction_isolation() {
    let gw = McpGateway::bind_loopback(16).await.unwrap();
    let (_, tools_a) = resolved_echo();
    let d_a = build_dispatcher(tools_a.clone());
    let a = gw
        .install_pending(
            TransactionId::generate(),
            tools_a,
            d_a,
            ExchangeId::generate(),
        )
        .unwrap();
    gw.activate(&a.token).unwrap();

    // Unknown token
    assert!(gw.routes().get_by_hex(&"00".repeat(32)).is_none());

    // Revoke A
    assert!(gw.revoke(&a.token));
    assert!(gw.routes().get(&a.token).is_none());
    // Second revoke is idempotent false
    assert!(!gw.revoke(&a.token));

    // Transaction B cannot use A's token
    let (_, tools_b) = resolved_echo();
    let d_b = build_dispatcher(tools_b.clone());
    let b = gw
        .install_pending(
            TransactionId::generate(),
            tools_b,
            d_b,
            ExchangeId::generate(),
        )
        .unwrap();
    assert_ne!(a.token, b.token);
    assert!(gw.routes().get(&a.token).is_none());
    gw.activate(&b.token).unwrap();
    // Stale A hex still unknown
    assert!(gw.routes().get(&a.token).is_none());

    gw.shutdown().await;
}

#[tokio::test]
async fn delayed_capability_a_cannot_enter_b() {
    let gw = McpGateway::bind_loopback(16).await.unwrap();
    let (_, tools) = resolved_echo();
    let d1 = build_dispatcher(tools.clone());
    let a = gw
        .install_pending(
            TransactionId::generate(),
            tools.clone(),
            d1,
            ExchangeId::generate(),
        )
        .unwrap();
    gw.activate(&a.token).unwrap();
    let token_a = a.token.clone();
    gw.revoke(&token_a);

    let d2 = build_dispatcher(tools);
    let b = gw
        .install_pending(
            TransactionId::generate(),
            ResolvedToolSet::empty(),
            d2,
            ExchangeId::generate(),
        )
        .unwrap();
    gw.activate(&b.token).unwrap();

    // Late use of A after B is active
    assert!(gw.routes().get(&token_a).is_none());
    assert!(gw.routes().get(&b.token).is_some());
    gw.shutdown().await;
}

#[tokio::test]
async fn capability_redacted_in_debug() {
    let token = CapabilityToken::generate().unwrap();
    let dbg = format!("{token:?}");
    assert!(dbg.contains("redacted"));
    assert!(!dbg.contains(&token.to_hex()));

    let gw = McpGateway::bind_loopback(4).await.unwrap();
    let (_, tools) = resolved_echo();
    let d = build_dispatcher(tools.clone());
    let pending = gw
        .install_pending(TransactionId::generate(), tools, d, ExchangeId::generate())
        .unwrap();
    let desc_dbg = format!("{:?}", pending.descriptor);
    assert!(desc_dbg.contains("redacted") || desc_dbg.contains("<redacted>"));
    assert!(!desc_dbg.contains(&pending.token.to_hex()));
    // Descriptor Debug must not include full capability URL path token
    assert!(!desc_dbg.contains("/mcp/"));
    gw.shutdown().await;
}

#[tokio::test]
async fn mcp_and_local_paths_same_handler_and_definitions() {
    let host = HostToolRegistry::build(vec![RegisteredTool::new(
        make_spec("echo", "echo"),
        ok_handler(),
    )])
    .unwrap();
    let registered = host.get(&ToolId::try_new("echo").unwrap()).unwrap().clone();
    let tools = ResolvedToolSet::from_registered(vec![registered]);
    let mcp_defs = tool_definitions_from_resolved(&tools);
    assert_eq!(mcp_defs.len(), 1);
    assert_eq!(mcp_defs[0].name, "echo");
    // Encoder projection uses the same specs()
    assert_eq!(tools.specs()[0].name.as_str(), mcp_defs[0].name.as_ref());

    let dispatcher = build_dispatcher(tools.clone());
    let gw = McpGateway::bind_loopback(8).await.unwrap();
    let pending = gw
        .install_pending(
            TransactionId::generate(),
            tools.clone(),
            Arc::clone(&dispatcher),
            ExchangeId::generate(),
        )
        .unwrap();
    gw.activate(&pending.token).unwrap();

    // Local path
    let local = dispatch_ready_tool(
        &dispatcher,
        ExchangeId::generate(),
        ToolActionId::new("local-1"),
        "echo",
        "p1",
        0,
        r#"{"q":"hi"}"#,
    )
    .await;
    assert!(matches!(local, DispatchOutcome::Canonical { .. }));

    // MCP path (same dispatcher Arc)
    let binding = gw.routes().get(&pending.token).unwrap();
    let mut args = serde_json::Map::new();
    args.insert("q".into(), serde_json::json!("hi"));
    let mcp = binding
        .handler
        .call_tool_direct("echo", Some(args))
        .await
        .unwrap();
    assert_eq!(mcp.is_error, Some(false));

    // Disallowed tool on MCP
    let bad = binding.handler.call_tool_direct("nope", None).await;
    assert!(bad.is_err());

    gw.shutdown().await;
}

#[tokio::test]
async fn disallowed_tool_and_schema_invalid_on_mcp() {
    let gw = McpGateway::bind_loopback(8).await.unwrap();
    let (_, tools) = resolved_echo();
    let d = build_dispatcher(tools.clone());
    let pending = gw
        .install_pending(TransactionId::generate(), tools, d, ExchangeId::generate())
        .unwrap();
    gw.activate(&pending.token).unwrap();
    let binding = gw.routes().get(&pending.token).unwrap();

    let mut bad_args = serde_json::Map::new();
    bad_args.insert("q".into(), serde_json::json!(1));
    let invalid = binding
        .handler
        .call_tool_direct("echo", Some(bad_args))
        .await
        .unwrap();
    assert_eq!(invalid.is_error, Some(true));

    gw.shutdown().await;
}

#[tokio::test]
async fn http_unknown_capability_is_404() {
    let gw = McpGateway::bind_loopback(4).await.unwrap();
    let addr = gw.local_addr();
    let client = reqwest::Client::new();
    let url = format!("http://{addr}/mcp/{}", "ab".repeat(32));
    let resp = client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    gw.shutdown().await;
}

#[tokio::test]
async fn token_hex_roundtrip() {
    let t = CapabilityToken::generate().unwrap();
    let hex = t.to_hex();
    assert_eq!(hex.len(), 64);
    let back = CapabilityToken::from_hex(&hex).unwrap();
    assert_eq!(t, back);
    assert!(CapabilityToken::from_hex("short").is_none());
    assert!(CapabilityToken::from_hex(&"zz".repeat(32)).is_none());
}
