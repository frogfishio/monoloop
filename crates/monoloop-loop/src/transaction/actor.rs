//! Transaction actor: session establish + one provider exchange + finalization.

use super::active_registry::{ActiveTransactionRegistry, ClaimSessionError, ControlMessage};
use super::dispatcher::{DispatchOutcome, DispatcherLimits, TransactionToolDispatcher};
use super::events::{BoundedEventSender, QueuedEvent};
use super::exchange::{
    run_encoded_exchange, run_exchange, EncodedExchangeParams, ExchangeFailure, ExchangeParams,
};
use super::finalization::{build_transaction_end, FinalizationGuard};
use super::loop_adapters::dispatch_ready_tool;
use super::mcp::{CapabilityToken, McpGatewayHandle, PendingMcpBinding};
use super::resolved_tools::ResolvedToolSet;
use super::tool_capacity::SharedToolCapacity;
use monoloop_connector::{Connector, SessionAdapter};
use monoloop_contracts::{
    CanonicalAssistantToolCall, CanonicalMessage, CanonicalToolError, CanonicalToolResult,
    CanonicalUnit, CanonicalUnitEvent, ChannelId, ChannelKind, ContinuationContext,
    ContinuationPolicy, EffectiveConfig, EventDeliveryOutcome, ExternalSessionId,
    InterpretationLimits, McpConfigurationCapability, McpReachability, OutboundDialectEncoder,
    SessionId, SessionKey, TextChannel, TextPart, ToolExecutionMode, ToolId, ToolLifecycleEvent,
    ToolName, ToolRequestState, TransactionEndKind, TransactionEvent, TransactionEventPayload,
    TransactionId,
};
use monoloop_interpreter::InterpreterFactory;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
// oneshot used for start_gate and create-session claim.

/// Inputs for the transaction actor.
pub struct ActorSpawn {
    /// Transaction id.
    pub transaction_id: TransactionId,
    /// Channel id.
    pub channel_id: ChannelId,
    /// Channel kind (session topology).
    #[allow(dead_code)]
    pub channel_kind: ChannelKind,
    /// Tool execution mode (controls Loop fan-out; MCP/None skip).
    pub tool_mode: ToolExecutionMode,
    /// MCP install mode for external agents.
    pub mcp_configuration: McpConfigurationCapability,
    /// MCP reachability declaration.
    pub mcp_reachability: McpReachability,
    /// MCP gateway handle (required for McpGateway tool mode when enabled).
    pub mcp: Option<McpGatewayHandle>,
    /// Session key if known at admission.
    pub session_key: Option<SessionKey>,
    /// Provisional external create (no session yet).
    pub provisional_external: bool,
    /// Session was supplied by caller (existing external session).
    pub existing_session: bool,
    /// Session adapter for external Channels.
    pub sessions: Option<Arc<dyn SessionAdapter>>,
    /// Connector for open/send/receive.
    pub connector: Arc<dyn Connector>,
    /// Outbound encoder.
    pub encoder: Arc<dyn OutboundDialectEncoder>,
    /// Interpreter factory.
    pub interpreter: Arc<dyn InterpreterFactory>,
    /// Endpoint ref for open.
    pub endpoint_ref: String,
    /// Credential ref.
    pub credential_ref: Option<String>,
    /// Canonical input.
    pub input: monoloop_contracts::CanonicalInput,
    /// Effective configuration.
    pub effective: EffectiveConfig,
    /// Resolved tools (specs for encoder).
    pub tools: ResolvedToolSet,
    /// Finalization guard.
    pub guard: Arc<FinalizationGuard>,
    /// Control receiver (capacity 1).
    pub control_rx: mpsc::Receiver<ControlMessage>,
    /// Event queue to delivery task (item + byte bounded).
    pub event_tx: BoundedEventSender,
    /// Delivery failure signal.
    pub delivery_fail_rx: mpsc::Receiver<()>,
    /// Shared registry.
    pub registry: Arc<Mutex<ActiveTransactionRegistry>>,
    /// Capacity release on exit.
    pub release_capacity: Arc<dyn Fn() + Send + Sync>,
    /// Transaction deadline.
    pub deadline: Duration,
    /// Maximum inline tool continuations (provider exchanges after the first).
    pub max_continuations: usize,
    /// Maximum total provider exchanges including the first.
    pub max_provider_exchanges: usize,
    /// Max concurrent tools (from runtime limits).
    pub max_concurrent_tools: usize,
    /// Max queued tools (from runtime limits).
    pub max_queued_tools: usize,
    /// Cleanup budget after terminal selection.
    pub cleanup_deadline: Duration,
    /// Terminal Ended delivery budget.
    pub terminal_event_delivery_deadline: Duration,
    /// Callback deadline.
    pub callback_deadline: Duration,
    /// Max total provider input bytes across exchanges.
    pub max_total_provider_input_bytes: usize,
    /// Max total provider output bytes across exchanges.
    pub max_total_provider_output_bytes: usize,
    /// Max continuation context bytes.
    pub max_continuation_context_bytes: usize,
    /// Max tool schema aggregate for this transaction's resolved set.
    pub max_tool_schema_bytes: usize,
    /// Transaction-wide tool payload cap (D-015).
    pub max_tool_payload_bytes: usize,
    /// Transaction-wide tool output cap (D-015).
    pub max_tool_output_bytes: usize,
    /// Closed only after registry install succeeds (D-009).
    pub start_gate: oneshot::Receiver<()>,
}

/// Spawn the actor task.
pub fn spawn_actor(spawn: ActorSpawn) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        run_actor(spawn).await;
    })
}

struct ActorResult {
    kind: TransactionEndKind,
    prior: Option<TransactionEndKind>,
    delivery: EventDeliveryOutcome,
    session_key: Option<SessionKey>,
}

async fn run_actor(spawn: ActorSpawn) {
    let ActorSpawn {
        transaction_id,
        channel_id,
        channel_kind: _,
        tool_mode,
        mcp_configuration,
        mcp_reachability,
        mcp,
        mut session_key,
        provisional_external,
        existing_session,
        sessions,
        connector,
        encoder,
        interpreter,
        endpoint_ref,
        credential_ref,
        input,
        effective,
        tools,
        guard,
        mut control_rx,
        event_tx,
        mut delivery_fail_rx,
        registry,
        release_capacity,
        deadline,
        max_continuations,
        max_provider_exchanges,
        max_concurrent_tools,
        max_queued_tools,
        cleanup_deadline,
        terminal_event_delivery_deadline,
        callback_deadline,
        max_total_provider_input_bytes,
        max_total_provider_output_bytes,
        max_continuation_context_bytes,
        max_tool_schema_bytes,
        max_tool_payload_bytes,
        max_tool_output_bytes,
        start_gate,
    } = spawn;

    // D-009: do not perform I/O or callbacks until admission installs us.
    match start_gate.await {
        Ok(()) => {}
        Err(_) => {
            release_capacity();
            let mut reg = registry.lock().unwrap_or_else(|e| e.into_inner());
            let _ = reg.remove(&transaction_id);
            return;
        }
    }

    // D-015: reject oversized tool schema aggregates at start.
    {
        let mut schema_total = 0usize;
        for spec in tools.specs() {
            schema_total = schema_total.saturating_add(
                serde_json::to_vec(spec.input_schema.as_value())
                    .map(|b| b.len())
                    .unwrap_or(0),
            );
        }
        if schema_total > max_tool_schema_bytes {
            let result = ActorResult {
                kind: TransactionEndKind::LimitExceeded,
                prior: None,
                delivery: EventDeliveryOutcome::Failed,
                session_key: session_key.clone(),
            };
            finalize_and_cleanup(
                transaction_id,
                channel_id,
                guard,
                event_tx,
                registry,
                release_capacity,
                result,
                terminal_event_delivery_deadline,
                callback_deadline,
            )
            .await;
            return;
        }
    }
    // Join/abort grace for exchange children after cancel or terminal (D-012).
    let cleanup_deadline = cleanup_deadline.max(Duration::from_millis(50));

    let mut terminal_kind = TransactionEndKind::Completed;
    let mut attachment: Option<Arc<monoloop_connector::SessionAttachment>> = None;
    let mcp_token: Arc<Mutex<Option<CapabilityToken>>> = Arc::new(Mutex::new(None));
    let mcp_token_work = Arc::clone(&mcp_token);

    let work = async {
        // --- EstablishingSession (D-013): attach for create *and* explicit load ---
        let mut pending_mcp: Option<PendingMcpBinding> = None;
        let mut create_mode_attach = false;

        if let Some(ref adapter) = sessions {
            // D-014: install pending MCP *before* attach when tools present and create.
            let mut initial_mcp = None;
            if tool_mode == ToolExecutionMode::McpGateway
                && !tools.is_empty()
                && mcp_reachability == McpReachability::SameLoopbackNamespace
            {
                if mcp_configuration == McpConfigurationCapability::CreationOnly && existing_session
                {
                    return Err(TransactionEndKind::InvariantFailed);
                }
                if !existing_session || mcp_configuration == McpConfigurationCapability::Refreshable
                {
                    let handle = mcp.as_ref().ok_or(TransactionEndKind::InvariantFailed)?;
                    // SessionKey may be provisional for create; use channel+placeholder.
                    let sk = session_key.clone().unwrap_or_else(|| {
                        SessionKey::new(channel_id.clone(), SessionId::generate())
                    });
                    let dispatcher = TransactionToolDispatcher::with_limits(
                        transaction_id,
                        sk,
                        tools.clone(),
                        SharedToolCapacity::unlimited(),
                        DispatcherLimits {
                            max_concurrent_tools,
                            max_queued_tools,
                            max_tool_payload_bytes,
                            max_tool_output_bytes,
                        },
                    );
                    let pending = handle
                        .install_pending(
                            transaction_id,
                            tools.clone(),
                            dispatcher,
                            monoloop_contracts::ExchangeId::generate(),
                        )
                        .map_err(|_| TransactionEndKind::InvariantFailed)?;
                    {
                        let mut g = mcp_token_work.lock().unwrap_or_else(|e| e.into_inner());
                        *g = Some(pending.token.clone());
                    }
                    if !existing_session
                        && mcp_configuration == McpConfigurationCapability::CreationOnly
                    {
                        initial_mcp = Some(pending.descriptor.clone());
                    }
                    pending_mcp = Some(pending);
                }
            }

            let requested = if provisional_external {
                None
            } else {
                session_key.as_ref().map(|k| k.session_id.clone())
            };
            let req = monoloop_connector::SessionAttachRequest {
                transaction_id,
                channel_id: channel_id.clone(),
                requested_session_id: requested,
                session_config: effective.session.clone(),
                initial_mcp,
                deadline: std::time::Instant::now() + deadline,
            };
            let pending = adapter
                .begin_attach(req)
                .map_err(|_| TransactionEndKind::ChannelOpenFailed)?;
            let att = tokio::select! {
                biased;
                ctrl = control_rx.recv() => {
                    let _ = pending.control.cancel();
                    return Err(match ctrl {
                        Some(ControlMessage::ForceTerminate) => TransactionEndKind::Terminated,
                        _ => TransactionEndKind::Cancelled,
                    });
                }
                r = pending.completion => {
                    r.map_err(|_| TransactionEndKind::ChannelOpenFailed)?
                }
            };
            create_mode_attach = att.create_mode;

            if !att.create_mode {
                // Load: claim SessionKey with the authoritative loaded id now.
                let sid = SessionId::from_external(&att.external_session_id);
                if let Some(ref expected) = session_key {
                    if expected.session_id.as_str() != sid.as_str() {
                        return Err(TransactionEndKind::InvariantFailed);
                    }
                }
                let key = SessionKey::new(channel_id.clone(), sid);
                if session_key.is_none() {
                    let mut reg = registry.lock().unwrap_or_else(|e| e.into_inner());
                    match reg.claim_session(transaction_id, key.clone()) {
                        Ok(()) => {}
                        Err(ClaimSessionError::Collision) => {
                            return Err(TransactionEndKind::InvariantFailed);
                        }
                        Err(_) => return Err(TransactionEndKind::InvariantFailed),
                    }
                }
                session_key = Some(key);
                if !emit_unit_or_session(
                    &event_tx,
                    &guard,
                    transaction_id,
                    &channel_id,
                    &session_key,
                    TransactionEventPayload::SessionEstablished {
                        external_session_id: att.external_session_id.clone(),
                    },
                )
                .await
                {
                    return Err(TransactionEndKind::EventDeliveryFailed);
                }
            }
            // Create: claim after first open returns the provider id (below).
            attachment = Some(att);

            // D-014: activate MCP route after attach; CreationOnly uses initial_mcp only
            // (never begin_refresh_mcp). Refreshable may still refresh after claim.
            if let Some(ref pending) = pending_mcp {
                if mcp_configuration == McpConfigurationCapability::Refreshable {
                    if let (Some(att_ref), Some(adapter)) = (attachment.as_ref(), sessions.as_ref())
                    {
                        if let Ok(pending_cfg) = adapter.begin_refresh_mcp(
                            Arc::clone(att_ref),
                            Some(pending.descriptor.clone()),
                        ) {
                            tokio::select! {
                                biased;
                                ctrl = control_rx.recv() => {
                                    let _ = pending_cfg.control.cancel();
                                    return Err(match ctrl {
                                        Some(ControlMessage::ForceTerminate) => {
                                            TransactionEndKind::Terminated
                                        }
                                        _ => TransactionEndKind::Cancelled,
                                    });
                                }
                                r = pending_cfg.completion => {
                                    r.map_err(|_| TransactionEndKind::InvariantFailed)?;
                                }
                            }
                        }
                    }
                }
                // CreationOnly: descriptor already passed via initial_mcp; activate route.
                if let Some(handle) = mcp.as_ref() {
                    handle
                        .activate(&pending.token)
                        .map_err(|_| TransactionEndKind::InvariantFailed)?;
                }
            }
        } else if provisional_external {
            // No SessionAdapter: synthetic DirectLlm-style claim only.
            let sid = SessionId::generate();
            let key = SessionKey::new(channel_id.clone(), sid.clone());
            {
                let mut reg = registry.lock().unwrap_or_else(|e| e.into_inner());
                match reg.claim_session(transaction_id, key.clone()) {
                    Ok(()) => {}
                    Err(ClaimSessionError::Collision) => {
                        return Err(TransactionEndKind::InvariantFailed);
                    }
                    Err(_) => return Err(TransactionEndKind::InvariantFailed),
                }
            }
            session_key = Some(key);
            let ext = ExternalSessionId::try_new(sid.as_str())
                .map_err(|_| TransactionEndKind::InvariantFailed)?;
            if !emit_unit_or_session(
                &event_tx,
                &guard,
                transaction_id,
                &channel_id,
                &session_key,
                TransactionEventPayload::SessionEstablished {
                    external_session_id: ext,
                },
            )
            .await
            {
                return Err(TransactionEndKind::EventDeliveryFailed);
            }
        }
        let _ = (existing_session, provisional_external);

        // --- Provider exchanges (initial + optional inline tool continuations) ---
        let tool_specs: Vec<_> = tools.specs().into_iter().cloned().collect();
        let max_exchanges = max_provider_exchanges.max(1);
        let max_inline = max_continuations;
        let mut exchanges_done = 0usize;
        let mut continuations_done = 0usize;
        let mut provider_input_bytes = 0usize;
        let mut provider_output_bytes = 0usize;
        let _ = max_total_provider_output_bytes;

        // D-011: fan units to the event sink as they are produced (not only after EOF).
        let (live_tx, mut live_rx) =
            mpsc::channel::<CanonicalUnitEvent>(max_provider_exchanges.saturating_mul(64).max(64));
        let event_tx_live = event_tx.clone();
        let guard_live = Arc::clone(&guard);
        let channel_live = channel_id.clone();
        let session_live = session_key.clone();
        let live_join = tokio::spawn(async move {
            while let Some(unit) = live_rx.recv().await {
                if !emit_canonical_unit(
                    &event_tx_live,
                    &guard_live,
                    transaction_id,
                    &channel_live,
                    &session_live,
                    unit,
                )
                .await
                {
                    return Err(TransactionEndKind::EventDeliveryFailed);
                }
            }
            Ok(())
        });

        let (session_id_tx, claim_join) = if create_mode_attach {
            let (sess_tx, sess_rx) = oneshot::channel();
            let registry_c = Arc::clone(&registry);
            let event_tx_c = event_tx.clone();
            let guard_c = Arc::clone(&guard);
            let channel_c = channel_id.clone();
            let join = tokio::spawn(async move {
                let Ok(ext) = sess_rx.await else {
                    return Ok::<(), TransactionEndKind>(());
                };
                let sid = SessionId::from_external(&ext);
                let key = SessionKey::new(channel_c.clone(), sid);
                {
                    let mut reg = registry_c.lock().unwrap_or_else(|e| e.into_inner());
                    match reg.claim_session(transaction_id, key.clone()) {
                        Ok(()) => {}
                        Err(ClaimSessionError::Collision) => {
                            return Err(TransactionEndKind::InvariantFailed);
                        }
                        Err(_) => return Err(TransactionEndKind::InvariantFailed),
                    }
                }
                let sk = Some(key);
                guard_c.set_session_id(
                    sk.as_ref()
                        .map(|k| k.session_id.clone())
                        .unwrap_or_else(SessionId::generate),
                );
                if !emit_unit_or_session(
                    &event_tx_c,
                    &guard_c,
                    transaction_id,
                    &channel_c,
                    &sk,
                    TransactionEventPayload::SessionEstablished {
                        external_session_id: ext,
                    },
                )
                .await
                {
                    return Err(TransactionEndKind::EventDeliveryFailed);
                }
                Ok(())
            });
            (Some(sess_tx), Some(join))
        } else {
            (None, None)
        };

        let mut outcome = tokio::select! {
            biased;
            ctrl = control_rx.recv() => {
                // Drop exchange future → ExchangeGuard terminates connector + aborts units (D-012).
                // Join sibling fan-out/claim tasks within cleanup_deadline.
                live_join.abort();
                let _ = tokio::time::timeout(cleanup_deadline, live_join).await;
                if let Some(j) = claim_join {
                    j.abort();
                    let _ = tokio::time::timeout(cleanup_deadline, j).await;
                }
                return Err(match ctrl {
                    Some(ControlMessage::ForceTerminate) => TransactionEndKind::Terminated,
                    _ => TransactionEndKind::Cancelled,
                });
            }
            r = run_exchange(ExchangeParams {
                transaction_id,
                connector: connector.as_ref(),
                encoder: encoder.as_ref(),
                interpreter: interpreter.as_ref(),
                endpoint_ref: &endpoint_ref,
                credential_ref: credential_ref.as_deref(),
                session_attachment: attachment.clone(),
                input: &input,
                config: &effective,
                tools: &tool_specs,
                interpretation_limits: InterpretationLimits::default(),
                deadline,
                cleanup_deadline,
                unit_tx: Some(live_tx),
                session_id_tx,
            }) => r,
        }
        .map_err(map_exchange_failure)?;
        exchanges_done += 1;
        for u in &outcome.units {
            provider_output_bytes = provider_output_bytes.saturating_add(estimate_unit_bytes(u));
        }
        if provider_output_bytes > max_total_provider_output_bytes {
            return Err(TransactionEndKind::LimitExceeded);
        }
        if let Some(j) = claim_join {
            j.await.map_err(|_| TransactionEndKind::InvariantFailed)??;
            if let Some(ext) = outcome.external_session_id.clone() {
                session_key = Some(SessionKey::new(
                    channel_id.clone(),
                    SessionId::from_external(&ext),
                ));
            }
        }
        live_join
            .await
            .map_err(|_| TransactionEndKind::InvariantFailed)??;

        loop {
            // Units already published live (D-011). outcome.units retained for tools/continuation.

            if let Some(fail) = outcome.failure {
                return Err(map_exchange_failure(fail));
            }

            // --- Linked tools (ModelToolCalls) ---
            if tool_mode != ToolExecutionMode::ModelToolCalls {
                break;
            }

            let ready = collect_ready_tools(&outcome.units);
            if ready.is_empty() || tools.is_empty() {
                // No tool requests, or empty admitted set (zero external effects).
                break;
            }

            let sk = session_key
                .clone()
                .ok_or(TransactionEndKind::InvariantFailed)?;

            let dispatcher = TransactionToolDispatcher::with_limits(
                transaction_id,
                sk.clone(),
                tools.clone(),
                SharedToolCapacity::unlimited(),
                DispatcherLimits {
                    max_concurrent_tools,
                    max_queued_tools,
                    max_tool_payload_bytes,
                    max_tool_output_bytes,
                },
            );

            let mut results: Vec<CanonicalToolResult> = Vec::with_capacity(ready.len());
            for (ord, (action_id, name, payload, provider_id)) in ready.into_iter().enumerate() {
                let dispatch_outcome = tokio::select! {
                    biased;
                    ctrl = control_rx.recv() => {
                        return Err(match ctrl {
                            Some(ControlMessage::ForceTerminate) => {
                                TransactionEndKind::Terminated
                            }
                            _ => TransactionEndKind::Cancelled,
                        });
                    }
                    r = dispatch_ready_tool(
                        &dispatcher,
                        outcome.exchange_id,
                        action_id,
                        &name,
                        &provider_id,
                        ord as u32,
                        &payload,
                    ) => r,
                };
                match &dispatch_outcome {
                    DispatchOutcome::Canonical { result, .. } => {
                        results.push(result.clone());
                    }
                    DispatchOutcome::Rejected {
                        tool_action_id,
                        code,
                        message,
                        ..
                    } => {
                        // D-022: ordinary rejections are correlated tool outcomes.
                        let err = CanonicalToolError::try_new(*code, message.as_str(), None, 256)
                            .unwrap_or_else(|_| {
                                CanonicalToolError::try_new("tool_rejected", "rejected", None, 256)
                                    .expect("static")
                            });
                        results.push(CanonicalToolResult {
                            transaction_id,
                            session_key: sk.clone(),
                            exchange_id: outcome.exchange_id,
                            tool_action_id: tool_action_id.clone(),
                            tool_id: ToolId::try_new("rejected")
                                .unwrap_or_else(|_| ToolId::try_new("unknown").expect("static")),
                            provider_tool_call_id: provider_id.clone(),
                            request_ordinal: ord as u32,
                            outcome: monoloop_contracts::CanonicalToolResultOutcome::DomainFailed(
                                err,
                            ),
                        });
                    }
                    DispatchOutcome::RuntimeFailed { .. } => {}
                }
                emit_dispatch_outcome(
                    &event_tx,
                    &guard,
                    transaction_id,
                    &channel_id,
                    &session_key,
                    dispatch_outcome,
                )
                .await?;
            }

            if results.is_empty() {
                // All rejected/runtime-failed handled by emit_dispatch_outcome.
                break;
            }

            match effective.continuation_policy {
                ContinuationPolicy::CallerControlled => {
                    return Err(TransactionEndKind::ContinuationRequired);
                }
                ContinuationPolicy::InlineToolContinuation => {
                    if continuations_done >= max_inline || exchanges_done >= max_exchanges {
                        return Err(TransactionEndKind::LimitExceeded);
                    }
                    let context = build_continuation_context(&input, &outcome.units)
                        .map_err(|_| TransactionEndKind::EncodingFailed)?;
                    let exchange_id = monoloop_contracts::ExchangeId::generate();
                    let encoded = encoder
                        .encode_tool_continuation(
                            monoloop_contracts::ToolContinuationEncodeRequest {
                                transaction_id: &transaction_id,
                                exchange_id: &exchange_id,
                                context: &context,
                                results: &results,
                                config: &effective,
                                tools: &tool_specs,
                            },
                        )
                        .map_err(|_| TransactionEndKind::EncodingFailed)?;
                    // D-015: continuation context + provider input aggregate bounds.
                    if encoded.bytes.len() > max_continuation_context_bytes {
                        return Err(TransactionEndKind::LimitExceeded);
                    }
                    provider_input_bytes = provider_input_bytes.saturating_add(encoded.bytes.len());
                    if provider_input_bytes > max_total_provider_input_bytes {
                        return Err(TransactionEndKind::LimitExceeded);
                    }
                    let (live_tx2, mut live_rx2) = mpsc::channel::<CanonicalUnitEvent>(64);
                    let event_tx_live = event_tx.clone();
                    let guard_live = Arc::clone(&guard);
                    let channel_live = channel_id.clone();
                    let session_live = session_key.clone();
                    let live_join2 = tokio::spawn(async move {
                        while let Some(unit) = live_rx2.recv().await {
                            if !emit_canonical_unit(
                                &event_tx_live,
                                &guard_live,
                                transaction_id,
                                &channel_live,
                                &session_live,
                                unit,
                            )
                            .await
                            {
                                return Err(TransactionEndKind::EventDeliveryFailed);
                            }
                        }
                        Ok(())
                    });
                    outcome = tokio::select! {
                        biased;
                        ctrl = control_rx.recv() => {
                            live_join2.abort();
                            let _ = tokio::time::timeout(cleanup_deadline, live_join2).await;
                            return Err(match ctrl {
                                Some(ControlMessage::ForceTerminate) => {
                                    TransactionEndKind::Terminated
                                }
                                _ => TransactionEndKind::Cancelled,
                            });
                        }
                        r = run_encoded_exchange(EncodedExchangeParams {
                            transaction_id,
                            exchange_id,
                            connector: connector.as_ref(),
                            interpreter: interpreter.as_ref(),
                            endpoint_ref: &endpoint_ref,
                            credential_ref: credential_ref.as_deref(),
                            session_attachment: attachment.clone(),
                            encoded,
                            interpretation_limits: InterpretationLimits::default(),
                            deadline,
                            cleanup_deadline,
                            unit_tx: Some(live_tx2),
                        }) => r,
                    }
                    .map_err(map_exchange_failure)?;
                    live_join2
                        .await
                        .map_err(|_| TransactionEndKind::InvariantFailed)??;
                    exchanges_done += 1;
                    continuations_done += 1;
                }
            }
        }

        Ok::<(), TransactionEndKind>(())
    };

    // Race work against deadline / delivery failure only (control is selected inside work).
    let cancelled = tokio::select! {
        biased;
        fail = delivery_fail_rx.recv() => {
            if fail.is_some() {
                terminal_kind = TransactionEndKind::EventDeliveryFailed;
            }
            true
        }
        _ = tokio::time::sleep(deadline) => {
            terminal_kind = TransactionEndKind::DeadlineExceeded;
            true
        }
        work_res = work => {
            match work_res {
                Ok(()) => {
                    terminal_kind = TransactionEndKind::Completed;
                }
                Err(k) => terminal_kind = k,
            }
            false
        }
    };
    let _ = cancelled;

    // Local revocation before terminal publication / external descriptor removal.
    let revoked_token = {
        let mut g = mcp_token.lock().unwrap_or_else(|e| e.into_inner());
        g.take()
    };
    if let Some(token) = revoked_token {
        if let Some(handle) = &mcp {
            handle.revoke(&token);
            if let (Some(att), Some(adapter)) = (attachment.as_ref(), sessions.as_ref()) {
                if let Ok(pending_cfg) = adapter.begin_refresh_mcp(Arc::clone(att), None) {
                    let _ =
                        tokio::time::timeout(Duration::from_millis(200), pending_cfg.completion)
                            .await;
                }
            }
        }
    }

    let result = ActorResult {
        kind: terminal_kind,
        prior: None,
        delivery: EventDeliveryOutcome::Accepted,
        session_key: session_key.clone(),
    };

    finalize_and_cleanup(
        transaction_id,
        channel_id,
        guard,
        event_tx,
        registry,
        release_capacity,
        result,
        terminal_event_delivery_deadline,
        callback_deadline,
    )
    .await;
}

fn map_exchange_failure(f: ExchangeFailure) -> TransactionEndKind {
    match f {
        ExchangeFailure::ChannelOpenFailed => TransactionEndKind::ChannelOpenFailed,
        ExchangeFailure::EncodingFailed => TransactionEndKind::EncodingFailed,
        ExchangeFailure::ConnectorFailed => TransactionEndKind::ConnectorFailed,
        ExchangeFailure::InterpretationFailed => TransactionEndKind::InterpretationFailed,
        ExchangeFailure::Cancelled => TransactionEndKind::Cancelled,
        ExchangeFailure::Terminated => TransactionEndKind::Terminated,
    }
}

fn estimate_unit_bytes(unit: &CanonicalUnitEvent) -> usize {
    match unit.snapshot().unit {
        CanonicalUnit::Text(ref t) => t.content.len().saturating_add(32),
        CanonicalUnit::Tool(ref t) => t
            .request_payload
            .as_ref()
            .map(|p| p.len())
            .unwrap_or(0)
            .saturating_add(64),
        _ => 64,
    }
}

/// Ready tool requests: (action_id, name, payload_json, provider_call_id).
fn collect_ready_tools(
    units: &[CanonicalUnitEvent],
) -> Vec<(monoloop_contracts::ToolActionId, String, String, String)> {
    let mut out = Vec::new();
    for unit in units {
        let snap = unit.snapshot();
        let CanonicalUnit::Tool(tool) = &snap.unit else {
            continue;
        };
        if tool.request_state != ToolRequestState::Ready {
            continue;
        }
        let Some(name) = tool.tool_name.clone() else {
            continue;
        };
        let Some(payload) = tool.request_payload.clone() else {
            continue;
        };
        let provider_id = tool.tool_action_id.as_str().to_string();
        out.push((tool.tool_action_id.clone(), name, payload, provider_id));
    }
    out
}

/// Build continuation context: original input + assistant message with tool calls.
fn build_continuation_context(
    input: &monoloop_contracts::CanonicalInput,
    units: &[CanonicalUnitEvent],
) -> Result<ContinuationContext, ()> {
    let mut text = String::new();
    let mut tool_calls = Vec::new();
    for unit in units {
        let snap = unit.snapshot();
        match &snap.unit {
            CanonicalUnit::Text(t) if t.channel == TextChannel::PublicResponse => {
                text.push_str(&t.content);
                text.push(' ');
            }
            CanonicalUnit::Tool(tool) if tool.request_state == ToolRequestState::Ready => {
                let name = tool.tool_name.as_deref().ok_or(())?;
                let payload = tool.request_payload.as_deref().unwrap_or("{}");
                let args: serde_json::Value =
                    serde_json::from_str(payload).unwrap_or_else(|_| serde_json::json!({}));
                tool_calls.push(CanonicalAssistantToolCall {
                    tool_call_id: tool.tool_action_id.as_str().to_string(),
                    tool_name: ToolName::try_new(name).map_err(|_| ())?,
                    arguments: args,
                });
            }
            _ => {}
        }
    }
    let mut messages = input.messages().to_vec();
    let content = if text.trim().is_empty() {
        Vec::new()
    } else {
        vec![TextPart::try_new(text.trim(), 256 * 1024).map_err(|_| ())?]
    };
    if content.is_empty() && tool_calls.is_empty() {
        return Err(());
    }
    messages.push(CanonicalMessage::Assistant {
        content,
        tool_calls,
    });
    ContinuationContext::try_new(messages).map_err(|_| ())
}

async fn emit_unit_or_session(
    event_tx: &BoundedEventSender,
    guard: &FinalizationGuard,
    transaction_id: TransactionId,
    channel_id: &ChannelId,
    session_key: &Option<SessionKey>,
    payload: TransactionEventPayload,
) -> bool {
    let seq = guard.sequencer().allocate();
    let session_id = session_key
        .as_ref()
        .map(|k| k.session_id.clone())
        .unwrap_or_else(SessionId::generate);
    let event = TransactionEvent {
        transaction_id,
        channel_id: channel_id.clone(),
        session_id,
        sequence: seq,
        payload,
    };
    event_tx.send(QueuedEvent::new(event, None)).await.is_ok()
}

async fn emit_canonical_unit(
    event_tx: &BoundedEventSender,
    guard: &FinalizationGuard,
    transaction_id: TransactionId,
    channel_id: &ChannelId,
    session_key: &Option<SessionKey>,
    unit: CanonicalUnitEvent,
) -> bool {
    emit_unit_or_session(
        event_tx,
        guard,
        transaction_id,
        channel_id,
        session_key,
        TransactionEventPayload::CanonicalUnit(unit),
    )
    .await
}

async fn emit_dispatch_outcome(
    event_tx: &BoundedEventSender,
    guard: &FinalizationGuard,
    transaction_id: TransactionId,
    channel_id: &ChannelId,
    session_key: &Option<SessionKey>,
    outcome: DispatchOutcome,
) -> Result<(), TransactionEndKind> {
    match outcome {
        DispatchOutcome::Canonical { lifecycle, .. } => {
            for ev in lifecycle {
                if !emit_tool_lifecycle(
                    event_tx,
                    guard,
                    transaction_id,
                    channel_id,
                    session_key,
                    ev,
                )
                .await
                {
                    return Err(TransactionEndKind::EventDeliveryFailed);
                }
            }
            Ok(())
        }
        DispatchOutcome::Rejected { lifecycle, .. } => {
            for ev in lifecycle {
                if !emit_tool_lifecycle(
                    event_tx,
                    guard,
                    transaction_id,
                    channel_id,
                    session_key,
                    ev,
                )
                .await
                {
                    return Err(TransactionEndKind::EventDeliveryFailed);
                }
            }
            // Rejected arguments are ordinary tool outcomes; transaction continues.
            Ok(())
        }
        DispatchOutcome::RuntimeFailed { lifecycle, .. } => {
            for ev in lifecycle {
                if !emit_tool_lifecycle(
                    event_tx,
                    guard,
                    transaction_id,
                    channel_id,
                    session_key,
                    ev,
                )
                .await
                {
                    return Err(TransactionEndKind::EventDeliveryFailed);
                }
            }
            Err(TransactionEndKind::ToolExchangeFailed)
        }
    }
}

async fn emit_tool_lifecycle(
    event_tx: &BoundedEventSender,
    guard: &FinalizationGuard,
    transaction_id: TransactionId,
    channel_id: &ChannelId,
    session_key: &Option<SessionKey>,
    lifecycle: ToolLifecycleEvent,
) -> bool {
    emit_unit_or_session(
        event_tx,
        guard,
        transaction_id,
        channel_id,
        session_key,
        TransactionEventPayload::ToolLifecycle(lifecycle),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn finalize_and_cleanup(
    transaction_id: TransactionId,
    channel_id: ChannelId,
    guard: Arc<FinalizationGuard>,
    event_tx: BoundedEventSender,
    registry: Arc<Mutex<ActiveTransactionRegistry>>,
    release_capacity: Arc<dyn Fn() + Send + Sync>,
    result: ActorResult,
    terminal_event_delivery_deadline: Duration,
    callback_deadline: Duration,
) {
    let Some(payload) = guard.try_claim() else {
        release_capacity();
        let mut reg = registry.lock().unwrap_or_else(|e| e.into_inner());
        let _ = reg.remove(&transaction_id);
        return;
    };

    let session_for_event = payload
        .session_id
        .clone()
        .or_else(|| result.session_key.as_ref().map(|k| k.session_id.clone()))
        .unwrap_or_else(SessionId::generate);

    let seq = guard.sequencer().allocate();
    let mut kind = result.kind;
    let mut prior = result.prior;
    let mut delivery = result.delivery;

    let end_preview = build_transaction_end(&payload, kind, prior, delivery, seq);
    let event = TransactionEvent {
        transaction_id: payload.transaction_id,
        channel_id: channel_id.clone(),
        session_id: session_for_event,
        sequence: seq,
        payload: TransactionEventPayload::Ended(end_preview),
    };

    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
    let send_ok = event_tx
        .send(QueuedEvent::new(event, Some(ack_tx)))
        .await
        .is_ok();

    if !send_ok {
        delivery = EventDeliveryOutcome::Failed;
        prior = Some(kind);
        kind = TransactionEndKind::EventDeliveryFailed;
    } else {
        match tokio::time::timeout(terminal_event_delivery_deadline, ack_rx).await {
            Ok(Ok(Ok(()))) => {}
            _ => {
                delivery = EventDeliveryOutcome::Failed;
                prior = Some(kind);
                kind = TransactionEndKind::EventDeliveryFailed;
            }
        }
    }

    let end = build_transaction_end(&payload, kind, prior, delivery, seq);
    drop(event_tx);

    {
        let mut reg = registry.lock().unwrap_or_else(|e| e.into_inner());
        let _ = reg.remove(&transaction_id);
    }
    release_capacity();

    guard.mark_callback_scheduled();
    // D-021: invoke + poll panics must not kill the actor; run on an owned child task.
    invoke_completion_callback(payload.callback, end, callback_deadline).await;
}

/// Run host completion callback with panic + deadline isolation (D-021).
async fn invoke_completion_callback(
    callback: Box<dyn monoloop_contracts::CompletionCallback>,
    end: monoloop_contracts::TransactionEnd,
    deadline: Duration,
) {
    let call_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback.call(end)));
    match call_result {
        Ok(fut) => {
            let handle = tokio::spawn(fut);
            let abort = handle.abort_handle();
            match tokio::time::timeout(deadline, handle).await {
                Ok(Ok(_)) | Ok(Err(_)) => {
                    // Join error = panic inside future; terminal cause unchanged.
                }
                Err(_) => {
                    abort.abort();
                }
            }
        }
        Err(_) => {
            // Callback panicked at invoke; terminal cause already selected.
        }
    }
}
