//! Transaction actor: session establish + one provider exchange + finalization.

use super::active_registry::{ActiveTransactionRegistry, ClaimSessionError, ControlMessage};
use super::callback_service::{CallbackReservation, CallbackService};
use super::dispatcher::{DispatchOutcome, DispatcherLimits, TransactionToolDispatcher};
use super::events::{BoundedEventSender, OrderedEventPublisher};
use super::exchange::{
    run_encoded_exchange, run_exchange, EncodedExchangeParams, ExchangeFailure, ExchangeParams,
};
use super::executor_spawn::try_spawn;
use super::finalization::{build_transaction_end, ClaimedFinalization, FinalizationGuard};
use super::loop_adapters::dispatch_ready_tool_cancellable;
use super::mcp::{CapabilityToken, McpGatewayHandle, PendingMcpBinding};
use super::resolved_tools::ResolvedToolSet;
use super::tool_capacity::SharedToolCapacity;
use monoloop_connector::{Connector, SessionAdapter};
use monoloop_contracts::{
    CanonicalAssistantToolCall, CanonicalMessage, CanonicalToolError, CanonicalToolResult,
    CanonicalUnit, CanonicalUnitEvent, ChannelId, ChannelKind, ContinuationContext,
    ContinuationPolicy, EffectiveConfig, EventDeliveryOutcome, ExchangeId, ExternalSessionId,
    InterpretationLimits, McpConfigurationCapability, McpReachability, OutboundDialectEncoder,
    SessionId, SessionKey, TextChannel, TextPart, ToolActionId, ToolExecutionMode, ToolId,
    ToolLifecycleEvent, ToolName, ToolRequestState, TransactionEndKind, TransactionEventPayload,
    TransactionId,
};
use monoloop_interpreter::InterpreterFactory;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, watch};
// oneshot used for start_gate and create-session claim.

/// Inputs for the transaction actor.
pub struct ActorSpawn {
    /// Injected Tokio handle for runtime-owned child tasks (D-032).
    pub executor: Handle,
    /// Spawn gate closed at shutdown start (D-032).
    pub spawn_gate: super::spawn_gate::SpawnGate,
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
    /// Channel-bound max encoded exchange body (D-015).
    pub max_encoded_exchange_bytes: usize,
    /// Channel-bound max concurrent distinct sessions (D-015).
    pub max_distinct_sessions: usize,
    /// Max diagnostics retained on terminal (D-015).
    pub max_diagnostic_count: usize,
    /// Max bytes per diagnostic message (D-015).
    pub max_diagnostic_bytes: usize,
    /// Runtime-owned completion callback service (D-021).
    pub callbacks: CallbackService,
    /// Admission-reserved callback capacity retained through terminal (D-029).
    pub callback_reservation: CallbackReservation,
    /// Closed only after registry install succeeds (D-009).
    pub start_gate: oneshot::Receiver<()>,
}

/// Spawn the actor task on the injected executor (D-032).
pub fn spawn_actor(spawn: ActorSpawn) -> Result<tokio::task::JoinHandle<()>, ()> {
    let executor = spawn.executor.clone();
    let gate = spawn.spawn_gate.clone();
    try_spawn(&executor, &gate, async move {
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
        executor,
        spawn_gate,
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
        max_encoded_exchange_bytes,
        max_distinct_sessions,
        max_diagnostic_count,
        max_diagnostic_bytes,
        callbacks,
        callback_reservation,
        start_gate,
    } = spawn;
    let _ = (max_diagnostic_count, max_diagnostic_bytes);
    // D-036: sole ordinary event allocator/publisher for this transaction.
    let events = OrderedEventPublisher::new(event_tx.clone(), Arc::clone(guard.sequencer()));
    // D-026: live fan-out waits for authoritative SessionKey before publishing.
    let (session_watch_tx, session_watch_rx) = watch::channel(session_key.clone());

    // D-009: do not perform I/O or callbacks until admission installs us.
    match start_gate.await {
        Ok(()) => {}
        Err(_) => {
            // Drop reservation via Drop; release capacity/registry.
            drop(callback_reservation);
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
                events,
                registry,
                release_capacity,
                result,
                terminal_event_delivery_deadline,
                callback_deadline,
                &callbacks,
                callback_reservation,
            )
            .await;
            return;
        }
    }
    // Join/abort grace for exchange children after cancel or terminal (D-012).
    // Honor the configured cleanup_deadline exactly (no silent minimum floor).

    let mut terminal_kind = TransactionEndKind::Completed;
    let mut attachment: Option<Arc<monoloop_connector::SessionAttachment>> = None;
    let mcp_token: Arc<Mutex<Option<CapabilityToken>>> = Arc::new(Mutex::new(None));
    let mcp_token_work = Arc::clone(&mcp_token);
    // Shared cancel so deadline / delivery-failure can terminate+join in-flight tools
    // instead of dropping the work future mid-dispatch (D-028 residual).
    let tools_cancel = Arc::new(super::sticky_cancel::StickyCancel::new());
    let tools_cancel_work = Arc::clone(&tools_cancel);

    let work = async {
        let tools_cancel = tools_cancel_work;
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
                    match reg.claim_session(
                        transaction_id,
                        key.clone(),
                        Some(max_distinct_sessions),
                    ) {
                        Ok(()) => {}
                        Err(ClaimSessionError::Collision) => {
                            return Err(TransactionEndKind::InvariantFailed);
                        }
                        Err(ClaimSessionError::CapacityExceeded) => {
                            return Err(TransactionEndKind::LimitExceeded);
                        }
                        Err(_) => return Err(TransactionEndKind::InvariantFailed),
                    }
                }
                session_key = Some(key);
                if !emit_unit_or_session(
                    &events,
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
                let _ = session_watch_tx.send(session_key.clone());
                // Load path: activate MCP only after authoritative claim (D-026).
                if let Some(ref pending) = pending_mcp {
                    if mcp_configuration == McpConfigurationCapability::Refreshable {
                        if let (Some(att_ref), Some(adapter)) =
                            (Some(Arc::clone(&att)), sessions.as_ref())
                        {
                            if let Ok(pending_cfg) =
                                adapter.begin_refresh_mcp(att_ref, Some(pending.descriptor.clone()))
                            {
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
                    if let Some(handle) = mcp.as_ref() {
                        handle
                            .activate(&pending.token)
                            .map_err(|_| TransactionEndKind::InvariantFailed)?;
                    }
                }
            }
            // Create: claim + MCP activate after first open returns the provider id (below).
            attachment = Some(att);
        } else if provisional_external {
            // No SessionAdapter: synthetic DirectLlm-style claim only.
            let sid = SessionId::generate();
            let key = SessionKey::new(channel_id.clone(), sid.clone());
            {
                let mut reg = registry.lock().unwrap_or_else(|e| e.into_inner());
                match reg.claim_session(transaction_id, key.clone(), Some(max_distinct_sessions)) {
                    Ok(()) => {}
                    Err(ClaimSessionError::Collision) => {
                        return Err(TransactionEndKind::InvariantFailed);
                    }
                    Err(ClaimSessionError::CapacityExceeded) => {
                        return Err(TransactionEndKind::LimitExceeded);
                    }
                    Err(_) => return Err(TransactionEndKind::InvariantFailed),
                }
            }
            session_key = Some(key);
            let _ = session_watch_tx.send(session_key.clone());
            let ext = ExternalSessionId::try_new(sid.as_str())
                .map_err(|_| TransactionEndKind::InvariantFailed)?;
            if !emit_unit_or_session(
                &events,
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
        // ACP / external-agent encoders reject model tool arrays; MCP carries tools (D-014 residual).
        let encode_tools: &[monoloop_contracts::ToolSpec] =
            if tool_mode == ToolExecutionMode::McpGateway {
                &[]
            } else {
                &tool_specs
            };
        let max_exchanges = max_provider_exchanges.max(1);
        let max_inline = max_continuations;
        let mut exchanges_done = 0usize;
        let mut continuations_done = 0usize;
        let mut provider_input_bytes = 0usize;
        let mut provider_output_bytes = 0usize;
        // D-031: cumulative continuation transcript (original input + each
        // assistant tool-call group and ordered results, appended once).
        let mut continuation_messages = input.messages().to_vec();

        // D-011: fan units to the event sink as they are produced (not only after EOF).
        // D-026: wait for authoritative SessionKey before publishing any unit.
        // D-027: item capacity derived from provider-output byte budget (byte-aware bound).
        let live_cap = (max_total_provider_output_bytes / 256)
            .clamp(8, max_provider_exchanges.saturating_mul(64).max(64));
        let (live_tx, mut live_rx) = mpsc::channel::<CanonicalUnitEvent>(live_cap);
        let events_live = events.clone();
        let channel_live = channel_id.clone();
        let mut session_watch_live = session_watch_rx.clone();
        let live_join = match try_spawn(&executor, &spawn_gate, async move {
            // Block until claim publishes a SessionKey (already Some for load/DirectLlm).
            if session_watch_live
                .wait_for(|sk| sk.is_some())
                .await
                .is_err()
            {
                return Err(TransactionEndKind::EventDeliveryFailed);
            }
            while let Some(unit) = live_rx.recv().await {
                let session_live = session_watch_live.borrow().clone();
                if !emit_canonical_unit(
                    &events_live,
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
        }) {
            Ok(h) => h,
            Err(()) => return Err(TransactionEndKind::InvariantFailed),
        };

        let (session_id_tx, claim_join, prompt_ready_rx) = if create_mode_attach {
            let (sess_tx, sess_rx) = oneshot::channel();
            let (prompt_tx, prompt_rx) = oneshot::channel::<()>();
            let registry_c = Arc::clone(&registry);
            let events_c = events.clone();
            let guard_c = Arc::clone(&guard);
            let channel_c = channel_id.clone();
            let session_watch_c = session_watch_tx.clone();
            let max_distinct = max_distinct_sessions;
            let pending_mcp_c = pending_mcp.clone();
            let mcp_c = mcp.clone();
            let join = match try_spawn(&executor, &spawn_gate, async move {
                let Ok(ext) = sess_rx.await else {
                    return Err(TransactionEndKind::InvariantFailed);
                };
                let sid = SessionId::from_external(&ext);
                let key = SessionKey::new(channel_c.clone(), sid);
                {
                    let mut reg = registry_c.lock().unwrap_or_else(|e| e.into_inner());
                    match reg.claim_session(transaction_id, key.clone(), Some(max_distinct)) {
                        Ok(()) => {}
                        Err(ClaimSessionError::Collision) => {
                            return Err(TransactionEndKind::InvariantFailed);
                        }
                        Err(ClaimSessionError::CapacityExceeded) => {
                            return Err(TransactionEndKind::LimitExceeded);
                        }
                        Err(_) => return Err(TransactionEndKind::InvariantFailed),
                    }
                }
                let claimed = key.clone();
                let sk = Some(key);
                guard_c.set_session_id(claimed.session_id.clone());
                // Publish SessionEstablished before any live units (D-026/D-036).
                if !emit_unit_or_session(
                    &events_c,
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
                let _ = session_watch_c.send(sk);
                // Rebind MCP dispatcher to claimed key, then activate before prompt (D-026).
                if let Some(ref pending) = pending_mcp_c {
                    pending.dispatcher.rebind_session(claimed);
                    if let Some(handle) = mcp_c.as_ref() {
                        handle
                            .activate(&pending.token)
                            .map_err(|_| TransactionEndKind::InvariantFailed)?;
                    }
                }
                let _ = prompt_tx.send(());
                Ok(())
            }) {
                Ok(h) => h,
                Err(()) => {
                    live_join.abort();
                    return Err(TransactionEndKind::InvariantFailed);
                }
            };
            (Some(sess_tx), Some(join), Some(prompt_rx))
        } else {
            (None, None, None)
        };

        let exchange_result = tokio::select! {
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
                executor: &executor,
                spawn_gate: &spawn_gate,
                transaction_id,
                connector: connector.as_ref(),
                encoder: encoder.as_ref(),
                interpreter: interpreter.as_ref(),
                endpoint_ref: &endpoint_ref,
                credential_ref: credential_ref.as_deref(),
                session_attachment: attachment.clone(),
                input: &input,
                config: &effective,
                tools: encode_tools,
                interpretation_limits: InterpretationLimits::default(),
                deadline,
                cleanup_deadline,
                max_encoded_exchange_bytes,
                unit_tx: Some(live_tx),
                session_id_tx,
                prompt_ready_rx,
                // Remaining aggregate output budget for this exchange (D-027).
                max_retained_unit_bytes: max_total_provider_output_bytes
                    .saturating_sub(provider_output_bytes),
                max_remaining_provider_input_bytes: max_total_provider_input_bytes
                    .saturating_sub(provider_input_bytes),
            }) => r,
        };
        let mut outcome = match exchange_result {
            Ok(o) => o,
            Err(e) => {
                // Join claim so typed claim/MCP errors are not lost / remapped (D-026).
                live_join.abort();
                let _ = tokio::time::timeout(cleanup_deadline, live_join).await;
                if let Some(j) = claim_join {
                    match tokio::time::timeout(cleanup_deadline, j).await {
                        Ok(Ok(Ok(()))) => {}
                        Ok(Ok(Err(kind))) => return Err(kind),
                        Ok(Err(_)) | Err(_) => return Err(TransactionEndKind::InvariantFailed),
                    }
                }
                return Err(map_exchange_failure(e));
            }
        };
        exchanges_done += 1;
        // D-027: body already checked against remaining budget before send.
        provider_input_bytes = provider_input_bytes.saturating_add(outcome.encoded_request_bytes);
        if let Some(j) = claim_join {
            j.await.map_err(|_| TransactionEndKind::InvariantFailed)??;
            let Some(ext) = outcome.external_session_id.clone() else {
                return Err(TransactionEndKind::InvariantFailed);
            };
            session_key = Some(SessionKey::new(
                channel_id.clone(),
                SessionId::from_external(&ext),
            ));
        }
        for u in &outcome.units {
            provider_output_bytes = provider_output_bytes.saturating_add(estimate_unit_bytes(u));
        }
        if provider_output_bytes > max_total_provider_output_bytes {
            return Err(TransactionEndKind::LimitExceeded);
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

            // Interpreter units keep provider call IDs; allocate internal action IDs here.
            let ready = collect_ready_tools(outcome.exchange_id, &outcome.units);
            if ready.is_empty() {
                break;
            }

            let sk = session_key
                .clone()
                .ok_or(TransactionEndKind::InvariantFailed)?;

            // Empty allowlist still dispatches so each request gets a correlated rejection.
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
                if tools_cancel.is_cancelled() {
                    return Err(TransactionEndKind::Cancelled);
                }
                // D-028: notify cancel into dispatch so the worker is terminated+joined
                // instead of dropping the dispatch future (which would detach work).
                let tool_cancel_dispatch = Arc::clone(&tools_cancel);
                let dispatch_fut = dispatch_ready_tool_cancellable(
                    &dispatcher,
                    outcome.exchange_id,
                    action_id,
                    &name,
                    &provider_id,
                    ord as u32,
                    &payload,
                    Some(tool_cancel_dispatch),
                );
                tokio::pin!(dispatch_fut);
                let dispatch_outcome = tokio::select! {
                    biased;
                    ctrl = control_rx.recv() => {
                        tools_cancel.cancel();
                        // Join the in-flight dispatch (cancel path terminates worker).
                        let _ = dispatch_fut.await;
                        return Err(match ctrl {
                            Some(ControlMessage::ForceTerminate) => {
                                TransactionEndKind::Terminated
                            }
                            _ => TransactionEndKind::Cancelled,
                        });
                    }
                    _ = tools_cancel.cancelled() => {
                        // Outer deadline / delivery failure: join then exit.
                        let _ = dispatch_fut.await;
                        return Err(TransactionEndKind::Cancelled);
                    }
                    r = &mut dispatch_fut => r,
                };
                if tools_cancel.is_cancelled() {
                    return Err(TransactionEndKind::Cancelled);
                }
                let mut rejection_result: Option<CanonicalToolResult> = None;
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
                        // Ordinary rejections are correlated domain-error results (D-022/D-030).
                        let err = CanonicalToolError::try_new(*code, message.as_str(), None, 256)
                            .unwrap_or_else(|_| {
                                CanonicalToolError::try_new("tool_rejected", "rejected", None, 256)
                                    .expect("static")
                            });
                        let result = CanonicalToolResult {
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
                        };
                        results.push(result.clone());
                        rejection_result = Some(result);
                    }
                    DispatchOutcome::RuntimeFailed { .. } => {}
                }
                emit_dispatch_outcome(
                    &events,
                    transaction_id,
                    &channel_id,
                    &session_key,
                    dispatch_outcome,
                )
                .await?;
                // Rejected dispatcher paths omit Completed; publish before continuation decision.
                if let Some(result) = rejection_result {
                    if !emit_tool_lifecycle(
                        &events,
                        transaction_id,
                        &channel_id,
                        &session_key,
                        ToolLifecycleEvent::Completed { result },
                    )
                    .await
                    {
                        return Err(TransactionEndKind::EventDeliveryFailed);
                    }
                }
            }

            if results.is_empty() {
                // All runtime-failed (ToolExchangeFailed already returned) or no products.
                break;
            }

            match effective.continuation_policy {
                ContinuationPolicy::CallerControlled => {
                    // Results are already on the event stream (Canonical Completed / rejection Completed).
                    return Err(TransactionEndKind::ContinuationRequired);
                }
                ContinuationPolicy::InlineToolContinuation => {
                    if continuations_done >= max_inline || exchanges_done >= max_exchanges {
                        return Err(TransactionEndKind::LimitExceeded);
                    }
                    // Append this exchange's assistant tool-calls + results once (D-031).
                    append_exchange_to_transcript(
                        &mut continuation_messages,
                        &outcome.units,
                        &results,
                    )
                    .map_err(|_| TransactionEndKind::EncodingFailed)?;
                    let context = ContinuationContext::try_new(continuation_messages.clone())
                        .map_err(|_| TransactionEndKind::EncodingFailed)?;
                    // Enforce cumulative context size, not only the newest body.
                    let context_bytes = estimate_input_bytes_from_messages(&continuation_messages);
                    if context_bytes > max_continuation_context_bytes {
                        return Err(TransactionEndKind::LimitExceeded);
                    }
                    let exchange_id = monoloop_contracts::ExchangeId::generate();
                    let encoded = encoder
                        .encode_tool_continuation(
                            monoloop_contracts::ToolContinuationEncodeRequest {
                                transaction_id: &transaction_id,
                                exchange_id: &exchange_id,
                                context: &context,
                                results: &results,
                                config: &effective,
                                tools: encode_tools,
                            },
                        )
                        .map_err(|_| TransactionEndKind::EncodingFailed)?;
                    // D-015: continuation context + channel encoded + provider input aggregates.
                    if encoded.bytes.len() > max_continuation_context_bytes
                        || encoded.bytes.len() > max_encoded_exchange_bytes
                    {
                        return Err(TransactionEndKind::LimitExceeded);
                    }
                    // Do not pre-add here: run_encoded_exchange checks remaining budget
                    // against max_total - provider_input_bytes (D-027; avoid double-count).
                    let live_cap2 = (max_total_provider_output_bytes / 256).clamp(8, 64);
                    let (live_tx2, mut live_rx2) = mpsc::channel::<CanonicalUnitEvent>(live_cap2);
                    let events_live2 = events.clone();
                    let channel_live = channel_id.clone();
                    let session_live = session_key.clone();
                    let live_join2 = match try_spawn(&executor, &spawn_gate, async move {
                        while let Some(unit) = live_rx2.recv().await {
                            if !emit_canonical_unit(
                                &events_live2,
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
                    }) {
                        Ok(h) => h,
                        Err(()) => return Err(TransactionEndKind::InvariantFailed),
                    };
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
                            executor: &executor,
                            spawn_gate: &spawn_gate,
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
                            max_encoded_exchange_bytes,
                            unit_tx: Some(live_tx2),
                            // Remaining aggregate output budget for this continuation (D-027).
                            max_retained_unit_bytes: max_total_provider_output_bytes
                                .saturating_sub(provider_output_bytes),
                            max_remaining_provider_input_bytes: max_total_provider_input_bytes
                                .saturating_sub(provider_input_bytes),
                        }) => r,
                    }
                    .map_err(map_exchange_failure)?;
                    live_join2
                        .await
                        .map_err(|_| TransactionEndKind::InvariantFailed)??;
                    provider_input_bytes =
                        provider_input_bytes.saturating_add(outcome.encoded_request_bytes);
                    for u in &outcome.units {
                        provider_output_bytes =
                            provider_output_bytes.saturating_add(estimate_unit_bytes(u));
                    }
                    if provider_output_bytes > max_total_provider_output_bytes {
                        return Err(TransactionEndKind::LimitExceeded);
                    }
                    exchanges_done += 1;
                    continuations_done += 1;
                }
            }
        }

        Ok::<(), TransactionEndKind>(())
    };

    // Race work against deadline / delivery failure only (control is selected inside work).
    // On forced terminal, cancel in-flight tools and join within cleanup_deadline so
    // workers are not detached by dropping `work` mid-dispatch (D-028 residual).
    // Scoped so the pinned future's borrows end before finalization moves locals.
    {
        tokio::pin!(work);
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
            work_res = &mut work => {
                match work_res {
                    Ok(()) => {
                        terminal_kind = TransactionEndKind::Completed;
                    }
                    Err(k) => terminal_kind = k,
                }
                false
            }
        };
        if cancelled {
            tools_cancel.cancel();
            // Join within cleanup_deadline only. Sticky cancel drives kill+join
            // inside dispatch; do not await unboundedly past the budget (would
            // stall terminal events and callbacks).
            if !cleanup_deadline.is_zero() {
                let _ = tokio::time::timeout(cleanup_deadline, work.as_mut()).await;
            }
            // Dropping `work` after the budget ends any remaining wait.
        }
    }

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
        events,
        registry,
        release_capacity,
        result,
        terminal_event_delivery_deadline,
        callback_deadline,
        &callbacks,
        callback_reservation,
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
        ExchangeFailure::LimitExceeded => TransactionEndKind::LimitExceeded,
        ExchangeFailure::InvariantFailed => TransactionEndKind::InvariantFailed,
        ExchangeFailure::ClaimDeadlineExceeded => TransactionEndKind::DeadlineExceeded,
    }
}

fn estimate_unit_bytes(unit: &CanonicalUnitEvent) -> usize {
    // Keep aggregate output accounting aligned with retained-unit estimation
    // (structure, diagnostics, tool names/results, and envelope identifiers).
    let snap = unit.snapshot();
    let id_bytes = snap.unit_id.as_str().len()
        + snap.interpretation_id.as_str().len()
        + snap.connection_id.as_str().len()
        + snap
            .external_session_id
            .as_ref()
            .map(|s| s.as_str().len())
            .unwrap_or(0)
        + snap.flow_id.as_str().len()
        + snap.lane_id.as_str().len();
    let content = match &snap.unit {
        CanonicalUnit::Text(t) => t.content.len(),
        CanonicalUnit::Structure(s) => s.content.len(),
        CanonicalUnit::Paragraph(_) => 16,
        CanonicalUnit::Tool(t) => t
            .tool_name
            .as_ref()
            .map(|n| n.len())
            .unwrap_or(0)
            .saturating_add(t.request_payload.as_ref().map(|p| p.len()).unwrap_or(0))
            .saturating_add(t.result_payload.as_ref().map(|p| p.len()).unwrap_or(0))
            .saturating_add(t.tool_action_id.as_str().len()),
        CanonicalUnit::Usage(_) => 32,
        CanonicalUnit::Diagnostic(d) => d.message.len().saturating_add(16),
        CanonicalUnit::Boundary(_) => 16,
    };
    id_bytes.saturating_add(content).saturating_add(64)
}

/// Internal action id scoped by exchange so reused provider IDs stay distinct.
fn internal_tool_action_id(exchange_id: ExchangeId, provider_call_id: &str) -> ToolActionId {
    ToolActionId::new(format!("{}:{provider_call_id}", exchange_id.as_uuid()))
}

/// Ready tool requests: (internal_action_id, name, payload_json, provider_call_id).
///
/// Interpreter fragments keep the provider call ID on `ToolActionEvent.tool_action_id`;
/// the Loop allocates a distinct internal id for dispatch/lifecycle correlation.
fn collect_ready_tools(
    exchange_id: ExchangeId,
    units: &[CanonicalUnitEvent],
) -> Vec<(ToolActionId, String, String, String)> {
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
        let action_id = internal_tool_action_id(exchange_id, &provider_id);
        out.push((action_id, name, payload, provider_id));
    }
    out
}

/// Append one exchange's assistant tool-call group and ordered tool results (D-031).
fn append_exchange_to_transcript(
    messages: &mut Vec<CanonicalMessage>,
    units: &[CanonicalUnitEvent],
    results: &[CanonicalToolResult],
) -> Result<(), ()> {
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
                // Provider call ID stays on the wire for correlation (D-030/D-031).
                tool_calls.push(CanonicalAssistantToolCall {
                    tool_call_id: tool.tool_action_id.as_str().to_string(),
                    tool_name: ToolName::try_new(name).map_err(|_| ())?,
                    arguments: args,
                });
            }
            _ => {}
        }
    }
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
    for result in results {
        let body = match &result.outcome {
            monoloop_contracts::CanonicalToolResultOutcome::Succeeded(out) => match out {
                monoloop_contracts::CanonicalToolOutput::Text(t) => t.clone(),
                monoloop_contracts::CanonicalToolOutput::Json(v) => v.to_string(),
            },
            monoloop_contracts::CanonicalToolResultOutcome::DomainFailed(err) => {
                err.message.clone()
            }
        };
        let part = TextPart::try_new(if body.is_empty() { "ok" } else { &body }, 256 * 1024)
            .map_err(|_| ())?;
        messages.push(CanonicalMessage::Tool {
            tool_call_id: result.provider_tool_call_id.clone(),
            content: vec![part],
        });
    }
    Ok(())
}

fn estimate_input_bytes_from_messages(messages: &[CanonicalMessage]) -> usize {
    // Mirror admission `estimate_input_bytes` without re-validating CanonicalInput.
    messages
        .iter()
        .map(|m| match m {
            CanonicalMessage::System { content, name }
            | CanonicalMessage::User { content, name } => {
                content.iter().map(|p| p.text().len()).sum::<usize>()
                    + name.as_ref().map(|n| n.len()).unwrap_or(0)
            }
            CanonicalMessage::Assistant {
                content,
                tool_calls,
            } => {
                content.iter().map(|p| p.text().len()).sum::<usize>()
                    + tool_calls
                        .iter()
                        .map(|c| {
                            let args = serde_json::to_vec(&c.arguments)
                                .map(|b| b.len())
                                .unwrap_or(usize::MAX / 4);
                            c.tool_call_id
                                .len()
                                .saturating_add(c.tool_name.as_str().len())
                                .saturating_add(args)
                        })
                        .sum::<usize>()
            }
            CanonicalMessage::Tool {
                tool_call_id,
                content,
            } => tool_call_id.len() + content.iter().map(|p| p.text().len()).sum::<usize>(),
        })
        .sum()
}

async fn emit_unit_or_session(
    events: &OrderedEventPublisher,
    transaction_id: TransactionId,
    channel_id: &ChannelId,
    session_key: &Option<SessionKey>,
    payload: TransactionEventPayload,
) -> bool {
    // D-026: never invent a random SessionId for ordinary events. Unclaimed
    // create paths must claim before publishing (SessionEstablished first).
    let Some(session_id) = session_key.as_ref().map(|k| k.session_id.clone()) else {
        return false;
    };
    events
        .publish(transaction_id, channel_id.clone(), session_id, payload)
        .await
        .is_ok()
}

async fn emit_canonical_unit(
    events: &OrderedEventPublisher,
    transaction_id: TransactionId,
    channel_id: &ChannelId,
    session_key: &Option<SessionKey>,
    unit: CanonicalUnitEvent,
) -> bool {
    emit_unit_or_session(
        events,
        transaction_id,
        channel_id,
        session_key,
        TransactionEventPayload::CanonicalUnit(unit),
    )
    .await
}

async fn emit_dispatch_outcome(
    events: &OrderedEventPublisher,
    transaction_id: TransactionId,
    channel_id: &ChannelId,
    session_key: &Option<SessionKey>,
    outcome: DispatchOutcome,
) -> Result<(), TransactionEndKind> {
    match outcome {
        DispatchOutcome::Canonical { lifecycle, .. } => {
            for ev in lifecycle {
                if !emit_tool_lifecycle(events, transaction_id, channel_id, session_key, ev).await {
                    return Err(TransactionEndKind::EventDeliveryFailed);
                }
            }
            Ok(())
        }
        DispatchOutcome::Rejected { lifecycle, .. } => {
            for ev in lifecycle {
                if !emit_tool_lifecycle(events, transaction_id, channel_id, session_key, ev).await {
                    return Err(TransactionEndKind::EventDeliveryFailed);
                }
            }
            // Rejected arguments are ordinary tool outcomes; transaction continues.
            Ok(())
        }
        DispatchOutcome::RuntimeFailed { lifecycle, .. } => {
            for ev in lifecycle {
                if !emit_tool_lifecycle(events, transaction_id, channel_id, session_key, ev).await {
                    return Err(TransactionEndKind::EventDeliveryFailed);
                }
            }
            Err(TransactionEndKind::ToolExchangeFailed)
        }
    }
}

async fn emit_tool_lifecycle(
    events: &OrderedEventPublisher,
    transaction_id: TransactionId,
    channel_id: &ChannelId,
    session_key: &Option<SessionKey>,
    lifecycle: ToolLifecycleEvent,
) -> bool {
    emit_unit_or_session(
        events,
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
    events: OrderedEventPublisher,
    registry: Arc<Mutex<ActiveTransactionRegistry>>,
    release_capacity: Arc<dyn Fn() + Send + Sync>,
    result: ActorResult,
    terminal_event_delivery_deadline: Duration,
    callback_deadline: Duration,
    callbacks: &CallbackService,
    callback_reservation: CallbackReservation,
) {
    let Some(payload) = guard.try_claim() else {
        drop(callback_reservation);
        release_capacity();
        let mut reg = registry.lock().unwrap_or_else(|e| e.into_inner());
        let _ = reg.remove(&transaction_id);
        return;
    };
    // If this future is aborted mid-finalize, restore payload for the supervisor.
    let claimed = ClaimedFinalization::new(Arc::clone(&guard), payload);

    let session_for_event = claimed
        .payload()
        .session_id
        .clone()
        .or_else(|| result.session_key.as_ref().map(|k| k.session_id.clone()))
        .unwrap_or_else(SessionId::generate);

    let mut kind = result.kind;
    let mut prior = result.prior;
    let mut delivery = result.delivery;

    let seq_preview = events.sequencer().peek_next();
    let end_preview = build_transaction_end(claimed.payload(), kind, prior, delivery, seq_preview);
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
    // Bound enqueue+ack by the terminal delivery deadline so a stuck sink cannot
    // hold a claimed finalization forever (blocks shutdown supervisor claim).
    let send_ok = matches!(
        tokio::time::timeout(
            terminal_event_delivery_deadline,
            events.publish_terminal(
                claimed.payload().transaction_id,
                channel_id.clone(),
                session_for_event,
                TransactionEventPayload::Ended(end_preview),
                ack_tx,
            ),
        )
        .await,
        Ok(Ok(_))
    );
    let seq = if send_ok {
        events.sequencer().last_allocated()
    } else {
        seq_preview
    };

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

    let end = build_transaction_end(claimed.payload(), kind, prior, delivery, seq);
    drop(events);

    {
        let mut reg = registry.lock().unwrap_or_else(|e| e.into_inner());
        let _ = reg.remove(&transaction_id);
    }
    release_capacity();

    let payload = claimed.take();
    guard.mark_callback_scheduled();
    // D-021 / D-029: schedule with admission-reserved capacity; actor does not await.
    callbacks.schedule_reserved(
        callback_reservation,
        payload.callback,
        end,
        Some(callback_deadline),
    );
}
