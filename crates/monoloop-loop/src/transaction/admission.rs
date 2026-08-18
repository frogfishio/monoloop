//! Synchronous admission: validate, reserve, install, then start (no I/O).
//!
//! Normative order (D-009): SessionKey + capacity reserve → create resources →
//! install active entry while Accepting → only then start actor/delivery work.

use super::active_registry::{ActiveTransaction, ActiveTransactionRegistry, ControlMessage};
use super::actor::{spawn_actor, ActorSpawn};
use super::callback_service::CallbackService;
use super::capacity::CapacityManagers;
use super::channel_registry::LiveChannel;
use super::events::{spawn_delivery_task, BoundedEventSender};
use super::executor_spawn::try_spawn;
use super::finalization::{EventSequencer, FinalizationGuard};
use super::host_tools::HostToolRegistry;
use super::mcp::McpGatewayHandle;
use super::resolved_tools::ResolvedToolSet;
use monoloop_contracts::{
    merge_effective_config, AdmissionError, AdmissionErrorKind, AdmissionReceipt, ChannelId,
    ChannelKind, ExtensionLimits, McpConfigurationCapability, OptionPolicy, SessionId, SessionKey,
    ToolExecutionMode, TransactionId, TransactionLimits, TransactionRequest,
};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};

/// Runtime state constants (must match `runtime.rs`).
pub const STATE_ACCEPTING: u8 = 1;

/// Shared state used by admission.
pub struct AdmissionContext {
    /// Live channels (shared with runtime).
    pub channels: Arc<HashMap<ChannelId, LiveChannel>>,
    /// Host tools.
    pub tools: HostToolRegistry,
    /// Capacity.
    pub capacity: Arc<CapacityManagers>,
    /// Active registry.
    pub registry: Arc<Mutex<ActiveTransactionRegistry>>,
    /// Transaction limits.
    pub limits: TransactionLimits,
    /// Optional MCP gateway handle (loopback listener).
    pub mcp: Option<McpGatewayHandle>,
    /// Shared runtime lifecycle atomic (Accepting / Draining / Stopped).
    pub runtime_state: Arc<AtomicU8>,
    /// Runtime-owned completion callbacks (D-021).
    pub callbacks: CallbackService,
    /// Injected Tokio handle for all runtime-owned spawns (D-032).
    pub executor: Handle,
}

/// Perform synchronous admission (no network/tool I/O).
pub fn admit(
    ctx: &AdmissionContext,
    request: TransactionRequest,
) -> Result<AdmissionReceipt, AdmissionError> {
    if request.tools.len() > ctx.limits.max_tools_per_transaction {
        return Err(AdmissionError::new(
            AdmissionErrorKind::InvalidInput,
            "too many tools on request",
        ));
    }

    let live = ctx.channels.get(&request.channel_id).ok_or_else(|| {
        AdmissionError::new(AdmissionErrorKind::UnknownChannel, "unknown channel")
    })?;

    if live.binding.kind == ChannelKind::DirectLlm && request.session_config.is_some() {
        return Err(AdmissionError::new(
            AdmissionErrorKind::InvalidConfiguration,
            "DirectLlm rejects SessionConfig",
        ));
    }

    if !live
        .binding
        .capabilities
        .continuation_policies
        .contains(&request.invocation_config.continuation_policy)
    {
        return Err(AdmissionError::new(
            AdmissionErrorKind::CapabilityMismatch,
            "continuation policy not supported by channel",
        ));
    }

    // D-014: CreationOnly + existing session + tools → fail at admission (no callback).
    if live.binding.tool_mode == ToolExecutionMode::McpGateway
        && live.binding.capabilities.mcp_configuration == McpConfigurationCapability::CreationOnly
        && request.session_id.is_some()
        && !request.tools.is_empty()
    {
        return Err(AdmissionError::new(
            AdmissionErrorKind::CapabilityMismatch,
            "CreationOnly MCP cannot refresh tools on an existing session",
        ));
    }

    let resolved = resolve_tools(&ctx.tools, &request.tools)?;

    // D-015: enforce aggregate input bound at admission.
    let input_bytes = estimate_input_bytes(&request.input);
    if input_bytes > ctx.limits.max_input_bytes {
        return Err(AdmissionError::new(
            AdmissionErrorKind::InvalidInput,
            "canonical input exceeds max_input_bytes",
        ));
    }
    if request.input.messages().len() > ctx.limits.max_messages {
        return Err(AdmissionError::new(
            AdmissionErrorKind::InvalidInput,
            "canonical input exceeds max_messages",
        ));
    }
    for msg in request.input.messages() {
        let parts = match msg {
            monoloop_contracts::CanonicalMessage::System { content, .. }
            | monoloop_contracts::CanonicalMessage::User { content, .. }
            | monoloop_contracts::CanonicalMessage::Assistant { content, .. }
            | monoloop_contracts::CanonicalMessage::Tool { content, .. } => content.len(),
        };
        if parts > ctx.limits.max_content_parts {
            return Err(AdmissionError::new(
                AdmissionErrorKind::InvalidInput,
                "canonical message exceeds max_content_parts",
            ));
        }
    }
    // Tool schema aggregate for selected tools.
    let mut schema_total = 0usize;
    for spec in resolved.specs() {
        schema_total = schema_total.saturating_add(
            serde_json::to_vec(spec.input_schema.as_value())
                .map(|b| b.len())
                .unwrap_or(0),
        );
    }
    if schema_total > ctx.limits.max_tool_schema_bytes {
        return Err(AdmissionError::new(
            AdmissionErrorKind::InvalidInput,
            "selected tool schemas exceed max_tool_schema_bytes",
        ));
    }

    // D-023: Channel-declared option policy is authoritative (not a liberal hard-code).
    let policy = channel_option_policy(&live.binding);
    let effective = merge_effective_config(
        &live.binding.defaults,
        request.session_config.as_ref(),
        None,
        &request.invocation_config,
        &policy,
        &ExtensionLimits::default(),
    )
    .map_err(|e| {
        AdmissionError::new(
            AdmissionErrorKind::InvalidConfiguration,
            format!("config merge failed: {e}"),
        )
    })?;

    let transaction_id = TransactionId::generate();
    let existing_session = request.session_id.is_some();
    let (session_id, session_key, provisional_external) = allocate_session(
        live.binding.kind,
        &request.channel_id,
        request.session_id.clone(),
    )?;

    // Fast path session check (still re-checked under registry lock).
    if let Some(ref sk) = session_key {
        let reg = ctx.registry.lock().unwrap_or_else(|e| e.into_inner());
        if reg.session_active(sk) {
            return Err(AdmissionError::new(
                AdmissionErrorKind::SessionAlreadyActive,
                "session already has an active transaction",
            ));
        }
    }

    if ctx.runtime_state.load(Ordering::SeqCst) != STATE_ACCEPTING {
        return Err(AdmissionError::new(
            AdmissionErrorKind::RuntimeShuttingDown,
            "runtime is not accepting submissions",
        ));
    }

    if !ctx.capacity.try_reserve(&request.channel_id) {
        return Err(AdmissionError::new(
            AdmissionErrorKind::CapacityExceeded,
            "active transaction capacity exceeded",
        ));
    }

    // D-029: reserve callback capacity at admission; retain through callback terminal.
    let callback_reservation = match ctx.callbacks.try_reserve() {
        Some(r) => r,
        None => {
            ctx.capacity.release(&request.channel_id);
            return Err(AdmissionError::new(
                AdmissionErrorKind::CapacityExceeded,
                "callback capacity exceeded",
            ));
        }
    };

    let channel_for_release = request.channel_id.clone();
    let capacity = Arc::clone(&ctx.capacity);
    let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let release_capacity: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
        if released.swap(true, Ordering::SeqCst) {
            return;
        }
        capacity.release(&channel_for_release);
    });

    // Control is cancel/force only; capacity is still taken from max_actor_commands (D-015).
    let control_cap = ctx.limits.max_actor_commands.max(1);
    let (control_tx, control_rx) = mpsc::channel::<ControlMessage>(control_cap);
    let event_cap = ctx.limits.max_event_queue.max(1);
    let (raw_event_tx, event_rx) = mpsc::channel(event_cap);
    let event_tx = BoundedEventSender::new(raw_event_tx, ctx.limits.max_event_queue_bytes);
    let byte_counter = event_tx.byte_counter();
    let (fail_tx, fail_rx) = mpsc::channel::<()>(1);
    // D-009: actor must not run work until install succeeds.
    let (start_tx, start_rx) = oneshot::channel::<()>();

    let sequencer = Arc::new(EventSequencer::new());
    let guard = FinalizationGuard::new(
        transaction_id,
        request.channel_id.clone(),
        session_id.clone(),
        request.completion,
        Arc::clone(&sequencer),
    );

    let delivery_join = match spawn_delivery_task(
        &ctx.executor,
        event_rx,
        request.events,
        fail_tx,
        byte_counter,
        ctx.limits.terminal_event_delivery_deadline,
    ) {
        Ok(h) => h,
        Err(()) => {
            release_capacity();
            drop(callback_reservation);
            return Err(AdmissionError::new(
                AdmissionErrorKind::RuntimeShuttingDown,
                "executor unavailable for delivery task",
            ));
        }
    };

    let actor_join = match spawn_actor(ActorSpawn {
        executor: ctx.executor.clone(),
        transaction_id,
        channel_id: request.channel_id.clone(),
        channel_kind: live.binding.kind,
        tool_mode: live.binding.tool_mode,
        mcp_configuration: live.binding.capabilities.mcp_configuration,
        mcp_reachability: live.binding.capabilities.mcp_reachability,
        mcp: ctx.mcp.clone(),
        session_key: session_key.clone(),
        provisional_external,
        existing_session,
        sessions: live.instance.sessions.clone(),
        connector: Arc::clone(&live.instance.connector),
        encoder: Arc::clone(&live.binding.encoder),
        interpreter: Arc::clone(&live.binding.interpreter),
        endpoint_ref: live.binding.endpoint_ref.clone(),
        credential_ref: live.binding.credential_ref.clone(),
        input: request.input,
        effective,
        tools: resolved,
        guard: Arc::clone(&guard),
        control_rx,
        event_tx,
        delivery_fail_rx: fail_rx,
        registry: Arc::clone(&ctx.registry),
        release_capacity: Arc::clone(&release_capacity),
        deadline: request
            .invocation_config
            .deadline
            .unwrap_or(ctx.limits.transaction_deadline),
        max_continuations: ctx.limits.max_continuations,
        max_provider_exchanges: ctx.limits.max_provider_exchanges,
        max_concurrent_tools: ctx.limits.max_concurrent_tools_per_transaction,
        max_queued_tools: ctx.limits.max_queued_tools_per_transaction,
        cleanup_deadline: ctx.limits.cleanup_deadline,
        terminal_event_delivery_deadline: ctx.limits.terminal_event_delivery_deadline,
        callback_deadline: ctx.limits.callback_deadline,
        max_total_provider_input_bytes: ctx.limits.max_total_provider_input_bytes,
        max_total_provider_output_bytes: ctx.limits.max_total_provider_output_bytes,
        max_continuation_context_bytes: ctx.limits.max_continuation_context_bytes,
        max_tool_schema_bytes: ctx.limits.max_tool_schema_bytes,
        max_tool_payload_bytes: ctx.limits.max_tool_payload_bytes,
        max_tool_output_bytes: ctx.limits.max_tool_output_bytes,
        max_encoded_exchange_bytes: live.binding.limits.max_encoded_exchange_bytes,
        max_distinct_sessions: live.binding.limits.max_distinct_sessions,
        max_diagnostic_count: ctx.limits.max_diagnostic_count,
        max_diagnostic_bytes: ctx.limits.max_diagnostic_bytes,
        callbacks: ctx.callbacks.clone(),
        callback_reservation,
        start_gate: start_rx,
    }) {
        Ok(h) => h,
        Err(()) => {
            delivery_join.abort();
            release_capacity();
            return Err(AdmissionError::new(
                AdmissionErrorKind::RuntimeShuttingDown,
                "executor unavailable for actor task",
            ));
        }
    };

    // D-009 + D-010: Accepting check + session claim + insert under one lock.
    // Defer reaper wrap until install succeeds so failure can abort owned tasks (LAW 23).
    {
        let mut reg = ctx.registry.lock().unwrap_or_else(|e| e.into_inner());
        if ctx.runtime_state.load(Ordering::SeqCst) != STATE_ACCEPTING {
            drop(reg);
            release_capacity();
            drop(start_tx);
            actor_join.abort();
            delivery_join.abort();
            return Err(AdmissionError::new(
                AdmissionErrorKind::RuntimeShuttingDown,
                "runtime is not accepting submissions",
            ));
        }
        // Pre-check distinct session / collision without moving joins into a dropped entry.
        if let Some(ref sk) = session_key {
            if reg.session_active(sk) {
                drop(reg);
                release_capacity();
                drop(start_tx);
                actor_join.abort();
                delivery_join.abort();
                return Err(AdmissionError::new(
                    AdmissionErrorKind::SessionAlreadyActive,
                    "session already has an active transaction",
                ));
            }
            let max = live.binding.limits.max_distinct_sessions;
            if reg.distinct_sessions_on_channel(&sk.channel_id) >= max {
                drop(reg);
                release_capacity();
                drop(start_tx);
                actor_join.abort();
                delivery_join.abort();
                return Err(AdmissionError::new(
                    AdmissionErrorKind::CapacityExceeded,
                    "channel distinct session capacity exceeded",
                ));
            }
        }

        let actor_abort = actor_join.abort_handle();
        let delivery_abort = delivery_join.abort_handle();
        let reaper = match try_spawn(&ctx.executor, async move {
            let _ = actor_join.await;
            delivery_join.abort();
            let _ = delivery_join.await;
        }) {
            Ok(h) => h,
            Err(()) => {
                drop(reg);
                release_capacity();
                drop(start_tx);
                actor_abort.abort();
                delivery_abort.abort();
                return Err(AdmissionError::new(
                    AdmissionErrorKind::RuntimeShuttingDown,
                    "executor unavailable for reaper task",
                ));
            }
        };
        let entry = ActiveTransaction {
            transaction_id,
            session_key,
            channel_id: request.channel_id.clone(),
            guard,
            control_tx,
            actor_join: reaper,
            release_capacity: Arc::clone(&release_capacity),
        };
        if let Err((kind, failed)) =
            reg.insert(entry, Some(live.binding.limits.max_distinct_sessions))
        {
            drop(reg);
            release_capacity();
            drop(start_tx);
            // Abort owned reaper (covers actor + delivery) — sync admit cannot await.
            failed.actor_join.abort();
            return Err(AdmissionError::new(kind, "registry install failed"));
        }
    }

    // Start work only after a successful install.
    let _ = start_tx.send(());

    Ok(AdmissionReceipt {
        transaction_id,
        session_id,
    })
}

fn resolve_tools(
    host: &HostToolRegistry,
    tools: &[monoloop_contracts::ToolId],
) -> Result<ResolvedToolSet, AdmissionError> {
    if tools.is_empty() {
        return Ok(ResolvedToolSet::empty());
    }
    let mut seen = HashSet::new();
    let mut registered = Vec::with_capacity(tools.len());
    for id in tools {
        if !seen.insert(id.clone()) {
            return Err(AdmissionError::new(
                AdmissionErrorKind::DuplicateTool,
                "duplicate tool id on request",
            ));
        }
        let tool = host.get(id).ok_or_else(|| {
            AdmissionError::new(AdmissionErrorKind::UnknownTool, "unknown tool id")
        })?;
        registered.push(tool.clone());
    }
    Ok(ResolvedToolSet::from_registered(registered))
}

fn allocate_session(
    kind: ChannelKind,
    channel_id: &ChannelId,
    supplied: Option<SessionId>,
) -> Result<(Option<SessionId>, Option<SessionKey>, bool), AdmissionError> {
    match kind {
        ChannelKind::DirectLlm => {
            let sid = supplied.unwrap_or_else(SessionId::generate);
            let key = SessionKey::new(channel_id.clone(), sid.clone());
            Ok((Some(sid), Some(key), false))
        }
        ChannelKind::ExternalAgent => {
            if let Some(sid) = supplied {
                let key = SessionKey::new(channel_id.clone(), sid.clone());
                Ok((Some(sid), Some(key), false))
            } else {
                Ok((None, None, true))
            }
        }
    }
}

fn channel_option_policy(binding: &super::channel_registry::ChannelBinding) -> OptionPolicy {
    let mut p = binding.capabilities.option_policy.clone();
    // Default extension keys declared on the Channel are always permitted.
    p.allowed_extension_keys
        .extend(binding.defaults.extensions.keys().cloned());
    p
}

/// Deterministic canonical-input byte estimate covering every counted field (D-035).
///
/// Counts text parts, optional message names, tool-call IDs/names/arguments JSON,
/// and Tool-message correlation IDs. Serialization failure for arguments fails closed
/// by counting the configured argument as oversized rather than zero.
pub(crate) fn estimate_input_bytes(input: &monoloop_contracts::CanonicalInput) -> usize {
    input.messages().iter().map(estimate_message_bytes).sum()
}

fn estimate_message_bytes(m: &monoloop_contracts::CanonicalMessage) -> usize {
    match m {
        monoloop_contracts::CanonicalMessage::System { content, name }
        | monoloop_contracts::CanonicalMessage::User { content, name } => {
            content.iter().map(|p| p.text().len()).sum::<usize>()
                + name.as_ref().map(|n| n.len()).unwrap_or(0)
        }
        monoloop_contracts::CanonicalMessage::Assistant {
            content,
            tool_calls,
        } => {
            content.iter().map(|p| p.text().len()).sum::<usize>()
                + tool_calls
                    .iter()
                    .map(estimate_tool_call_bytes)
                    .sum::<usize>()
        }
        monoloop_contracts::CanonicalMessage::Tool {
            tool_call_id,
            content,
        } => tool_call_id.len() + content.iter().map(|p| p.text().len()).sum::<usize>(),
    }
}

fn estimate_tool_call_bytes(c: &monoloop_contracts::CanonicalAssistantToolCall) -> usize {
    let args_bytes = match serde_json::to_vec(&c.arguments) {
        Ok(b) => b.len(),
        // Fail closed: treat unserializable args as at least one byte so they
        // cannot bypass max_input_bytes via a zero fallback (D-035).
        Err(_) => usize::MAX / 4,
    };
    c.tool_call_id
        .len()
        .saturating_add(c.tool_name.as_str().len())
        .saturating_add(args_bytes)
}
