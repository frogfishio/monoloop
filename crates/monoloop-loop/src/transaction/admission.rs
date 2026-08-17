//! Synchronous admission: validate, reserve, install, spawn (no I/O).

use super::active_registry::{ActiveTransaction, ActiveTransactionRegistry, ControlMessage};
use super::actor::{spawn_actor, ActorSpawn};
use super::capacity::CapacityManagers;
use super::channel_registry::LiveChannel;
use super::events::spawn_delivery_task;
use super::finalization::{EventSequencer, FinalizationGuard};
use super::host_tools::HostToolRegistry;
use super::mcp::McpGatewayHandle;
use super::resolved_tools::ResolvedToolSet;
use monoloop_contracts::{
    merge_effective_config, AdmissionError, AdmissionErrorKind, AdmissionReceipt, ChannelId,
    ChannelKind, ConfigOption, ExtensionLimits, OptionPolicy, SessionId, SessionKey, TransactionId,
    TransactionLimits, TransactionRequest,
};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

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

    let resolved = resolve_tools(&ctx.tools, &request.tools)?;

    let policy = liberal_option_policy();
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
    let (session_id, session_key, provisional_external) =
        allocate_session(live.binding.kind, &request.channel_id, request.session_id.clone())?;

    if let Some(ref sk) = session_key {
        let reg = ctx.registry.lock().unwrap_or_else(|e| e.into_inner());
        if reg.session_active(sk) {
            return Err(AdmissionError::new(
                AdmissionErrorKind::SessionAlreadyActive,
                "session already has an active transaction",
            ));
        }
    }

    if !ctx.capacity.try_reserve(&request.channel_id) {
        return Err(AdmissionError::new(
            AdmissionErrorKind::CapacityExceeded,
            "active transaction capacity exceeded",
        ));
    }

    let channel_for_release = request.channel_id.clone();
    let capacity = Arc::clone(&ctx.capacity);
    // Once-only: actor finalize and shutdown supervisor may both observe the entry.
    let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let release_capacity: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
        if released.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        capacity.release(&channel_for_release);
    });

    let (control_tx, control_rx) = mpsc::channel::<ControlMessage>(1);
    let event_cap = ctx.limits.max_event_queue.max(1);
    let (event_tx, event_rx) = mpsc::channel(event_cap);
    let (fail_tx, fail_rx) = mpsc::channel::<()>(1);

    let sequencer = Arc::new(EventSequencer::new());
    let guard = FinalizationGuard::new(
        transaction_id,
        request.channel_id.clone(),
        session_id.clone(),
        request.completion,
        Arc::clone(&sequencer),
    );

    let delivery_join = spawn_delivery_task(event_rx, request.events, fail_tx);

    let actor_join = spawn_actor(ActorSpawn {
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
    });

    let reaper = tokio::spawn(async move {
        let _ = actor_join.await;
        delivery_join.abort();
    });

    let entry = ActiveTransaction {
        transaction_id,
        session_key,
        channel_id: request.channel_id.clone(),
        guard,
        control_tx,
        actor_join: reaper,
        release_capacity: Arc::clone(&release_capacity),
    };

    {
        let mut reg = ctx.registry.lock().unwrap_or_else(|e| e.into_inner());
        if let Err(kind) = reg.insert(entry) {
            release_capacity();
            return Err(AdmissionError::new(kind, "registry install failed"));
        }
    }

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

fn liberal_option_policy() -> OptionPolicy {
    let mut p = OptionPolicy::default();
    p.supported_invocation.extend([
        ConfigOption::Model,
        ConfigOption::Temperature,
        ConfigOption::ReasoningEffort,
        ConfigOption::MaxOutputTokens,
        ConfigOption::Stop,
        ConfigOption::ResponseFormat,
        ConfigOption::ContinuationPolicy,
        ConfigOption::Deadline,
        ConfigOption::Extensions,
    ]);
    p
}
