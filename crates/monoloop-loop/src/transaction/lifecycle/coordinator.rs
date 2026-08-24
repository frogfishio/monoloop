//! Per-transaction coordinator (v2 §11) — supervised exchange + Loop tools.

use super::event_publisher::{EventPublisherCommand, OrdinaryCmdAdmit};
use super::exchange::{
    run_direct_llm_continuation, run_direct_llm_exchange, DirectExchangeOutcome,
    PromptProceedError, PromptReadyGate,
};
use super::ledger::LedgerInsertError;
use super::loop_dispatch::{
    needs_loop_dispatch, run_supervised_empty_loop, run_supervised_tool_loop,
    scope_tool_units_for_exchange, LoopDispatchError, LoopDispatchReport,
};
use super::session_identity::{provider_tool_call_id_from_action, session_key_for};
use super::supervisor::RuntimeShared;
use super::task_spawner::TransactionTaskSpawner;
use super::terminal::TerminalProposal;
use crate::transaction::channel_registry::LiveChannel;
use crate::transaction::dispatcher::{OrphanToolPermitSet, TransactionToolDispatcher};
use crate::transaction::host_tools::HostToolRegistry;
use crate::transaction::loop_adapters::{HostToolRuntime, ResolvedToolRegistry};
use crate::transaction::mcp::{CapabilityToken, McpGatewayHandle};
use crate::transaction::resolved_tools::ResolvedToolSet;
use crate::transaction::sticky_cancel::StickyCancel;
use crate::transaction::tool_capacity::SharedToolCapacity;
use monoloop_connector::{Connector, SessionAttachRequest};
use monoloop_contracts::{
    merge_effective_config, CanonicalAssistantToolCall, CanonicalInput, CanonicalMessage,
    CanonicalToolOutput, CanonicalToolResult, CanonicalToolResultOutcome, CanonicalUnit,
    CanonicalUnitEvent, ChannelId, ChannelKind, ContinuationContext, ContinuationPolicy,
    EffectiveConfig, ExchangeId, ExtensionLimits, InputLimits, InvocationConfig,
    McpConfigurationCapability, OutboundDialectEncoder, SessionConfig, SessionId, SessionKey,
    TextPart, ToolExecutionMode, ToolId, ToolName, ToolRequestState, ToolSpec, TransactionEndKind,
    TransactionEventPayload, TransactionId,
};
use monoloop_interpreter::InterpreterFactory;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

/// DirectLlm pieces needed for bounded inline tool continuation rounds.
struct InlineContinuationDeps<'a> {
    input: &'a CanonicalInput,
    config: &'a EffectiveConfig,
    connector: &'a dyn Connector,
    encoder: &'a dyn OutboundDialectEncoder,
    interpreter: &'a dyn InterpreterFactory,
    endpoint_ref: &'a str,
    credential_ref: Option<&'a str>,
    tool_specs: &'a [ToolSpec],
    deadline: Duration,
    cleanup_deadline: Duration,
    max_encoded: usize,
    max_retained: usize,
    max_continuations: usize,
    max_continuation_context_bytes: usize,
    max_provider_exchanges: usize,
    max_total_provider_input_bytes: usize,
    max_total_provider_output_bytes: usize,
    /// Exchanges already completed (includes the initial tool exchange).
    exchanges_used: usize,
    /// Cumulative encoded provider request bytes so far.
    provider_input_used: usize,
    /// Cumulative raw provider output bytes so far.
    provider_output_used: usize,
    /// Tool dispatcher caps from `TransactionLimits`.
    dispatcher_limits: crate::transaction::DispatcherLimits,
}

/// Message from coordinator workers to the supervisor.
#[derive(Debug)]
pub enum WorkerMessage {
    /// Coordinator finished with a terminal proposal.
    WorkerExited {
        /// Transaction.
        transaction_id: TransactionId,
        /// Proposed cause.
        proposal: TerminalProposal,
    },
}

/// Inputs for one coordinator run.
pub struct CoordinatorParams {
    /// Transaction id.
    pub transaction_id: TransactionId,
    /// Sticky cancel (flag before notify).
    pub cancel: Arc<StickyCancel>,
    /// Channel id.
    pub channel_id: ChannelId,
    /// Session id when known.
    pub session_id: Option<SessionId>,
    /// Canonical input.
    pub input: CanonicalInput,
    /// Invocation config.
    pub invocation_config: InvocationConfig,
    /// Session config.
    pub session_config: Option<SessionConfig>,
    /// Live channel map.
    pub channels: Arc<HashMap<ChannelId, LiveChannel>>,
    /// Event publisher ordinary-command admit gate.
    pub publish_tx: OrdinaryCmdAdmit,
    /// Worker → supervisor.
    pub worker_tx: mpsc::Sender<WorkerMessage>,
    /// Task supervisor spawn proxy.
    pub tasks: TransactionTaskSpawner,
    /// Transaction deadline.
    pub deadline: Duration,
    /// Cleanup join grace for exchange children.
    pub cleanup_deadline: Duration,
    /// Admitted tool ids for this transaction.
    pub selected_tools: Vec<ToolId>,
    /// Host tool registry (read-only clone).
    pub tools_registry: HostToolRegistry,
    /// Shared tool concurrency budget.
    pub shared_tool_capacity: Arc<SharedToolCapacity>,
    /// Runtime-scoped tool join spill (shared with supervisor Stopped proof).
    pub tool_spill: Arc<OrphanToolPermitSet>,
    /// Runtime-scoped live ProcessIsolated child count.
    pub owned_processes: Arc<std::sync::atomic::AtomicU32>,
    /// ProcessIsolated children retained until OS exit (D-048).
    pub process_registry: Arc<crate::transaction::owned_process_registry::OwnedProcessRegistry>,
    /// Runtime MCP gateway when `enable_mcp_listener` (CreationOnly install).
    pub mcp_gateway: Option<McpGatewayHandle>,
    /// Shared runtime state (ledger SessionKey claim after external create).
    pub shared: Arc<RuntimeShared>,
}

/// Run the coordinator to a terminal proposal and report `WorkerExited`.
pub async fn run_coordinator(params: CoordinatorParams) {
    let transaction_id = params.transaction_id;
    let worker_tx = params.worker_tx.clone();
    let shared = Arc::clone(&params.shared);
    let proposal = execute(params).await;
    // Park the authoritative proposal on the ledger before notify/exit so
    // `on_task_exit` cannot invent InvariantFailed over a lost `try_send`
    // (concurrent DirectLlm: units published, completion still InvariantFailed).
    {
        let mut ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = ledger.get_mut(&transaction_id) {
            entry.pending_worker_proposal = Some(proposal.clone());
        }
    }
    // Non-blocking: supervisor may be in abort_and_drain and not polling
    // `worker_rx`. Lost notify is recovered from `pending_worker_proposal`.
    let _ = worker_tx.try_send(WorkerMessage::WorkerExited {
        transaction_id,
        proposal,
    });
}

async fn execute(params: CoordinatorParams) -> TerminalProposal {
    let CoordinatorParams {
        transaction_id,
        cancel,
        channel_id,
        mut session_id,
        input,
        invocation_config,
        session_config,
        channels,
        publish_tx,
        worker_tx: _,
        tasks,
        deadline,
        cleanup_deadline,
        selected_tools,
        tools_registry,
        shared_tool_capacity,
        tool_spill,
        owned_processes,
        process_registry,
        mcp_gateway,
        shared,
    } = params;

    let Some(live) = channels.get(&channel_id) else {
        return TerminalProposal::new(TransactionEndKind::InvariantFailed);
    };

    let extension_limits = ExtensionLimits::default();
    let config = match merge_effective_config(
        &live.binding.defaults,
        session_config.as_ref(),
        None,
        &invocation_config,
        &live.binding.capabilities.option_policy,
        &extension_limits,
    ) {
        Ok(c) => c,
        Err(_) => return TerminalProposal::new(TransactionEndKind::InvariantFailed),
    };

    let max_encoded = live.binding.limits.max_encoded_exchange_bytes;
    let max_retained = live
        .binding
        .limits
        .max_encoded_exchange_bytes
        .max(64 * 1024);
    let max_provider_exchanges = shared.transaction_limits.max_provider_exchanges;
    let max_total_provider_input_bytes = shared.transaction_limits.max_total_provider_input_bytes;
    let max_total_provider_output_bytes = shared.transaction_limits.max_total_provider_output_bytes;
    if max_provider_exchanges == 0 {
        return TerminalProposal::new(TransactionEndKind::LimitExceeded);
    }

    // One ExchangeId for MCP install + supervised exchange (tool lifecycle correlation).
    let exchange_id = ExchangeId::generate();

    let mut mcp_token: Option<CapabilityToken> = None;
    let mut mcp_dispatcher: Option<Arc<TransactionToolDispatcher>> = None;
    let tool_mode = live.binding.tool_mode;

    let revoke_mcp = |token: Option<CapabilityToken>, gw: &Option<McpGatewayHandle>| {
        if let Some(token) = token {
            if let Some(gw) = gw.as_ref() {
                let _ = gw.revoke(&token);
            }
        }
    };

    if live.binding.kind == ChannelKind::ExternalAgent {
        let Some(sessions) = live.instance.sessions.as_ref() else {
            return TerminalProposal::new(TransactionEndKind::InvariantFailed);
        };

        // CreationOnly must install pending MCP before attach (not via refresh).
        // Empty tool sets skip MCP (D-026).
        let initial_mcp = if tool_mode == ToolExecutionMode::McpGateway
            && !selected_tools.is_empty()
            && live.binding.capabilities.mcp_configuration
                == McpConfigurationCapability::CreationOnly
        {
            let Some(gw) = mcp_gateway.as_ref() else {
                return TerminalProposal::new(TransactionEndKind::InvariantFailed);
            };
            let registered: Vec<_> = selected_tools
                .iter()
                .filter_map(|id| tools_registry.get(id).cloned())
                .collect();
            let resolved = ResolvedToolSet::from_registered(registered);
            // Provisional SessionKey until claim; rebind_session before activate (D-026).
            let dispatcher = TransactionToolDispatcher::with_runtime_resources(
                transaction_id,
                session_key_for(channel_id.clone(), session_id.clone(), transaction_id),
                resolved.clone(),
                Arc::clone(&shared_tool_capacity),
                Arc::clone(&tool_spill),
                Arc::clone(&owned_processes),
                Arc::clone(&process_registry),
                TransactionToolDispatcher::limits_from_transaction(&shared.transaction_limits),
            );
            match gw.install_pending(
                transaction_id,
                resolved,
                Arc::clone(&dispatcher),
                exchange_id,
            ) {
                Ok(pending) => {
                    mcp_token = Some(pending.token.clone());
                    mcp_dispatcher = Some(pending.dispatcher);
                    Some(pending.descriptor)
                }
                Err(_) => {
                    return TerminalProposal::new(TransactionEndKind::InvariantFailed);
                }
            }
        } else {
            None
        };

        let attach_req = SessionAttachRequest {
            transaction_id,
            channel_id: channel_id.clone(),
            requested_session_id: session_id.clone(),
            session_config: session_config.clone().unwrap_or_default(),
            initial_mcp,
            deadline: Instant::now() + deadline,
        };
        let pending_attach = match sessions.begin_attach(attach_req) {
            Ok(p) => p,
            Err(_) => {
                // Installed route must not survive a failed attach (route leak).
                revoke_mcp(mcp_token.take(), &mcp_gateway);
                return TerminalProposal::new(TransactionEndKind::InvariantFailed);
            }
        };
        let attachment = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                let _ = pending_attach.control.cancel();
                revoke_mcp(mcp_token.take(), &mcp_gateway);
                return TerminalProposal::new(TransactionEndKind::Cancelled);
            }
            res = pending_attach.completion => match res {
                Ok(a) => a,
                Err(_) => {
                    revoke_mcp(mcp_token.take(), &mcp_gateway);
                    return TerminalProposal::new(TransactionEndKind::InvariantFailed);
                }
            }
        };
        let (opened_tx, opened_rx) = oneshot::channel();
        let (proceed_tx, proceed_rx) = oneshot::channel();
        let prompt_ready = Some(PromptReadyGate {
            opened: opened_tx,
            proceed: proceed_rx,
        });

        // Claim SessionKey + rebind + activate before any prompt send (D-026).
        // Fail closed if open returns no authoritative external session id.
        let publish_gate = publish_tx.clone();
        let cancel_gate = Arc::clone(&cancel);
        let mcp_gateway_gate = mcp_gateway.clone();
        let mcp_token_gate = mcp_token.clone();
        let mcp_dispatcher_gate = mcp_dispatcher.clone();
        let channel_id_gate = channel_id.clone();
        let shared_gate = Arc::clone(&shared);
        let tx_gate = transaction_id;
        let max_distinct_gate = live.binding.limits.max_distinct_sessions;
        let gate_task = async move {
            let external = match opened_rx.await {
                Ok(Some(ext)) => ext,
                Ok(None) | Err(_) => {
                    let _ = proceed_tx.send(Err(PromptProceedError::Failed));
                    return (None, false);
                }
            };
            if cancel_gate.is_cancelled() {
                let _ = proceed_tx.send(Err(PromptProceedError::Failed));
                return (None, false);
            }
            let Ok(sid) = SessionId::try_new(external.as_str()) else {
                let _ = proceed_tx.send(Err(PromptProceedError::Failed));
                return (None, false);
            };
            let key = SessionKey {
                channel_id: channel_id_gate.clone(),
                session_id: sid.clone(),
            };
            // Reserve claimed SessionKey in the ledger before activate / prompt.
            {
                let mut ledger = shared_gate.ledger.lock().unwrap_or_else(|e| e.into_inner());
                match ledger.bind_session(&tx_gate, key.clone(), Some(max_distinct_gate)) {
                    Ok(()) => {}
                    Err(LedgerInsertError::DistinctSessionsExceeded) => {
                        let _ = proceed_tx.send(Err(PromptProceedError::DistinctSessionsExceeded));
                        return (None, false);
                    }
                    Err(_) => {
                        let _ = proceed_tx.send(Err(PromptProceedError::Failed));
                        return (None, false);
                    }
                }
            }
            if publish_gate
                .send(EventPublisherCommand::EstablishExternal(external.clone()))
                .await
                .is_err()
            {
                let _ = proceed_tx.send(Err(PromptProceedError::Failed));
                return (None, false);
            }
            // Authoritative SessionKey before activate (no synthetic key on MCP path).
            if let Some(disp) = mcp_dispatcher_gate.as_ref() {
                disp.rebind_session(key);
            }
            if let Some(token) = mcp_token_gate.as_ref() {
                let Some(gw) = mcp_gateway_gate.as_ref() else {
                    let _ = proceed_tx.send(Err(PromptProceedError::Failed));
                    return (Some(external), false);
                };
                if gw.activate(token).is_err() {
                    let _ = proceed_tx.send(Err(PromptProceedError::Failed));
                    return (Some(external), false);
                }
            }
            let _ = proceed_tx.send(Ok(()));
            (Some(external), true)
        };

        let tool_specs: Vec<_> = selected_tools
            .iter()
            .filter_map(|id| tools_registry.get_spec(id).cloned())
            .collect();
        let exchange_fut = run_direct_llm_exchange(
            transaction_id,
            exchange_id,
            &tasks,
            live.instance.connector.as_ref(),
            live.binding.encoder.as_ref(),
            live.binding.interpreter.as_ref(),
            &live.binding.endpoint_ref,
            live.binding.credential_ref.as_deref(),
            &input,
            &config,
            Arc::clone(&cancel),
            deadline,
            cleanup_deadline,
            max_encoded,
            max_retained,
            max_total_provider_input_bytes,
            max_total_provider_output_bytes,
            Some(attachment),
            prompt_ready,
            &tool_specs,
        );

        let (outcome, gate_result) = tokio::join!(exchange_fut, gate_task);
        let (external_from_gate, established_before_prompt) = gate_result;
        if let Some(ext) = external_from_gate {
            if let Ok(sid) = SessionId::try_new(ext.as_str()) {
                session_id = Some(sid);
            }
        }

        let (exchanges_used, provider_input_used, provider_output_used) =
            provider_usage_after_exchange(&outcome);
        let inline = InlineContinuationDeps {
            input: &input,
            config: &config,
            connector: live.instance.connector.as_ref(),
            encoder: live.binding.encoder.as_ref(),
            interpreter: live.binding.interpreter.as_ref(),
            endpoint_ref: &live.binding.endpoint_ref,
            credential_ref: live.binding.credential_ref.as_deref(),
            tool_specs: &tool_specs,
            deadline,
            cleanup_deadline,
            max_encoded,
            max_retained,
            max_continuations: shared.transaction_limits.max_continuations,
            max_continuation_context_bytes: shared
                .transaction_limits
                .max_continuation_context_bytes,
            max_provider_exchanges,
            max_total_provider_input_bytes,
            max_total_provider_output_bytes,
            exchanges_used,
            provider_input_used,
            provider_output_used,
            dispatcher_limits: TransactionToolDispatcher::limits_from_transaction(
                &shared.transaction_limits,
            ),
        };
        let terminal = finish_after_exchange(
            outcome,
            established_before_prompt,
            tool_mode,
            invocation_config.continuation_policy,
            &publish_tx,
            &cancel,
            &tasks,
            transaction_id,
            channel_id,
            session_id,
            selected_tools,
            tools_registry,
            shared_tool_capacity,
            tool_spill,
            owned_processes,
            process_registry,
            Some(inline),
            TransactionToolDispatcher::limits_from_transaction(&shared.transaction_limits),
        )
        .await;

        revoke_mcp(mcp_token.take(), &mcp_gateway);
        let _ = mcp_dispatcher;
        return terminal;
    }

    if live.binding.kind != ChannelKind::DirectLlm {
        return TerminalProposal::new(TransactionEndKind::InvariantFailed);
    }

    let tool_specs: Vec<_> = selected_tools
        .iter()
        .filter_map(|id| tools_registry.get_spec(id).cloned())
        .collect();
    let outcome = run_direct_llm_exchange(
        transaction_id,
        exchange_id,
        &tasks,
        live.instance.connector.as_ref(),
        live.binding.encoder.as_ref(),
        live.binding.interpreter.as_ref(),
        &live.binding.endpoint_ref,
        live.binding.credential_ref.as_deref(),
        &input,
        &config,
        Arc::clone(&cancel),
        deadline,
        cleanup_deadline,
        max_encoded,
        max_retained,
        max_total_provider_input_bytes,
        max_total_provider_output_bytes,
        None,
        None,
        &tool_specs,
    )
    .await;

    let (exchanges_used, provider_input_used, provider_output_used) =
        provider_usage_after_exchange(&outcome);
    let inline = InlineContinuationDeps {
        input: &input,
        config: &config,
        connector: live.instance.connector.as_ref(),
        encoder: live.binding.encoder.as_ref(),
        interpreter: live.binding.interpreter.as_ref(),
        endpoint_ref: &live.binding.endpoint_ref,
        credential_ref: live.binding.credential_ref.as_deref(),
        tool_specs: &tool_specs,
        deadline,
        cleanup_deadline,
        max_encoded,
        max_retained,
        max_continuations: shared.transaction_limits.max_continuations,
        max_continuation_context_bytes: shared.transaction_limits.max_continuation_context_bytes,
        max_provider_exchanges,
        max_total_provider_input_bytes,
        max_total_provider_output_bytes,
        exchanges_used,
        provider_input_used,
        provider_output_used,
        dispatcher_limits: TransactionToolDispatcher::limits_from_transaction(
            &shared.transaction_limits,
        ),
    };
    finish_after_exchange(
        outcome,
        false,
        tool_mode,
        invocation_config.continuation_policy,
        &publish_tx,
        &cancel,
        &tasks,
        transaction_id,
        channel_id,
        session_id,
        selected_tools,
        tools_registry,
        shared_tool_capacity,
        tool_spill,
        owned_processes,
        process_registry,
        Some(inline),
        TransactionToolDispatcher::limits_from_transaction(&shared.transaction_limits),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn finish_after_exchange(
    outcome: DirectExchangeOutcome,
    already_established: bool,
    tool_mode: ToolExecutionMode,
    continuation_policy: ContinuationPolicy,
    publish_tx: &OrdinaryCmdAdmit,
    cancel: &Arc<StickyCancel>,
    tasks: &TransactionTaskSpawner,
    transaction_id: TransactionId,
    channel_id: ChannelId,
    session_id: Option<SessionId>,
    selected_tools: Vec<ToolId>,
    tools_registry: HostToolRegistry,
    shared_tool_capacity: Arc<SharedToolCapacity>,
    tool_spill: Arc<OrphanToolPermitSet>,
    owned_processes: Arc<std::sync::atomic::AtomicU32>,
    process_registry: Arc<crate::transaction::owned_process_registry::OwnedProcessRegistry>,
    inline: Option<InlineContinuationDeps<'_>>,
    dispatcher_limits: crate::transaction::DispatcherLimits,
) -> TerminalProposal {
    // §22.6: establish external session before ordinary events when not done
    // at the prompt-ready gate (DirectLlm / late discovery).
    if !already_established {
        if let Some(external) = outcome.external_session_id.clone() {
            let send = publish_tx.send(EventPublisherCommand::EstablishExternal(external));
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    return TerminalProposal::new(TransactionEndKind::Cancelled);
                }
                res = send => {
                    if res.is_err() {
                        return TerminalProposal::new(TransactionEndKind::EventDeliveryFailed);
                    }
                }
            }
        }
    }

    let first_exchange_id = outcome.exchange_id;
    let mut terminal = outcome.terminal;
    let scoped_units = scope_tool_units_for_exchange(outcome.units, first_exchange_id);
    for unit in &scoped_units {
        let send = publish_tx.send(EventPublisherCommand::Publish(Box::new(
            TransactionEventPayload::CanonicalUnit(unit.clone()),
        )));
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                terminal = TransactionEndKind::Cancelled;
                break;
            }
            res = send => {
                if res.is_err() {
                    terminal = TransactionEndKind::EventDeliveryFailed;
                    break;
                }
            }
        }
    }

    // McpGateway: external agent calls Monoloop MCP — Loop must not dual-dispatch
    // the same Ready units via HostToolRuntime (authoritative path is MCP only).
    if tool_mode == ToolExecutionMode::McpGateway {
        return TerminalProposal::new(terminal);
    }

    if matches!(
        terminal,
        TransactionEndKind::Completed | TransactionEndKind::ContinuationRequired
    ) && needs_loop_dispatch(&scoped_units)
    {
        let loop_result = if selected_tools.is_empty() {
            run_supervised_empty_loop(
                tasks,
                transaction_id,
                channel_id.clone(),
                session_id.clone(),
                first_exchange_id,
                scoped_units.clone(),
                publish_tx.clone(),
                Arc::clone(cancel),
            )
            .await
        } else {
            let registered: Vec<_> = selected_tools
                .iter()
                .filter_map(|id| tools_registry.get(id).cloned())
                .collect();
            let resolved = ResolvedToolSet::from_registered(registered);
            let dispatcher = TransactionToolDispatcher::with_runtime_resources(
                transaction_id,
                session_key_for(channel_id.clone(), session_id.clone(), transaction_id),
                resolved.clone(),
                Arc::clone(&shared_tool_capacity),
                Arc::clone(&tool_spill),
                Arc::clone(&owned_processes),
                Arc::clone(&process_registry),
                dispatcher_limits,
            );
            let runtime = HostToolRuntime::with_spawner(
                Arc::clone(&dispatcher),
                first_exchange_id,
                transaction_id,
                tasks.clone(),
            );
            run_supervised_tool_loop(
                tasks,
                transaction_id,
                channel_id.clone(),
                session_id.clone(),
                first_exchange_id,
                scoped_units.clone(),
                publish_tx.clone(),
                Arc::clone(cancel),
                Arc::new(ResolvedToolRegistry::new(resolved)),
                Arc::new(runtime),
            )
            .await
        };
        match loop_result {
            Ok(report) => {
                // WP-10 / Phase B: CallerControlled tool exchange ends without a
                // second provider open — host continues via ContinuationRequired.
                if matches!(terminal, TransactionEndKind::Completed)
                    && continuation_policy == ContinuationPolicy::CallerControlled
                {
                    terminal = TransactionEndKind::ContinuationRequired;
                } else if matches!(terminal, TransactionEndKind::Completed)
                    && continuation_policy == ContinuationPolicy::InlineToolContinuation
                {
                    terminal = match run_inline_tool_continuation(
                        &report,
                        &scoped_units,
                        inline.as_ref(),
                        publish_tx,
                        cancel,
                        tasks,
                        transaction_id,
                        channel_id.clone(),
                        session_id.clone(),
                        &selected_tools,
                        &tools_registry,
                        Arc::clone(&shared_tool_capacity),
                        Arc::clone(&tool_spill),
                        Arc::clone(&owned_processes),
                        Arc::clone(&process_registry),
                    )
                    .await
                    {
                        Ok(kind) => kind,
                        Err(kind) => kind,
                    };
                }
            }
            Err(LoopDispatchError::Cancelled) => {
                terminal = TransactionEndKind::Cancelled;
            }
            Err(LoopDispatchError::PublishFailed) => {
                terminal = TransactionEndKind::EventDeliveryFailed;
            }
            Err(_) => {
                terminal = TransactionEndKind::InvariantFailed;
            }
        }
    }

    TerminalProposal::new(terminal)
}

/// Bounded InlineToolContinuation: accumulate transcript and open up to
/// `max_continuations` further provider exchanges, dispatching Loop tools
/// between rounds. Exceeding the bound fails closed as `LimitExceeded`.
#[allow(clippy::too_many_arguments)]
async fn run_inline_tool_continuation(
    report: &LoopDispatchReport,
    first_units: &[CanonicalUnitEvent],
    inline: Option<&InlineContinuationDeps<'_>>,
    publish_tx: &OrdinaryCmdAdmit,
    cancel: &Arc<StickyCancel>,
    tasks: &TransactionTaskSpawner,
    transaction_id: TransactionId,
    channel_id: ChannelId,
    session_id: Option<SessionId>,
    selected_tools: &[ToolId],
    tools_registry: &HostToolRegistry,
    shared_tool_capacity: Arc<SharedToolCapacity>,
    tool_spill: Arc<OrphanToolPermitSet>,
    owned_processes: Arc<std::sync::atomic::AtomicU32>,
    process_registry: Arc<crate::transaction::owned_process_registry::OwnedProcessRegistry>,
) -> Result<TransactionEndKind, TransactionEndKind> {
    let Some(deps) = inline else {
        return Err(TransactionEndKind::InvariantFailed);
    };
    if report.tool_results.is_empty() {
        return Err(TransactionEndKind::InvariantFailed);
    }

    let mut messages = deps.input.messages().to_vec();
    let mut ready_units = first_units.to_vec();
    let mut tool_results = report.tool_results.clone();
    let mut exchanges_used = deps.exchanges_used;
    let mut provider_input_used = deps.provider_input_used;
    let mut provider_output_used = deps.provider_output_used;

    for _round in 1..=deps.max_continuations {
        if exchanges_used >= deps.max_provider_exchanges {
            return Err(TransactionEndKind::LimitExceeded);
        }
        append_tool_round(&mut messages, &ready_units, &tool_results)
            .map_err(|_| TransactionEndKind::EncodingFailed)?;
        let estimated = estimate_continuation_messages_bytes(&messages)
            .map_err(|_| TransactionEndKind::EncodingFailed)?;
        if estimated > deps.max_continuation_context_bytes {
            return Err(TransactionEndKind::LimitExceeded);
        }
        let context = ContinuationContext::try_new(messages.clone())
            .map_err(|_| TransactionEndKind::EncodingFailed)?;

        let remaining_input = deps
            .max_total_provider_input_bytes
            .saturating_sub(provider_input_used);
        let remaining_output = deps
            .max_total_provider_output_bytes
            .saturating_sub(provider_output_used);
        // Fail closed before mutate/open when a prior exchange exhausted a byte ceiling.
        if remaining_input == 0 || remaining_output == 0 {
            return Err(TransactionEndKind::LimitExceeded);
        }
        let continuation_exchange_id = ExchangeId::generate();
        let next = run_direct_llm_continuation(
            transaction_id,
            continuation_exchange_id,
            tasks,
            deps.connector,
            deps.encoder,
            deps.interpreter,
            deps.endpoint_ref,
            deps.credential_ref,
            &context,
            &tool_results,
            deps.config,
            deps.tool_specs,
            Arc::clone(cancel),
            deps.deadline,
            deps.cleanup_deadline,
            deps.max_encoded,
            deps.max_retained,
            remaining_input,
            remaining_output,
        )
        .await;
        if !matches!(next.terminal, TransactionEndKind::Completed) {
            return Err(next.terminal);
        }
        exchanges_used = exchanges_used.saturating_add(1);
        provider_input_used = provider_input_used.saturating_add(next.encoded_input_bytes);
        provider_output_used = provider_output_used.saturating_add(next.received_output_bytes);
        let scoped = scope_tool_units_for_exchange(next.units, continuation_exchange_id);
        for unit in &scoped {
            let send = publish_tx.send(EventPublisherCommand::Publish(Box::new(
                TransactionEventPayload::CanonicalUnit(unit.clone()),
            )));
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    return Err(TransactionEndKind::Cancelled);
                }
                res = send => {
                    if res.is_err() {
                        return Err(TransactionEndKind::EventDeliveryFailed);
                    }
                }
            }
        }
        if !needs_loop_dispatch(&scoped) {
            return Ok(TransactionEndKind::Completed);
        }

        let loop_result = if selected_tools.is_empty() {
            run_supervised_empty_loop(
                tasks,
                transaction_id,
                channel_id.clone(),
                session_id.clone(),
                continuation_exchange_id,
                scoped.clone(),
                publish_tx.clone(),
                Arc::clone(cancel),
            )
            .await
        } else {
            let registered: Vec<_> = selected_tools
                .iter()
                .filter_map(|id| tools_registry.get(id).cloned())
                .collect();
            let resolved = ResolvedToolSet::from_registered(registered);
            let dispatcher = TransactionToolDispatcher::with_runtime_resources(
                transaction_id,
                session_key_for(channel_id.clone(), session_id.clone(), transaction_id),
                resolved.clone(),
                Arc::clone(&shared_tool_capacity),
                Arc::clone(&tool_spill),
                Arc::clone(&owned_processes),
                Arc::clone(&process_registry),
                deps.dispatcher_limits,
            );
            let runtime = HostToolRuntime::with_spawner(
                Arc::clone(&dispatcher),
                continuation_exchange_id,
                transaction_id,
                tasks.clone(),
            );
            run_supervised_tool_loop(
                tasks,
                transaction_id,
                channel_id.clone(),
                session_id.clone(),
                continuation_exchange_id,
                scoped.clone(),
                publish_tx.clone(),
                Arc::clone(cancel),
                Arc::new(ResolvedToolRegistry::new(resolved)),
                Arc::new(runtime),
            )
            .await
        };
        match loop_result {
            Ok(next_report) => {
                if next_report.tool_results.is_empty() {
                    return Err(TransactionEndKind::InvariantFailed);
                }
                ready_units = scoped;
                tool_results = next_report.tool_results;
            }
            Err(LoopDispatchError::Cancelled) => {
                return Err(TransactionEndKind::Cancelled);
            }
            Err(LoopDispatchError::PublishFailed) => {
                return Err(TransactionEndKind::EventDeliveryFailed);
            }
            Err(_) => {
                return Err(TransactionEndKind::InvariantFailed);
            }
        }
    }

    Err(TransactionEndKind::LimitExceeded)
}

/// After a successful provider exchange, seed continuation budget counters.
fn provider_usage_after_exchange(outcome: &DirectExchangeOutcome) -> (usize, usize, usize) {
    if matches!(outcome.terminal, TransactionEndKind::Completed) {
        (
            1,
            outcome.encoded_input_bytes,
            outcome.received_output_bytes,
        )
    } else {
        (0, 0, 0)
    }
}

fn append_tool_round(
    messages: &mut Vec<CanonicalMessage>,
    units: &[CanonicalUnitEvent],
    results: &[CanonicalToolResult],
) -> Result<(), ()> {
    let limits = InputLimits::default();
    let mut tool_calls = Vec::new();
    for unit in units {
        let snap = unit.snapshot();
        let CanonicalUnit::Tool(t) = &snap.unit else {
            continue;
        };
        if t.request_state != ToolRequestState::Ready {
            continue;
        }
        let Some(name) = t.tool_name.as_deref() else {
            return Err(());
        };
        let Some(payload) = t.request_payload.as_deref() else {
            return Err(());
        };
        let exchange_id = results.first().map(|r| r.exchange_id);
        let provider_id = results
            .iter()
            .find(|r| r.tool_action_id == t.tool_action_id)
            .map(|r| r.provider_tool_call_id.clone())
            .or_else(|| {
                exchange_id.and_then(|eid| {
                    provider_tool_call_id_from_action(eid, &t.tool_action_id).map(str::to_string)
                })
            })
            .unwrap_or_else(|| t.tool_action_id.as_str().to_string());
        let arguments = serde_json::from_str(payload).map_err(|_| ())?;
        let tool_name = ToolName::try_new(name).map_err(|_| ())?;
        tool_calls.push(CanonicalAssistantToolCall {
            tool_call_id: provider_id,
            tool_name,
            arguments,
        });
    }
    if tool_calls.is_empty() {
        return Err(());
    }
    messages.push(CanonicalMessage::Assistant {
        content: vec![],
        tool_calls,
    });
    for result in results {
        let text = match &result.outcome {
            CanonicalToolResultOutcome::Succeeded(CanonicalToolOutput::Json(v)) => {
                serde_json::to_string(v).map_err(|_| ())?
            }
            CanonicalToolResultOutcome::Succeeded(CanonicalToolOutput::Text(t)) => t.clone(),
            CanonicalToolResultOutcome::DomainFailed(err) => {
                serde_json::to_string(&serde_json::json!({
                    "error": { "code": err.code, "message": err.message, "data": err.data },
                }))
                .map_err(|_| ())?
            }
        };
        let part = TextPart::try_new(text, limits.max_text_part_bytes).map_err(|_| ())?;
        messages.push(CanonicalMessage::Tool {
            tool_call_id: result.provider_tool_call_id.clone(),
            content: vec![part],
        });
    }
    Ok(())
}

/// Byte estimate for cumulative continuation transcript (mirrors admission estimate).
fn estimate_continuation_messages_bytes(messages: &[CanonicalMessage]) -> Result<usize, ()> {
    let mut total = 0usize;
    for msg in messages {
        match msg {
            CanonicalMessage::System { content, name }
            | CanonicalMessage::User { content, name } => {
                if let Some(n) = name {
                    total = total.saturating_add(n.len());
                }
                for part in content {
                    total = total.saturating_add(part.text().len());
                }
            }
            CanonicalMessage::Assistant {
                content,
                tool_calls,
            } => {
                for part in content {
                    total = total.saturating_add(part.text().len());
                }
                for call in tool_calls {
                    total = total.saturating_add(call.tool_call_id.len());
                    total = total.saturating_add(call.tool_name.as_str().len());
                    let encoded = serde_json::to_vec(&call.arguments).map_err(|_| ())?;
                    total = total.saturating_add(encoded.len());
                }
            }
            CanonicalMessage::Tool {
                tool_call_id,
                content,
            } => {
                total = total.saturating_add(tool_call_id.len());
                for part in content {
                    total = total.saturating_add(part.text().len());
                }
            }
        }
    }
    Ok(total)
}
