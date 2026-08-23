//! Per-transaction coordinator (v2 §11) — supervised exchange + Loop tools.

use super::event_publisher::EventPublisherCommand;
use super::exchange::{run_direct_llm_exchange, DirectExchangeOutcome, PromptReadyGate};
use super::loop_dispatch::{
    needs_loop_dispatch, run_supervised_empty_loop, run_supervised_tool_loop, LoopDispatchError,
};
use super::session_identity::session_key_for;
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
use monoloop_connector::SessionAttachRequest;
use monoloop_contracts::{
    merge_effective_config, CanonicalInput, ChannelId, ChannelKind, ExchangeId, ExtensionLimits,
    InvocationConfig, McpConfigurationCapability, SessionConfig, SessionId, SessionKey,
    ToolExecutionMode, ToolId, TransactionEndKind, TransactionEventPayload, TransactionId,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

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
    /// Event publisher commands.
    pub publish_tx: mpsc::Sender<EventPublisherCommand>,
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
    /// Runtime MCP gateway when `enable_mcp_listener` (CreationOnly install).
    pub mcp_gateway: Option<McpGatewayHandle>,
    /// Shared runtime state (ledger SessionKey claim after external create).
    pub shared: Arc<RuntimeShared>,
}

/// Run the coordinator to a terminal proposal and report `WorkerExited`.
pub async fn run_coordinator(params: CoordinatorParams) {
    let transaction_id = params.transaction_id;
    let worker_tx = params.worker_tx.clone();
    let proposal = execute(params).await;
    // Non-blocking: supervisor may be in abort_and_drain and not polling
    // `worker_rx`. A lost message is recovered in `on_task_exit` for
    // `TransactionCoordinator` so every admission still gets one terminal.
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
    let max_provider_input = max_encoded;

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
                8,
                16,
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
        let gate_task = async move {
            let external = match opened_rx.await {
                Ok(Some(ext)) => ext,
                Ok(None) | Err(_) => {
                    let _ = proceed_tx.send(Err(()));
                    return (None, false);
                }
            };
            if cancel_gate.is_cancelled() {
                let _ = proceed_tx.send(Err(()));
                return (None, false);
            }
            let Ok(sid) = SessionId::try_new(external.as_str()) else {
                let _ = proceed_tx.send(Err(()));
                return (None, false);
            };
            let key = SessionKey {
                channel_id: channel_id_gate.clone(),
                session_id: sid.clone(),
            };
            // Reserve claimed SessionKey in the ledger before activate / prompt.
            {
                let mut ledger = shared_gate.ledger.lock().unwrap_or_else(|e| e.into_inner());
                if ledger.bind_session(&tx_gate, key.clone()).is_err() {
                    let _ = proceed_tx.send(Err(()));
                    return (None, false);
                }
            }
            if publish_gate
                .send(EventPublisherCommand::EstablishExternal(external.clone()))
                .await
                .is_err()
            {
                let _ = proceed_tx.send(Err(()));
                return (None, false);
            }
            // Authoritative SessionKey before activate (no synthetic key on MCP path).
            if let Some(disp) = mcp_dispatcher_gate.as_ref() {
                disp.rebind_session(key);
            }
            if let Some(token) = mcp_token_gate.as_ref() {
                let Some(gw) = mcp_gateway_gate.as_ref() else {
                    let _ = proceed_tx.send(Err(()));
                    return (Some(external), false);
                };
                if gw.activate(token).is_err() {
                    let _ = proceed_tx.send(Err(()));
                    return (Some(external), false);
                }
            }
            let _ = proceed_tx.send(Ok(()));
            (Some(external), true)
        };

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
            max_provider_input,
            Some(attachment),
            prompt_ready,
        );

        let (outcome, gate_result) = tokio::join!(exchange_fut, gate_task);
        let (external_from_gate, established_before_prompt) = gate_result;
        if let Some(ext) = external_from_gate {
            if let Ok(sid) = SessionId::try_new(ext.as_str()) {
                session_id = Some(sid);
            }
        }

        let terminal = finish_after_exchange(
            outcome,
            established_before_prompt,
            tool_mode,
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
        )
        .await;

        revoke_mcp(mcp_token.take(), &mcp_gateway);
        let _ = mcp_dispatcher;
        return terminal;
    }

    if live.binding.kind != ChannelKind::DirectLlm {
        return TerminalProposal::new(TransactionEndKind::InvariantFailed);
    }

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
        max_provider_input,
        None,
        None,
    )
    .await;

    finish_after_exchange(
        outcome,
        false,
        tool_mode,
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
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn finish_after_exchange(
    outcome: DirectExchangeOutcome,
    already_established: bool,
    tool_mode: ToolExecutionMode,
    publish_tx: &mpsc::Sender<EventPublisherCommand>,
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

    let mut terminal = outcome.terminal;
    for unit in &outcome.units {
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
    ) && needs_loop_dispatch(&outcome.units)
    {
        let loop_result = if selected_tools.is_empty() {
            run_supervised_empty_loop(
                tasks,
                transaction_id,
                channel_id,
                session_id,
                outcome.exchange_id,
                outcome.units,
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
                shared_tool_capacity,
                tool_spill,
                owned_processes,
                8,
                16,
            );
            let runtime = HostToolRuntime::with_spawner(
                Arc::clone(&dispatcher),
                outcome.exchange_id,
                transaction_id,
                tasks.clone(),
            );
            run_supervised_tool_loop(
                tasks,
                transaction_id,
                channel_id,
                session_id,
                outcome.exchange_id,
                outcome.units,
                publish_tx.clone(),
                Arc::clone(cancel),
                Arc::new(ResolvedToolRegistry::new(resolved)),
                Arc::new(runtime),
            )
            .await
        };
        match loop_result {
            Ok(_report) => {}
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
