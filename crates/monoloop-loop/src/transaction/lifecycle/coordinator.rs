//! Per-transaction coordinator (v2 §11) — supervised DirectLlm exchange + empty tools (M5).

use super::event_publisher::EventPublisherCommand;
use super::exchange::run_direct_llm_exchange;
use super::loop_dispatch::{
    needs_loop_dispatch, run_supervised_empty_loop, run_supervised_tool_loop, LoopDispatchError,
};
use super::session_identity::session_key_for;
use super::task_spawner::TransactionTaskSpawner;
use super::terminal::TerminalProposal;
use crate::transaction::channel_registry::LiveChannel;
use crate::transaction::dispatcher::TransactionToolDispatcher;
use crate::transaction::host_tools::HostToolRegistry;
use crate::transaction::loop_adapters::{HostToolRuntime, ResolvedToolRegistry};
use crate::transaction::resolved_tools::ResolvedToolSet;
use crate::transaction::sticky_cancel::StickyCancel;
use crate::transaction::tool_capacity::SharedToolCapacity;
use monoloop_contracts::{
    merge_effective_config, CanonicalInput, ChannelId, ChannelKind, ExtensionLimits,
    InvocationConfig, SessionConfig, SessionId, ToolId, TransactionEndKind,
    TransactionEventPayload, TransactionId,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

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
        session_id,
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
    } = params;

    let Some(live) = channels.get(&channel_id) else {
        return TerminalProposal::new(TransactionEndKind::InvariantFailed);
    };

    if live.binding.kind != ChannelKind::DirectLlm {
        return TerminalProposal::new(TransactionEndKind::InvariantFailed);
    }

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

    let outcome = run_direct_llm_exchange(
        transaction_id,
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
    )
    .await;

    // §22.6: establish external session before ordinary events when Connector
    // returned an authoritative id (SessionEstablished at sequence 1).
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

    // Publish complete units without silent drop (LAW 24).
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

    if matches!(
        terminal,
        TransactionEndKind::Completed | TransactionEndKind::ContinuationRequired
    ) && needs_loop_dispatch(&outcome.units)
    {
        // M5: single canonical Loop state machine under TaskSupervisor.
        let loop_result = if selected_tools.is_empty() {
            run_supervised_empty_loop(
                &tasks,
                transaction_id,
                channel_id,
                session_id,
                outcome.exchange_id,
                outcome.units,
                publish_tx,
                cancel,
            )
            .await
        } else {
            let registered: Vec<_> = selected_tools
                .iter()
                .filter_map(|id| tools_registry.get(id).cloned())
                .collect();
            let resolved = ResolvedToolSet::from_registered(registered);
            let dispatcher = TransactionToolDispatcher::new(
                transaction_id,
                session_key_for(channel_id.clone(), session_id.clone(), transaction_id),
                resolved.clone(),
                shared_tool_capacity,
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
                &tasks,
                transaction_id,
                channel_id,
                session_id,
                outcome.exchange_id,
                outcome.units,
                publish_tx,
                cancel,
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
