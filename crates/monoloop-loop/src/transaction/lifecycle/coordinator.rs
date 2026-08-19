//! Per-transaction coordinator (v2 §11) — supervised DirectLlm exchange + empty tools (M5).

use super::empty_tools::{has_ready_tool_units, run_empty_tool_pass, EmptyToolPassError};
use super::event_publisher::EventPublisherCommand;
use super::exchange::run_direct_llm_exchange;
use super::task_spawner::TransactionTaskSpawner;
use super::task_supervisor::TaskClass;
use super::terminal::TerminalProposal;
use crate::transaction::channel_registry::LiveChannel;
use monoloop_contracts::{
    merge_effective_config, CanonicalInput, ChannelId, ChannelKind, ExtensionLimits,
    InvocationConfig, SessionConfig, SessionId, ToolExecutionId, TransactionEndKind,
    TransactionEventPayload, TransactionId,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Notify};

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
    /// Cancel notify.
    pub cancel: Arc<Notify>,
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
}

/// Run the coordinator to a terminal proposal and report `WorkerExited`.
pub async fn run_coordinator(params: CoordinatorParams) {
    let transaction_id = params.transaction_id;
    let worker_tx = params.worker_tx.clone();
    let proposal = execute(params).await;
    let _ = worker_tx
        .send(WorkerMessage::WorkerExited {
            transaction_id,
            proposal,
        })
        .await;
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

    // Publish complete units without silent drop (LAW 24).
    let mut terminal = outcome.terminal;
    for unit in &outcome.units {
        let send = publish_tx.send(EventPublisherCommand::Publish(Box::new(
            TransactionEventPayload::CanonicalUnit(unit.clone()),
        )));
        tokio::select! {
            biased;
            _ = cancel.notified() => {
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
    ) && has_ready_tool_units(&outcome.units)
    {
        // M5: EmptyToolRegistry pass. Prefer TaskSupervisor ToolWorker; if the
        // spawn mailbox rejects, await the returned future on this coordinator
        // (already supervised) — never drop the work or invent a second machine.
        let (done_tx, done_rx) = oneshot::channel();
        let exec_id = ToolExecutionId::generate();
        let units = outcome.units.clone();
        let publish_tools = publish_tx.clone();
        let cancel_tools = Arc::clone(&cancel);
        let channel_tools = channel_id.clone();
        let session_tools = session_id.clone();
        let exchange_id = outcome.exchange_id;
        let worker = async move {
            let report = run_empty_tool_pass(
                transaction_id,
                channel_tools,
                session_tools,
                exchange_id,
                &units,
                publish_tools,
                cancel_tools,
            )
            .await;
            let _ = done_tx.send(report);
        };
        match tasks
            .spawn(TaskClass::ToolWorker(transaction_id, exec_id), worker)
            .await
        {
            Ok(_) => {}
            Err((_err, returned)) => {
                // Busy/Closed: drive the same future on the coordinator task.
                returned.await;
            }
        }
        match done_rx.await {
            Ok(Ok(_report)) => {}
            Ok(Err(EmptyToolPassError::Cancelled)) => {
                terminal = TransactionEndKind::Cancelled;
            }
            Ok(Err(EmptyToolPassError::PublishFailed)) => {
                terminal = TransactionEndKind::EventDeliveryFailed;
            }
            Ok(Err(_)) | Err(_) => {
                terminal = TransactionEndKind::InvariantFailed;
            }
        }
    }

    TerminalProposal::new(terminal)
}
