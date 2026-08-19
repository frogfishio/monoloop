//! Per-transaction coordinator (v2 §11) — M3 DirectLlm path.

use super::event_publisher::EventPublisherCommand;
use super::exchange_direct::run_direct_llm_exchange;
use super::terminal::TerminalProposal;
use crate::transaction::channel_registry::LiveChannel;
use monoloop_contracts::{
    merge_effective_config, CanonicalInput, ChannelId, ChannelKind, ExtensionLimits,
    InvocationConfig, SessionConfig, SessionId, TransactionEndKind, TransactionEventPayload,
    TransactionId,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Notify};

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
    /// Session id when known (reserved for SessionEstablished ordering).
    #[allow(dead_code)]
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
    /// Transaction deadline.
    pub deadline: Duration,
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
        session_id: _,
        input,
        invocation_config,
        session_config,
        channels,
        publish_tx,
        worker_tx: _,
        deadline,
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

    // M3: synthetic exchange (real Fake I/O → M4 TaskSpawner). LoopRuntime
    // still ambient-spawns; wiring under TaskSupervisor is M4/M5.
    let outcome = run_direct_llm_exchange(
        transaction_id,
        live.instance.connector.as_ref(),
        live.binding.encoder.as_ref(),
        live.binding.interpreter.as_ref(),
        &live.binding.endpoint_ref,
        live.binding.credential_ref.as_deref(),
        &input,
        &config,
        Arc::clone(&cancel),
        deadline,
    )
    .await;

    for unit in &outcome.units {
        let _ = tokio::time::timeout(
            Duration::from_millis(50),
            publish_tx.send(EventPublisherCommand::Publish(Box::new(
                TransactionEventPayload::CanonicalUnit(unit.clone()),
            ))),
        )
        .await;
    }

    TerminalProposal::new(outcome.terminal)
}
