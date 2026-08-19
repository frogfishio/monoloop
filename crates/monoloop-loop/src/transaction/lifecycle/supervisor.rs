//! Supervisor command loop and terminal authority (v2 §10 / §13).
//!
//! Start commands use a dedicated bounded queue (capacity ≥ max_active).
//! Cancel / shutdown use a separate control queue. WorkerExited uses a third
//! worker queue so coordinators cannot starve start or control.

use super::coordinator::{run_coordinator, CoordinatorParams, WorkerMessage};
use super::event_publisher::{run_event_publisher, EventPublisherCommand};
use super::ledger::{LifecycleLedger, TransactionPhase};
use super::task_supervisor::{TaskClass, TaskSupervisor};
use super::terminal::{build_completion, end_event, TerminalDecision, TerminalProposal};
use crate::transaction::channel_registry::LiveChannel;
use monoloop_contracts::{
    ChannelId, CleanupStatus, CompletionPublishResult, ShutdownReport, ShutdownSnapshot,
    TerminalEventDelivery, TransactionEndKind, TransactionId,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Notify};

/// Start-queue commands (admission only).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartCommand {
    /// Start an admitted queued transaction.
    Start(TransactionId),
}

/// Control-queue commands (cancel / shutdown).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlCommand {
    /// Request cooperative cancel.
    Cancel(TransactionId),
    /// Request forced terminate.
    ForceTerminate(TransactionId),
    /// Begin shutdown generation (idempotent).
    BeginShutdown,
    /// Driver thread asks the supervisor to stop after quiesce work.
    StopSupervisor,
}

/// Backward-compatible aggregate name for exports.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SupervisorCommand {
    /// Start.
    Start(TransactionId),
    /// Cancel.
    Cancel(TransactionId),
    /// Force terminate.
    ForceTerminate(TransactionId),
    /// Begin shutdown.
    BeginShutdown,
    /// Stop supervisor.
    StopSupervisor,
}

pub(crate) const STATE_STARTING: u8 = 0;
pub(crate) const STATE_ACCEPTING: u8 = 1;
pub(crate) const STATE_QUIESCING: u8 = 2;
pub(crate) const STATE_STOPPED: u8 = 3;

/// Shared runtime state visible to admission handle and owner.
pub(crate) struct RuntimeShared {
    pub state: AtomicU8,
    pub ledger: Mutex<LifecycleLedger>,
    pub start_tx: mpsc::Sender<StartCommand>,
    pub control_tx: mpsc::Sender<ControlCommand>,
    pub worker_tx: mpsc::Sender<WorkerMessage>,
    pub wake: Notify,
    pub channels: Arc<HashMap<ChannelId, LiveChannel>>,
    pub default_deadline: Duration,
    pub shutdown_generation: AtomicU64,
    pub shutdown_report: Mutex<Option<ShutdownReport>>,
    pub completions_published: AtomicU64,
    pub completions_receiver_dropped: AtomicU64,
    pub completions_invariant_failed: AtomicU64,
    pub runtime_shutdown_terminals: AtomicU64,
}

impl RuntimeShared {
    pub fn runtime_state(&self) -> super::super::state::RuntimeState {
        match self.state.load(Ordering::SeqCst) {
            STATE_ACCEPTING => super::super::state::RuntimeState::Accepting,
            STATE_QUIESCING => super::super::state::RuntimeState::Quiescing,
            STATE_STOPPED => super::super::state::RuntimeState::Stopped,
            _ => super::super::state::RuntimeState::Starting,
        }
    }

    pub fn snapshot(&self, tasks: u32) -> ShutdownSnapshot {
        let ledger_entries = self.ledger.lock().map(|l| l.len()).unwrap_or(0) as u32;
        ShutdownSnapshot {
            generation: self.shutdown_generation.load(Ordering::SeqCst),
            ledger_entries,
            owned_tasks: tasks,
            owned_processes: 0,
            mcp_routes: 0,
            completions_published: self.completions_published.load(Ordering::SeqCst),
        }
    }

    pub fn final_report(&self) -> ShutdownReport {
        ShutdownReport {
            completions_published: self.completions_published.load(Ordering::SeqCst),
            completions_receiver_dropped: self.completions_receiver_dropped.load(Ordering::SeqCst),
            completions_invariant_failed: self.completions_invariant_failed.load(Ordering::SeqCst),
            runtime_shutdown_terminals: self.runtime_shutdown_terminals.load(Ordering::SeqCst),
        }
    }
}

/// Run the supervisor until stopped invariants hold.
pub(crate) async fn run_supervisor(
    shared: Arc<RuntimeShared>,
    mut start_rx: mpsc::Receiver<StartCommand>,
    mut control_rx: mpsc::Receiver<ControlCommand>,
    mut worker_rx: mpsc::Receiver<WorkerMessage>,
) {
    let mut tasks = TaskSupervisor::new();
    let mut stopping = false;

    loop {
        for (_id, class, _exit) in tasks.try_reap_finished() {
            on_task_exit(&shared, &mut tasks, &class);
        }

        if !stopping && shared.state.load(Ordering::SeqCst) == STATE_QUIESCING {
            begin_shutdown_inner(&shared, &mut tasks, &mut stopping);
        }

        if stopping && shared.ledger.lock().map(|l| l.is_empty()).unwrap_or(true) {
            if !tasks.is_empty() {
                tasks.abort_and_drain().await;
            }
            shared.state.store(STATE_STOPPED, Ordering::SeqCst);
            *shared
                .shutdown_report
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some(shared.final_report());
            return;
        }

        tokio::select! {
            biased;
            finished = tasks.join_next(), if !tasks.is_empty() => {
                if let Some((_id, class, _exit)) = finished {
                    on_task_exit(&shared, &mut tasks, &class);
                }
            }
            ctrl = control_rx.recv() => {
                match ctrl {
                    None => {
                        begin_shutdown_inner(&shared, &mut tasks, &mut stopping);
                        tasks.abort_all();
                    }
                    Some(ControlCommand::Cancel(tx)) => {
                        accept_terminal(&shared, &mut tasks, tx, TerminalProposal::new(TransactionEndKind::Cancelled), false);
                    }
                    Some(ControlCommand::ForceTerminate(tx)) => {
                        accept_terminal(&shared, &mut tasks, tx, TerminalProposal::new(TransactionEndKind::Terminated), true);
                    }
                    Some(ControlCommand::BeginShutdown) => {
                        begin_shutdown_inner(&shared, &mut tasks, &mut stopping);
                    }
                    Some(ControlCommand::StopSupervisor) => {
                        begin_shutdown_inner(&shared, &mut tasks, &mut stopping);
                        tasks.abort_all();
                    }
                }
            }
            msg = worker_rx.recv() => {
                if let Some(WorkerMessage::WorkerExited { transaction_id, proposal }) = msg {
                    accept_terminal(&shared, &mut tasks, transaction_id, proposal, false);
                }
            }
            _ = shared.wake.notified() => {
                if shared.state.load(Ordering::SeqCst) == STATE_QUIESCING {
                    begin_shutdown_inner(&shared, &mut tasks, &mut stopping);
                }
            }
            start = start_rx.recv() => {
                if let Some(StartCommand::Start(tx)) = start {
                    if shared.state.load(Ordering::SeqCst) == STATE_ACCEPTING {
                        handle_start(&shared, &mut tasks, tx);
                    }
                }
            }
        }
    }
}

fn on_task_exit(shared: &Arc<RuntimeShared>, tasks: &mut TaskSupervisor, class: &TaskClass) {
    let Some(tx) = class.transaction_id() else {
        return;
    };
    if tasks.tasks_for(&tx).is_empty() {
        try_remove_tombstone(shared, &tx);
    }
}

fn handle_start(shared: &Arc<RuntimeShared>, tasks: &mut TaskSupervisor, tx: TransactionId) {
    let (
        cancel,
        channel_id,
        session_id,
        input,
        invocation_config,
        session_config,
        event_tx,
        deadline,
    ) = {
        let mut ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
        let Some(entry) = ledger.get_mut(&tx) else {
            return;
        };
        if entry.phase != TransactionPhase::Queued {
            return;
        }
        let Some(delivery) = entry.delivery.take() else {
            return;
        };
        entry.completion_tx = Some(delivery.completion_tx);
        entry.phase = TransactionPhase::Running;
        let session_id = entry.session_key.as_ref().map(|k| k.session_id.clone());
        (
            Arc::clone(&entry.resources.cancel),
            entry.channel_id.clone(),
            session_id,
            entry.input.clone(),
            entry.invocation_config.clone(),
            entry.session_config.clone(),
            delivery.event_tx,
            shared.default_deadline,
        )
    };

    let (pub_tx, pub_rx) = mpsc::channel::<EventPublisherCommand>(64);
    {
        let mut ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = ledger.get_mut(&tx) {
            entry.publisher_cmd_tx = Some(pub_tx.clone());
        }
    }

    let pub_tx_task = pub_tx.clone();
    let channel_id_pub = channel_id.clone();
    let session_id_pub = session_id.clone();
    tasks.spawn(TaskClass::EventPublisher(tx), async move {
        let _ = run_event_publisher(tx, channel_id_pub, session_id_pub, event_tx, pub_rx).await;
        let _ = pub_tx_task;
    });

    let params = CoordinatorParams {
        transaction_id: tx,
        cancel,
        channel_id,
        session_id,
        input,
        invocation_config,
        session_config,
        channels: Arc::clone(&shared.channels),
        publish_tx: pub_tx,
        worker_tx: shared.worker_tx.clone(),
        deadline,
    };
    tasks.spawn(TaskClass::TransactionCoordinator(tx), async move {
        run_coordinator(params).await;
    });
}

fn accept_terminal(
    shared: &Arc<RuntimeShared>,
    tasks: &mut TaskSupervisor,
    tx: TransactionId,
    proposal: TerminalProposal,
    force_upgrade: bool,
) {
    let (first_decision, pub_cmd, kind) = {
        let mut ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
        let Some(entry) = ledger.get_mut(&tx) else {
            return;
        };
        let first = entry.terminal.is_none();
        if let Some(existing) = entry.terminal.as_ref() {
            if force_upgrade
                && existing.kind == TransactionEndKind::Cancelled
                && proposal.kind == TransactionEndKind::Terminated
            {
                entry.terminal = Some(TerminalDecision::new(TransactionEndKind::Terminated));
            }
        } else {
            entry.terminal = Some(TerminalDecision::new(proposal.kind));
            entry.phase = TransactionPhase::Finalizing;
        }
        entry.resources.cancel.notify_waiters();
        let kind = entry
            .terminal
            .as_ref()
            .map(|t| t.kind)
            .unwrap_or(proposal.kind);
        (first, entry.publisher_cmd_tx.clone(), kind)
    };

    // Wake coordinator; do not abort publisher until Seal is sent.
    if let Ok(mut ledger) = shared.ledger.lock() {
        if let Some(entry) = ledger.get_mut(&tx) {
            entry.resources.cancel.notify_waiters();
        }
    }

    if first_decision {
        let shared2 = Arc::clone(shared);
        tasks.spawn(TaskClass::RuntimeService, async move {
            finalize_after_terminal(shared2, tx, kind, pub_cmd).await;
        });
    }
}

async fn finalize_after_terminal(
    shared: Arc<RuntimeShared>,
    tx: TransactionId,
    kind: TransactionEndKind,
    pub_cmd: Option<mpsc::Sender<EventPublisherCommand>>,
) {
    let (channel_id, session_id) = {
        let ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
        let Some(entry) = ledger.get(&tx) else {
            return;
        };
        (
            entry.channel_id.clone(),
            entry.session_key.as_ref().map(|k| k.session_id.clone()),
        )
    };

    let mut terminal_delivery = TerminalEventDelivery::Published;
    let mut last_seq = 0u64;
    if let Some(cmd_tx) = pub_cmd {
        let (reply_tx, reply_rx) = oneshot::channel();
        let terminal = end_event(tx, channel_id.clone(), session_id.clone(), kind, 0);
        if cmd_tx
            .send(EventPublisherCommand::Seal {
                terminal,
                reply: reply_tx,
            })
            .await
            .is_ok()
        {
            if let Ok(Ok(res)) = tokio::time::timeout(Duration::from_secs(5), reply_rx).await {
                terminal_delivery = res.delivery;
                last_seq = res.last_sequence;
            } else {
                terminal_delivery = TerminalEventDelivery::DeadlineExceeded;
            }
        } else {
            terminal_delivery = TerminalEventDelivery::QueueClosed;
        }
    }

    let completion_tx = {
        let mut ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
        let Some(entry) = ledger.get_mut(&tx) else {
            return;
        };
        // Shutdown may win before Start splits delivery ports.
        if entry.completion_tx.is_none() {
            if let Some(delivery) = entry.delivery.take() {
                entry.completion_tx = Some(delivery.completion_tx);
                // Drop unused event sender — no publisher was started.
                drop(delivery.event_tx);
            }
        }
        entry.event_sequence = last_seq;
        entry.phase = TransactionPhase::CleanupPending;
        entry.completion_tx.take()
    };

    let end = end_event(tx, channel_id, session_id, kind, last_seq);
    let completion = build_completion(
        end,
        terminal_delivery,
        CleanupStatus::Pending {
            owned_tasks: 1,
            owned_processes: 0,
            cooperative_tools: 0,
        },
    );
    if let Some(sender) = completion_tx {
        match sender.send(completion) {
            CompletionPublishResult::Published => {
                shared.completions_published.fetch_add(1, Ordering::SeqCst);
            }
            CompletionPublishResult::ReceiverDropped => {
                shared.completions_published.fetch_add(1, Ordering::SeqCst);
                shared
                    .completions_receiver_dropped
                    .fetch_add(1, Ordering::SeqCst);
            }
            CompletionPublishResult::InvariantFailed => {
                shared
                    .completions_invariant_failed
                    .fetch_add(1, Ordering::SeqCst);
            }
        }
    } else {
        shared
            .completions_invariant_failed
            .fetch_add(1, Ordering::SeqCst);
    }

    // Completion published — remove tombstone once no per-tx tasks remain.
    // (RuntimeService finalize has no TransactionId, so on_task_exit alone
    // would miss this transition.)
    try_remove_tombstone(&shared, &tx);
}

fn begin_shutdown_inner(
    shared: &Arc<RuntimeShared>,
    tasks: &mut TaskSupervisor,
    stopping: &mut bool,
) {
    let _ = shared.state.compare_exchange(
        STATE_ACCEPTING,
        STATE_QUIESCING,
        Ordering::SeqCst,
        Ordering::SeqCst,
    );
    if shared.state.load(Ordering::SeqCst) == STATE_STARTING {
        shared.state.store(STATE_QUIESCING, Ordering::SeqCst);
    }
    *stopping = true;

    let ids = shared
        .ledger
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .transaction_ids();
    for tx in ids {
        let already = {
            let ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
            ledger.get(&tx).and_then(|e| e.terminal.as_ref()).is_some()
        };
        if !already {
            shared
                .runtime_shutdown_terminals
                .fetch_add(1, Ordering::SeqCst);
            accept_terminal(
                shared,
                tasks,
                tx,
                TerminalProposal::new(TransactionEndKind::RuntimeShutdown),
                false,
            );
        } else {
            // Ensure cancel wake + abort for residual work.
            if let Ok(mut ledger) = shared.ledger.lock() {
                if let Some(entry) = ledger.get_mut(&tx) {
                    entry.resources.cancel.notify_waiters();
                }
            }
            tasks.abort_transaction(&tx);
        }
    }
}

fn try_remove_tombstone(shared: &Arc<RuntimeShared>, tx: &TransactionId) {
    let mut ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
    let Some(entry) = ledger.get(tx) else {
        return;
    };
    // After completion publish, M3 synthetic path has no residual owned work
    // that requires a tombstone — remove so shutdown can reach Stopped.
    if entry.completion_tx.is_none()
        && matches!(
            entry.phase,
            TransactionPhase::CleanupPending | TransactionPhase::Finalizing
        )
    {
        let _ = ledger.remove(tx);
    }
}

/// Wait helper used by owner.
pub(crate) async fn wait_until_stopped(
    shared: &Arc<RuntimeShared>,
    deadline: Duration,
) -> monoloop_contracts::ShutdownWaitOutcome {
    let start = tokio::time::Instant::now();
    loop {
        if shared.state.load(Ordering::SeqCst) == STATE_STOPPED {
            let report = shared
                .shutdown_report
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
                .unwrap_or_else(|| shared.final_report());
            return monoloop_contracts::ShutdownWaitOutcome::Stopped(report);
        }
        if start.elapsed() >= deadline {
            return monoloop_contracts::ShutdownWaitOutcome::TimedOut(shared.snapshot(0));
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
