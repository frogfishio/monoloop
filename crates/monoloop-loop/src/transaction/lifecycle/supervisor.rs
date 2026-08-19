//! Supervisor command loop and terminal authority (v2 §10 / §13).
//!
//! Start commands use a dedicated bounded queue (capacity ≥ max_active).
//! Cancel / shutdown use a separate control queue so control cannot be starved
//! when the start queue is full.

use super::ledger::{LifecycleLedger, TransactionPhase};
use super::task_supervisor::{TaskClass, TaskSupervisor};
use super::terminal::{build_completion, end_event, TerminalDecision};
use monoloop_contracts::{
    CleanupStatus, CompletionPublishResult, ShutdownReport, ShutdownSnapshot,
    TerminalEventDelivery, TransactionEndKind, TransactionId,
};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, Notify};

/// Start-queue commands (admission only).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartCommand {
    /// Start an admitted queued transaction.
    Start(TransactionId),
}

/// Control-queue commands (cancel / shutdown). Never share the start queue.
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

/// Backward-compatible name for docs / exports (control + start vocabulary).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SupervisorCommand {
    /// Start an admitted queued transaction.
    Start(TransactionId),
    /// Request cooperative cancel.
    Cancel(TransactionId),
    /// Request forced terminate.
    ForceTerminate(TransactionId),
    /// Begin shutdown generation (idempotent).
    BeginShutdown,
    /// Driver thread asks the supervisor to stop after quiesce work.
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
    /// Admission → supervisor start queue (capacity = max_active).
    pub start_tx: mpsc::Sender<StartCommand>,
    /// Cancel / shutdown control queue (never shares start capacity).
    pub control_tx: mpsc::Sender<ControlCommand>,
    /// Wakes the supervisor when Quiescing is set even if control send fails.
    pub wake: Notify,
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
) {
    let mut tasks = TaskSupervisor::new();
    let mut stopping = false;

    loop {
        // Prefer reaping finished tasks without waiting when busy.
        for (_id, class, _exit) in tasks.try_reap_finished() {
            if let TaskClass::TransactionCoordinator(tx) = class {
                finish_cleanup_if_pending(&shared, &tx);
            }
        }

        // Quiescing may be set by the owner even when control try_send fails.
        if !stopping && shared.state.load(Ordering::SeqCst) == STATE_QUIESCING {
            begin_shutdown_inner(&shared, &mut tasks, &mut stopping);
        }

        if stopping
            && shared.ledger.lock().map(|l| l.is_empty()).unwrap_or(true)
            && tasks.is_empty()
        {
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
                if let Some((
                    _id,
                    TaskClass::TransactionCoordinator(tx),
                    _exit,
                )) = finished
                {
                    finish_cleanup_if_pending(&shared, &tx);
                }
            }
            ctrl = control_rx.recv() => {
                match ctrl {
                    None => {
                        begin_shutdown_inner(&shared, &mut tasks, &mut stopping);
                        tasks.abort_all();
                    }
                    Some(ControlCommand::Cancel(tx)) => {
                        request_cancel(&shared, &mut tasks, tx, false);
                    }
                    Some(ControlCommand::ForceTerminate(tx)) => {
                        request_cancel(&shared, &mut tasks, tx, true);
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
            _ = shared.wake.notified() => {
                if shared.state.load(Ordering::SeqCst) == STATE_QUIESCING {
                    begin_shutdown_inner(&shared, &mut tasks, &mut stopping);
                }
            }
            start = start_rx.recv() => {
                match start {
                    None => {
                        // Start queue closed — do not treat as shutdown by itself.
                    }
                    Some(StartCommand::Start(tx)) => {
                        // Reject starting new work once quiescing.
                        if shared.state.load(Ordering::SeqCst) != STATE_ACCEPTING {
                            // Leave the ledger entry for shutdown finalization.
                            continue;
                        }
                        handle_start(&shared, &mut tasks, tx);
                    }
                }
            }
        }
    }
}

fn handle_start(shared: &Arc<RuntimeShared>, tasks: &mut TaskSupervisor, tx: TransactionId) {
    let cancel = {
        let mut ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
        let Some(entry) = ledger.get_mut(&tx) else {
            return;
        };
        if entry.phase != TransactionPhase::Queued {
            return;
        }
        // M2: no Connector/Interpreter yet — park in Running until cancel/shutdown.
        entry.phase = TransactionPhase::Running;
        std::sync::Arc::clone(&entry.resources.cancel)
    };
    tasks.spawn(TaskClass::TransactionCoordinator(tx), async move {
        cancel.notified().await;
    });
    let _ = shared;
}

fn request_cancel(
    shared: &Arc<RuntimeShared>,
    tasks: &mut TaskSupervisor,
    tx: TransactionId,
    force: bool,
) {
    let kind = if force {
        TransactionEndKind::Terminated
    } else {
        TransactionEndKind::Cancelled
    };
    {
        let mut ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
        let Some(entry) = ledger.get_mut(&tx) else {
            return;
        };
        if entry.terminal.is_some() {
            return;
        }
        entry.phase = TransactionPhase::Cancelling;
        entry.resources.cancel.notify_waiters();
        entry.terminal = Some(TerminalDecision::new(kind));
        entry.phase = TransactionPhase::Finalizing;
    }
    tasks.abort_transaction(&tx);
    publish_and_tombstone(shared, tx, kind);
    if tasks.tasks_for(&tx).is_empty() {
        finish_cleanup_if_pending(shared, &tx);
    }
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
        let should_publish = {
            let mut ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
            let Some(entry) = ledger.get_mut(&tx) else {
                continue;
            };
            if entry.terminal.is_some() {
                false
            } else {
                entry.terminal = Some(TerminalDecision::new(TransactionEndKind::RuntimeShutdown));
                entry.phase = TransactionPhase::Finalizing;
                entry.resources.cancel.notify_waiters();
                true
            }
        };
        tasks.abort_transaction(&tx);
        if should_publish {
            shared
                .runtime_shutdown_terminals
                .fetch_add(1, Ordering::SeqCst);
            publish_and_tombstone(shared, tx, TransactionEndKind::RuntimeShutdown);
        }
        if tasks.tasks_for(&tx).is_empty() {
            finish_cleanup_if_pending(shared, &tx);
        }
    }
    tasks.abort_all();
}

fn publish_and_tombstone(shared: &Arc<RuntimeShared>, tx: TransactionId, kind: TransactionEndKind) {
    let (delivery, channel_id, session_id, seq) = {
        let mut ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
        let Some(entry) = ledger.get_mut(&tx) else {
            return;
        };
        let delivery = entry.delivery.take();
        let channel_id = entry.channel_id.clone();
        let session_id = entry.session_key.as_ref().map(|k| k.session_id.clone());
        let seq = entry.event_sequence.saturating_add(1);
        entry.event_sequence = seq;
        entry.phase = TransactionPhase::CleanupPending;
        (delivery, channel_id, session_id, seq)
    };

    let Some(delivery) = delivery else {
        shared
            .completions_invariant_failed
            .fetch_add(1, Ordering::SeqCst);
        // Still remove to avoid permanent ledger leak when delivery missing.
        let mut ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
        let _ = ledger.remove(&tx);
        return;
    };

    let end = end_event(tx, channel_id, session_id, kind, seq);
    // M2: skip event stream Ended enqueue details; record delivery as Published
    // when completion send is attempted (event path fills in M3).
    let terminal_event_delivery = TerminalEventDelivery::Published;
    let completion = build_completion(
        end,
        terminal_event_delivery,
        CleanupStatus::Pending {
            owned_tasks: 1,
            owned_processes: 0,
            cooperative_tools: 0,
        },
    );
    match delivery.completion_tx.send(completion) {
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

    // If coordinator already gone, finish cleanup now; else wait for join.
    // Reservations release on ledger remove.
}

fn finish_cleanup_if_pending(shared: &Arc<RuntimeShared>, tx: &TransactionId) {
    let mut ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
    let Some(entry) = ledger.get(tx) else {
        return;
    };
    if matches!(
        entry.phase,
        TransactionPhase::CleanupPending | TransactionPhase::Finalizing
    ) {
        let _ = ledger.remove(tx);
    }
}

/// Wait helper used by owner: poll until Stopped or deadline.
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
