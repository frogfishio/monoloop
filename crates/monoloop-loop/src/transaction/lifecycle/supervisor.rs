//! Supervisor command loop and terminal authority (v2 §10 / §13).
//!
//! Start commands use a dedicated bounded queue (capacity ≥ max_active).
//! Cancel / shutdown use a separate control queue. WorkerExited uses a third
//! worker queue so coordinators cannot starve start or control.

use super::coordinator::{run_coordinator, CoordinatorParams, WorkerMessage};
use super::event_publisher::{run_event_publisher, EventPublisherCommand};
use super::ledger::{LifecycleLedger, TransactionPhase};
use super::task_spawner::{SpawnRequest, TransactionTaskSpawner};
use super::task_supervisor::{TaskClass, TaskSupervisor};
use super::terminal::{build_completion, end_event, TerminalDecision, TerminalProposal};
use crate::transaction::bootstrap::StoppedGate;
use crate::transaction::channel_registry::LiveChannel;
use monoloop_contracts::{
    ChannelId, CleanupStatus, CompletionPublishResult, ShutdownReport, ShutdownSnapshot,
    TerminalEventDelivery, TransactionEndKind, TransactionId,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
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
    pub cleanup_deadline: Duration,
    pub task_spawner: TransactionTaskSpawner,
    pub shutdown_generation: AtomicU64,
    pub shutdown_report: Mutex<Option<ShutdownReport>>,
    pub completions_published: AtomicU64,
    pub completions_receiver_dropped: AtomicU64,
    pub completions_invariant_failed: AtomicU64,
    pub runtime_shutdown_terminals: AtomicU64,
    /// Live TaskSupervisor task count (updated by supervisor loop).
    pub owned_tasks: AtomicU32,
    /// When true, supervisor owns a loopback MCP placeholder listener (D-043).
    pub enable_mcp_listener: bool,
    /// Bound address once the MCP listener task reports ready.
    pub mcp_listen_addr: Mutex<Option<std::net::SocketAddr>>,
    /// When `Some`, defer `Stopped` until the gate is released (§22.5).
    pub block_stopped: Option<Arc<StoppedGate>>,
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

    pub fn snapshot(&self) -> ShutdownSnapshot {
        let ledger_entries = self.ledger.lock().map(|l| l.len()).unwrap_or(0) as u32;
        ShutdownSnapshot {
            generation: self.shutdown_generation.load(Ordering::SeqCst),
            ledger_entries,
            owned_tasks: self.owned_tasks.load(Ordering::SeqCst),
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
    mut spawn_rx: mpsc::Receiver<SpawnRequest>,
    mcp_listener: Option<std::net::TcpListener>,
) {
    let mut tasks = TaskSupervisor::new();
    let mut stopping = false;
    let mut quiesce_started: Option<tokio::time::Instant> = None;

    if let Some(std_listener) = mcp_listener {
        let mcp_shared = Arc::clone(&shared);
        tasks.spawn(
            TaskClass::RuntimeService,
            super::mcp_listener::run_loopback_mcp_listener(mcp_shared, std_listener),
        );
    }
    shared
        .owned_tasks
        .store(tasks.registered_count() as u32, Ordering::SeqCst);

    loop {
        // Authoritative WorkerExited proposals before reaping coordinators, so
        // on_task_exit does not invent a terminal over a queued proposal.
        while let Ok(msg) = worker_rx.try_recv() {
            if let WorkerMessage::WorkerExited {
                transaction_id,
                proposal,
            } = msg
            {
                accept_terminal(&shared, &mut tasks, transaction_id, proposal, false);
            }
        }

        for (_id, class, _exit) in tasks.try_reap_finished() {
            on_task_exit(&shared, &mut tasks, &class);
        }

        // Preferential drain: never starve spawn registration behind join_next.
        while let Ok(req) = spawn_rx.try_recv() {
            let id = tasks.spawn(req.class, req.future);
            let _ = req.reply.send(id);
        }
        shared
            .owned_tasks
            .store(tasks.registered_count() as u32, Ordering::SeqCst);

        if !stopping && shared.state.load(Ordering::SeqCst) == STATE_QUIESCING {
            begin_shutdown_inner(&shared, &mut tasks, &mut stopping);
        }
        if stopping && quiesce_started.is_none() {
            quiesce_started = Some(tokio::time::Instant::now());
        }

        if stopping {
            // Drive residual abort every lap so coordinator/tool work cannot
            // strand Finalizer → tombstone → Stopped.
            let tx_ids: Vec<TransactionId> = {
                let ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
                ledger.transaction_ids()
            };
            for tx in &tx_ids {
                tasks.abort_transaction_residuals(tx);
            }

            // Failsafe: any row with no remaining tx tasks must clear while
            // quiescing. SessionKey stays reserved until tasks_for is empty
            // (LAW 7); phase may still be Running if WorkerExited was lost.
            let idle: Vec<TransactionId> = tx_ids
                .iter()
                .copied()
                .filter(|tx| tasks.tasks_for(tx).is_empty())
                .collect();
            for tx in idle {
                force_remove_tombstone(&shared, &tx);
            }

            // Hard quiesce deadline: abort leftover tx work (including Finalizer
            // after grace). Do NOT force-remove SessionKey while tasks remain.
            let grace = shared.cleanup_deadline.max(Duration::from_secs(2));
            if quiesce_started.is_some_and(|t| t.elapsed() >= grace) {
                let leftover: Vec<TransactionId> = {
                    let ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
                    ledger.transaction_ids()
                };
                for tx in &leftover {
                    tasks.abort_transaction(tx);
                }
                // Only clear rows whose tasks have already exited.
                for tx in leftover {
                    if tasks.tasks_for(&tx).is_empty() {
                        force_remove_tombstone(&shared, &tx);
                    }
                }
            }
        }

        let mut ready_to_stop = false;
        if stopping && shared.ledger.lock().map(|l| l.is_empty()).unwrap_or(true) {
            // Reject any queued spawns so workers cannot block on reply forever.
            while let Ok(req) = spawn_rx.try_recv() {
                drop(req);
            }
            while let Ok(msg) = worker_rx.try_recv() {
                if let WorkerMessage::WorkerExited {
                    transaction_id,
                    proposal,
                } = msg
                {
                    accept_terminal(&shared, &mut tasks, transaction_id, proposal, false);
                }
            }
            // Ledger may have been re-populated if a late WorkerExited arrived.
            if shared.ledger.lock().map(|l| l.is_empty()).unwrap_or(true) {
                if tasks.is_empty() {
                    ready_to_stop = true;
                } else {
                    // One bounded drain; on timeout fall through to select (50ms
                    // tick) — never tight-loop abort_and_drain (§18.2 / Drop).
                    if tasks.abort_and_drain().await {
                        ready_to_stop = shared
                            .ledger
                            .lock()
                            .map(|l| l.is_empty())
                            .unwrap_or(true)
                            && tasks.is_empty();
                    } else {
                        shared
                            .owned_tasks
                            .store(tasks.registered_count() as u32, Ordering::SeqCst);
                    }
                }
            }
        }
        if ready_to_stop {
            shared.owned_tasks.store(0, Ordering::SeqCst);
            // §22.5 test gate: hold Quiescing until release so TimedOut is deterministic.
            if let Some(gate) = shared.block_stopped.as_ref() {
                gate.wait_released().await;
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
            // Control / worker / start before join_next so Accepting is not
            // starved when many tasks complete in a burst.
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
            start = start_rx.recv() => {
                if let Some(StartCommand::Start(tx)) = start {
                    if shared.state.load(Ordering::SeqCst) == STATE_ACCEPTING {
                        handle_start(&shared, &mut tasks, tx);
                    }
                }
            }
            spawn = spawn_rx.recv() => {
                match spawn {
                    Some(req) => {
                        let id = tasks.spawn(req.class, req.future);
                        let _ = req.reply.send(id);
                    }
                    None => {
                        // Spawner dropped — treat as shutdown pressure.
                        begin_shutdown_inner(&shared, &mut tasks, &mut stopping);
                    }
                }
            }
            // While quiescing, wake periodically so grace / residual-abort
            // checks run even if join_next stalls on a non-yielding task.
            _ = tokio::time::sleep(Duration::from_millis(50)), if stopping => {}
            _ = shared.wake.notified() => {
                if shared.state.load(Ordering::SeqCst) == STATE_QUIESCING {
                    begin_shutdown_inner(&shared, &mut tasks, &mut stopping);
                }
            }
            finished = tasks.join_next(), if !tasks.is_empty() => {
                if let Some((_id, class, _exit)) = finished {
                    on_task_exit(&shared, &mut tasks, &class);
                }
            }
        }
    }
}

fn on_task_exit(shared: &Arc<RuntimeShared>, tasks: &mut TaskSupervisor, class: &TaskClass) {
    let Some(tx) = class.transaction_id() else {
        return;
    };

    // Recover lost WorkerExited: coordinator finished (or was aborted) without
    // an accepted terminal — install one so every admission gets a completion.
    if matches!(class, TaskClass::TransactionCoordinator(_)) {
        let (missing, cancelled, quiescing) = {
            let ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
            match ledger.get(&tx) {
                Some(entry) => (
                    entry.terminal.is_none(),
                    entry.resources.cancel.is_cancelled(),
                    shared.state.load(Ordering::SeqCst) == STATE_QUIESCING,
                ),
                None => (false, false, false),
            }
        };
        if missing {
            let kind = if quiescing {
                TransactionEndKind::RuntimeShutdown
            } else if cancelled {
                TransactionEndKind::Cancelled
            } else {
                TransactionEndKind::InvariantFailed
            };
            accept_terminal(
                shared,
                tasks,
                tx,
                TerminalProposal::new(kind),
                false,
            );
        }
    }

    if tasks.tasks_for(&tx).is_empty() {
        try_remove_tombstone(shared, &tx);
        return;
    }
    // Finalizer finished (published or aborted): abort residual tx-scoped work
    // so the tombstone (and SessionKey) can clear once joins are observed.
    if matches!(class, TaskClass::Finalizer(_)) {
        tasks.abort_transaction(&tx);
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
        tasks: shared.task_spawner.clone(),
        deadline,
        cleanup_deadline: shared.cleanup_deadline,
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
        entry.resources.cancel.cancel();
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
            entry.resources.cancel.cancel();
        }
    }

    if first_decision {
        let shared2 = Arc::clone(shared);
        // Tx-scoped so tombstone stays until finalizer (+ other tx tasks) exit.
        tasks.spawn(TaskClass::Finalizer(tx), async move {
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
        // try_send: never block Finalizer on a non-polling publisher (abort races).
        match cmd_tx.try_send(EventPublisherCommand::Seal {
            terminal,
            reply: reply_tx,
        }) {
            Ok(()) => {
                if let Ok(Ok(res)) = tokio::time::timeout(Duration::from_secs(2), reply_rx).await {
                    terminal_delivery = res.delivery;
                    last_seq = res.last_sequence;
                } else {
                    terminal_delivery = TerminalEventDelivery::DeadlineExceeded;
                }
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                terminal_delivery = TerminalEventDelivery::QueueClosed;
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                terminal_delivery = TerminalEventDelivery::DeadlineExceeded;
            }
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

    // Tombstone removal is deferred to `on_task_exit` once all tx-scoped tasks
    // (including this Finalizer) have exited — keeps SessionKey reserved while
    // residual work is live.
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
    // Wake MCP listener and any other runtime-wide waiters (D-043).
    shared.wake.notify_waiters();

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
            // Ensure cancel wake + abort residual work; never abort Finalizer
            // (must publish the one completion for this admission).
            if let Ok(mut ledger) = shared.ledger.lock() {
                if let Some(entry) = ledger.get_mut(&tx) {
                    entry.resources.cancel.cancel();
                }
            }
            tasks.abort_transaction_residuals(&tx);
        }
    }
}

fn try_remove_tombstone(shared: &Arc<RuntimeShared>, tx: &TransactionId) {
    let mut ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
    let Some(entry) = ledger.get(tx) else {
        return;
    };
    // Caller (`on_task_exit`) only invokes this when no tx-scoped tasks remain,
    // so removing here releases SessionKey only after residual work is gone.
    // CleanupPending: completion published. Finalizing with no tasks: Finalizer
    // never ran or was lost — still clear so Stopped is reachable (fail-closed).
    if matches!(
        entry.phase,
        TransactionPhase::CleanupPending | TransactionPhase::Finalizing
    ) {
        let _ = ledger.remove(tx);
    }
}

fn force_remove_tombstone(shared: &Arc<RuntimeShared>, tx: &TransactionId) {
    let mut ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
    let _ = ledger.remove(tx);
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
            return monoloop_contracts::ShutdownWaitOutcome::TimedOut(shared.snapshot());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
