//! Supervisor command loop and terminal authority (v2 §10 / §13).
//!
//! Start commands use a dedicated bounded queue (capacity ≥ max_active).
//! Cancel / shutdown use a separate control queue. WorkerExited uses a third
//! worker queue so coordinators cannot starve start or control.

use super::coordinator::{run_coordinator, CoordinatorParams, WorkerMessage};
use super::event_publisher::run_event_publisher;
use super::ledger::{LifecycleLedger, TransactionPhase};
use super::task_spawner::{SpawnRequest, TransactionTaskSpawner};
use super::task_supervisor::{TaskClass, TaskExit, TaskSupervisor};
use super::terminal::{build_completion, end_event, TerminalDecision, TerminalProposal};
use crate::transaction::bootstrap::{
    ControlHoldGate, FinalizerHoldGate, JoinOnlySpillInject, StartHoldGate, StoppedGate,
};
use crate::transaction::channel_registry::LiveChannel;
use crate::transaction::dispatcher::OrphanToolPermitSet;
use crate::transaction::host_tools::HostToolRegistry;
use crate::transaction::mcp::{McpGatewayHandle, PreparedMcpGateway};
use crate::transaction::tool_capacity::SharedToolCapacity;
use monoloop_contracts::{
    ChannelId, CleanupStatus, CompletionPublishResult, ShutdownReport, ShutdownSnapshot,
    TerminalEventDelivery, TransactionEndKind, TransactionId,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Notify};
use tokio_util::sync::CancellationToken;

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
    /// Independent budget for terminal `Ended` enqueue (spec §8 / D-047).
    pub terminal_event_delivery_deadline: Duration,
    pub task_spawner: TransactionTaskSpawner,
    pub shutdown_generation: AtomicU64,
    pub shutdown_report: Mutex<Option<ShutdownReport>>,
    pub completions_published: AtomicU64,
    pub completions_receiver_dropped: AtomicU64,
    pub completions_invariant_failed: AtomicU64,
    pub runtime_shutdown_terminals: AtomicU64,
    /// Live TaskSupervisor task count (updated by supervisor loop).
    pub owned_tasks: AtomicU32,
    /// When true, supervisor owns a loopback MCP gateway as RuntimeService (D-043 / §17).
    pub enable_mcp_listener: bool,
    /// Bound address once the MCP gateway task reports ready.
    pub mcp_listen_addr: Mutex<Option<std::net::SocketAddr>>,
    /// Cloneable MCP install/activate handle while the RuntimeService is live.
    pub mcp_gateway: Mutex<Option<McpGatewayHandle>>,
    /// Cancels the MCP axum serve future on quiesce.
    pub mcp_cancel: Mutex<Option<CancellationToken>>,
    /// When `Some`, defer drain-complete until the gate is released (§22.5).
    pub block_stopped: Option<Arc<StoppedGate>>,
    /// When `Some` and held, supervisor does not drain the start queue (D-040).
    pub hold_start: Option<Arc<StartHoldGate>>,
    /// When `Some` and held, supervisor does not drain the control queue (§23).
    pub hold_control: Option<Arc<ControlHoldGate>>,
    /// When `Some`, Finalizer waits after Seal before completion send (§22.2).
    pub hold_finalizer_after_seal: Option<Arc<FinalizerHoldGate>>,
    /// When `Some`, executor thread waits after drain before teardown (D-049).
    pub hold_executor_teardown: Option<Arc<StoppedGate>>,
    /// When `Some`, spawn a never-awaiting RuntimeService that signals before park.
    pub inject_non_yielding_service: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// When `Some`, register TaskSupervisor-owned JoinOnly-style work at start.
    pub inject_join_only_spill: Option<Arc<JoinOnlySpillInject>>,
    /// Supervisor finished drain; OS thread may still be in executor teardown (D-049).
    /// Public `Stopped` is published by the owner only after thread join.
    pub drain_complete: std::sync::atomic::AtomicBool,
    /// Host tool definitions available to admission / coordinators.
    pub tools_registry: HostToolRegistry,
    /// Process-wide concurrent tool execution budget.
    pub shared_tool_capacity: Arc<SharedToolCapacity>,
    /// Runtime-scoped unfinished tool joins/permits (Law 8 — not process-global).
    pub tool_spill: Arc<OrphanToolPermitSet>,
    /// Live ProcessIsolated OS children (§18.2 ShutdownSnapshot.owned_processes).
    pub owned_processes: Arc<AtomicU32>,
    /// ProcessIsolated children retained until OS exit is observed (D-048).
    pub process_registry: Arc<crate::transaction::owned_process_registry::OwnedProcessRegistry>,
    /// Runtime admission / capacity limits (D-035 input accounting uses these).
    pub transaction_limits: monoloop_contracts::TransactionLimits,
}

impl RuntimeShared {
    fn start_drain_enabled(&self) -> bool {
        !self.hold_start.as_ref().is_some_and(|g| g.is_held())
    }

    fn control_drain_enabled(&self) -> bool {
        !self.hold_control.as_ref().is_some_and(|g| g.is_held())
    }

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
        let mcp_routes = self
            .mcp_gateway
            .lock()
            .ok()
            .and_then(|g| g.as_ref().map(|h| h.routes().len() as u32))
            .unwrap_or(0);
        // Prefer registry live count (D-048); fall back to lease counter.
        let owned_processes =
            self.process_registry
                .live_count()
                .max(self.owned_processes.load(Ordering::SeqCst) as usize) as u32;
        ShutdownSnapshot {
            generation: self.shutdown_generation.load(Ordering::SeqCst),
            ledger_entries,
            owned_tasks: self.owned_tasks.load(Ordering::SeqCst),
            owned_processes,
            mcp_routes,
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
    mcp_prepared: Option<PreparedMcpGateway>,
) {
    let mut tasks = TaskSupervisor::new();
    let mut stopping = false;
    let mut quiesce_started: Option<tokio::time::Instant> = None;

    if let Some(prepared) = mcp_prepared {
        // Handle/addr already published before start ready (§7.1).
        let serve = super::mcp_listener::serve_runtime_mcp(Arc::clone(&shared), prepared);
        tasks.spawn(TaskClass::RuntimeService, serve);
    }
    // §22.3 sacrificial: never-awaiting future pins one worker; abort cannot
    // join it, so shutdown must stay Quiescing (never false Stopped).
    if let Some(entered) = shared.inject_non_yielding_service.clone() {
        tasks.spawn(TaskClass::RuntimeService, async move {
            // Signal before park so the harness does not shut down while the
            // task is still only registered (abort-before-poll would join cleanly
            // and falsely allow Stopped).
            entered.store(true, Ordering::SeqCst);
            // Park the worker thread with no `.await` — Tokio abort cannot stop
            // this task (spin would burn a core for the same proof).
            loop {
                std::thread::park();
            }
        });
    }
    // §22.4 / Law 23 / M5.4: JoinOnly-style work under TaskSupervisor (not spill,
    // not ambient tokio::spawn). Park the worker thread so abort cannot join
    // until release() unparks — same abort-resistance as §22.3 sacrificial.
    if let Some(inject) = shared.inject_join_only_spill.clone() {
        tasks.spawn(TaskClass::RuntimeService, async move {
            inject.store_parked_thread(std::thread::current());
            inject.mark_entered();
            loop {
                if inject.is_released() {
                    break;
                }
                std::thread::park();
            }
        });
    }
    shared
        .owned_tasks
        .store(tasks.registered_count() as u32, Ordering::SeqCst);

    loop {
        // Authoritative WorkerExited proposals before reaping coordinators, so
        // on_task_exit does not invent a terminal over a queued proposal.
        while let Ok(msg) = worker_rx.try_recv() {
            let WorkerMessage::WorkerExited {
                transaction_id,
                proposal,
            } = msg;
            accept_terminal(&shared, &mut tasks, transaction_id, proposal, false);
        }

        // Preferential control drain (D-039): process BeginShutdown/StopSupervisor
        // and Cancels without waiting behind a biased select recv, so a terminate
        // flood cannot delay observing Quiescing. Skipped while `hold_control`
        // is held so §23 max_actor_commands plus-one can fill the bound.
        if shared.control_drain_enabled() {
            while let Ok(cmd) = control_rx.try_recv() {
                match cmd {
                    ControlCommand::Cancel(tx) => {
                        accept_terminal(
                            &shared,
                            &mut tasks,
                            tx,
                            TerminalProposal::new(TransactionEndKind::Cancelled),
                            false,
                        );
                    }
                    ControlCommand::ForceTerminate(tx) => {
                        accept_terminal(
                            &shared,
                            &mut tasks,
                            tx,
                            TerminalProposal::new(TransactionEndKind::Terminated),
                            true,
                        );
                    }
                    ControlCommand::BeginShutdown => {
                        begin_shutdown_inner(&shared, &mut tasks, &mut stopping);
                    }
                    ControlCommand::StopSupervisor => {
                        begin_shutdown_inner(&shared, &mut tasks, &mut stopping);
                        tasks.abort_all();
                    }
                }
            }
        }

        for (_id, class, exit) in tasks.try_reap_finished() {
            on_task_exit(&shared, &mut tasks, &class, exit);
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
            // Release orphan tool permits (capacity); joins are TaskSupervisor-owned.
            let _ = shared.tool_spill.shutdown_progress();
            // D-048: kill+poll ProcessIsolated children; do not clear without reap.
            let _ = shared.process_registry.shutdown_progress();

            // Drive residual abort every lap so coordinator/tool work cannot
            // strand Finalizer → tombstone → Stopped.
            let tx_ids: Vec<TransactionId> = {
                let ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
                ledger.transaction_ids()
            };
            for tx in &tx_ids {
                tasks.abort_transaction_residuals(tx);
            }

            // Failsafe: idle rows clear only after Finalizing/CleanupPending
            // (one completion path started). Laws 8–9 + one-active key; admission
            // already closed.
            let idle: Vec<TransactionId> = tx_ids
                .iter()
                .copied()
                .filter(|tx| tasks.tasks_for(tx).is_empty())
                .collect();
            for tx in idle {
                try_remove_tombstone(&shared, &tx);
            }

            // Hard quiesce deadline (D-045 / §22.2): never abort Finalizer —
            // EventPublisher may be aborted after grace (Seal already tried or
            // stuck); clear row only when no tx tasks remain so Seal→completion
            // cannot lose the one completion attempt.
            let grace = shared.cleanup_deadline.max(Duration::from_secs(2));
            if quiesce_started.is_some_and(|t| t.elapsed() >= grace) {
                let leftover: Vec<TransactionId> = {
                    let ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
                    ledger.transaction_ids()
                };
                for tx in &leftover {
                    tasks.abort_transaction_except_finalizer(tx);
                    if tasks.tasks_for(tx).is_empty() {
                        force_remove_tombstone(&shared, tx);
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
                let WorkerMessage::WorkerExited {
                    transaction_id,
                    proposal,
                } = msg;
                accept_terminal(&shared, &mut tasks, transaction_id, proposal, false);
            }
            // Ledger may have been re-populated if a late WorkerExited arrived.
            if shared.ledger.lock().map(|l| l.is_empty()).unwrap_or(true) {
                // M5.4: orphan permits never block Stopped after quiesce release.
                let _ = shared.tool_spill.shutdown_progress();
                let _ = shared.process_registry.shutdown_progress();
                // D-048: Stopped requires process registry empty (reaped), not
                // merely owned_processes counter zero via lease Drop.
                if tasks.is_empty() && shared.process_registry.is_empty() {
                    ready_to_stop = true;
                } else {
                    // One bounded drain; on timeout fall through to select (50ms
                    // tick) — never tight-loop abort_and_drain (§18.2 / Drop).
                    if tasks.abort_and_drain().await {
                        let _ = shared.tool_spill.shutdown_progress();
                        let _ = shared.process_registry.shutdown_progress();
                        ready_to_stop = shared.ledger.lock().map(|l| l.is_empty()).unwrap_or(true)
                            && tasks.is_empty()
                            && shared.process_registry.is_empty();
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
            // D-049: drain-complete ≠ public Stopped. Owner publishes Stopped
            // only after the executor OS thread join is observed.
            *shared
                .shutdown_report
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some(shared.final_report());
            shared.drain_complete.store(true, Ordering::SeqCst);
            shared.wake.notify_waiters();
            return;
        }

        tokio::select! {
            biased;
            // Control / worker / start before join_next so Accepting is not
            // starved when many tasks complete in a burst.
            ctrl = control_rx.recv(), if shared.control_drain_enabled() => {
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
            start = start_rx.recv(), if shared.start_drain_enabled() => {
                if let Some(StartCommand::Start(tx)) = start {
                    if shared.state.load(Ordering::SeqCst) == STATE_ACCEPTING {
                        handle_start(&shared, &mut tasks, tx);
                    } else if stopping {
                        // Late Start after Quiescing: entry was already snapshotted
                        // (or must still terminalize). Never drop a Queued admit.
                        accept_terminal(
                            &shared,
                            &mut tasks,
                            tx,
                            TerminalProposal::new(TransactionEndKind::RuntimeShutdown),
                            false,
                        );
                    }
                }
            }
            // While start/control drain is held, poll so release is observed without a notify.
            _ = tokio::time::sleep(Duration::from_millis(5)),
                if !shared.start_drain_enabled() || !shared.control_drain_enabled() => {}
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
                // Re-drain WorkerExited before inventing terminals. A coordinator
                // can try_send + complete in the same wakeup as join_next; the
                // loop-head try_recv alone does not cover that race under load.
                while let Ok(msg) = worker_rx.try_recv() {
                    let WorkerMessage::WorkerExited {
                        transaction_id,
                        proposal,
                    } = msg;
                    accept_terminal(&shared, &mut tasks, transaction_id, proposal, false);
                }
                if let Some((_id, class, exit)) = finished {
                    on_task_exit(&shared, &mut tasks, &class, exit);
                }
            }
        }
    }
}

fn on_task_exit(
    shared: &Arc<RuntimeShared>,
    tasks: &mut TaskSupervisor,
    class: &TaskClass,
    exit: TaskExit,
) {
    let Some(tx) = class.transaction_id() else {
        return;
    };

    // Recover lost WorkerExited: coordinator finished/panicked/aborted without
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
            let stashed = {
                let mut ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
                ledger
                    .get_mut(&tx)
                    .and_then(|e| e.pending_worker_proposal.take())
            };
            let proposal = if let Some(p) = stashed {
                p
            } else {
                let kind = if matches!(exit, TaskExit::Panicked) {
                    // §22.2: coordinator panic → one InvariantFailed completion.
                    TransactionEndKind::InvariantFailed
                } else if quiescing {
                    TransactionEndKind::RuntimeShutdown
                } else if cancelled || matches!(exit, TaskExit::Cancelled) {
                    TransactionEndKind::Cancelled
                } else {
                    TransactionEndKind::InvariantFailed
                };
                TerminalProposal::new(kind)
            };
            accept_terminal(shared, tasks, tx, proposal, false);
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
        selected_tools,
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
        let selected_tools = entry.tools.clone();
        (
            Arc::clone(&entry.resources.cancel),
            entry.channel_id.clone(),
            session_id,
            entry.input.clone(),
            entry.invocation_config.clone(),
            entry.session_config.clone(),
            delivery.event_tx,
            shared.default_deadline,
            selected_tools,
        )
    };

    let (pub_admit, pub_rx) = super::event_publisher::OrdinaryCmdAdmit::channel(64);
    // D-047: Seal never shares the ordinary command queue — capacity 1 priority path.
    let (seal_tx, seal_rx) = mpsc::channel::<super::event_publisher::SealCommand>(1);
    {
        let mut ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = ledger.get_mut(&tx) {
            entry.publisher_cmd_tx = Some(pub_admit.clone());
            entry.publisher_seal_tx = Some(seal_tx);
        }
    }

    let channel_id_pub = channel_id.clone();
    let session_id_pub = session_id.clone();
    let cancel_pub = Arc::clone(&cancel);
    let deadline_pub = std::time::Instant::now() + deadline;
    let admit_pub = pub_admit.clone();
    // Do not retain a Sender inside the publisher task — that prevented natural
    // channel closure after Finalizer took the seal sender (D-047).
    tasks.spawn(TaskClass::EventPublisher(tx), async move {
        let _ = run_event_publisher(
            tx,
            channel_id_pub,
            session_id_pub,
            event_tx,
            pub_rx,
            admit_pub,
            seal_rx,
            cancel_pub,
            deadline_pub,
        )
        .await;
    });

    let mcp_gateway = shared.mcp_gateway.lock().ok().and_then(|g| g.clone());
    let params = CoordinatorParams {
        transaction_id: tx,
        cancel,
        channel_id,
        session_id,
        input,
        invocation_config,
        session_config,
        channels: Arc::clone(&shared.channels),
        publish_tx: pub_admit,
        worker_tx: shared.worker_tx.clone(),
        tasks: shared.task_spawner.clone(),
        deadline,
        cleanup_deadline: shared.cleanup_deadline,
        selected_tools,
        tools_registry: shared.tools_registry.clone(),
        shared_tool_capacity: Arc::clone(&shared.shared_tool_capacity),
        tool_spill: Arc::clone(&shared.tool_spill),
        owned_processes: Arc::clone(&shared.owned_processes),
        process_registry: Arc::clone(&shared.process_registry),
        mcp_gateway,
        shared: Arc::clone(shared),
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
    let (first_decision, seal_tx, kind) = {
        let mut ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
        let Some(entry) = ledger.get_mut(&tx) else {
            return;
        };
        let first = entry.terminal.is_none();
        entry.pending_worker_proposal = None;
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
        // Take Seal sender so Finalizer owns the only remaining publisher control.
        (first, entry.publisher_seal_tx.take(), kind)
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
        // Kind is re-read at Seal time so Cancel→Terminated upgrade can win (§22.2).
        let _ = kind;
        tasks.spawn(TaskClass::Finalizer(tx), async move {
            finalize_after_terminal(shared2, tx, seal_tx).await;
        });
    }
}

async fn finalize_after_terminal(
    shared: Arc<RuntimeShared>,
    tx: TransactionId,
    seal_tx: Option<mpsc::Sender<super::event_publisher::SealCommand>>,
) {
    // Brief yield so a racing ForceTerminate can upgrade Cancelled → Terminated
    // in the ledger before we snapshot the kind (§22.2).
    tokio::task::yield_now().await;

    // Take completion sender *before* Seal so hard-grace / force_remove cannot
    // strand the one completion attempt after the terminal-event try (§22.2).
    let (channel_id, session_id, kind, completion_tx) = {
        let mut ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
        let Some(entry) = ledger.get_mut(&tx) else {
            return;
        };
        let kind = entry
            .terminal
            .as_ref()
            .map(|t| t.kind)
            .unwrap_or(TransactionEndKind::InvariantFailed);
        if entry.completion_tx.is_none() {
            if let Some(delivery) = entry.delivery.take() {
                entry.completion_tx = Some(delivery.completion_tx);
                drop(delivery.event_tx);
            }
        }
        entry.phase = TransactionPhase::CleanupPending;
        // Close ordinary admission before Seal so parked/new sends cannot cross
        // the fence after the publisher begins draining (D-047 linearization).
        if let Some(admit) = entry.publisher_cmd_tx.take() {
            admit.close();
        }
        (
            entry.channel_id.clone(),
            entry.session_key.as_ref().map(|k| k.session_id.clone()),
            kind,
            entry.completion_tx.take(),
        )
    };

    // D-041: never-attempted is not Published (spec §6.4).
    let mut terminal_delivery = TerminalEventDelivery::NotAttempted;
    let mut last_seq = 0u64;
    let mut kind = kind;
    if let Some(seal_tx) = seal_tx {
        let (reply_tx, reply_rx) = oneshot::channel();
        let terminal = end_event(tx, channel_id.clone(), session_id.clone(), kind, 0);
        // Dedicated seal channel (cap 1): not blocked by a full ordinary cmd queue.
        // One authoritative terminal-delivery Instant for Finalizer + publisher
        // (`terminal_event_delivery_deadline` exactly — never cleanup/tx deadline,
        // never a silent floor).
        let seal_budget = shared.terminal_event_delivery_deadline;
        let seal_deadline = std::time::Instant::now() + seal_budget;
        match seal_tx.try_send(super::event_publisher::SealCommand {
            terminal,
            reply: reply_tx,
            deadline: seal_deadline,
        }) {
            Ok(()) => {
                // Publisher uses the same Instant. Small reply slack only — does
                // not extend the configured terminal delivery budget itself.
                let wait_budget = seal_budget.saturating_add(Duration::from_millis(100));
                match tokio::time::timeout(wait_budget, reply_rx).await {
                    Ok(Ok(res)) => {
                        terminal_delivery = res.delivery;
                        last_seq = res.last_sequence;
                    }
                    _ => {
                        // Publisher must have timed out or died; do not leave a
                        // path where Ended can still publish after completion.
                        terminal_delivery = TerminalEventDelivery::DeadlineExceeded;
                    }
                }
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                terminal_delivery = TerminalEventDelivery::QueueClosed;
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                // Prior Seal already in flight (should not happen with take()).
                terminal_delivery = TerminalEventDelivery::DeadlineExceeded;
            }
        }
    }

    // D-047: sticky ordinary/establish publication failure must not report
    // Completed / ContinuationRequired with a truncated event stream.
    if matches!(
        terminal_delivery,
        TerminalEventDelivery::LimitExceeded
            | TerminalEventDelivery::DeadlineExceeded
            | TerminalEventDelivery::QueueClosed
    ) && matches!(
        kind,
        TransactionEndKind::Completed | TransactionEndKind::ContinuationRequired
    ) {
        kind = TransactionEndKind::EventDeliveryFailed;
        if let Ok(mut ledger) = shared.ledger.lock() {
            if let Some(entry) = ledger.get_mut(&tx) {
                entry.terminal = Some(TerminalDecision::new(kind));
            }
        }
    }

    // §22.2 test gate: hold after Seal so shutdown cannot drop completion.
    if let Some(gate) = shared.hold_finalizer_after_seal.as_ref() {
        gate.wait_released().await;
    }

    // Best-effort: refresh sequence on the ledger row if it still exists.
    if let Ok(mut ledger) = shared.ledger.lock() {
        if let Some(entry) = ledger.get_mut(&tx) {
            entry.event_sequence = last_seq;
        }
    }

    let end = end_event(tx, channel_id, session_id, kind, last_seq);
    // Snapshot live ownership at completion publish (§18.2 honesty — no hardcodes).
    let owned_tasks = shared.owned_tasks.load(Ordering::SeqCst);
    let owned_processes = shared.owned_processes.load(Ordering::SeqCst);
    let cooperative_tools = shared.tool_spill.pending_count() as u32;
    let completion = build_completion(
        end,
        terminal_delivery,
        CleanupStatus::Pending {
            owned_tasks,
            owned_processes,
            cooperative_tools,
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
    // §18.2 / D-010: flip Quiescing and snapshot ids under the admit install lock.
    let ids = {
        let ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
        let _ = shared.state.compare_exchange(
            STATE_ACCEPTING,
            STATE_QUIESCING,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        if shared.state.load(Ordering::SeqCst) == STATE_STARTING {
            shared.state.store(STATE_QUIESCING, Ordering::SeqCst);
        }
        ledger.transaction_ids()
    };
    *stopping = true;
    // Revoke MCP routes + cancel axum serve so RuntimeService can join (§17).
    super::mcp_listener::signal_mcp_shutdown(shared);
    // Wake any other runtime-wide waiters (D-043).
    shared.wake.notify_waiters();

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

/// Wait until supervisor drain is complete (D-049) or the deadline elapses.
///
/// Does **not** mean public `Stopped` — the owner must still join the executor
/// OS thread before publishing `STATE_STOPPED`.
pub(crate) async fn wait_until_drain_complete(
    shared: &Arc<RuntimeShared>,
    deadline: Duration,
) -> Result<(), monoloop_contracts::ShutdownWaitOutcome> {
    let start = tokio::time::Instant::now();
    loop {
        if shared.state.load(Ordering::SeqCst) == STATE_STOPPED
            || shared.drain_complete.load(Ordering::SeqCst)
        {
            return Ok(());
        }
        if start.elapsed() >= deadline {
            return Err(monoloop_contracts::ShutdownWaitOutcome::TimedOut(
                shared.snapshot(),
            ));
        }
        // D-039: while Quiescing, re-send BeginShutdown + wake so a dropped
        // control command or missed notify cannot strand shutdown.
        if shared.state.load(Ordering::SeqCst) == STATE_QUIESCING {
            let _ = shared.control_tx.try_send(ControlCommand::BeginShutdown);
            shared.wake.notify_waiters();
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
