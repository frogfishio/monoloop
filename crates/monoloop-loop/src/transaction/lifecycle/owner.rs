//! Unique runtime owner, cloneable handle, and production start (v2 §7).

use super::super::bootstrap::RuntimeBootstrap;
use super::super::channel_registry::{ChannelBinding, LiveChannel};
use super::super::error::StartupError;
use super::super::host_tools::HostToolRegistry;
use super::super::mcp::McpGatewayHandle;
use super::super::state::RuntimeState;
use super::admission::admit;
use super::capacity::{ReservationPool, ReservationPoolError};
use super::coordinator::WorkerMessage;
use super::ledger::LifecycleLedger;
use super::mcp_listener::DEFAULT_MCP_MAX_ROUTES;
use super::shutdown::ShutdownTicket;
use super::supervisor::{
    run_supervisor, wait_until_drain_complete, ControlCommand, RuntimeShared, STATE_ACCEPTING,
    STATE_QUIESCING, STATE_STARTING, STATE_STOPPED,
};
use crate::transaction::mcp::McpGateway;
use monoloop_contracts::{
    AdmissionError, AdmissionReceipt, ChannelId, ChannelKind, ShutdownWaitOutcome,
    TerminationDisposition, TerminationMode, TransactionSelector, TransactionSubmitRequest,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle as OsJoinHandle;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Notify};

/// Unique owner of the executor, supervisor, ledger, connectors, and shutdown state.
#[must_use = "RuntimeOwner must begin_shutdown and wait_stopped until Stopped"]
pub struct RuntimeOwner {
    shared: Arc<RuntimeShared>,
    /// Dedicated OS thread that owns the Tokio runtime.
    thread: Option<OsJoinHandle<()>>,
    /// Signaled after executor `shutdown_timeout` completes (D-049).
    thread_exited: Option<oneshot::Receiver<()>>,
    pool: Arc<ReservationPool>,
    /// Realized Connector instances (owner-held; handle clones the Arc for lookup).
    channels: Arc<HashMap<ChannelId, LiveChannel>>,
}

/// Cloneable admission/control handle (no executor shutdown authority).
#[derive(Clone)]
pub struct TransactionRuntimeHandle {
    shared: Arc<RuntimeShared>,
    pool: Arc<ReservationPool>,
    max_tools: usize,
    /// Read-only channel map (same Arc owned by [`RuntimeOwner`]).
    channels: Arc<HashMap<ChannelId, LiveChannel>>,
    tools: HostToolRegistry,
}

/// Result of a successful production start handshake.
pub struct StartedRuntime {
    /// Unique owner.
    pub owner: RuntimeOwner,
    /// Cloneable control handle.
    pub handle: TransactionRuntimeHandle,
}

impl StartedRuntime {
    /// Production start: owns a dedicated multi-thread Tokio executor.
    pub fn start(bootstrap: RuntimeBootstrap) -> Result<Self, StartupError> {
        bootstrap.config.validate()?;

        // §23: TransactionLimits.max_tool_schema_bytes — fail closed at start
        // so HostToolRegistry construction with the default ceiling cannot
        // bypass a tighter runtime limit (D-056).
        let max_schema = bootstrap
            .config
            .transaction_limits
            .max_tool_schema_bytes
            .max(1);
        for spec in bootstrap.tools.specs_sorted() {
            let schema_bytes = serde_json::to_vec(spec.input_schema.as_value())
                .map(|b| b.len())
                .unwrap_or(usize::MAX);
            if schema_bytes > max_schema {
                return Err(StartupError::InvalidConfig(
                    "tool schema exceeds max_tool_schema_bytes",
                ));
            }
        }

        let mut live: HashMap<ChannelId, LiveChannel> = HashMap::new();
        let mut capacity_pairs: Vec<(ChannelId, usize)> = Vec::new();
        for (id, binding) in bootstrap.channels.iter() {
            binding.descriptor().validate()?;
            let instance = binding
                .connector_factory
                .create()
                .map_err(StartupError::from)?;
            match binding.kind {
                ChannelKind::ExternalAgent if instance.sessions.is_none() => {
                    return Err(StartupError::SessionAdapterMismatch(
                        "ExternalAgent requires SessionAdapter",
                    ));
                }
                ChannelKind::DirectLlm if instance.sessions.is_some() => {
                    return Err(StartupError::SessionAdapterMismatch(
                        "DirectLlm must not carry SessionAdapter",
                    ));
                }
                _ => {}
            }
            let channel_max = binding
                .limits
                .max_active_transactions
                .min(bootstrap.config.transaction_limits.max_active_per_channel);
            if channel_max == 0 {
                return Err(StartupError::InvalidConfig(
                    "channel max_active_transactions must be nonzero",
                ));
            }
            capacity_pairs.push((id.clone(), channel_max));
            live.insert(
                id.clone(),
                LiveChannel {
                    binding: clone_binding(binding),
                    instance,
                },
            );
        }

        let max_active = bootstrap.config.transaction_limits.max_active_transactions;
        if max_active == 0 {
            return Err(StartupError::InvalidConfig(
                "max_active_transactions must be nonzero",
            ));
        }
        let pool = ReservationPool::try_new(max_active, capacity_pairs).map_err(|e| match e {
            ReservationPoolError::ZeroGlobal => {
                StartupError::InvalidConfig("max_active_transactions must be nonzero")
            }
            ReservationPoolError::ZeroChannel => {
                StartupError::InvalidConfig("channel capacity must be nonzero")
            }
        })?;

        // Start queue: exactly max_active (spec §9.2), unless a test overrides
        // capacity to prove start-full rollback with reservation headroom (D-040).
        // Control queue is separate so cancel/shutdown cannot be starved; capacity
        // comes from `TransactionLimits.max_actor_commands` (not max_active+8).
        let start_capacity = bootstrap.config.start_queue_capacity.unwrap_or(max_active);
        let (start_tx, start_rx) = mpsc::channel(start_capacity);
        let control_capacity = bootstrap
            .config
            .transaction_limits
            .max_actor_commands
            .max(1);
        let (control_tx, control_rx) = mpsc::channel(control_capacity);
        let worker_capacity = max_active.saturating_add(8);
        let (worker_tx, worker_rx) = mpsc::channel::<WorkerMessage>(worker_capacity);
        let spawn_capacity = max_active.saturating_mul(8).max(32);
        let (task_spawner, spawn_rx) =
            super::task_spawner::TransactionTaskSpawner::channel(spawn_capacity);
        let channels = Arc::new(live);
        let shared = Arc::new(RuntimeShared {
            state: AtomicU8::new(STATE_STARTING),
            ledger: Mutex::new(LifecycleLedger::new()),
            start_tx,
            control_tx,
            worker_tx,
            wake: Notify::new(),
            channels: Arc::clone(&channels),
            default_deadline: bootstrap.config.transaction_limits.transaction_deadline,
            cleanup_deadline: bootstrap.config.transaction_limits.cleanup_deadline,
            terminal_event_delivery_deadline: bootstrap
                .config
                .transaction_limits
                .terminal_event_delivery_deadline,
            task_spawner,
            shutdown_generation: AtomicU64::new(0),
            shutdown_report: Mutex::new(None),
            completions_published: AtomicU64::new(0),
            completions_receiver_dropped: AtomicU64::new(0),
            completions_invariant_failed: AtomicU64::new(0),
            runtime_shutdown_terminals: AtomicU64::new(0),
            owned_tasks: AtomicU32::new(0),
            enable_mcp_listener: bootstrap.config.enable_mcp_listener,
            mcp_listen_addr: Mutex::new(None),
            mcp_gateway: Mutex::new(None),
            mcp_cancel: Mutex::new(None),
            block_stopped: bootstrap.config.block_stopped.clone(),
            hold_start: bootstrap.config.hold_start.clone(),
            hold_control: bootstrap.config.hold_control.clone(),
            hold_finalizer_after_seal: bootstrap.config.hold_finalizer_after_seal.clone(),
            hold_executor_teardown: bootstrap.config.hold_executor_teardown.clone(),
            inject_non_yielding_service: bootstrap.config.inject_non_yielding_service,
            inject_join_only_spill: bootstrap.config.inject_join_only_spill.clone(),
            drain_complete: std::sync::atomic::AtomicBool::new(false),
            tools_registry: bootstrap.tools.clone(),
            shared_tool_capacity: crate::transaction::tool_capacity::SharedToolCapacity::new(
                bootstrap
                    .config
                    .transaction_limits
                    .max_active_transactions
                    .saturating_mul(4)
                    .max(8),
            ),
            tool_spill: Arc::new(crate::transaction::dispatcher::OrphanToolPermitSet::new()),
            owned_processes: Arc::new(AtomicU32::new(0)),
            process_registry: Arc::new(
                crate::transaction::owned_process_registry::OwnedProcessRegistry::new(),
            ),
            transaction_limits: bootstrap.config.transaction_limits.clone(),
        });

        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<(), StartupError>>();
        let (exited_tx, exited_rx) = oneshot::channel::<()>();
        let teardown_gate = bootstrap.config.hold_executor_teardown.clone();
        let shared_thread = Arc::clone(&shared);
        let mcp_max_routes = bootstrap
            .config
            .transaction_limits
            .max_active_transactions
            .saturating_mul(2)
            .max(DEFAULT_MCP_MAX_ROUTES);
        let thread = std::thread::Builder::new()
            .name("monoloop-runtime".into())
            .spawn(move || {
                // Two workers is enough for coordinator+publisher; keep the
                // pool small so parallel integration tests do not oversubscribe.
                let rt = match tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .max_blocking_threads(2)
                    .enable_all()
                    .thread_name("monoloop-worker")
                    .build()
                {
                    Ok(rt) => rt,
                    Err(_) => {
                        let _ = ready_tx.send(Err(StartupError::ExecutorUnavailable));
                        return;
                    }
                };
                // Bind + prepare + publish MCP inside the executor before
                // Accepting so enable_mcp_listener fails closed and §7.1
                // `start` returns only after the gateway handle is ready.
                // Serve is TaskSupervisor-owned (no ambient spawn).
                rt.block_on(async {
                    let mcp_prepared = if shared_thread.enable_mcp_listener {
                        match tokio::net::TcpListener::bind("127.0.0.1:0").await {
                            Ok(listener) => {
                                let request_owner: Option<
                                    std::sync::Arc<dyn crate::transaction::mcp::McpRequestOwner>,
                                > = Some(std::sync::Arc::new(
                                    super::mcp_request_owner::SupervisedMcpRequestOwner::new(
                                        shared_thread.task_spawner.clone(),
                                    ),
                                ));
                                match McpGateway::prepare_from_tokio_listener(
                                    listener,
                                    mcp_max_routes,
                                    request_owner,
                                ) {
                                    Ok(prepared) => {
                                        super::mcp_listener::publish_runtime_mcp(
                                            &shared_thread,
                                            &prepared,
                                        );
                                        Some(prepared)
                                    }
                                    Err(_) => {
                                        let _ = ready_tx.send(Err(StartupError::InvalidConfig(
                                            "MCP gateway prepare failed",
                                        )));
                                        return;
                                    }
                                }
                            }
                            Err(_) => {
                                let _ = ready_tx.send(Err(StartupError::InvalidConfig(
                                    "MCP loopback bind failed",
                                )));
                                return;
                            }
                        }
                    } else {
                        None
                    };
                    shared_thread.state.store(STATE_ACCEPTING, Ordering::SeqCst);
                    let _ = ready_tx.send(Ok(()));
                    run_supervisor(
                        shared_thread,
                        start_rx,
                        control_rx,
                        worker_rx,
                        spawn_rx,
                        mcp_prepared,
                    )
                    .await;
                });
                // D-049: optional test gate after drain, before executor teardown.
                if let Some(gate) = teardown_gate {
                    let rt_gate = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build();
                    if let Ok(rt_gate) = rt_gate {
                        rt_gate.block_on(gate.wait_released());
                    }
                }
                // Bounded executor teardown so Drop/join cannot strand on
                // residual background work after the supervisor has returned.
                rt.shutdown_timeout(Duration::from_secs(2));
                let _ = exited_tx.send(());
            })
            .map_err(|_| StartupError::ExecutorUnavailable)?;

        ready_rx
            .recv_timeout(Duration::from_secs(5))
            .map_err(|_| StartupError::ExecutorUnavailable)??;

        let tools = bootstrap.tools;
        let max_tools = bootstrap
            .config
            .transaction_limits
            .max_tools_per_transaction;
        let handle = TransactionRuntimeHandle {
            shared: Arc::clone(&shared),
            pool: Arc::clone(&pool),
            max_tools,
            channels: Arc::clone(&channels),
            tools,
        };
        let owner = RuntimeOwner {
            shared,
            thread: Some(thread),
            thread_exited: Some(exited_rx),
            pool,
            channels,
        };
        Ok(StartedRuntime { owner, handle })
    }
}

impl RuntimeOwner {
    /// Current lifecycle state.
    pub fn state(&self) -> RuntimeState {
        self.shared.runtime_state()
    }

    /// Active ledger entry count.
    pub fn ledger_len(&self) -> usize {
        self.shared.ledger.lock().map(|l| l.len()).unwrap_or(0)
    }

    /// Supervisor-owned task count (§22.3 stopped proof).
    pub fn owned_task_count(&self) -> u32 {
        self.shared.owned_tasks.load(Ordering::SeqCst)
    }

    /// Runtime-scoped tool spill pending count (joins + orphans; §22.4 / Stopped gate).
    pub fn tool_spill_pending(&self) -> usize {
        self.shared.tool_spill.pending_count()
    }

    /// Global reservation count.
    pub fn global_reservations(&self) -> usize {
        self.pool.global_active()
    }

    /// Channel reservation count (D-040 rollback observability).
    pub fn channel_reservations(&self, channel: &ChannelId) -> usize {
        self.pool.channel_active(channel)
    }

    /// Number of realized channels owned by this runtime.
    pub fn channel_count(&self) -> usize {
        self.channels.len()
    }

    /// Bound MCP loopback address when `enable_mcp_listener` and the gateway is live.
    pub fn mcp_local_addr(&self) -> Option<std::net::SocketAddr> {
        self.shared.mcp_listen_addr.lock().ok().and_then(|g| *g)
    }

    /// Cloneable MCP gateway handle while the RuntimeService is live.
    pub fn mcp_gateway(&self) -> Option<McpGatewayHandle> {
        self.shared.mcp_gateway.lock().ok().and_then(|g| g.clone())
    }

    /// Begin shutdown (idempotent). Synchronously moves admission to Quiescing.
    ///
    /// Control delivery is best-effort on the control queue; the supervisor also
    /// observes `Quiescing` via an internal wake so a full control queue cannot
    /// strand the runtime.
    pub fn begin_shutdown(&self) -> ShutdownTicket {
        // §18.2 / D-010: Quiescing transition under the same lock admit uses for
        // install, so a concurrent admit either inserts before the flip (and is
        // visible to the shutdown snapshot) or sees Quiescing and rejects.
        {
            let _ledger = self.shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
            let _ = self.shared.state.compare_exchange(
                STATE_ACCEPTING,
                STATE_QUIESCING,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            if self.shared.state.load(Ordering::SeqCst) == STATE_STARTING {
                self.shared.state.store(STATE_QUIESCING, Ordering::SeqCst);
            }
        }
        // CAS 0→1 elects a single announcer; losers observe the published
        // generation immediately — no spin/yield race (§22.5).
        let generation = match self.shared.shutdown_generation.compare_exchange(
            0,
            1,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => 1,
            Err(existing) => existing,
        };
        let _ = self
            .shared
            .control_tx
            .try_send(ControlCommand::BeginShutdown);
        self.shared.wake.notify_waiters();
        ShutdownTicket { generation }
    }

    /// Wait until Stopped or the deadline elapses (v2: timeout ⇒ Quiescing, not false Stopped).
    ///
    /// D-049: the deadline bounds the **entire** API — including the executor OS
    /// thread join. Public `Stopped` is published only after that join.
    pub async fn wait_stopped(&mut self, deadline: Duration) -> ShutdownWaitOutcome {
        if self.shared.state.load(Ordering::SeqCst) == STATE_ACCEPTING {
            let _ = self.begin_shutdown();
        }
        if self.shared.state.load(Ordering::SeqCst) == STATE_STOPPED && self.thread.is_none() {
            let report = self
                .shared
                .shutdown_report
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
                .unwrap_or_else(|| self.shared.final_report());
            return ShutdownWaitOutcome::Stopped(report);
        }

        let start = tokio::time::Instant::now();
        // Phase 1: supervisor drain (state stays Quiescing until join).
        if let Err(timed_out) = wait_until_drain_complete(&self.shared, deadline).await {
            return timed_out;
        }

        let _ = self
            .shared
            .control_tx
            .try_send(ControlCommand::StopSupervisor);
        self.shared.wake.notify_one();

        // Phase 2: wait for executor thread exit within remaining budget.
        let remaining = deadline.saturating_sub(start.elapsed());
        if let Some(rx) = self.thread_exited.as_mut() {
            match tokio::time::timeout(remaining, &mut *rx).await {
                Ok(Ok(())) | Ok(Err(_)) => {
                    self.thread_exited = None;
                }
                Err(_) => {
                    // Retain join handle + exited receiver for a later wait.
                    return ShutdownWaitOutcome::TimedOut(self.shared.snapshot());
                }
            }
        }
        if let Some(thread) = self.thread.take() {
            // Exited signal observed — join should return promptly.
            let _ = thread.join();
        }
        self.shared.state.store(STATE_STOPPED, Ordering::SeqCst);
        let report = self
            .shared
            .shutdown_report
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .unwrap_or_else(|| self.shared.final_report());
        ShutdownWaitOutcome::Stopped(report)
    }
}

impl Drop for RuntimeOwner {
    fn drop(&mut self) {
        // Never strand Drop behind test-only hold gates.
        if let Some(gate) = self.shared.block_stopped.as_ref() {
            gate.release();
        }
        if let Some(gate) = self.shared.hold_start.as_ref() {
            gate.release();
        }
        if let Some(gate) = self.shared.hold_control.as_ref() {
            gate.release();
        }
        if let Some(gate) = self.shared.hold_finalizer_after_seal.as_ref() {
            gate.release();
        }
        if let Some(gate) = self.shared.hold_executor_teardown.as_ref() {
            gate.release();
        }
        if let Some(inject) = self.shared.inject_join_only_spill.as_ref() {
            inject.release();
        }
        if self.shared.state.load(Ordering::SeqCst) != STATE_STOPPED {
            let _ = self.begin_shutdown();
            let _ = self
                .shared
                .control_tx
                .try_send(ControlCommand::StopSupervisor);
            self.shared.wake.notify_one();
        }
        // §18.4: Drop MUST preserve ownership — join the executor OS thread.
        // MAY block indefinitely on non-cooperative in-process work. MUST NOT
        // detach, abandon a live join handle, or invent a successful stop.
        // Hosts that need bounded process-exit MUST use ProcessIsolated for
        // untrusted work and complete explicit shutdown before dropping.
        if let Some(thread) = self.thread.take() {
            // Observe the join either way — panic on the executor thread is still
            // ownership-complete (§18.4). Publish Stopped only after join (D-049).
            match thread.join() {
                Ok(()) => {
                    self.shared.state.store(STATE_STOPPED, Ordering::SeqCst);
                }
                Err(_) => {
                    self.shared.state.store(STATE_STOPPED, Ordering::SeqCst);
                }
            }
        }
        self.thread_exited = None;
    }
}

impl TransactionRuntimeHandle {
    /// Current lifecycle state.
    pub fn state(&self) -> RuntimeState {
        self.shared.runtime_state()
    }

    /// Bound MCP loopback address when the gateway RuntimeService is live.
    pub fn mcp_local_addr(&self) -> Option<std::net::SocketAddr> {
        self.shared.mcp_listen_addr.lock().ok().and_then(|g| *g)
    }

    /// Cloneable MCP gateway handle while the RuntimeService is live.
    pub fn mcp_gateway(&self) -> Option<McpGatewayHandle> {
        self.shared.mcp_gateway.lock().ok().and_then(|g| g.clone())
    }

    /// Synchronously admit a v2 transaction (no spawn / no executor wait).
    pub fn submit(
        &self,
        request: TransactionSubmitRequest,
    ) -> Result<AdmissionReceipt, AdmissionError> {
        admit(
            &self.shared,
            &self.pool,
            self.channels.as_ref(),
            &self.tools,
            self.max_tools,
            request,
        )
    }

    /// Request cancellation or forced termination.
    pub fn terminate(
        &self,
        selector: TransactionSelector,
        mode: TerminationMode,
    ) -> TerminationDisposition {
        let tx = match selector {
            TransactionSelector::Transaction(id) => id,
            TransactionSelector::Session(key) => {
                let ledger = self.shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
                match ledger.transaction_for_session(&key) {
                    Some(id) => id,
                    None => return TerminationDisposition::NotFound,
                }
            }
        };
        // Honest ledger check before enqueue (D-039): never lie Full→AlreadyTerminal.
        // §22.2: Cancelled may still be upgraded to ForceTerminate.
        {
            let ledger = self.shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
            match ledger.get(&tx) {
                None => return TerminationDisposition::NotFound,
                Some(entry) => {
                    if let Some(term) = entry.terminal.as_ref() {
                        let upgrade = matches!(mode, TerminationMode::ForceTerminate { .. })
                            && term.kind == monoloop_contracts::TransactionEndKind::Cancelled;
                        if !upgrade {
                            return TerminationDisposition::AlreadyTerminal;
                        }
                    }
                }
            }
        }
        let cmd = match mode {
            TerminationMode::Cancel { .. } => ControlCommand::Cancel(tx),
            TerminationMode::ForceTerminate { .. } => ControlCommand::ForceTerminate(tx),
        };
        match self.shared.control_tx.try_send(cmd) {
            Ok(()) => {
                self.shared.wake.notify_one();
                TerminationDisposition::Accepted
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                TerminationDisposition::ControlCapacityExceeded
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                TerminationDisposition::RuntimeClosed
            }
        }
    }
}

fn clone_binding(binding: &ChannelBinding) -> ChannelBinding {
    ChannelBinding {
        id: binding.id.clone(),
        kind: binding.kind,
        tool_mode: binding.tool_mode,
        connector_factory: Arc::clone(&binding.connector_factory),
        encoder: Arc::clone(&binding.encoder),
        interpreter: Arc::clone(&binding.interpreter),
        endpoint_ref: binding.endpoint_ref.clone(),
        credential_ref: binding.credential_ref.clone(),
        defaults: binding.defaults.clone(),
        capabilities: binding.capabilities.clone(),
        limits: binding.limits.clone(),
    }
}
