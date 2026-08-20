//! Unique runtime owner, cloneable handle, and production start (v2 §7).

use super::super::bootstrap::RuntimeBootstrap;
use super::super::channel_registry::{ChannelBinding, LiveChannel};
use super::super::error::StartupError;
use super::super::host_tools::HostToolRegistry;
use super::super::state::RuntimeState;
use super::admission::admit;
use super::capacity::{ReservationPool, ReservationPoolError};
use super::coordinator::WorkerMessage;
use super::ledger::LifecycleLedger;
use super::shutdown::ShutdownTicket;
use super::supervisor::{
    run_supervisor, wait_until_stopped, ControlCommand, RuntimeShared, STATE_ACCEPTING,
    STATE_QUIESCING, STATE_STARTING, STATE_STOPPED,
};
use monoloop_contracts::{
    AdmissionError, AdmissionReceipt, ChannelId, ChannelKind, ShutdownWaitOutcome,
    TerminationDisposition, TerminationMode, TransactionSelector, TransactionSubmitRequest,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle as OsJoinHandle;
use std::time::Duration;
use tokio::sync::{mpsc, Notify};

/// Unique owner of the executor, supervisor, ledger, connectors, and shutdown state.
#[must_use = "RuntimeOwner must begin_shutdown and wait_stopped until Stopped"]
pub struct RuntimeOwner {
    shared: Arc<RuntimeShared>,
    /// Dedicated OS thread that owns the Tokio runtime.
    thread: Option<OsJoinHandle<()>>,
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

        // Start queue: exactly max_active (spec §9.2). Control queue is separate so
        // cancel/shutdown cannot be dropped when starts fill the start queue.
        let (start_tx, start_rx) = mpsc::channel(max_active);
        let control_capacity = max_active.saturating_add(8);
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
            block_stopped: bootstrap.config.block_stopped.clone(),
        });

        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<(), StartupError>>();
        let shared_thread = Arc::clone(&shared);
        let thread = std::thread::Builder::new()
            .name("monoloop-runtime".into())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
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
                // Bind MCP before Accepting so enable_mcp_listener fails closed (D-043).
                let mcp_listener = if shared_thread.enable_mcp_listener {
                    match std::net::TcpListener::bind("127.0.0.1:0") {
                        Ok(l) => {
                            if l.set_nonblocking(true).is_err() {
                                let _ = ready_tx.send(Err(StartupError::InvalidConfig(
                                    "MCP loopback set_nonblocking failed",
                                )));
                                return;
                            }
                            Some(l)
                        }
                        Err(_) => {
                            let _ = ready_tx
                                .send(Err(StartupError::InvalidConfig("MCP loopback bind failed")));
                            return;
                        }
                    }
                } else {
                    None
                };
                shared_thread.state.store(STATE_ACCEPTING, Ordering::SeqCst);
                let _ = ready_tx.send(Ok(()));
                rt.block_on(run_supervisor(
                    shared_thread,
                    start_rx,
                    control_rx,
                    worker_rx,
                    spawn_rx,
                    mcp_listener,
                ));
                // Bounded executor teardown so Drop/join cannot strand on
                // residual background work after the supervisor has returned.
                rt.shutdown_timeout(Duration::from_secs(2));
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

    /// Global reservation count.
    pub fn global_reservations(&self) -> usize {
        self.pool.global_active()
    }

    /// Number of realized channels owned by this runtime.
    pub fn channel_count(&self) -> usize {
        self.channels.len()
    }

    /// Begin shutdown (idempotent). Synchronously moves admission to Quiescing.
    ///
    /// Control delivery is best-effort on the control queue; the supervisor also
    /// observes `Quiescing` via an internal wake so a full control queue cannot
    /// strand the runtime.
    pub fn begin_shutdown(&self) -> ShutdownTicket {
        // Move to Quiescing (idempotent).
        let _ = self.shared.state.compare_exchange(
            STATE_ACCEPTING,
            STATE_QUIESCING,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        let _ = self.shared.state.compare_exchange(
            STATE_STARTING,
            STATE_QUIESCING,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
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
    pub async fn wait_stopped(&mut self, deadline: Duration) -> ShutdownWaitOutcome {
        if self.shared.state.load(Ordering::SeqCst) == STATE_ACCEPTING {
            let _ = self.begin_shutdown();
        }
        let outcome = wait_until_stopped(&self.shared, deadline).await;
        if matches!(outcome, ShutdownWaitOutcome::Stopped(_)) {
            let _ = self
                .shared
                .control_tx
                .try_send(ControlCommand::StopSupervisor);
            self.shared.wake.notify_one();
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
        outcome
    }
}

impl Drop for RuntimeOwner {
    fn drop(&mut self) {
        // Never strand Drop behind a test-only Stopped gate.
        if let Some(gate) = self.shared.block_stopped.as_ref() {
            gate.release();
        }
        if self.shared.state.load(Ordering::SeqCst) != STATE_STOPPED {
            let _ = self.begin_shutdown();
            let _ = self
                .shared
                .control_tx
                .try_send(ControlCommand::StopSupervisor);
            self.shared.wake.notify_one();
            // May block indefinitely on non-cooperative work (v2 §18.4).
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }
}

impl TransactionRuntimeHandle {
    /// Current lifecycle state.
    pub fn state(&self) -> RuntimeState {
        self.shared.runtime_state()
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
        let cmd = match mode {
            TerminationMode::Cancel { .. } => ControlCommand::Cancel(tx),
            TerminationMode::ForceTerminate { .. } => ControlCommand::ForceTerminate(tx),
        };
        match self.shared.control_tx.try_send(cmd) {
            Ok(()) => {
                self.shared.wake.notify_one();
                TerminationDisposition::Accepted
            }
            Err(_) => TerminationDisposition::AlreadyTerminal,
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
