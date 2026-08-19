//! DefaultTransactionRuntime: start, admit, terminate, shutdown.

use super::active_registry::{ActiveTransactionRegistry, ControlMessage};
use super::admission::{admit, AdmissionContext};
use super::bootstrap::RuntimeBootstrap;
use super::callback_service::CallbackService;
use super::capacity::CapacityManagers;
use super::channel_registry::{ChannelBinding, LiveChannel};
use super::error::StartupError;
use super::executor_spawn::try_spawn;
use super::finalization::build_transaction_end;
use super::host_tools::HostToolRegistry;
use super::mcp::McpGateway;
use super::spawn_gate::SpawnGate;
use super::state::RuntimeState;
use monoloop_contracts::{
    AdmissionError, AdmissionErrorKind, AdmissionReceipt, ChannelId, ChannelKind,
    EventDeliveryOutcome, Shutdown, ShutdownDisposition, TerminationDisposition, TerminationMode,
    TransactionEndKind, TransactionRequest, TransactionRuntime, TransactionSelector,
};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;

/// Startup future type.
pub type Startup = Pin<
    Box<dyn Future<Output = Result<Arc<DefaultTransactionRuntime>, StartupError>> + Send + 'static>,
>;

const STATE_ACCEPTING: u8 = 1;
const STATE_DRAINING: u8 = 2;
const STATE_STOPPED: u8 = 3;

fn decode_state(v: u8) -> RuntimeState {
    match v {
        STATE_ACCEPTING => RuntimeState::Accepting,
        STATE_DRAINING => RuntimeState::Draining,
        STATE_STOPPED => RuntimeState::Stopped,
        _ => RuntimeState::Starting,
    }
}

struct RuntimeInner {
    state: Arc<AtomicU8>,
    config: super::bootstrap::RuntimeConfig,
    channels: Arc<HashMap<ChannelId, LiveChannel>>,
    tools: HostToolRegistry,
    capacity: Arc<CapacityManagers>,
    registry: Arc<Mutex<ActiveTransactionRegistry>>,
    mcp: AsyncMutex<Option<McpGateway>>,
    /// Cloneable handle for admission/actors (None when MCP listener disabled).
    mcp_handle: Option<super::mcp::McpGatewayHandle>,
    /// Runtime-owned completion callbacks (D-021).
    callbacks: CallbackService,
    /// Injected Tokio handle for all runtime-owned spawns (D-032).
    executor: tokio::runtime::Handle,
    /// Closed at shutdown start so try_spawn fails closed (D-032).
    spawn_gate: SpawnGate,
    /// Shared shutdown result for concurrent callers (D-029).
    shutdown_disposition: AsyncMutex<Option<ShutdownDisposition>>,
}

/// Production transaction runtime.
pub struct DefaultTransactionRuntime {
    inner: Arc<RuntimeInner>,
}

impl DefaultTransactionRuntime {
    /// Only startup path.
    pub fn start(bootstrap: RuntimeBootstrap) -> Startup {
        Box::pin(async move { Self::start_inner(bootstrap).await })
    }

    async fn start_inner(bootstrap: RuntimeBootstrap) -> Result<Arc<Self>, StartupError> {
        bootstrap.config.validate()?;
        let executor = bootstrap.executor.clone();
        let _ = executor.id();

        let mut realized: Vec<(ChannelId, LiveChannel)> = Vec::new();
        let mut capacity_pairs: Vec<(ChannelId, usize)> = Vec::new();

        for (id, binding) in bootstrap.channels.iter() {
            binding.descriptor().validate()?;

            let instance = match binding.connector_factory.create() {
                Ok(i) => i,
                Err(e) => {
                    cleanup_partial(realized, None).await;
                    return Err(StartupError::from(e));
                }
            };

            match binding.kind {
                ChannelKind::DirectLlm => {
                    if instance.sessions.is_some() {
                        cleanup_partial(realized, None).await;
                        return Err(StartupError::SessionAdapterMismatch(
                            "DirectLlm must not have SessionAdapter",
                        ));
                    }
                }
                ChannelKind::ExternalAgent => {
                    if instance.sessions.is_none() {
                        cleanup_partial(realized, None).await;
                        return Err(StartupError::SessionAdapterMismatch(
                            "ExternalAgent requires SessionAdapter",
                        ));
                    }
                }
            }

            capacity_pairs.push((
                id.clone(),
                binding
                    .limits
                    .max_active_transactions
                    .min(bootstrap.config.transaction_limits.max_active_per_channel),
            ));

            realized.push((
                id.clone(),
                LiveChannel {
                    binding: clone_binding(binding),
                    instance,
                },
            ));
        }

        let (mcp, mcp_handle) = if bootstrap.config.enable_mcp_listener {
            match McpGateway::bind_loopback(256).await {
                Ok(gw) => {
                    let handle = gw.handle();
                    (Some(gw), Some(handle))
                }
                Err(_) => {
                    cleanup_partial(realized, None).await;
                    return Err(StartupError::McpBindFailed);
                }
            }
        } else {
            (None, None)
        };

        let capacity = Arc::new(CapacityManagers::new(
            bootstrap.config.transaction_limits.max_active_transactions,
            capacity_pairs,
        ));

        let spawn_gate = SpawnGate::open();
        // One concurrent callback slot per active-transaction budget (D-021).
        let callbacks = CallbackService::new(
            bootstrap
                .config
                .transaction_limits
                .max_active_transactions
                .max(1),
            bootstrap.config.transaction_limits.callback_deadline,
            executor.clone(),
            spawn_gate.clone(),
        );

        let mut channels = HashMap::with_capacity(realized.len());
        for (id, live) in realized {
            channels.insert(id, live);
        }

        Ok(Arc::new(Self {
            inner: Arc::new(RuntimeInner {
                state: Arc::new(AtomicU8::new(STATE_ACCEPTING)),
                config: bootstrap.config,
                channels: Arc::new(channels),
                tools: bootstrap.tools,
                capacity,
                registry: Arc::new(Mutex::new(ActiveTransactionRegistry::new())),
                mcp: AsyncMutex::new(mcp),
                mcp_handle,
                callbacks,
                executor,
                spawn_gate,
                shutdown_disposition: AsyncMutex::new(None),
            }),
        }))
    }

    /// Current lifecycle state.
    pub fn state(&self) -> RuntimeState {
        decode_state(self.inner.state.load(Ordering::SeqCst))
    }

    /// Tools shell.
    pub fn tools(&self) -> &HostToolRegistry {
        &self.inner.tools
    }

    /// Capacity managers.
    pub fn capacity(&self) -> &Arc<CapacityManagers> {
        &self.inner.capacity
    }

    /// Active transaction count.
    pub fn active_count(&self) -> usize {
        self.inner.registry.lock().map(|r| r.len()).unwrap_or(0)
    }

    /// Channel count.
    pub fn channel_count(&self) -> usize {
        self.inner.channels.len()
    }

    /// MCP address.
    pub async fn mcp_local_addr(&self) -> Option<std::net::SocketAddr> {
        self.inner.mcp.lock().await.as_ref().map(|m| m.local_addr())
    }

    /// Live channel lookup.
    pub fn live_channel(&self, id: &ChannelId) -> Option<&LiveChannel> {
        self.inner.channels.get(id)
    }

    async fn shutdown_inner(&self, deadline: Duration) -> ShutdownDisposition {
        // D-020: one absolute global deadline for the whole shutdown.
        let global = if deadline.is_zero() {
            self.inner.config.default_shutdown_deadline
        } else {
            deadline
        };
        let deadline_at = tokio::time::Instant::now() + global;

        // D-032: reject new spawns before draining so try_spawn fails closed.
        self.inner.spawn_gate.close();

        let prev = self.inner.state.swap(STATE_DRAINING, Ordering::SeqCst);
        if prev == STATE_STOPPED || prev == STATE_DRAINING {
            // D-029: concurrent callers must share the same complete disposition.
            // Wait for the leader to publish — never fabricate zeroed counts when
            // a local deadline expires before the leader finishes.
            loop {
                if let Some(d) = self.inner.shutdown_disposition.lock().await.clone() {
                    return d;
                }
                if self.inner.state.load(Ordering::SeqCst) == STATE_STOPPED {
                    // Leader stores disposition before STOPPED; re-read once.
                    if let Some(d) = self.inner.shutdown_disposition.lock().await.clone() {
                        return d;
                    }
                    // Ordering violated only under severe fault; still one shared value.
                    return ShutdownDisposition::default();
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        let active = {
            let mut reg = self
                .inner
                .registry
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            reg.drain_all()
        };

        // Signal all actors first (group), then join concurrently under remaining time.
        for entry in &active {
            let _ = entry.control_tx.try_send(ControlMessage::ForceTerminate);
        }

        let mut normally_finalized = 0u64;
        let mut supervisor_finalized = 0u64;
        let mut callback_failed = 0u64;
        let mut callback_aborted = 0u64;
        let mut invariant_failed = 0u64;
        let cb_cfg = self.inner.config.transaction_limits.callback_deadline;

        let n = active.len().max(1);
        let mut handles = Vec::with_capacity(active.len());
        for entry in active {
            let abort = entry.actor_join.abort_handle();
            handles.push((entry, abort));
        }

        for (entry, abort) in handles {
            let remaining = deadline_at.saturating_duration_since(tokio::time::Instant::now());
            let mut join = entry.actor_join;
            let actor_abort = entry.actor_abort.clone();
            let delivery_abort = entry.delivery_abort.clone();

            // Abort actor+delivery first so the reaper can observe child completion.
            // Then join the reaper within the absolute deadline (no yield padding).
            actor_abort.abort();
            delivery_abort.abort();

            let join_result: Option<Result<(), tokio::task::JoinError>> = if remaining.is_zero() {
                abort.abort();
                drop(join);
                None
            } else {
                let per = remaining / n as u32;
                match tokio::time::timeout(per, &mut join).await {
                    Ok(r) => Some(r),
                    Err(_) => {
                        abort.abort();
                        let join_budget =
                            deadline_at.saturating_duration_since(tokio::time::Instant::now());
                        if join_budget.is_zero() {
                            drop(join);
                            None
                        } else {
                            match tokio::time::timeout(join_budget, &mut join).await {
                                Ok(r) => Some(r),
                                Err(_) => {
                                    drop(join);
                                    None
                                }
                            }
                        }
                    }
                }
            };

            if join_result.is_none() {
                invariant_failed += 1;
            }

            // Claim after join (or with deadline-bounded restore wait) so
            // ClaimedFinalization Drop cannot race past an empty try_claim.
            let claim_budget = deadline_at.saturating_duration_since(tokio::time::Instant::now());
            if entry.guard.callback_was_scheduled() {
                normally_finalized += 1;
            } else if let Some(payload) = entry.guard.claim_for_shutdown(claim_budget).await {
                entry.guard.mark_callback_scheduled();
                let end = build_transaction_end(
                    &payload,
                    TransactionEndKind::RuntimeShutdown,
                    None,
                    EventDeliveryOutcome::Failed,
                    entry.guard.sequencer().last_allocated(),
                );
                let cb_budget =
                    cb_cfg.min(deadline_at.saturating_duration_since(tokio::time::Instant::now()));
                match run_callback_isolated(
                    &self.inner.executor,
                    &self.inner.spawn_gate,
                    payload.callback,
                    end,
                    cb_budget,
                )
                .await
                {
                    CallbackRun::Ok => supervisor_finalized += 1,
                    CallbackRun::Failed => {
                        supervisor_finalized += 1;
                        callback_failed += 1;
                    }
                    CallbackRun::Aborted => {
                        supervisor_finalized += 1;
                        callback_aborted += 1;
                    }
                }
            } else if matches!(join_result, Some(Err(_))) {
                invariant_failed += 1;
            } else if join_result.is_some() {
                normally_finalized += 1;
            }
            (entry.release_capacity)();
        }

        // D-029: use only remaining global shutdown time; never pad after expiry.
        let mcp_budget = deadline_at.saturating_duration_since(tokio::time::Instant::now());
        if let Some(mcp) = self.inner.mcp.lock().await.take() {
            if !mcp_budget.is_zero() {
                let _ = tokio::time::timeout(mcp_budget, mcp.shutdown()).await;
            }
        }

        // Drain runtime-owned host callbacks (D-021 / D-029).
        let cb_drain = deadline_at.saturating_duration_since(tokio::time::Instant::now());
        if !cb_drain.is_zero() {
            self.inner.callbacks.drain(cb_drain).await;
        }

        let disposition = ShutdownDisposition {
            normally_finalized,
            supervisor_finalized,
            callback_failed,
            callback_aborted,
            invariant_failed,
        };
        *self.inner.shutdown_disposition.lock().await = Some(disposition.clone());
        self.inner.state.store(STATE_STOPPED, Ordering::SeqCst);
        disposition
    }
}

/// Outcome of a supervisor-invoked completion callback (D-021).
enum CallbackRun {
    Ok,
    Failed,
    Aborted,
}

/// Invoke + await host callback with panic isolation on the injected executor (D-021 / D-032).
async fn run_callback_isolated(
    executor: &Handle,
    gate: &SpawnGate,
    callback: Box<dyn monoloop_contracts::CompletionCallback>,
    end: monoloop_contracts::TransactionEnd,
    deadline: Duration,
) -> CallbackRun {
    let call = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback.call(end)));
    match call {
        Ok(fut) => {
            let mut handle = match try_spawn(executor, gate, fut) {
                Ok(h) => h,
                Err(()) => return CallbackRun::Failed,
            };
            let abort = handle.abort_handle();
            match tokio::time::timeout(deadline, &mut handle).await {
                Ok(Ok(Ok(()))) => CallbackRun::Ok,
                Ok(Ok(Err(_))) => CallbackRun::Failed,
                Ok(Err(_)) => CallbackRun::Failed, // join error = panic in future
                Err(_) => {
                    // Abort; keep awaiting within no extra pad — put-back style:
                    // after abort, join should complete for async work.
                    abort.abort();
                    let _ = handle.await;
                    CallbackRun::Aborted
                }
            }
        }
        Err(_) => CallbackRun::Failed, // panic at invoke
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

async fn cleanup_partial(realized: Vec<(ChannelId, LiveChannel)>, mcp: Option<McpGateway>) {
    drop(realized);
    if let Some(m) = mcp {
        m.shutdown().await;
    }
}

impl TransactionRuntime for DefaultTransactionRuntime {
    fn submit(&self, request: TransactionRequest) -> Result<AdmissionReceipt, AdmissionError> {
        match self.state() {
            RuntimeState::Accepting => {}
            RuntimeState::Starting | RuntimeState::Draining | RuntimeState::Stopped => {
                return Err(AdmissionError::new(
                    AdmissionErrorKind::RuntimeShuttingDown,
                    "runtime is not accepting submissions",
                ));
            }
        }

        let ctx = AdmissionContext {
            channels: Arc::clone(&self.inner.channels),
            tools: self.inner.tools.clone(),
            capacity: Arc::clone(&self.inner.capacity),
            registry: Arc::clone(&self.inner.registry),
            limits: self.inner.config.transaction_limits.clone(),
            mcp: self.inner.mcp_handle.clone(),
            runtime_state: Arc::clone(&self.inner.state),
            callbacks: self.inner.callbacks.clone(),
            executor: self.inner.executor.clone(),
            spawn_gate: self.inner.spawn_gate.clone(),
        };
        admit(&ctx, request)
    }

    fn terminate(
        &self,
        selector: TransactionSelector,
        mode: TerminationMode,
    ) -> TerminationDisposition {
        if !matches!(
            self.state(),
            RuntimeState::Accepting | RuntimeState::Draining
        ) {
            return TerminationDisposition::NotFound;
        }
        let reg = self
            .inner
            .registry
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let tx = match selector {
            TransactionSelector::Transaction(id) => reg.control_tx(&id),
            TransactionSelector::Session(key) => reg.control_tx_by_session(&key),
        };
        drop(reg);
        let Some(tx) = tx else {
            return TerminationDisposition::NotFound;
        };
        let msg = match mode {
            TerminationMode::Cancel { .. } => ControlMessage::Cancel,
            TerminationMode::ForceTerminate { .. } => ControlMessage::ForceTerminate,
        };
        match tx.try_send(msg) {
            Ok(()) => TerminationDisposition::Accepted,
            Err(mpsc::error::TrySendError::Full(_)) => TerminationDisposition::AlreadyRequested,
            Err(mpsc::error::TrySendError::Closed(_)) => TerminationDisposition::AlreadyTerminal,
        }
    }

    fn shutdown(&self, deadline: Duration) -> Shutdown {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let view = DefaultTransactionRuntime { inner };
            view.shutdown_inner(deadline).await
        })
    }
}
