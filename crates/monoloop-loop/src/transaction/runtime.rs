//! DefaultTransactionRuntime: start, admit, terminate, shutdown.

use super::active_registry::{ActiveTransactionRegistry, ControlMessage};
use super::admission::{admit, AdmissionContext};
use super::bootstrap::RuntimeBootstrap;
use super::capacity::CapacityManagers;
use super::channel_registry::{ChannelBinding, LiveChannel};
use super::error::StartupError;
use super::finalization::build_transaction_end;
use super::host_tools::HostToolRegistry;
use super::mcp::McpGateway;
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
    state: AtomicU8,
    config: super::bootstrap::RuntimeConfig,
    channels: Arc<HashMap<ChannelId, LiveChannel>>,
    tools: HostToolRegistry,
    capacity: Arc<CapacityManagers>,
    registry: Arc<Mutex<ActiveTransactionRegistry>>,
    mcp: AsyncMutex<Option<McpGateway>>,
    /// Cloneable handle for admission/actors (None when MCP listener disabled).
    mcp_handle: Option<super::mcp::McpGatewayHandle>,
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
        let _ = bootstrap.executor.id();

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

        let mut channels = HashMap::with_capacity(realized.len());
        for (id, live) in realized {
            channels.insert(id, live);
        }

        Ok(Arc::new(Self {
            inner: Arc::new(RuntimeInner {
                state: AtomicU8::new(STATE_ACCEPTING),
                config: bootstrap.config,
                channels: Arc::new(channels),
                tools: bootstrap.tools,
                capacity,
                registry: Arc::new(Mutex::new(ActiveTransactionRegistry::new())),
                mcp: AsyncMutex::new(mcp),
                mcp_handle,
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
        self.inner
            .mcp
            .lock()
            .await
            .as_ref()
            .map(|m| m.local_addr())
    }

    /// Live channel lookup.
    pub fn live_channel(&self, id: &ChannelId) -> Option<&LiveChannel> {
        self.inner.channels.get(id)
    }

    async fn shutdown_inner(&self, deadline: Duration) -> ShutdownDisposition {
        let slice = if deadline.is_zero() {
            self.inner.config.default_shutdown_deadline
        } else {
            deadline
        };

        let prev = self.inner.state.swap(STATE_DRAINING, Ordering::SeqCst);
        if prev == STATE_STOPPED {
            self.inner.state.store(STATE_STOPPED, Ordering::SeqCst);
            return ShutdownDisposition::default();
        }

        let active = {
            let mut reg = self.inner.registry.lock().unwrap_or_else(|e| e.into_inner());
            reg.drain_all()
        };

        let mut normally_finalized = 0u64;
        let mut supervisor_finalized = 0u64;
        let mut callback_failed = 0u64;
        let mut callback_aborted = 0u64;
        let mut invariant_failed = 0u64;
        let cb_deadline = self.inner.config.transaction_limits.callback_deadline;

        for entry in active {
            let _ = entry.control_tx.try_send(ControlMessage::ForceTerminate);
            let join_budget = (slice / 4).max(Duration::from_millis(100));
            let abort = entry.actor_join.abort_handle();
            match tokio::time::timeout(join_budget, entry.actor_join).await {
                Ok(Ok(())) => {
                    if entry.guard.callback_was_scheduled() {
                        normally_finalized += 1;
                    } else if let Some(payload) = entry.guard.try_claim() {
                        entry.guard.mark_callback_scheduled();
                        let end = build_transaction_end(
                            &payload,
                            TransactionEndKind::RuntimeShutdown,
                            None,
                            EventDeliveryOutcome::Failed,
                            entry.guard.sequencer().last_allocated(),
                        );
                        let fut = payload.callback.call(end);
                        match tokio::time::timeout(cb_deadline, fut).await {
                            Ok(Ok(())) => supervisor_finalized += 1,
                            Ok(Err(_)) => {
                                supervisor_finalized += 1;
                                callback_failed += 1;
                            }
                            Err(_) => {
                                supervisor_finalized += 1;
                                callback_aborted += 1;
                            }
                        }
                    } else {
                        normally_finalized += 1;
                    }
                }
                Ok(Err(_)) => {
                    invariant_failed += 1;
                    if let Some(payload) = entry.guard.try_claim() {
                        entry.guard.mark_callback_scheduled();
                        let end = build_transaction_end(
                            &payload,
                            TransactionEndKind::RuntimeShutdown,
                            None,
                            EventDeliveryOutcome::Failed,
                            0,
                        );
                        let fut = payload.callback.call(end);
                        let _ = tokio::time::timeout(Duration::from_millis(100), fut).await;
                        supervisor_finalized += 1;
                    }
                }
                Err(_) => {
                    // Actor did not finish within budget (e.g. blocked on sink).
                    // Abort owned work; JoinHandle drop alone would only detach.
                    abort.abort();
                    if let Some(payload) = entry.guard.try_claim() {
                        entry.guard.mark_callback_scheduled();
                        let end = build_transaction_end(
                            &payload,
                            TransactionEndKind::RuntimeShutdown,
                            None,
                            EventDeliveryOutcome::Failed,
                            entry.guard.sequencer().last_allocated(),
                        );
                        let fut = payload.callback.call(end);
                        match tokio::time::timeout(Duration::from_millis(200), fut).await {
                            Ok(Ok(())) => supervisor_finalized += 1,
                            Ok(Err(_)) => {
                                supervisor_finalized += 1;
                                callback_failed += 1;
                            }
                            Err(_) => {
                                supervisor_finalized += 1;
                                callback_aborted += 1;
                            }
                        }
                    } else {
                        supervisor_finalized += 1;
                    }
                }
            }
            // Always release capacity once (idempotent with actor finalize).
            (entry.release_capacity)();
        }

        if let Some(mcp) = self.inner.mcp.lock().await.take() {
            mcp.shutdown().await;
        }

        self.inner.state.store(STATE_STOPPED, Ordering::SeqCst);
        ShutdownDisposition {
            normally_finalized,
            supervisor_finalized,
            callback_failed,
            callback_aborted,
            invariant_failed,
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

async fn cleanup_partial(
    realized: Vec<(ChannelId, LiveChannel)>,
    mcp: Option<McpGateway>,
) {
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
        let reg = self.inner.registry.lock().unwrap_or_else(|e| e.into_inner());
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

