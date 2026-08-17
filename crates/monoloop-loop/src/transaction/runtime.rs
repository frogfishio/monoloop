//! DefaultTransactionRuntime startup, state, and shutdown shell.

use super::bootstrap::RuntimeBootstrap;
use super::capacity::CapacityManagers;
use super::channel_registry::ChannelBinding;
use super::error::StartupError;
use super::host_tools::HostToolRegistry;
use super::mcp_shell::McpListenerShell;
use super::state::RuntimeState;
use monoloop_connector::ConnectorInstance;
use monoloop_contracts::{
    AdmissionError, AdmissionErrorKind, AdmissionReceipt, ChannelId, ChannelKind, Shutdown,
    ShutdownDisposition, TerminationDisposition, TerminationMode, TransactionRequest,
    TransactionRuntime, TransactionSelector,
};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

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

/// Live Channel after connector instance realization.
pub struct LiveChannel {
    /// Static binding.
    pub binding: ChannelBinding,
    /// Matched connector instance.
    pub instance: ConnectorInstance,
}

struct RuntimeInner {
    state: AtomicU8,
    config: super::bootstrap::RuntimeConfig,
    channels: HashMap<ChannelId, LiveChannel>,
    tools: HostToolRegistry,
    capacity: Arc<CapacityManagers>,
    mcp: Mutex<Option<McpListenerShell>>,
}

/// Production transaction runtime (WP-03: start/stop; admission in WP-04).
pub struct DefaultTransactionRuntime {
    inner: Arc<RuntimeInner>,
}

impl DefaultTransactionRuntime {
    /// Only startup path. Returns after `Accepting` or cleans up and errors.
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

        let mcp = if bootstrap.config.enable_mcp_listener {
            match McpListenerShell::bind_loopback().await {
                Ok(shell) => Some(shell),
                Err(_) => {
                    cleanup_partial(realized, None).await;
                    return Err(StartupError::McpBindFailed);
                }
            }
        } else {
            None
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
                channels,
                tools: bootstrap.tools,
                capacity,
                mcp: Mutex::new(mcp),
            }),
        }))
    }

    /// Current lifecycle state.
    pub fn state(&self) -> RuntimeState {
        decode_state(self.inner.state.load(Ordering::SeqCst))
    }

    /// Immutable tools shell.
    pub fn tools(&self) -> &HostToolRegistry {
        &self.inner.tools
    }

    /// Capacity managers.
    pub fn capacity(&self) -> &Arc<CapacityManagers> {
        &self.inner.capacity
    }

    /// Number of live Channels.
    pub fn channel_count(&self) -> usize {
        self.inner.channels.len()
    }

    /// MCP loopback address when enabled.
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
        let _deadline = if deadline.is_zero() {
            self.inner.config.default_shutdown_deadline
        } else {
            deadline
        };

        let prev = self.inner.state.swap(STATE_DRAINING, Ordering::SeqCst);
        if prev == STATE_STOPPED {
            self.inner.state.store(STATE_STOPPED, Ordering::SeqCst);
            return ShutdownDisposition::default();
        }

        if let Some(mcp) = self.inner.mcp.lock().await.take() {
            mcp.shutdown().await;
        }

        self.inner.state.store(STATE_STOPPED, Ordering::SeqCst);
        ShutdownDisposition {
            normally_finalized: 0,
            supervisor_finalized: 0,
            callback_failed: 0,
            callback_aborted: 0,
            invariant_failed: 0,
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
        defaults: binding.defaults.clone(),
        capabilities: binding.capabilities.clone(),
        limits: binding.limits.clone(),
    }
}

async fn cleanup_partial(
    realized: Vec<(ChannelId, LiveChannel)>,
    mcp: Option<McpListenerShell>,
) {
    drop(realized);
    if let Some(m) = mcp {
        m.shutdown().await;
    }
}

impl TransactionRuntime for DefaultTransactionRuntime {
    fn submit(&self, _request: TransactionRequest) -> Result<AdmissionReceipt, AdmissionError> {
        match self.state() {
            RuntimeState::Accepting => Err(AdmissionError::new(
                AdmissionErrorKind::InvalidConfiguration,
                "transaction admission is not available until WP-04",
            )),
            RuntimeState::Starting | RuntimeState::Draining | RuntimeState::Stopped => {
                Err(AdmissionError::new(
                    AdmissionErrorKind::RuntimeShuttingDown,
                    "runtime is not accepting submissions",
                ))
            }
        }
    }

    fn terminate(
        &self,
        _selector: TransactionSelector,
        _mode: TerminationMode,
    ) -> TerminationDisposition {
        TerminationDisposition::NotFound
    }

    fn shutdown(&self, deadline: Duration) -> Shutdown {
        // Clone the inner Arc so the future is independent of &self lifetime.
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let view = DefaultTransactionRuntime { inner };
            view.shutdown_inner(deadline).await
        })
    }
}
