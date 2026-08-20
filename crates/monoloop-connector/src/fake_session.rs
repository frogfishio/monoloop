//! Deterministic FakeSessionAdapter and FakeConnectorFactory for lifecycle tests.

use crate::control::ControlDisposition;
use crate::fake::FakeConnector;
use crate::instance::{
    ConnectorBuildError, ConnectorFactory, ConnectorInstance, ConnectorInstanceId,
};
use crate::session::{
    validate_session_id_match, McpServerDescriptor, PendingOperationControl,
    PendingSessionAttachment, PendingSessionConfiguration, SessionAdapter, SessionAttachError,
    SessionAttachRequest, SessionAttachment, SessionAttachmentCompletion,
    SessionConfigurationCompletion, SessionConfigurationError, SessionRoute,
};
use monoloop_contracts::{ExternalSessionId, SessionConfig};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use uuid::Uuid;

/// Opaque fake route bound to one instance.
#[derive(Debug)]
pub struct FakeSessionRoute {
    owner: ConnectorInstanceId,
    /// Stable route token (tests only; not a secret).
    pub route_token: String,
}

impl FakeSessionRoute {
    /// Create a route for `owner`.
    pub fn new(owner: ConnectorInstanceId) -> Self {
        Self {
            owner,
            route_token: Uuid::new_v4().to_string(),
        }
    }
}

impl SessionRoute for FakeSessionRoute {
    fn owner(&self) -> &ConnectorInstanceId {
        &self.owner
    }
}

/// Shared control for a pending fake session operation.
struct FakePendingControl {
    cancel: AtomicBool,
    terminate: AtomicBool,
    terminal: AtomicBool,
    notify: Notify,
}

impl FakePendingControl {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            cancel: AtomicBool::new(false),
            terminate: AtomicBool::new(false),
            terminal: AtomicBool::new(false),
            notify: Notify::new(),
        })
    }

    fn mark_terminal(&self) {
        self.terminal.store(true, Ordering::SeqCst);
    }

    async fn wait_interrupt_or_delay(&self, delay: Duration) -> Result<(), SessionAttachError> {
        if delay.is_zero() {
            if self.terminate.load(Ordering::SeqCst) {
                return Err(SessionAttachError::Terminated);
            }
            if self.cancel.load(Ordering::SeqCst) {
                return Err(SessionAttachError::Cancelled);
            }
            return Ok(());
        }
        tokio::select! {
            _ = tokio::time::sleep(delay) => {
                if self.terminate.load(Ordering::SeqCst) {
                    Err(SessionAttachError::Terminated)
                } else if self.cancel.load(Ordering::SeqCst) {
                    Err(SessionAttachError::Cancelled)
                } else {
                    Ok(())
                }
            }
            _ = self.interrupted() => {
                if self.terminate.load(Ordering::SeqCst) {
                    Err(SessionAttachError::Terminated)
                } else {
                    Err(SessionAttachError::Cancelled)
                }
            }
        }
    }

    async fn interrupted(&self) {
        loop {
            if self.cancel.load(Ordering::SeqCst) || self.terminate.load(Ordering::SeqCst) {
                return;
            }
            self.notify.notified().await;
        }
    }
}

impl PendingOperationControl for FakePendingControl {
    fn cancel(&self) -> ControlDisposition {
        if self.terminal.load(Ordering::SeqCst) {
            return ControlDisposition::AlreadyTerminal;
        }
        if self
            .cancel
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return ControlDisposition::AlreadyRequested;
        }
        self.notify.notify_waiters();
        ControlDisposition::Accepted
    }

    fn force_terminate(&self) -> ControlDisposition {
        if self.terminal.load(Ordering::SeqCst) {
            return ControlDisposition::AlreadyTerminal;
        }
        if self
            .terminate
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return ControlDisposition::AlreadyRequested;
        }
        self.notify.notify_waiters();
        ControlDisposition::Accepted
    }
}

/// Configuration for [`FakeSessionAdapter`].
#[derive(Clone, Debug)]
pub struct FakeSessionAdapterConfig {
    /// Artificial attach delay (cancel/terminate race tests).
    pub attach_delay: Duration,
    /// Fail attach with SessionFailed.
    pub fail_attach: bool,
    /// Reject `begin_attach` immediately with SessionFailed (route-leak proofs).
    pub reject_begin_attach: bool,
    /// Maximum concurrent in-flight attach operations (0 = unlimited).
    pub max_in_flight: usize,
    /// When true, `begin_refresh_mcp` is unsupported.
    pub mcp_refresh_unsupported: bool,
}

impl Default for FakeSessionAdapterConfig {
    fn default() -> Self {
        Self {
            attach_delay: Duration::ZERO,
            fail_attach: false,
            reject_begin_attach: false,
            max_in_flight: 64,
            mcp_refresh_unsupported: false,
        }
    }
}

struct SessionTable {
    by_id: HashMap<String, SessionConfig>,
}

struct AdapterState {
    owner: ConnectorInstanceId,
    config: FakeSessionAdapterConfig,
    table: Mutex<SessionTable>,
    in_flight: AtomicUsize,
    completed_attaches: AtomicUsize,
}

/// Deterministic session adapter for external-agent Channel tests.
pub struct FakeSessionAdapter {
    state: Arc<AdapterState>,
}

impl FakeSessionAdapter {
    /// Create an adapter owned by `owner`.
    pub fn new(owner: ConnectorInstanceId, config: FakeSessionAdapterConfig) -> Self {
        Self {
            state: Arc::new(AdapterState {
                owner,
                config,
                table: Mutex::new(SessionTable {
                    by_id: HashMap::new(),
                }),
                in_flight: AtomicUsize::new(0),
                completed_attaches: AtomicUsize::new(0),
            }),
        }
    }

    /// Owning instance id.
    pub fn owner(&self) -> &ConnectorInstanceId {
        &self.state.owner
    }

    /// Pre-register an existing session for load tests.
    pub fn register_existing(&self, session_id: &str, config: SessionConfig) {
        let mut t = self.state.table.lock().expect("session table");
        t.by_id.insert(session_id.to_string(), config);
    }

    /// Number of successfully completed attaches.
    pub fn completed_attaches(&self) -> usize {
        self.state.completed_attaches.load(Ordering::SeqCst)
    }

    /// Current in-flight attach operations.
    pub fn in_flight(&self) -> usize {
        self.state.in_flight.load(Ordering::SeqCst)
    }
}

impl SessionAdapter for FakeSessionAdapter {
    fn begin_attach(
        &self,
        request: SessionAttachRequest,
    ) -> Result<PendingSessionAttachment, SessionAttachError> {
        if self.state.config.reject_begin_attach {
            return Err(SessionAttachError::SessionFailed);
        }
        let max = self.state.config.max_in_flight;
        if max > 0 {
            let cur = self.state.in_flight.load(Ordering::SeqCst);
            if cur >= max {
                return Err(SessionAttachError::CapacityExceeded);
            }
        }
        self.state.in_flight.fetch_add(1, Ordering::SeqCst);

        let control = FakePendingControl::new();
        let control_api: Arc<dyn PendingOperationControl> = control.clone();
        let state = Arc::clone(&self.state);

        let completion: SessionAttachmentCompletion = Box::pin(async move {
            let result = run_attach(state.clone(), request, control.clone()).await;
            state.in_flight.fetch_sub(1, Ordering::SeqCst);
            if result.is_ok() {
                state.completed_attaches.fetch_add(1, Ordering::SeqCst);
            }
            control.mark_terminal();
            result
        });

        Ok(PendingSessionAttachment {
            control: control_api,
            completion,
        })
    }

    fn begin_refresh_mcp(
        &self,
        attachment: Arc<SessionAttachment>,
        descriptor: Option<McpServerDescriptor>,
    ) -> Result<PendingSessionConfiguration, SessionConfigurationError> {
        if attachment.owner != self.state.owner {
            return Err(SessionConfigurationError::OwnerMismatch);
        }
        if self.state.config.mcp_refresh_unsupported {
            return Err(SessionConfigurationError::Unsupported);
        }
        let control = FakePendingControl::new();
        let control_api: Arc<dyn PendingOperationControl> = control.clone();
        let completion: SessionConfigurationCompletion = Box::pin(async move {
            let _name = descriptor.as_ref().map(|d| d.server_name.clone());
            if control.terminate.load(Ordering::SeqCst) {
                control.mark_terminal();
                return Err(SessionConfigurationError::Terminated);
            }
            if control.cancel.load(Ordering::SeqCst) {
                control.mark_terminal();
                return Err(SessionConfigurationError::Cancelled);
            }
            control.mark_terminal();
            Ok(())
        });
        Ok(PendingSessionConfiguration {
            control: control_api,
            completion,
        })
    }
}

async fn run_attach(
    state: Arc<AdapterState>,
    request: SessionAttachRequest,
    control: Arc<FakePendingControl>,
) -> Result<Arc<SessionAttachment>, SessionAttachError> {
    let now = std::time::Instant::now();
    if now >= request.deadline {
        return Err(SessionAttachError::DeadlineExceeded);
    }

    control
        .wait_interrupt_or_delay(state.config.attach_delay)
        .await?;

    if state.config.fail_attach {
        return Err(SessionAttachError::SessionFailed);
    }

    let route = Arc::new(FakeSessionRoute::new(state.owner.clone()));
    if let Some(ref requested) = request.requested_session_id {
        // Explicit load only — never create a replacement session (D-013).
        let table = state.table.lock().expect("session table");
        let known = table
            .by_id
            .get(requested.as_str())
            .cloned()
            .ok_or(SessionAttachError::SessionFailed)?;
        if configs_conflict(&request.session_config, &known) {
            return Err(SessionAttachError::ConfigurationMismatch);
        }
        let external = ExternalSessionId::try_new(requested.as_str())
            .map_err(|_| SessionAttachError::SessionFailed)?;
        validate_session_id_match(Some(requested), &external)?;
        Ok(Arc::new(SessionAttachment::new(
            state.owner.clone(),
            external,
            known,
            route,
        )))
    } else {
        // Create: provisional placeholder; Connector returns authoritative id on open.
        let provisional = format!("fake-pending-{}", Uuid::new_v4());
        let external = ExternalSessionId::try_new(&provisional)
            .map_err(|_| SessionAttachError::SessionFailed)?;
        let effective = request.session_config.clone();
        // Reserve a create slot so concurrent reuse keys are distinct; open will
        // register the provider id (see FakeConnector create_mode).
        state
            .table
            .lock()
            .expect("session table")
            .by_id
            .insert(provisional, effective.clone());
        Ok(Arc::new(SessionAttachment::new_create(
            state.owner.clone(),
            external,
            effective,
            route,
            request.initial_mcp,
        )))
    }
}

fn configs_conflict(requested: &SessionConfig, known: &SessionConfig) -> bool {
    if let (Some(a), Some(b)) = (&requested.mode, &known.mode) {
        if a != b {
            return true;
        }
    }
    if let (Some(a), Some(b)) = (&requested.specialist_profile, &known.specialist_profile) {
        if a != b {
            return true;
        }
    }
    if let (Some(a), Some(b)) = (&requested.permission_profile, &known.permission_profile) {
        if a != b {
            return true;
        }
    }
    false
}

/// Factory producing a matched FakeConnector + FakeSessionAdapter instance.
pub struct FakeConnectorFactory {
    /// When true, instance includes a SessionAdapter (external agent).
    pub with_sessions: bool,
    session_config: FakeSessionAdapterConfig,
    connector_config: crate::fake::FakeConnectorConfig,
}

impl FakeConnectorFactory {
    /// Direct-LLM style (no session adapter).
    pub fn direct_llm() -> Self {
        Self {
            with_sessions: false,
            session_config: FakeSessionAdapterConfig::default(),
            connector_config: crate::fake::FakeConnectorConfig::default(),
        }
    }

    /// Direct-LLM with custom FakeConnector timing/endpoint config (tests).
    pub fn direct_llm_with_config(connector_config: crate::fake::FakeConnectorConfig) -> Self {
        Self {
            with_sessions: false,
            session_config: FakeSessionAdapterConfig::default(),
            connector_config,
        }
    }

    /// External-agent style with session adapter.
    pub fn external_agent(session_config: FakeSessionAdapterConfig) -> Self {
        Self {
            with_sessions: true,
            session_config,
            connector_config: crate::fake::FakeConnectorConfig::default(),
        }
    }

    /// External-agent with custom FakeConnector config (e.g. omit created session id).
    pub fn external_agent_with_connector_config(
        session_config: FakeSessionAdapterConfig,
        connector_config: crate::fake::FakeConnectorConfig,
    ) -> Self {
        Self {
            with_sessions: true,
            session_config,
            connector_config,
        }
    }
}

impl ConnectorFactory for FakeConnectorFactory {
    fn create(&self) -> Result<ConnectorInstance, ConnectorBuildError> {
        let instance_id = ConnectorInstanceId::generate();
        let connector = Arc::new(FakeConnector::with_instance_id_and_config(
            instance_id.clone(),
            self.connector_config.clone(),
        ));
        let sessions = if self.with_sessions {
            Some(Arc::new(FakeSessionAdapter::new(
                instance_id.clone(),
                self.session_config.clone(),
            )) as Arc<dyn SessionAdapter>)
        } else {
            None
        };
        Ok(ConnectorInstance::new(instance_id, connector, sessions))
    }
}

/// Build a pending attach whose completion sender is dropped (invariant path).
pub fn pending_attach_with_dropped_completion() -> PendingSessionAttachment {
    let control = FakePendingControl::new();
    let control_api: Arc<dyn PendingOperationControl> = control;
    let (tx, rx) =
        tokio::sync::oneshot::channel::<Result<Arc<SessionAttachment>, SessionAttachError>>();
    drop(tx);
    let completion: SessionAttachmentCompletion = Box::pin(async move {
        match rx.await {
            Ok(r) => r,
            Err(_) => Err(SessionAttachError::InvariantFailed),
        }
    });
    PendingSessionAttachment {
        control: control_api,
        completion,
    }
}
