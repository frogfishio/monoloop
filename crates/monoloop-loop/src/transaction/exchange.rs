//! One provider exchange: open → encode send → pump bytes → interpret → terminal reconcile.

use monoloop_connector::{
    ConnectionEnd, ConnectionEndKind, Connector, OpenConnection, OpenedRawConnection,
};
use monoloop_contracts::{
    CanonicalUnitEvent, ConnectionId, EffectiveConfig, EncodedExchange, ExchangeId,
    ExchangeInputPolicy, InterpretationEnd, InterpretationEndKind, InterpretationId,
    InterpretationLimits, OutboundDialectEncoder, TransactionId,
};
use monoloop_interpreter::{InterpreterFactory, StartInterpretation};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

use super::executor_spawn::try_spawn;

/// Result of one exchange cycle.
pub struct ExchangeOutcome {
    /// Exchange identity.
    pub exchange_id: ExchangeId,
    /// Connection identity.
    pub connection_id: ConnectionId,
    /// Interpretation identity.
    pub interpretation_id: InterpretationId,
    /// Authoritative external session id from open (create/load), if any.
    pub external_session_id: Option<monoloop_contracts::ExternalSessionId>,
    /// Bytes actually sent on the provider request body (D-027 aggregate input).
    pub encoded_request_bytes: usize,
    /// Complete canonical unit events observed (not Ended).
    pub units: Vec<CanonicalUnitEvent>,
    /// Connector terminal.
    pub connection_end: ConnectionEnd,
    /// Interpretation terminal.
    pub interpretation_end: InterpretationEnd,
    /// Mapped transaction failure kind, if any.
    pub failure: Option<ExchangeFailure>,
}

/// Exchange-level failure classification for the actor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExchangeFailure {
    /// Channel open failed.
    ChannelOpenFailed,
    /// Encoding failed.
    EncodingFailed,
    /// Connector transport failed.
    ConnectorFailed,
    /// Interpretation failed.
    InterpretationFailed,
    /// Cancelled.
    Cancelled,
    /// Terminated.
    Terminated,
    /// Exchange retained-output / aggregate limit exceeded (D-027).
    LimitExceeded,
    /// Create open without authoritative session id, or claim gate failed closed (D-026).
    InvariantFailed,
    /// Timed out waiting for claim / MCP activate before prompt send (D-026).
    ClaimDeadlineExceeded,
}

/// Parameters for running one exchange.
pub struct ExchangeParams<'a> {
    /// Injected Tokio handle for owned exchange children (D-032).
    pub executor: &'a Handle,
    /// Transaction id.
    pub transaction_id: TransactionId,
    /// Connector instance.
    pub connector: &'a dyn Connector,
    /// Encoder.
    pub encoder: &'a dyn OutboundDialectEncoder,
    /// Interpreter factory.
    pub interpreter: &'a dyn InterpreterFactory,
    /// Endpoint ref.
    pub endpoint_ref: &'a str,
    /// Credential ref.
    pub credential_ref: Option<&'a str>,
    /// Optional session attachment.
    pub session_attachment: Option<Arc<monoloop_connector::SessionAttachment>>,
    /// Canonical input.
    pub input: &'a monoloop_contracts::CanonicalInput,
    /// Effective config.
    pub config: &'a EffectiveConfig,
    /// Tool specs for encoder.
    pub tools: &'a [monoloop_contracts::ToolSpec],
    /// Interpretation limits.
    pub interpretation_limits: InterpretationLimits,
    /// Overall deadline for the exchange.
    pub deadline: Duration,
    /// Join/abort grace for exchange children after cancel or terminal (D-012).
    pub cleanup_deadline: Duration,
    /// Channel max encoded body size (D-015).
    pub max_encoded_exchange_bytes: usize,
    /// Optional live unit sink (D-011); when set, units are forwarded as produced.
    pub unit_tx: Option<mpsc::Sender<CanonicalUnitEvent>>,
    /// Optional oneshot: authoritative external session id immediately after open (D-013).
    pub session_id_tx: Option<oneshot::Sender<monoloop_contracts::ExternalSessionId>>,
    /// When set, wait for this signal after open (claim+MCP activate) before sending the prompt (D-026).
    pub prompt_ready_rx: Option<oneshot::Receiver<()>>,
    /// Max bytes retained in the in-exchange unit buffer (D-027); derived from provider output budget.
    pub max_retained_unit_bytes: usize,
    /// Remaining aggregate provider-input budget; checked after encode, before open/send (D-027).
    pub max_remaining_provider_input_bytes: usize,
}

/// Parameters for a continuation exchange (fresh identities, pre-encoded body).
pub struct EncodedExchangeParams<'a> {
    /// Injected Tokio handle for owned exchange children (D-032).
    pub executor: &'a Handle,
    /// Transaction id.
    pub transaction_id: TransactionId,
    /// Single exchange identity shared with encoder (D-017).
    pub exchange_id: ExchangeId,
    /// Connector instance.
    pub connector: &'a dyn Connector,
    /// Interpreter factory.
    pub interpreter: &'a dyn InterpreterFactory,
    /// Endpoint ref.
    pub endpoint_ref: &'a str,
    /// Credential ref.
    pub credential_ref: Option<&'a str>,
    /// Optional session attachment.
    pub session_attachment: Option<Arc<monoloop_connector::SessionAttachment>>,
    /// Already-encoded provider body.
    pub encoded: EncodedExchange,
    /// Interpretation limits.
    pub interpretation_limits: InterpretationLimits,
    /// Overall deadline for the exchange.
    pub deadline: Duration,
    /// Join/abort grace for exchange children after cancel or terminal (D-012).
    pub cleanup_deadline: Duration,
    /// Channel max encoded body size (D-015).
    pub max_encoded_exchange_bytes: usize,
    /// Optional live unit sink (D-011); when set, units are forwarded as produced.
    pub unit_tx: Option<mpsc::Sender<CanonicalUnitEvent>>,
    /// Max bytes retained in the in-exchange unit buffer (D-027).
    pub max_retained_unit_bytes: usize,
    /// Remaining aggregate provider-input budget (D-027).
    pub max_remaining_provider_input_bytes: usize,
}

/// Run one SendAndFinish exchange end-to-end (no raw bytes enter actor queues).
pub async fn run_exchange(params: ExchangeParams<'_>) -> Result<ExchangeOutcome, ExchangeFailure> {
    let exchange_id = ExchangeId::generate();
    let encoded = params
        .encoder
        .encode_initial(monoloop_contracts::InitialEncodeRequest {
            transaction_id: &params.transaction_id,
            exchange_id: &exchange_id,
            input: params.input,
            config: params.config,
            tools: params.tools,
        })
        .map_err(|_| ExchangeFailure::EncodingFailed)?;
    if encoded.bytes.len() > params.max_encoded_exchange_bytes {
        return Err(ExchangeFailure::EncodingFailed);
    }
    if encoded.bytes.len() > params.max_remaining_provider_input_bytes {
        return Err(ExchangeFailure::LimitExceeded);
    }

    open_and_run(
        params.executor,
        exchange_id,
        params.connector,
        params.endpoint_ref,
        params.credential_ref,
        params.session_attachment,
        encoded,
        params.interpreter,
        params.interpretation_limits,
        params.deadline,
        params.cleanup_deadline,
        params.unit_tx,
        params.session_id_tx,
        params.prompt_ready_rx,
        params.max_retained_unit_bytes,
    )
    .await
}

/// Run one exchange with a pre-encoded body (tool continuation).
pub async fn run_encoded_exchange(
    params: EncodedExchangeParams<'_>,
) -> Result<ExchangeOutcome, ExchangeFailure> {
    if params.encoded.bytes.len() > params.max_encoded_exchange_bytes {
        return Err(ExchangeFailure::EncodingFailed);
    }
    if params.encoded.bytes.len() > params.max_remaining_provider_input_bytes {
        return Err(ExchangeFailure::LimitExceeded);
    }
    open_and_run(
        params.executor,
        params.exchange_id,
        params.connector,
        params.endpoint_ref,
        params.credential_ref,
        params.session_attachment,
        params.encoded,
        params.interpreter,
        params.interpretation_limits,
        params.deadline,
        params.cleanup_deadline,
        params.unit_tx,
        None,
        None,
        params.max_retained_unit_bytes,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn open_and_run(
    executor: &Handle,
    exchange_id: ExchangeId,
    connector: &dyn Connector,
    endpoint_ref: &str,
    credential_ref: Option<&str>,
    session_attachment: Option<Arc<monoloop_connector::SessionAttachment>>,
    encoded: EncodedExchange,
    interpreter: &dyn InterpreterFactory,
    interpretation_limits: InterpretationLimits,
    deadline: Duration,
    cleanup_deadline: Duration,
    unit_tx: Option<mpsc::Sender<CanonicalUnitEvent>>,
    session_id_tx: Option<oneshot::Sender<monoloop_contracts::ExternalSessionId>>,
    prompt_ready_rx: Option<oneshot::Receiver<()>>,
    max_retained_unit_bytes: usize,
) -> Result<ExchangeOutcome, ExchangeFailure> {
    let connection_id = ConnectionId::generate();
    let interpretation_id = InterpretationId::generate();

    let mut open = OpenConnection::new(connection_id.clone(), endpoint_ref);
    open.credential_ref = credential_ref.map(|s| s.to_string());
    if let Some(att) = session_attachment {
        open = open.with_session_attachment(att);
    }

    let pending = connector.begin_open(open);
    // D-028: own pending Connector control from begin_open before first await.
    let mut open_guard = PendingOpenGuard {
        control: Some(pending.control.clone()),
    };
    let opened = match tokio::time::timeout(deadline, pending.opened).await {
        Ok(Ok(o)) => o,
        Ok(Err(_)) => return Err(ExchangeFailure::ChannelOpenFailed),
        Err(_) => return Err(ExchangeFailure::ChannelOpenFailed),
    };
    // Open succeeded — keep control until ExchangeGuard takes over inside run_opened_exchange.
    let opened_control = opened.control.clone();
    let _ = open_guard.control.take();
    // Early opened ownership: terminate if we drop before ExchangeGuard is installed (D-028).
    let mut early_opened = EarlyOpenedGuard {
        control: Some(opened_control),
    };

    if let Some(tx) = session_id_tx {
        let Some(ref ext) = opened.external_session_id else {
            return Err(ExchangeFailure::InvariantFailed);
        };
        let _ = tx.send(ext.clone());
    }

    // D-026: wait for claim (+ MCP activate) before sending the prompt on create.
    // RecvError → claim task exited without ready; actor awaits claim_join for typed kind.
    // Timeout is a deadline miss on the claim gate — not ChannelOpenFailed.
    if let Some(rx) = prompt_ready_rx {
        match tokio::time::timeout(deadline, rx).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return Err(ExchangeFailure::InvariantFailed),
            Err(_) => return Err(ExchangeFailure::ClaimDeadlineExceeded),
        }
    }

    let outcome = run_opened_exchange(
        executor,
        exchange_id,
        interpretation_id,
        opened,
        encoded,
        interpreter,
        interpretation_limits,
        deadline,
        cleanup_deadline,
        unit_tx,
        max_retained_unit_bytes,
        &mut early_opened,
    )
    .await;
    // ExchangeGuard (or failure paths) now own termination.
    let _ = early_opened.control.take();
    outcome
}

/// Terminates pending open control if dropped before open completes (D-028).
struct PendingOpenGuard {
    control: Option<monoloop_connector::ConnectionControlHandle>,
}

impl Drop for PendingOpenGuard {
    fn drop(&mut self) {
        if let Some(ctrl) = self.control.take() {
            let _ = ctrl.terminate(monoloop_connector::TerminationReason::CallerForced);
        }
    }
}

/// Terminates an opened connection if dropped before [`ExchangeGuard`] owns it (D-028).
struct EarlyOpenedGuard {
    control: Option<monoloop_connector::ConnectionControlHandle>,
}

impl Drop for EarlyOpenedGuard {
    fn drop(&mut self) {
        if let Some(ctrl) = self.control.take() {
            let _ = ctrl.terminate(monoloop_connector::TerminationReason::CallerForced);
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_opened_exchange(
    executor: &Handle,
    exchange_id: ExchangeId,
    interpretation_id: InterpretationId,
    opened: OpenedRawConnection,
    encoded: EncodedExchange,
    interpreter: &dyn InterpreterFactory,
    limits: InterpretationLimits,
    deadline: Duration,
    cleanup_deadline: Duration,
    unit_tx: Option<mpsc::Sender<CanonicalUnitEvent>>,
    max_retained_unit_bytes: usize,
    early_opened: &mut EarlyOpenedGuard,
) -> Result<ExchangeOutcome, ExchangeFailure> {
    let join_grace = cleanup_deadline.max(Duration::from_millis(50));
    let connection_id = opened.connection_id.clone();
    let encoded_request_bytes = encoded.bytes.len();

    // Install ExchangeGuard before interpreter/pump/send so cancel cannot detach (D-028).
    let mut guard = ExchangeGuard {
        control: early_opened.control.take(),
        joins: Some(JoinSet::new()),
        units_abort: None,
    };

    let interpretation = interpreter
        .start(StartInterpretation {
            interpretation_id: interpretation_id.clone(),
            connection_id: connection_id.clone(),
            external_session_id: opened.external_session_id.clone(),
            dialect: opened.dialect.clone(),
            limits,
        })
        .map_err(|_| ExchangeFailure::InterpretationFailed)?;

    // Pump raw output → interpretation (owned task on injected executor — D-032).
    let output = Arc::clone(&opened.output);
    let interp_in = interpretation.input.clone();
    if let Some(ref mut joins) = guard.joins {
        joins.spawn_on(
            async move {
                loop {
                    match output.receive().await {
                        Ok(Some(chunk)) => {
                            if interp_in.push_bytes(chunk).await.is_err() {
                                break;
                            }
                        }
                        Ok(None) => {
                            let _ = interp_in.finish_clean().await;
                            break;
                        }
                        Err(e) => {
                            use monoloop_contracts::ConnectorErrorKind;
                            match e.kind {
                                ConnectorErrorKind::Cancelled => {
                                    let _ = interp_in.cancel().await;
                                }
                                ConnectorErrorKind::Terminated => {
                                    let _ = interp_in.cancel().await;
                                }
                                _ => {
                                    let _ = interp_in.transport_failed().await;
                                }
                            }
                            break;
                        }
                    }
                }
            },
            executor,
        );
    }

    // Send encoded request body.
    if !encoded.bytes.is_empty() && opened.input.send(encoded.bytes.clone()).await.is_err() {
        return Err(ExchangeFailure::ConnectorFailed);
    }
    match encoded.input_policy {
        ExchangeInputPolicy::SendAndFinish => {
            if opened.input.finish().await.is_err() {
                return Err(ExchangeFailure::ConnectorFailed);
            }
        }
        ExchangeInputPolicy::SendAndRetain => {}
    }

    // Collect interpretation events; optionally fan out live (D-011).
    // D-027: retain only byte-bounded continuation state.
    let events_handle = interpretation.events;
    let max_retained = max_retained_unit_bytes.max(256);
    let units = Arc::new(tokio::sync::Mutex::new(Vec::<CanonicalUnitEvent>::new()));
    let retention_exceeded = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let units_task = {
        let units = Arc::clone(&units);
        let retention_exceeded = Arc::clone(&retention_exceeded);
        let unit_tx = unit_tx;
        try_spawn(executor, async move {
            let mut retained_bytes = 0usize;
            while let Some(ev) = events_handle.recv().await {
                match ev {
                    monoloop_contracts::InterpreterOutputEvent::Unit(u) => {
                        let unit = *u;
                        if let Some(ref tx) = unit_tx {
                            if tx.send(unit.clone()).await.is_err() {
                                break;
                            }
                        }
                        let add = estimate_retained_unit_bytes(&unit);
                        let mut guard = units.lock().await;
                        if retained_bytes.saturating_add(add) > max_retained {
                            retention_exceeded.store(true, std::sync::atomic::Ordering::SeqCst);
                            break;
                        }
                        retained_bytes = retained_bytes.saturating_add(add);
                        guard.push(unit);
                    }
                    monoloop_contracts::InterpreterOutputEvent::Ended(_) => break,
                }
            }
        })
        .map_err(|_| ExchangeFailure::ConnectorFailed)?
    };
    guard.units_abort = Some(units_task.abort_handle());

    let completion = interpretation.completion;
    let conn_completion = opened.completion;
    let external_session_id = opened.external_session_id.clone();
    let open_control = opened.control.clone();

    let (interp_end, conn_end) = tokio::select! {
        _ = tokio::time::sleep(deadline) => {
            let _ = open_control
                .terminate(monoloop_connector::TerminationReason::CallerForced);
            if let Some(abort) = guard.units_abort.take() {
                abort.abort();
            }
            let _ = tokio::time::timeout(join_grace, units_task).await;
            if let Some(mut joins) = guard.joins.take() {
                abort_joins(&mut joins).await;
            }
            let _ = guard.control.take();
            return Err(ExchangeFailure::ConnectorFailed);
        }
        ends = async {
            let i = completion.wait().await;
            let c = conn_completion.wait().await;
            (i, c)
        } => ends,
    };

    // Normal path: join children within cleanup_deadline (D-012).
    if let Some(mut joins) = guard.joins.take() {
        let _ = tokio::time::timeout(join_grace, async {
            while joins.join_next().await.is_some() {}
        })
        .await;
        abort_joins(&mut joins).await;
    }
    // D-028: keep abort handle until join settles; abort again if join times out
    // so the task is never silently detached.
    let mut units_task = units_task;
    if let Some(abort) = guard.units_abort.take() {
        match tokio::time::timeout(join_grace, &mut units_task).await {
            Ok(_) => {}
            Err(_) => {
                abort.abort();
                let _ = tokio::time::timeout(join_grace, units_task).await;
            }
        }
    } else {
        let _ = tokio::time::timeout(join_grace, units_task).await;
    }
    let _ = guard.control.take();

    if retention_exceeded.load(std::sync::atomic::Ordering::SeqCst) {
        return Err(ExchangeFailure::LimitExceeded);
    }

    let units = units.lock().await.clone();
    let failure = reconcile_terminals(&conn_end, &interp_end);

    Ok(ExchangeOutcome {
        exchange_id,
        connection_id,
        interpretation_id,
        external_session_id,
        encoded_request_bytes,
        units,
        connection_end: conn_end,
        interpretation_end: interp_end,
        failure,
    })
}

fn estimate_retained_unit_bytes(unit: &CanonicalUnitEvent) -> usize {
    use monoloop_contracts::CanonicalUnit;
    match &unit.snapshot().unit {
        CanonicalUnit::Text(t) => t.content.len().saturating_add(32),
        CanonicalUnit::Tool(t) => t
            .request_payload
            .as_ref()
            .map(|p| p.len())
            .unwrap_or(0)
            .saturating_add(64),
        _ => 64,
    }
}

/// Drop guard: terminate connector and abort child tasks if exchange is cancelled (D-012).
struct ExchangeGuard {
    control: Option<monoloop_connector::ConnectionControlHandle>,
    joins: Option<JoinSet<()>>,
    units_abort: Option<tokio::task::AbortHandle>,
}

impl Drop for ExchangeGuard {
    fn drop(&mut self) {
        if let Some(ctrl) = self.control.take() {
            let _ = ctrl.terminate(monoloop_connector::TerminationReason::CallerForced);
        }
        if let Some(h) = self.units_abort.take() {
            h.abort();
        }
        if let Some(mut joins) = self.joins.take() {
            joins.abort_all();
        }
    }
}

fn reconcile_terminals(
    conn: &ConnectionEnd,
    interp: &InterpretationEnd,
) -> Option<ExchangeFailure> {
    match conn.kind {
        ConnectionEndKind::Cancelled => return Some(ExchangeFailure::Cancelled),
        ConnectionEndKind::Terminated => return Some(ExchangeFailure::Terminated),
        ConnectionEndKind::TransportFailure => return Some(ExchangeFailure::ConnectorFailed),
        ConnectionEndKind::RemoteEof | ConnectionEndKind::LocalShutdown => {}
    }
    match interp.kind {
        InterpretationEndKind::Complete => None,
        InterpretationEndKind::Cancelled => Some(ExchangeFailure::Cancelled),
        InterpretationEndKind::Terminated => Some(ExchangeFailure::Terminated),
        InterpretationEndKind::TransportFailed => Some(ExchangeFailure::ConnectorFailed),
        InterpretationEndKind::DialectFailed
        | InterpretationEndKind::LimitExceeded
        | InterpretationEndKind::InvariantFailed => Some(ExchangeFailure::InterpretationFailed),
    }
}

async fn abort_joins(joins: &mut JoinSet<()>) {
    joins.abort_all();
    while joins.join_next().await.is_some() {}
}
