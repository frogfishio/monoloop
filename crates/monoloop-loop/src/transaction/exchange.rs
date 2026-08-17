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
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

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
}

/// Parameters for running one exchange.
pub struct ExchangeParams<'a> {
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
}

/// Parameters for a continuation exchange (fresh identities, pre-encoded body).
pub struct EncodedExchangeParams<'a> {
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

    open_and_run(
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
    open_and_run(
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
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn open_and_run(
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
) -> Result<ExchangeOutcome, ExchangeFailure> {
    let connection_id = ConnectionId::generate();
    let interpretation_id = InterpretationId::generate();

    let mut open = OpenConnection::new(connection_id.clone(), endpoint_ref);
    open.credential_ref = credential_ref.map(|s| s.to_string());
    if let Some(att) = session_attachment {
        open = open.with_session_attachment(att);
    }

    let pending = connector.begin_open(open);
    let opened = match tokio::time::timeout(deadline, pending.opened).await {
        Ok(Ok(o)) => o,
        Ok(Err(_)) => return Err(ExchangeFailure::ChannelOpenFailed),
        Err(_) => return Err(ExchangeFailure::ChannelOpenFailed),
    };

    if let Some(tx) = session_id_tx {
        if let Some(ref ext) = opened.external_session_id {
            let _ = tx.send(ext.clone());
        }
    }

    run_opened_exchange(
        exchange_id,
        interpretation_id,
        opened,
        encoded,
        interpreter,
        interpretation_limits,
        deadline,
        cleanup_deadline,
        unit_tx,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_opened_exchange(
    exchange_id: ExchangeId,
    interpretation_id: InterpretationId,
    opened: OpenedRawConnection,
    encoded: EncodedExchange,
    interpreter: &dyn InterpreterFactory,
    limits: InterpretationLimits,
    deadline: Duration,
    cleanup_deadline: Duration,
    unit_tx: Option<mpsc::Sender<CanonicalUnitEvent>>,
) -> Result<ExchangeOutcome, ExchangeFailure> {
    let join_grace = cleanup_deadline.max(Duration::from_millis(50));
    let connection_id = opened.connection_id.clone();
    let interpretation = interpreter
        .start(StartInterpretation {
            interpretation_id: interpretation_id.clone(),
            connection_id: connection_id.clone(),
            external_session_id: opened.external_session_id.clone(),
            dialect: opened.dialect.clone(),
            limits,
        })
        .map_err(|_| ExchangeFailure::InterpretationFailed)?;

    // Pump raw output → interpretation (owned task).
    let output = Arc::clone(&opened.output);
    let interp_in = interpretation.input.clone();
    let mut joins = JoinSet::new();
    joins.spawn(async move {
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
    });

    // Send encoded request body.
    if !encoded.bytes.is_empty() && opened.input.send(encoded.bytes.clone()).await.is_err() {
        abort_joins(&mut joins).await;
        return Err(ExchangeFailure::ConnectorFailed);
    }
    match encoded.input_policy {
        ExchangeInputPolicy::SendAndFinish => {
            if opened.input.finish().await.is_err() {
                abort_joins(&mut joins).await;
                return Err(ExchangeFailure::ConnectorFailed);
            }
        }
        ExchangeInputPolicy::SendAndRetain => {}
    }

    // Collect interpretation events; optionally fan out live (D-011).
    let events_handle = interpretation.events;
    let units = Arc::new(tokio::sync::Mutex::new(Vec::<CanonicalUnitEvent>::new()));
    let units_task = {
        let units = Arc::clone(&units);
        let unit_tx = unit_tx;
        tokio::spawn(async move {
            while let Some(ev) = events_handle.recv().await {
                match ev {
                    monoloop_contracts::InterpreterOutputEvent::Unit(u) => {
                        let unit = *u;
                        if let Some(ref tx) = unit_tx {
                            let _ = tx.send(unit.clone()).await;
                        }
                        units.lock().await.push(unit);
                    }
                    monoloop_contracts::InterpreterOutputEvent::Ended(_) => break,
                }
            }
        })
    };

    // D-012: abort pump + units collector + terminate connector if this future is dropped
    // (e.g. actor cancel wins select). On normal completion, take handles and join.
    let mut guard = ExchangeGuard {
        control: Some(opened.control.clone()),
        joins: Some(joins),
        units_abort: Some(units_task.abort_handle()),
    };

    let completion = interpretation.completion;
    let conn_completion = opened.completion;
    let external_session_id = opened.external_session_id.clone();
    let open_control = opened.control.clone();

    let (interp_end, conn_end) = tokio::select! {
        _ = tokio::time::sleep(deadline) => {
            let _ = open_control
                .terminate(monoloop_connector::TerminationReason::CallerForced);
            units_task.abort();
            let _ = tokio::time::timeout(join_grace, units_task).await;
            if let Some(mut joins) = guard.joins.take() {
                abort_joins(&mut joins).await;
            }
            let _ = guard.control.take();
            let _ = guard.units_abort.take();
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
    let _ = guard.units_abort.take();
    let _ = tokio::time::timeout(join_grace, units_task).await;
    let _ = guard.control.take();

    let units = units.lock().await.clone();
    let failure = reconcile_terminals(&conn_end, &interp_end);

    Ok(ExchangeOutcome {
        exchange_id,
        connection_id,
        interpretation_id,
        external_session_id,
        units,
        connection_end: conn_end,
        interpretation_end: interp_end,
        failure,
    })
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
