//! Supervised DirectLlm exchange (v2 §15–§16) — Fake/HTTP owner join path.
//!
//! Connector owner + interpreter pump + units collector are spawned through
//! [`TransactionTaskSpawner`]. Ambient `tokio::spawn` is forbidden here.
//! After any child is registered, failure paths terminate the connection and
//! await those children within `cleanup_deadline` (no orphan owners).

use super::task_spawner::{SpawnReject, TransactionTaskSpawner};
use super::task_supervisor::TaskClass;
use crate::transaction::sticky_cancel::StickyCancel;
use monoloop_connector::{
    ConnectionControlHandle, ConnectionEnd, ConnectionEndKind, Connector, OpenConnection,
    SessionAttachment, TerminationReason,
};
use monoloop_contracts::{
    CanonicalUnitEvent, ConnectionId, EffectiveConfig, ExchangeId, ExchangeInputPolicy,
    InterpretationEnd, InterpretationEndKind, InterpretationId, InterpretationLimits,
    OutboundDialectEncoder, TransactionEndKind, TransactionId,
};
use monoloop_interpreter::{InterpreterFactory, StartInterpretation};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, Mutex};

/// Outcome of one supervised exchange.
pub struct DirectExchangeOutcome {
    /// Complete unit lifecycle events observed (not Ended).
    pub units: Vec<CanonicalUnitEvent>,
    /// Mapped terminal proposal kind.
    pub terminal: TransactionEndKind,
    /// Exchange identity (empty-tool / Loop correlation).
    pub exchange_id: ExchangeId,
    /// Authoritative external session when the Connector returned one (§22.6).
    pub external_session_id: Option<monoloop_contracts::ExternalSessionId>,
}

/// Failure before a successful terminal mapping.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExchangeFailure {
    ChannelOpenFailed,
    EncodingFailed,
    ConnectorFailed,
    InterpretationFailed,
    Cancelled,
    Terminated,
    LimitExceeded,
    DeadlineExceeded,
    InvariantFailed,
    SpawnFailed,
}

impl ExchangeFailure {
    fn to_terminal(self) -> TransactionEndKind {
        match self {
            Self::Cancelled => TransactionEndKind::Cancelled,
            Self::Terminated => TransactionEndKind::Terminated,
            Self::LimitExceeded => TransactionEndKind::LimitExceeded,
            Self::DeadlineExceeded => TransactionEndKind::DeadlineExceeded,
            Self::InvariantFailed | Self::SpawnFailed => TransactionEndKind::InvariantFailed,
            Self::ChannelOpenFailed => TransactionEndKind::ChannelOpenFailed,
            Self::EncodingFailed => TransactionEndKind::EncodingFailed,
            Self::ConnectorFailed => TransactionEndKind::ConnectorFailed,
            Self::InterpretationFailed => TransactionEndKind::InterpretationFailed,
        }
    }
}

/// Oneshot joins for supervised exchange children (concurrent wait).
struct ChildJoins {
    owner: Option<oneshot::Receiver<()>>,
    pump: Option<oneshot::Receiver<()>>,
    units: Option<oneshot::Receiver<()>>,
}

impl ChildJoins {
    fn new() -> Self {
        Self {
            owner: None,
            pump: None,
            units: None,
        }
    }

    async fn wait(self, grace: Duration) {
        let owner = self.owner;
        let pump = self.pump;
        let units = self.units;
        let wait = async {
            tokio::join!(
                async {
                    if let Some(rx) = owner {
                        let _ = rx.await;
                    }
                },
                async {
                    if let Some(rx) = pump {
                        let _ = rx.await;
                    }
                },
                async {
                    if let Some(rx) = units {
                        let _ = rx.await;
                    }
                },
            );
        };
        let _ = tokio::time::timeout(grace, wait).await;
    }
}

/// Why the prompt-ready gate aborted after Connector open.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PromptProceedError {
    /// `ChannelLimits.max_distinct_sessions` exceeded at SessionKey claim.
    DistinctSessionsExceeded,
    /// Any other claim / activate / establish failure (fail closed).
    Failed,
}

/// After open succeeds, before prompt send (CreationOnly claim / MCP activate).
pub struct PromptReadyGate {
    /// Exchange reports the opened external session (if any).
    pub opened: oneshot::Sender<Option<monoloop_contracts::ExternalSessionId>>,
    /// Coordinator signals ready to send prompt (`Ok`) or abort (`Err`).
    pub proceed: oneshot::Receiver<Result<(), PromptProceedError>>,
}

/// Run one supervised exchange with TaskSupervisor-owned children.
///
/// `exchange_id` MUST be the same id used for MCP `install_pending` when tools
/// are CreationOnly-installed, so MCP tool lifecycle events correlate.
#[allow(clippy::too_many_arguments)]
pub async fn run_direct_llm_exchange(
    transaction_id: TransactionId,
    exchange_id: ExchangeId,
    tasks: &TransactionTaskSpawner,
    connector: &dyn Connector,
    encoder: &dyn OutboundDialectEncoder,
    interpreter: &dyn InterpreterFactory,
    endpoint_ref: &str,
    credential_ref: Option<&str>,
    input: &monoloop_contracts::CanonicalInput,
    config: &EffectiveConfig,
    cancel: Arc<StickyCancel>,
    deadline: Duration,
    cleanup_deadline: Duration,
    max_encoded_exchange_bytes: usize,
    max_retained_unit_bytes: usize,
    max_remaining_provider_input_bytes: usize,
    session_attachment: Option<std::sync::Arc<SessionAttachment>>,
    prompt_ready: Option<PromptReadyGate>,
) -> DirectExchangeOutcome {
    match run_inner(
        transaction_id,
        exchange_id,
        tasks,
        connector,
        encoder,
        interpreter,
        endpoint_ref,
        credential_ref,
        input,
        config,
        cancel,
        deadline,
        cleanup_deadline,
        max_encoded_exchange_bytes,
        max_retained_unit_bytes,
        max_remaining_provider_input_bytes,
        session_attachment,
        prompt_ready,
    )
    .await
    {
        Ok((units, external_session_id)) => DirectExchangeOutcome {
            units,
            terminal: TransactionEndKind::Completed,
            exchange_id,
            external_session_id,
        },
        Err(f) => DirectExchangeOutcome {
            units: vec![],
            terminal: f.to_terminal(),
            exchange_id,
            external_session_id: None,
        },
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_inner(
    transaction_id: TransactionId,
    exchange_id: ExchangeId,
    tasks: &TransactionTaskSpawner,
    connector: &dyn Connector,
    encoder: &dyn OutboundDialectEncoder,
    interpreter: &dyn InterpreterFactory,
    endpoint_ref: &str,
    credential_ref: Option<&str>,
    input: &monoloop_contracts::CanonicalInput,
    config: &EffectiveConfig,
    cancel: Arc<StickyCancel>,
    deadline: Duration,
    cleanup_deadline: Duration,
    max_encoded_exchange_bytes: usize,
    max_retained_unit_bytes: usize,
    max_remaining_provider_input_bytes: usize,
    session_attachment: Option<std::sync::Arc<SessionAttachment>>,
    prompt_ready: Option<PromptReadyGate>,
) -> Result<
    (
        Vec<CanonicalUnitEvent>,
        Option<monoloop_contracts::ExternalSessionId>,
    ),
    ExchangeFailure,
> {
    let encoded = encoder
        .encode_initial(monoloop_contracts::InitialEncodeRequest {
            transaction_id: &transaction_id,
            exchange_id: &exchange_id,
            input,
            config,
            tools: &[],
        })
        .map_err(|_| ExchangeFailure::EncodingFailed)?;
    if encoded.bytes.len() > max_encoded_exchange_bytes {
        return Err(ExchangeFailure::EncodingFailed);
    }
    if encoded.bytes.len() > max_remaining_provider_input_bytes {
        return Err(ExchangeFailure::LimitExceeded);
    }

    let started = Instant::now();
    let remaining = || deadline.saturating_sub(started.elapsed());

    let connection_id = ConnectionId::generate();
    let interpretation_id = InterpretationId::generate();

    let mut open = OpenConnection::new(connection_id.clone(), endpoint_ref);
    open.credential_ref = credential_ref.map(|s| s.to_string());
    if let Some(attachment) = session_attachment {
        open = open.with_session_attachment(attachment);
    }

    let pending = connector.begin_open(open);
    let control = pending.control.clone();

    let mut opened = tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            let _ = control.terminate(TerminationReason::CallerForced);
            return Err(ExchangeFailure::Cancelled);
        }
        res = tokio::time::timeout(remaining(), pending.opened) => {
            match res {
                Ok(Ok(o)) => o,
                Ok(Err(_)) => return Err(ExchangeFailure::ChannelOpenFailed),
                Err(_) => {
                    let _ = control.terminate(TerminationReason::CallerForced);
                    return Err(ExchangeFailure::DeadlineExceeded);
                }
            }
        }
    };

    let Some(owner_work) = opened.take_owner_work() else {
        let _ = opened.control.terminate(TerminationReason::CallerForced);
        return Err(ExchangeFailure::InvariantFailed);
    };

    let mut children = ChildJoins::new();
    let open_control = opened.control.clone();

    // Spawn Connector owner before send/receive (v2 §15).
    let owner_future = owner_work.into_future();
    let (owner_done_tx, owner_done_rx) = oneshot::channel::<()>();
    match tasks
        .spawn(
            TaskClass::ConnectorOwner(transaction_id, exchange_id),
            async move {
                owner_future.await;
                let _ = owner_done_tx.send(());
            },
        )
        .await
    {
        Ok(_) => {
            children.owner = Some(owner_done_rx);
        }
        Err(SpawnReject::Busy { future } | SpawnReject::Rejected { future }) => {
            // Spawner rejected before accept: drive owner inline after terminate.
            let _ = open_control.terminate(TerminationReason::CallerForced);
            let _ = tokio::time::timeout(cleanup_deadline, future).await;
            return Err(ExchangeFailure::SpawnFailed);
        }
        Err(SpawnReject::Orphaned) => {
            // Future left the caller; fail closed (Law 23/25).
            let _ = open_control.terminate(TerminationReason::CallerForced);
            children.wait(cleanup_deadline).await;
            return Err(ExchangeFailure::SpawnFailed);
        }
    }

    let interpretation = match interpreter.start(StartInterpretation {
        interpretation_id: interpretation_id.clone(),
        connection_id: opened.connection_id.clone(),
        external_session_id: opened.external_session_id.clone(),
        dialect: opened.dialect.clone(),
        limits: InterpretationLimits::default(),
    }) {
        Ok(i) => i,
        Err(_) => {
            return fail_cleanup(
                &open_control,
                children,
                cleanup_deadline,
                ExchangeFailure::InterpretationFailed,
            )
            .await;
        }
    };

    // Pump raw output → interpreter (supervised InterpreterOwner).
    let output = Arc::clone(&opened.output);
    let interp_in = interpretation.input.clone();
    let (pump_done_tx, pump_done_rx) = oneshot::channel::<()>();
    match tasks
        .spawn(
            TaskClass::InterpreterOwner(transaction_id, exchange_id),
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
                                ConnectorErrorKind::Cancelled | ConnectorErrorKind::Terminated => {
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
                let _ = pump_done_tx.send(());
            },
        )
        .await
    {
        Ok(_) => {
            children.pump = Some(pump_done_rx);
        }
        Err(SpawnReject::Busy { future } | SpawnReject::Rejected { future }) => {
            // Pump rejected before accept; drop unspawned work, join owner.
            drop(future);
            return fail_cleanup(
                &open_control,
                children,
                cleanup_deadline,
                ExchangeFailure::SpawnFailed,
            )
            .await;
        }
        Err(SpawnReject::Orphaned) => {
            return fail_cleanup(
                &open_control,
                children,
                cleanup_deadline,
                ExchangeFailure::SpawnFailed,
            )
            .await;
        }
    }

    // CreationOnly / ExternalAgent: claim + MCP activate before prompt (D-026).
    if let Some(gate) = prompt_ready {
        let external = opened.external_session_id.clone();
        if gate.opened.send(external).is_err() {
            return fail_cleanup(
                &open_control,
                children,
                cleanup_deadline,
                ExchangeFailure::InvariantFailed,
            )
            .await;
        }
        let proceed = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                let _ = open_control.terminate(TerminationReason::CallerForced);
                return fail_cleanup(
                    &open_control,
                    children,
                    cleanup_deadline,
                    ExchangeFailure::Cancelled,
                )
                .await;
            }
            res = gate.proceed => res,
        };
        match proceed {
            Ok(Ok(())) => {}
            Ok(Err(PromptProceedError::DistinctSessionsExceeded)) => {
                return fail_cleanup(
                    &open_control,
                    children,
                    cleanup_deadline,
                    ExchangeFailure::LimitExceeded,
                )
                .await;
            }
            Ok(Err(PromptProceedError::Failed)) | Err(_) => {
                return fail_cleanup(
                    &open_control,
                    children,
                    cleanup_deadline,
                    ExchangeFailure::InvariantFailed,
                )
                .await;
            }
        }
    }

    // Send encoded request body.
    if !encoded.bytes.is_empty() && opened.input.send(encoded.bytes.clone()).await.is_err() {
        return fail_cleanup(
            &open_control,
            children,
            cleanup_deadline,
            ExchangeFailure::ConnectorFailed,
        )
        .await;
    }
    match encoded.input_policy {
        ExchangeInputPolicy::SendAndFinish => {
            if opened.input.finish().await.is_err() {
                return fail_cleanup(
                    &open_control,
                    children,
                    cleanup_deadline,
                    ExchangeFailure::ConnectorFailed,
                )
                .await;
            }
        }
        ExchangeInputPolicy::SendAndRetain => {}
    }

    // Units collector — supervised InterpreterOwner fan-out (not inline on coordinator).
    let events_handle = interpretation.events;
    let units = Arc::new(Mutex::new(Vec::<CanonicalUnitEvent>::new()));
    let (limit_tx, limit_rx) = oneshot::channel::<()>();
    let units_arc = Arc::clone(&units);
    let max_retained = max_retained_unit_bytes;
    let (units_done_tx, units_done_rx) = oneshot::channel::<()>();
    match tasks
        .spawn(
            TaskClass::InterpreterOwner(transaction_id, exchange_id),
            async move {
                let mut retained_bytes = 0usize;
                let mut limit_tx = Some(limit_tx);
                while let Some(ev) = events_handle.recv().await {
                    match ev {
                        monoloop_contracts::InterpreterOutputEvent::Unit(u) => {
                            let unit = *u;
                            let add = estimate_retained_unit_bytes(&unit);
                            if retained_bytes.saturating_add(add) > max_retained {
                                if let Some(tx) = limit_tx.take() {
                                    let _ = tx.send(());
                                }
                                break;
                            }
                            retained_bytes = retained_bytes.saturating_add(add);
                            units_arc.lock().await.push(unit);
                        }
                        monoloop_contracts::InterpreterOutputEvent::Ended(_) => break,
                    }
                }
                let _ = units_done_tx.send(());
            },
        )
        .await
    {
        Ok(_) => {
            children.units = Some(units_done_rx);
        }
        Err(SpawnReject::Busy { future } | SpawnReject::Rejected { future }) => {
            drop(future);
            return fail_cleanup(
                &open_control,
                children,
                cleanup_deadline,
                ExchangeFailure::SpawnFailed,
            )
            .await;
        }
        Err(SpawnReject::Orphaned) => {
            return fail_cleanup(
                &open_control,
                children,
                cleanup_deadline,
                ExchangeFailure::SpawnFailed,
            )
            .await;
        }
    }

    let completion = interpretation.completion;
    let conn_completion = opened.completion;

    let result = tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            let _ = open_control.terminate(TerminationReason::CallerForced);
            Err(ExchangeFailure::Cancelled)
        }
        Ok(()) = limit_rx => {
            let _ = open_control.terminate(TerminationReason::CallerForced);
            Err(ExchangeFailure::LimitExceeded)
        }
        _ = tokio::time::sleep(remaining()) => {
            let _ = open_control.terminate(TerminationReason::CallerForced);
            Err(ExchangeFailure::DeadlineExceeded)
        }
        ends = async {
            let i = completion.wait().await;
            let c = conn_completion.wait().await;
            (i, c)
        } => {
            let (interp_end, conn_end) = ends;
            if let Some(f) = reconcile_terminals(&conn_end, &interp_end) {
                Err(f)
            } else {
                Ok(())
            }
        }
    };

    // Always await supervised children concurrently within cleanup_deadline.
    children.wait(cleanup_deadline).await;

    let external_session_id = opened.external_session_id.clone();
    result?;
    let collected = units.lock().await.clone();
    Ok((collected, external_session_id))
}

async fn fail_cleanup(
    control: &ConnectionControlHandle,
    children: ChildJoins,
    grace: Duration,
    err: ExchangeFailure,
) -> Result<
    (
        Vec<CanonicalUnitEvent>,
        Option<monoloop_contracts::ExternalSessionId>,
    ),
    ExchangeFailure,
> {
    let _ = control.terminate(TerminationReason::CallerForced);
    children.wait(grace).await;
    Err(err)
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

fn estimate_retained_unit_bytes(unit: &CanonicalUnitEvent) -> usize {
    use monoloop_contracts::CanonicalUnit;
    let snap = unit.snapshot();
    let content = match &snap.unit {
        CanonicalUnit::Text(t) => t.content.len(),
        CanonicalUnit::Structure(s) => s.content.len(),
        CanonicalUnit::Tool(t) => t
            .request_payload
            .as_ref()
            .map(|p| p.len())
            .unwrap_or(0)
            .saturating_add(t.result_payload.as_ref().map(|p| p.len()).unwrap_or(0)),
        CanonicalUnit::Diagnostic(d) => d.message.len(),
        _ => 32,
    };
    content.saturating_add(128)
}
