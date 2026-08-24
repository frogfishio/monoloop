//! Supervise [`DefaultLoopRuntime`] for transaction tool dispatch (M5).
//!
//! Feeds complete exchange units into the canonical Loop state machine under
//! [`TaskClass::LoopRuntime`]. Empty-registry composition uses
//! [`EmptyToolRegistry`] + [`NoToolRuntime`] — zero effects, truthful
//! `tool_unavailable`. Non-empty composition uses [`ResolvedToolRegistry`] +
//! [`HostToolRuntime`] (supervisor-owned tool workers).

use super::event_publisher::{EventPublisherCommand, OrdinaryCmdAdmit};
use super::session_identity::{
    provider_tool_call_id_from_action, session_key_for, tool_action_id_for_exchange,
};
use super::task_spawner::{SpawnReject, TransactionTaskSpawner};
use super::task_supervisor::TaskClass;
use crate::registry::{EmptyToolRegistry, ToolRegistry};
use crate::runtime::{DefaultLoopRuntime, StartLoop};
use crate::subscription::SubscriptionPublisher;
use crate::tools::{NoToolRuntime, ToolRuntime};
use crate::transaction::sticky_cancel::StickyCancel;
use monoloop_contracts::{
    CanonicalToolError, CanonicalToolResult, CanonicalToolResultOutcome, CanonicalUnit,
    CanonicalUnitEvent, ChannelId, ExchangeId, InterpreterOutputEvent, LoopEnd, LoopEndKind,
    LoopId, LoopLimits, LoopOutputEvent, LoopScope, MonoloopRunId, OutboundToolOutcome, SessionId,
    ToolActionEvent, ToolId, ToolLifecycleEvent, ToolRequestState, TransactionEventPayload,
    TransactionId,
};
use std::sync::Arc;

/// Report from one supervised Loop pass over exchange units.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LoopDispatchReport {
    /// Tools resolved unavailable (empty registry or unknown name).
    pub tools_unavailable: u32,
    /// OutboundToolResult events observed.
    pub outbound_results: u32,
    /// Successful tool completions published as lifecycle events.
    pub tools_completed: u32,
    /// Ordered CanonicalToolResult values (model/feed order) for continuation.
    pub tool_results: Vec<CanonicalToolResult>,
}

/// Error driving the supervised Loop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoopDispatchError {
    /// Cancelled.
    Cancelled,
    /// Publish / feed failed.
    PublishFailed,
    /// Loop start/prepare failed.
    StartFailed,
    /// Loop ended with a non-drained failure kind.
    LoopFailed,
}

/// True if any unit is Ready with name+payload.
pub fn needs_loop_dispatch(units: &[CanonicalUnitEvent]) -> bool {
    units.iter().any(|unit| {
        let snap = unit.snapshot();
        matches!(
            &snap.unit,
            CanonicalUnit::Tool(t)
                if t.request_state == ToolRequestState::Ready
                    && t.tool_name.is_some()
                    && t.request_payload.is_some()
        )
    })
}

/// Assign exchange-scoped `tool_action_id` while leaving raw provider ids recoverable.
pub fn scope_tool_units_for_exchange(
    units: Vec<CanonicalUnitEvent>,
    exchange_id: ExchangeId,
) -> Vec<CanonicalUnitEvent> {
    units
        .into_iter()
        .map(|ev| scope_one_tool_unit(ev, exchange_id))
        .collect()
}

fn scope_one_tool_unit(
    mut event: CanonicalUnitEvent,
    exchange_id: ExchangeId,
) -> CanonicalUnitEvent {
    let snap = match &mut event {
        CanonicalUnitEvent::Created(s)
        | CanonicalUnitEvent::Advanced(s)
        | CanonicalUnitEvent::Completed(s)
        | CanonicalUnitEvent::Incomplete(s) => s,
    };
    if let CanonicalUnit::Tool(tool) = &mut snap.unit {
        *tool = scope_tool_action(tool.clone(), exchange_id);
    }
    event
}

fn scope_tool_action(mut tool: ToolActionEvent, exchange_id: ExchangeId) -> ToolActionEvent {
    if provider_tool_call_id_from_action(exchange_id, &tool.tool_action_id).is_none() {
        let provider = tool.tool_action_id.as_str().to_string();
        tool.tool_action_id = tool_action_id_for_exchange(exchange_id, &provider);
    }
    tool
}

fn provider_id_for_result(
    exchange_id: ExchangeId,
    action: &monoloop_contracts::ToolActionId,
) -> String {
    provider_tool_call_id_from_action(exchange_id, action)
        .unwrap_or(action.as_str())
        .to_string()
}

/// Run EmptyToolRegistry Loop under TaskSupervisor; map outputs to transaction events.
#[allow(clippy::too_many_arguments)]
pub async fn run_supervised_empty_loop(
    tasks: &TransactionTaskSpawner,
    transaction_id: TransactionId,
    channel_id: ChannelId,
    session_id: Option<SessionId>,
    exchange_id: ExchangeId,
    units: Vec<CanonicalUnitEvent>,
    publish_tx: OrdinaryCmdAdmit,
    cancel: Arc<StickyCancel>,
) -> Result<LoopDispatchReport, LoopDispatchError> {
    run_supervised_loop(
        tasks,
        transaction_id,
        channel_id,
        session_id,
        exchange_id,
        units,
        publish_tx,
        cancel,
        Arc::new(EmptyToolRegistry::new()),
        Arc::new(NoToolRuntime::new()),
    )
    .await
}

/// Run Loop with a non-empty resolved registry + host tool runtime.
#[allow(clippy::too_many_arguments)]
pub async fn run_supervised_tool_loop(
    tasks: &TransactionTaskSpawner,
    transaction_id: TransactionId,
    channel_id: ChannelId,
    session_id: Option<SessionId>,
    exchange_id: ExchangeId,
    units: Vec<CanonicalUnitEvent>,
    publish_tx: OrdinaryCmdAdmit,
    cancel: Arc<StickyCancel>,
    tool_registry: Arc<dyn ToolRegistry>,
    tool_runtime: Arc<dyn ToolRuntime>,
) -> Result<LoopDispatchReport, LoopDispatchError> {
    run_supervised_loop(
        tasks,
        transaction_id,
        channel_id,
        session_id,
        exchange_id,
        units,
        publish_tx,
        cancel,
        tool_registry,
        tool_runtime,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_supervised_loop(
    tasks: &TransactionTaskSpawner,
    transaction_id: TransactionId,
    channel_id: ChannelId,
    session_id: Option<SessionId>,
    exchange_id: ExchangeId,
    units: Vec<CanonicalUnitEvent>,
    publish_tx: OrdinaryCmdAdmit,
    cancel: Arc<StickyCancel>,
    tool_registry: Arc<dyn ToolRegistry>,
    tool_runtime: Arc<dyn ToolRuntime>,
) -> Result<LoopDispatchReport, LoopDispatchError> {
    let (pub_, sub) = SubscriptionPublisher::channel(format!("tx-{transaction_id}"), 64);
    let limits = LoopLimits::default();
    let run_id = MonoloopRunId::new(format!("tx-{transaction_id}"));
    let loop_id = LoopId::new(format!("loop-{transaction_id}"));
    let scope = LoopScope {
        monoloop_run_id: run_id.clone(),
        loop_id: loop_id.clone(),
        accepted_interpretation_ids: vec![],
        accepted_connection_ids: vec![],
        accepted_external_session_ids: vec![],
        accept_all_in_run: true,
    };
    let (handle, fut) = DefaultLoopRuntime::new()
        .prepare(StartLoop {
            monoloop_run_id: run_id,
            loop_id,
            scope,
            subscription: sub,
            tool_registry,
            tool_runtime,
            output_capacity: limits.max_output_queue.max(16),
            limits,
        })
        .map_err(|_| LoopDispatchError::StartFailed)?;

    let units = scope_tool_units_for_exchange(units, exchange_id);

    let feed = async {
        for unit in &units {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    return Err(LoopDispatchError::Cancelled);
                }
                res = pub_.publish(InterpreterOutputEvent::Unit(Box::new(unit.clone()))) => {
                    if res.is_err() {
                        return Err(LoopDispatchError::PublishFailed);
                    }
                }
            }
        }
        // Close subscription so Loop can drain (must happen before Loop join).
        drop(pub_);
        Ok(())
    };

    // Prefer TaskSupervisor ownership (D-043): bounded Busy retries first.
    const BUSY_RETRIES: u32 = 8;
    match tasks
        .spawn_with_busy_retry(
            TaskClass::LoopRuntime(transaction_id),
            fut,
            BUSY_RETRIES,
            || cancel.is_cancelled(),
        )
        .await
    {
        Ok(_) => {
            // Supervisor owns the JoinHandle (Law 23). Feed while Loop runs.
            if let Err(e) = feed.await {
                handle.control.cancel();
                let _ = await_loop_end(&handle, &cancel).await;
                return Err(e);
            }
        }
        Err(SpawnReject::Busy { future }) => {
            // Last resort after retries: coordinator-owned join (still owned, not ambient).
            if let Err(e) = drive_busy_loop(future, feed, &handle, &cancel).await {
                handle.control.cancel();
                let _ = await_loop_end(&handle, &cancel).await;
                return Err(e);
            }
        }
        Err(SpawnReject::Rejected { future }) => {
            // Supervisor closed, or cancel observed during Busy retry — do not drive.
            drop(future);
            handle.control.cancel();
            return Err(if cancel.is_cancelled() {
                LoopDispatchError::Cancelled
            } else {
                LoopDispatchError::StartFailed
            });
        }
        Err(SpawnReject::Orphaned) => {
            // Future left this task; do not drive a substitute (Law 23/25).
            handle.control.cancel();
            return Err(LoopDispatchError::StartFailed);
        }
    }

    if cancel.is_cancelled() {
        handle.control.cancel();
        let _ = await_loop_end(&handle, &cancel).await;
        return Err(LoopDispatchError::Cancelled);
    }

    let session_key = session_key_for(channel_id, session_id, transaction_id);
    let mut report = LoopDispatchReport::default();
    // Take receiver once — no Mutex held across recv / cancel (Law 21).
    let mut out = handle.take_output().await;
    loop {
        let ev = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                handle.control.cancel();
                let _ = await_loop_end(&handle, &cancel).await;
                return Err(LoopDispatchError::Cancelled);
            }
            ev = out.recv() => ev,
        };
        let Some(ev) = ev else {
            break;
        };
        match ev {
            LoopOutputEvent::ToolUnavailable { .. } => {
                report.tools_unavailable = report.tools_unavailable.saturating_add(1);
            }
            LoopOutputEvent::OutboundToolResult(r) => {
                report.outbound_results = report.outbound_results.saturating_add(1);
                let fallback_provider = provider_id_for_result(exchange_id, &r.tool_action_id);
                let ordinal = report.tool_results.len() as u32;
                let result = match r.outcome {
                    OutboundToolOutcome::ToolUnavailable => {
                        let tool_id = ToolId::try_new("unavailable").expect("static");
                        let err = CanonicalToolError::try_new(
                            "tool_unavailable",
                            "no_registered_tool",
                            None,
                            256,
                        )
                        .expect("static error");
                        CanonicalToolResult {
                            transaction_id,
                            session_key: session_key.clone(),
                            exchange_id,
                            tool_action_id: r.tool_action_id.clone(),
                            tool_id,
                            provider_tool_call_id: fallback_provider,
                            request_ordinal: ordinal,
                            outcome: CanonicalToolResultOutcome::DomainFailed(err),
                        }
                    }
                    OutboundToolOutcome::Success | OutboundToolOutcome::ExecutionFailed => {
                        // Prefer round-tripped CanonicalToolResult from HostToolRuntime.
                        if let Ok(mut decoded) =
                            serde_json::from_str::<CanonicalToolResult>(&r.payload)
                        {
                            if matches!(decoded.outcome, CanonicalToolResultOutcome::Succeeded(_)) {
                                report.tools_completed = report.tools_completed.saturating_add(1);
                            }
                            // Keep envelope identities consistent with this Loop pass.
                            decoded.transaction_id = transaction_id;
                            decoded.session_key = session_key.clone();
                            decoded.exchange_id = exchange_id;
                            decoded.tool_action_id = r.tool_action_id.clone();
                            if decoded.provider_tool_call_id.is_empty() {
                                decoded.provider_tool_call_id = fallback_provider;
                            }
                            decoded.request_ordinal = ordinal;
                            decoded
                        } else if matches!(r.outcome, OutboundToolOutcome::Success) {
                            report.tools_completed = report.tools_completed.saturating_add(1);
                            let output = match serde_json::from_str::<serde_json::Value>(&r.payload)
                            {
                                Ok(v) => monoloop_contracts::CanonicalToolOutput::Json(v),
                                Err(_) => {
                                    monoloop_contracts::CanonicalToolOutput::Text(r.payload.clone())
                                }
                            };
                            CanonicalToolResult {
                                transaction_id,
                                session_key: session_key.clone(),
                                exchange_id,
                                tool_action_id: r.tool_action_id.clone(),
                                tool_id: ToolId::try_new("completed").unwrap_or_else(|_| {
                                    ToolId::try_new("unavailable").expect("static")
                                }),
                                provider_tool_call_id: fallback_provider,
                                request_ordinal: ordinal,
                                outcome: CanonicalToolResultOutcome::Succeeded(output),
                            }
                        } else {
                            let err = CanonicalToolError::try_new(
                                "tool_execution_failed",
                                r.payload.chars().take(128).collect::<String>(),
                                None,
                                256,
                            )
                            .unwrap_or_else(|_| {
                                CanonicalToolError::try_new(
                                    "tool_execution_failed",
                                    "failed",
                                    None,
                                    64,
                                )
                                .expect("static")
                            });
                            CanonicalToolResult {
                                transaction_id,
                                session_key: session_key.clone(),
                                exchange_id,
                                tool_action_id: r.tool_action_id.clone(),
                                tool_id: ToolId::try_new("failed").unwrap_or_else(|_| {
                                    ToolId::try_new("unavailable").expect("static")
                                }),
                                provider_tool_call_id: fallback_provider,
                                request_ordinal: ordinal,
                                outcome: CanonicalToolResultOutcome::DomainFailed(err),
                            }
                        }
                    }
                    OutboundToolOutcome::DispatchRejected
                    | OutboundToolOutcome::Cancelled
                    | OutboundToolOutcome::ExecutionLost => {
                        let err = CanonicalToolError::try_new(
                            "tool_execution_failed",
                            r.payload.chars().take(128).collect::<String>(),
                            None,
                            256,
                        )
                        .unwrap_or_else(|_| {
                            CanonicalToolError::try_new("tool_execution_failed", "failed", None, 64)
                                .expect("static")
                        });
                        CanonicalToolResult {
                            transaction_id,
                            session_key: session_key.clone(),
                            exchange_id,
                            tool_action_id: r.tool_action_id.clone(),
                            tool_id: ToolId::try_new("failed").unwrap_or_else(|_| {
                                ToolId::try_new("unavailable").expect("static")
                            }),
                            provider_tool_call_id: fallback_provider,
                            request_ordinal: ordinal,
                            outcome: CanonicalToolResultOutcome::DomainFailed(err),
                        }
                    }
                };
                report.tool_results.push(result.clone());
                let send = publish_tx.send(EventPublisherCommand::Publish(Box::new(
                    TransactionEventPayload::ToolLifecycle(ToolLifecycleEvent::Completed {
                        result,
                    }),
                )));
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => {
                        handle.control.cancel();
                        let _ = await_loop_end(&handle, &cancel).await;
                        return Err(LoopDispatchError::Cancelled);
                    }
                    res = send => {
                        if res.is_err() {
                            handle.control.cancel();
                            let _ = await_loop_end(&handle, &cancel).await;
                            return Err(LoopDispatchError::PublishFailed);
                        }
                    }
                }
            }
            LoopOutputEvent::LoopEnded(_) => break,
            _ => {}
        }
    }

    let end = await_loop_end(&handle, &cancel).await?;
    match end.kind {
        LoopEndKind::Drained => Ok(report),
        LoopEndKind::Cancelled => Err(LoopDispatchError::Cancelled),
        _ => Err(LoopDispatchError::LoopFailed),
    }
}

/// Busy/Closed fallback: run Loop future with feed; cancel control on StickyCancel.
async fn drive_busy_loop(
    returned: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
    feed: impl std::future::Future<Output = Result<(), LoopDispatchError>>,
    handle: &crate::runtime::LoopHandle,
    cancel: &StickyCancel,
) -> Result<(), LoopDispatchError> {
    let control = handle.control.clone();
    tokio::pin!(returned);
    tokio::pin!(feed);
    let mut feed_res: Option<Result<(), LoopDispatchError>> = None;
    let mut saw_cancel = cancel.is_cancelled();
    if saw_cancel {
        control.cancel();
    }
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled(), if !saw_cancel => {
                control.cancel();
                saw_cancel = true;
            }
            res = &mut feed, if feed_res.is_none() => {
                feed_res = Some(res);
            }
            () = &mut returned => {
                break;
            }
        }
    }
    // Leaving this function drops `feed`, closing the publisher if unfinished.
    if saw_cancel || cancel.is_cancelled() {
        control.cancel();
        return Err(LoopDispatchError::Cancelled);
    }
    match feed_res {
        Some(Ok(())) => Ok(()),
        Some(Err(e)) => Err(e),
        None => {
            control.cancel();
            Err(LoopDispatchError::Cancelled)
        }
    }
}

async fn await_loop_end(
    handle: &crate::runtime::LoopHandle,
    cancel: &StickyCancel,
) -> Result<LoopEnd, LoopDispatchError> {
    // Take the oneshot once, then select — never poll completion twice (Law 21).
    let mut rx = handle.completion.take_receiver().await;
    tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            handle.control.cancel();
            let _ = rx.await;
            Err(LoopDispatchError::Cancelled)
        }
        end = &mut rx => Ok(end.unwrap_or(LoopEnd {
            monoloop_run_id: MonoloopRunId::new("unknown"),
            loop_id: LoopId::new("unknown"),
            kind: LoopEndKind::InvariantFailed,
            delivery_events_received: 0,
            duplicate_events: 0,
            tools_unavailable: 0,
            outbound_results_emitted: 0,
            safe_diagnostics: vec!["completion dropped".into()],
        })),
    }
}
