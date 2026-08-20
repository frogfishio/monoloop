//! Supervise [`DefaultLoopRuntime`] for transaction tool dispatch (M5).
//!
//! Feeds complete exchange units into the canonical Loop state machine under
//! [`TaskClass::LoopRuntime`]. Empty-registry composition uses
//! [`EmptyToolRegistry`] + [`NoToolRuntime`] — zero effects, truthful
//! `tool_unavailable` via Loop output mapped to transaction lifecycle events.

use super::event_publisher::EventPublisherCommand;
use super::session_identity::session_key_for;
use super::task_spawner::{SpawnReject, TransactionTaskSpawner};
use super::task_supervisor::TaskClass;
use crate::registry::EmptyToolRegistry;
use crate::runtime::{DefaultLoopRuntime, StartLoop};
use crate::subscription::SubscriptionPublisher;
use crate::tools::NoToolRuntime;
use crate::transaction::sticky_cancel::StickyCancel;
use monoloop_contracts::{
    CanonicalToolError, CanonicalToolResult, CanonicalToolResultOutcome, CanonicalUnit,
    CanonicalUnitEvent, ChannelId, ExchangeId, InterpreterOutputEvent, LoopEnd, LoopEndKind,
    LoopId, LoopLimits, LoopOutputEvent, LoopScope, MonoloopRunId, OutboundToolOutcome, SessionId,
    ToolId, ToolLifecycleEvent, ToolRequestState, TransactionEventPayload, TransactionId,
};
use std::sync::Arc;
use tokio::sync::mpsc;

/// Report from one supervised Loop pass over exchange units.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LoopDispatchReport {
    /// Tools resolved unavailable by EmptyToolRegistry.
    pub tools_unavailable: u32,
    /// OutboundToolResult events observed.
    pub outbound_results: u32,
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

/// Run EmptyToolRegistry Loop under TaskSupervisor; map outputs to transaction events.
#[allow(clippy::too_many_arguments)]
pub async fn run_supervised_empty_loop(
    tasks: &TransactionTaskSpawner,
    transaction_id: TransactionId,
    channel_id: ChannelId,
    session_id: Option<SessionId>,
    exchange_id: ExchangeId,
    units: Vec<CanonicalUnitEvent>,
    publish_tx: mpsc::Sender<EventPublisherCommand>,
    cancel: Arc<StickyCancel>,
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
            tool_registry: Arc::new(EmptyToolRegistry::new()),
            tool_runtime: Arc::new(NoToolRuntime::new()),
            output_capacity: limits.max_output_queue.max(16),
            limits,
        })
        .map_err(|_| LoopDispatchError::StartFailed)?;

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
                if r.outcome != OutboundToolOutcome::ToolUnavailable {
                    continue;
                }
                let tool_id = ToolId::try_new("unavailable").expect("static");
                let err = CanonicalToolError::try_new(
                    "tool_unavailable",
                    "no_registered_tool",
                    None,
                    256,
                )
                .expect("static error");
                // Preserve Loop's action id; keep provider correlation distinct
                // from exchange-scoped construction helper (§22.6).
                let provider_tool_call_id = r.tool_action_id.as_str().to_string();
                let result = CanonicalToolResult {
                    transaction_id,
                    session_key: session_key.clone(),
                    exchange_id,
                    tool_action_id: r.tool_action_id.clone(),
                    tool_id,
                    provider_tool_call_id,
                    request_ordinal: 0,
                    outcome: CanonicalToolResultOutcome::DomainFailed(err),
                };
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
