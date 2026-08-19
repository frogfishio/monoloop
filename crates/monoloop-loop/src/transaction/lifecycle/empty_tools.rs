//! Empty-tool qualification path for Runtime v2 (M5 first slice).
//!
//! Dispatches only on complete [`ToolRequestState::Ready`] units through
//! [`EmptyToolRegistry`]. [`NoToolRuntime`] is never started — zero external
//! effects, truthful `tool_unavailable`.
//!
//! This path is the transaction-composition adapter for empty-registry
//! qualification until `DefaultLoopRuntime` is TaskSupervisor-owned (remaining
//! M5). It must not diverge into a second production tool state machine for
//! Available tools.

use crate::registry::{EmptyToolRegistry, ResolveToolRequest, ToolRegistry, ToolResolution};
use monoloop_contracts::{
    CanonicalToolError, CanonicalToolResult, CanonicalToolResultOutcome, CanonicalUnit,
    CanonicalUnitEvent, ChannelId, ExchangeId, SessionId, SessionKey, ToolId, ToolLifecycleEvent,
    ToolRequestState, ToolUnavailableReason, TransactionEventPayload, TransactionId,
};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{mpsc, Notify};

use super::event_publisher::EventPublisherCommand;

/// Outcome of one empty-tool pass over exchange units.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EmptyToolPassReport {
    /// Ready tool requests with name+payload that were considered for dispatch.
    pub ready_seen: u32,
    /// Unavailable resolutions published.
    pub unavailable_published: u32,
    /// Incomplete/waiting tool units ignored (must never dispatch).
    pub non_ready_ignored: u32,
}

/// True if any unit is a complete Ready tool request with name and payload.
pub fn has_ready_tool_units(units: &[CanonicalUnitEvent]) -> bool {
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

/// Session key for empty-tool results: prefer explicit session, else the same
/// transaction-scoped synthetic id the event publisher uses (no ambient
/// "unscoped" / most-recent heuristic).
fn session_key_for(
    channel_id: ChannelId,
    session_id: Option<SessionId>,
    transaction_id: TransactionId,
) -> SessionKey {
    let sid = session_id.unwrap_or_else(|| {
        SessionId::try_new(format!("tx-{transaction_id}"))
            .or_else(|_| SessionId::try_new("direct"))
            .expect("session id")
    });
    SessionKey::new(channel_id, sid)
}

/// Run EmptyToolRegistry resolution for Ready units; publish lifecycle events.
///
/// Must be driven under [`super::task_supervisor::TaskClass::ToolWorker`] when
/// invoked from the transaction coordinator (or inline on the coordinator task
/// when the spawn mailbox rejects — still coordinator-owned, never ambient).
pub async fn run_empty_tool_pass(
    transaction_id: TransactionId,
    channel_id: ChannelId,
    session_id: Option<SessionId>,
    exchange_id: ExchangeId,
    units: &[CanonicalUnitEvent],
    publish_tx: mpsc::Sender<EventPublisherCommand>,
    cancel: Arc<Notify>,
) -> Result<EmptyToolPassReport, EmptyToolPassError> {
    let registry = EmptyToolRegistry::new();
    let mut report = EmptyToolPassReport::default();
    let mut seen: HashSet<String> = HashSet::new();
    let session_key = session_key_for(channel_id, session_id, transaction_id);

    for unit in units {
        let snap = unit.snapshot();
        let CanonicalUnit::Tool(tool) = &snap.unit else {
            continue;
        };
        if tool.request_state != ToolRequestState::Ready {
            report.non_ready_ignored = report.non_ready_ignored.saturating_add(1);
            continue;
        }
        let Some(name) = tool.tool_name.clone() else {
            continue;
        };
        let Some(payload) = tool.request_payload.clone() else {
            continue;
        };
        let action_key = tool.tool_action_id.as_str().to_string();
        if !seen.insert(action_key) {
            // Duplicate Ready — ignore (same as Loop dedup).
            continue;
        }
        report.ready_seen = report.ready_seen.saturating_add(1);

        let resolution = registry
            .resolve(ResolveToolRequest {
                tool_action_id: tool.tool_action_id.clone(),
                tool_name: name.clone(),
                request_payload: payload,
            })
            .await
            .map_err(|_| EmptyToolPassError::RegistryFailed)?;

        match resolution {
            ToolResolution::Unavailable(reason) => {
                // Empty registry: never call ToolRuntime.start.
                debug_assert!(matches!(reason, ToolUnavailableReason::NoRegisteredTool));
                let tool_id = ToolId::try_new(&name)
                    .unwrap_or_else(|_| ToolId::try_new("unavailable").expect("static tool id"));
                // DomainFailed with stable code `tool_unavailable` is the
                // transaction-stream encoding of EmptyToolRegistry denial
                // (LoopOutputEvent::ToolUnavailable + OutboundToolResult on the
                // inner Loop port). Not a ToolExchangeFailed / RuntimeFailed.
                let err = CanonicalToolError::try_new(
                    "tool_unavailable",
                    "no_registered_tool",
                    None,
                    256,
                )
                .unwrap_or_else(|_| {
                    CanonicalToolError::try_new("tool_unavailable", "unavailable", None, 64)
                        .expect("static error")
                });
                let result = CanonicalToolResult {
                    transaction_id,
                    session_key: session_key.clone(),
                    exchange_id,
                    tool_action_id: tool.tool_action_id.clone(),
                    tool_id,
                    provider_tool_call_id: tool.tool_action_id.as_str().to_string(),
                    request_ordinal: snap.lane_ordinal.min(u32::MAX as u64) as u32,
                    outcome: CanonicalToolResultOutcome::DomainFailed(err),
                };
                let send = publish_tx.send(EventPublisherCommand::Publish(Box::new(
                    TransactionEventPayload::ToolLifecycle(ToolLifecycleEvent::Completed {
                        result,
                    }),
                )));
                tokio::select! {
                    biased;
                    _ = cancel.notified() => return Err(EmptyToolPassError::Cancelled),
                    res = send => {
                        if res.is_err() {
                            return Err(EmptyToolPassError::PublishFailed);
                        }
                    }
                }
                report.unavailable_published = report.unavailable_published.saturating_add(1);
            }
            ToolResolution::Available(_) => {
                // EmptyToolRegistry must never return Available.
                return Err(EmptyToolPassError::InvariantFailed);
            }
        }
    }

    Ok(report)
}

/// Empty-tool pass failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmptyToolPassError {
    /// Cancelled cooperatively.
    Cancelled,
    /// Registry returned an error.
    RegistryFailed,
    /// Event publish failed.
    PublishFailed,
    /// Empty registry returned Available (forbidden).
    InvariantFailed,
}
