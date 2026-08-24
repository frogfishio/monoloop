//! Host-side adapters that drain v2 mailboxes outside the runtime core (M1).
//!
//! The core runtime MUST NOT invoke these adapters. They exist so existing hosts
//! that still speak [`CompletionCallback`] / [`TransactionEventSink`] can be
//! bridged during migration without putting arbitrary futures on the runtime
//! executor.

use monoloop_contracts::{
    CompletionCallback, CompletionDeliveryError, TransactionCompletionReceiver, TransactionEvent,
    TransactionEventReceiver, TransactionEventSink,
};
use std::sync::Arc;

/// Invoke a legacy completion callback after the runtime publishes once.
///
/// Runs on the **caller**/host task — never on the runtime-owned executor.
pub async fn adapt_completion_callback(
    receiver: TransactionCompletionReceiver,
    callback: Box<dyn CompletionCallback>,
) -> Result<(), CompletionDeliveryError> {
    match receiver.recv().await {
        Ok(completion) => {
            // Map v2 completion into the legacy TransactionEnd shape for hosts
            // that have not migrated yet.
            let end = monoloop_contracts::TransactionEnd {
                transaction_id: completion.end.transaction_id,
                session_id: completion.end.session_id,
                channel_id: completion.end.channel_id,
                kind: completion.end.kind,
                prior_terminal_cause: None,
                event_delivery: match completion.terminal_event_delivery {
                    monoloop_contracts::TerminalEventDelivery::Published => {
                        monoloop_contracts::EventDeliveryOutcome::Accepted
                    }
                    monoloop_contracts::TerminalEventDelivery::NotAttempted
                    | monoloop_contracts::TerminalEventDelivery::QueueClosed
                    | monoloop_contracts::TerminalEventDelivery::DeadlineExceeded
                    | monoloop_contracts::TerminalEventDelivery::LimitExceeded => {
                        monoloop_contracts::EventDeliveryOutcome::Failed
                    }
                },
                emitted_events: completion.end.emitted_events,
                usage: completion.end.usage,
                diagnostics: completion.end.diagnostics,
            };
            callback.call(end).await
        }
        Err(_) => Err(CompletionDeliveryError::Failed),
    }
}

/// Forward mailbox events to a legacy sink until the sender is dropped.
///
/// Runs on the **caller**/host task — never on the runtime-owned executor.
pub async fn adapt_event_sink(
    mut receiver: TransactionEventReceiver,
    sink: Arc<dyn TransactionEventSink>,
) {
    while let Some(event) = receiver.recv().await {
        let _ = forward_one(&sink, event).await;
    }
}

async fn forward_one(
    sink: &Arc<dyn TransactionEventSink>,
    event: TransactionEvent,
) -> Result<(), monoloop_contracts::EventDeliveryError> {
    sink.deliver(event).await
}
