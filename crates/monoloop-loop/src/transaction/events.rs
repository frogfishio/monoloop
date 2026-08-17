//! Runtime-owned event delivery task (ordered, backpressured).

use monoloop_contracts::{
    EventDeliveryError, TransactionEvent, TransactionEventSink,
};
use std::sync::Arc;
use tokio::sync::mpsc;

/// Queued event for delivery.
pub struct QueuedEvent {
    /// Event to deliver.
    pub event: TransactionEvent,
    /// Optional oneshot for terminal delivery ack.
    pub ack: Option<tokio::sync::oneshot::Sender<Result<(), EventDeliveryError>>>,
}

/// Spawn the sequential delivery task for one transaction.
pub fn spawn_delivery_task(
    mut rx: mpsc::Receiver<QueuedEvent>,
    sink: Arc<dyn TransactionEventSink>,
    on_fail: mpsc::Sender<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(item) = rx.recv().await {
            let result = sink.deliver(item.event).await;
            let ok = result.is_ok();
            if let Some(ack) = item.ack {
                let _ = ack.send(if ok {
                    Ok(())
                } else {
                    Err(EventDeliveryError::Failed)
                });
            }
            if !ok {
                let _ = on_fail.try_send(());
                while let Some(rest) = rx.recv().await {
                    if let Some(ack) = rest.ack {
                        let _ = ack.send(Err(EventDeliveryError::Failed));
                    }
                }
                break;
            }
        }
    })
}
