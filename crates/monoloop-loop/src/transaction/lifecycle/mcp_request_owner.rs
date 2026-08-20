//! TaskSupervisor-backed MCP HTTP request ownership (V2 §17).

use super::task_spawner::{SpawnReject, TransactionTaskSpawner};
use super::task_supervisor::TaskClass;
use crate::transaction::mcp::McpRequestOwner;
use axum::body::Body;
use axum::http::{Response, StatusCode};
use monoloop_contracts::TransactionId;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::oneshot;

/// Runs each MCP HTTP request as [`TaskClass::McpRequest`] under the supervisor.
pub(crate) struct SupervisedMcpRequestOwner {
    spawner: TransactionTaskSpawner,
}

impl SupervisedMcpRequestOwner {
    pub(crate) fn new(spawner: TransactionTaskSpawner) -> Self {
        Self { spawner }
    }
}

impl McpRequestOwner for SupervisedMcpRequestOwner {
    fn run_owned(
        &self,
        transaction_id: TransactionId,
        work: Pin<Box<dyn Future<Output = Response<Body>> + Send>>,
    ) -> Pin<Box<dyn Future<Output = Response<Body>> + Send>> {
        let spawner = self.spawner.clone();
        Box::pin(async move {
            let (done_tx, done_rx) = oneshot::channel();
            match spawner
                .spawn(TaskClass::McpRequest(transaction_id), async move {
                    let response = work.await;
                    let _ = done_tx.send(response);
                })
                .await
            {
                Ok(_id) => match done_rx.await {
                    Ok(response) => response,
                    Err(_) => unavailable("mcp request task dropped"),
                },
                // try_send is non-blocking; Busy/Rejected return the future undriven
                // (Law 22/23 — no ambient inline MCP work on the acceptor path).
                Err(
                    SpawnReject::Busy { .. } | SpawnReject::Rejected { .. } | SpawnReject::Orphaned,
                ) => unavailable("mcp request spawn unavailable"),
            }
        })
    }
}

fn unavailable(msg: &'static str) -> Response<Body> {
    Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .body(Body::from(msg))
        .unwrap_or_else(|_| Response::new(Body::empty()))
}
