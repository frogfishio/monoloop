//! Loopback MCP gateway owned by [`TaskSupervisor`] as `RuntimeService` (V2 §17 / D-043).
//!
//! Serves [`crate::transaction::mcp::PreparedMcpGateway`] until cancelled on quiesce.
//! Bind + prepare + handle publish happen at runtime start (fail-closed, §7.1) before
//! `StartedRuntime` is returned; this task only serves until shutdown.

use super::supervisor::RuntimeShared;
use crate::transaction::mcp::PreparedMcpGateway;
use std::sync::Arc;

/// Default floor for MCP route capacity when scaling by max_active.
pub(crate) const DEFAULT_MCP_MAX_ROUTES: usize = 64;

/// Publish addr/handle/cancel from a prepared gateway (§7.1: before start ready).
///
/// Does not start serving — caller registers [`serve_runtime_mcp`] with TaskSupervisor.
pub(crate) fn publish_runtime_mcp(shared: &RuntimeShared, prepared: &PreparedMcpGateway) {
    let addr = prepared.local_addr();
    let handle = prepared.handle();
    let cancel = prepared.cancel_token();

    *shared
        .mcp_listen_addr
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = Some(addr);
    *shared.mcp_gateway.lock().unwrap_or_else(|e| e.into_inner()) = Some(handle);
    *shared.mcp_cancel.lock().unwrap_or_else(|e| e.into_inner()) = Some(cancel);
    shared.wake.notify_waiters();
}

/// Serve until cancel; best-effort clear of shared slots on graceful exit.
pub(crate) async fn serve_runtime_mcp(shared: Arc<RuntimeShared>, prepared: PreparedMcpGateway) {
    prepared.serve().await;
    // Prefer `signal_mcp_shutdown` for authoritative clear; this covers
    // graceful serve exit without a prior signal.
    *shared
        .mcp_listen_addr
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = None;
    *shared.mcp_gateway.lock().unwrap_or_else(|e| e.into_inner()) = None;
    *shared.mcp_cancel.lock().unwrap_or_else(|e| e.into_inner()) = None;
}

/// Cancel MCP serve + revoke routes (idempotent). Called on begin_shutdown.
///
/// Clears published handle/addr here: TaskSupervisor abort may cancel the
/// serve future before its post-exit cleanup runs, and Stopped must not leave
/// a live-looking MCP handle behind.
pub(crate) fn signal_mcp_shutdown(shared: &RuntimeShared) {
    if let Ok(mut guard) = shared.mcp_gateway.lock() {
        if let Some(handle) = guard.take() {
            handle.revoke_all_services();
        }
    }
    if let Ok(mut guard) = shared.mcp_cancel.lock() {
        if let Some(token) = guard.take() {
            token.cancel();
        }
    }
    *shared
        .mcp_listen_addr
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = None;
}
