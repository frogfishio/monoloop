//! Loopback MCP listener owned by [`TaskSupervisor`] (V2 §17 / D-043).
//!
//! Empty-tool era: accept connections and close immediately (no tool dispatch).
//! Bind happens at runtime start (fail-closed); this task only serves until quiescing.

use super::supervisor::{RuntimeShared, STATE_QUIESCING};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::net::TcpListener;

/// Serve a pre-bound loopback listener until runtime quiescing.
pub(crate) async fn run_loopback_mcp_listener(
    shared: Arc<RuntimeShared>,
    std_listener: std::net::TcpListener,
) {
    if let Ok(addr) = std_listener.local_addr() {
        *shared
            .mcp_listen_addr
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(addr);
    }
    let listener = match TcpListener::from_std(std_listener) {
        Ok(l) => l,
        Err(_) => {
            *shared
                .mcp_listen_addr
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = None;
            return;
        }
    };
    shared.wake.notify_waiters();

    loop {
        if shared.state.load(Ordering::SeqCst) >= STATE_QUIESCING {
            break;
        }
        tokio::select! {
            biased;
            _ = shared.wake.notified() => {
                if shared.state.load(Ordering::SeqCst) >= STATE_QUIESCING {
                    break;
                }
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((socket, _)) => {
                        // Empty-tool path: no MCP session protocol — drop peer.
                        drop(socket);
                    }
                    Err(_) => break,
                }
            }
        }
    }

    *shared
        .mcp_listen_addr
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = None;
}
