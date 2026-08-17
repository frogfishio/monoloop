//! MCP loopback listener lifecycle shell (no tool methods until WP-07).

use std::io;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// Bound loopback listener held for the runtime lifetime.
///
/// Does **not** advertise MCP tools; it only proves bind/cleanup ownership.
pub struct McpListenerShell {
    local_addr: SocketAddr,
    shutdown_tx: oneshot::Sender<()>,
    join: JoinHandle<()>,
}

impl McpListenerShell {
    /// Bind `127.0.0.1:0` and park an accept loop until shutdown.
    pub async fn bind_loopback() -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;
        if !local_addr.ip().is_loopback() {
            return Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "MCP shell must bind loopback",
            ));
        }
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let join = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    accept = listener.accept() => {
                        // Drop accepted streams immediately — no MCP protocol yet.
                        match accept {
                            Ok((_stream, _)) => {}
                            Err(_) => break,
                        }
                    }
                }
            }
            // Listener dropped here with the task.
        });
        Ok(Self {
            local_addr,
            shutdown_tx,
            join,
        })
    }

    /// Bound address (loopback).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Signal shutdown and join the accept task.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        let _ = self.join.await;
    }
}
