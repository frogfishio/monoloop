//! Owned local `grok agent serve` process for live test-kit runs.
//!
//! **Test kit only.** Product crates never spawn host agent processes.
//! The handle owns the child: ready-wait is bounded, drop/stop always kill.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};

/// How to launch a local Grok Build agent server.
#[derive(Clone, Debug)]
pub struct GrokServeOptions {
    /// Loopback bind port. When `None`, an ephemeral free port is chosen.
    pub port: Option<u16>,
    /// WebSocket server secret (also used as `server-key` query param).
    pub secret: String,
    /// Binary name or path (`grok` on PATH by default).
    pub grok_bin: PathBuf,
    /// Max time to wait for the listener after spawn.
    pub ready_timeout: Duration,
    /// Optional log file for child stdout/stderr (parent dirs created).
    pub log_path: Option<PathBuf>,
}

impl Default for GrokServeOptions {
    fn default() -> Self {
        Self {
            port: Some(2419),
            secret: "monoloop-live-test".into(),
            grok_bin: PathBuf::from("grok"),
            ready_timeout: Duration::from_secs(15),
            log_path: None,
        }
    }
}

/// Owned Grok serve child process.
///
/// Dropping the handle (or calling [`ManagedGrokServe::stop`]) terminates the
/// child. No fire-and-forget: the driver always owns cleanup.
#[derive(Debug)]
pub struct ManagedGrokServe {
    child: Child,
    port: u16,
    secret: String,
    log_path: Option<PathBuf>,
}

impl ManagedGrokServe {
    /// Spawn `grok agent --always-approve serve` and wait until the port listens.
    pub async fn start(opts: GrokServeOptions) -> Result<Self, String> {
        let port = match opts.port {
            Some(p) => p,
            None => free_loopback_port()?,
        };
        if port_is_listening(port).await {
            return Err(format!(
                "port {port} already in use — stop the other listener or choose another port"
            ));
        }

        if let Some(ref log) = opts.log_path {
            if let Some(parent) = log.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| format!("create log dir {}: {e}", parent.display()))?;
                }
            }
        }

        let bind = format!("127.0.0.1:{port}");
        let mut cmd = Command::new(&opts.grok_bin);
        cmd.arg("agent")
            .arg("--always-approve")
            .arg("serve")
            .arg("--bind")
            .arg(&bind)
            .arg("--secret")
            .arg(&opts.secret)
            .kill_on_drop(true)
            .stdin(Stdio::null());

        if let Some(ref log) = opts.log_path {
            let f = std::fs::File::create(log)
                .map_err(|e| format!("open log {}: {e}", log.display()))?;
            let f2 = f
                .try_clone()
                .map_err(|e| format!("clone log handle: {e}"))?;
            cmd.stdout(Stdio::from(f)).stderr(Stdio::from(f2));
        } else {
            cmd.stdout(Stdio::null()).stderr(Stdio::null());
        }

        let child = cmd.spawn().map_err(|e| {
            format!(
                "failed to spawn `{} agent serve`: {e} (is grok on PATH?)",
                opts.grok_bin.display()
            )
        })?;

        let serve = Self {
            child,
            port,
            secret: opts.secret,
            log_path: opts.log_path,
        };

        match timeout(opts.ready_timeout, wait_until_listening(port)).await {
            Ok(Ok(())) => Ok(serve),
            Ok(Err(e)) => {
                let _ = serve.stop().await;
                Err(e)
            }
            Err(_) => {
                let _ = serve.stop().await;
                Err(format!(
                    "grok serve not listening on 127.0.0.1:{port} within {:?}",
                    opts.ready_timeout
                ))
            }
        }
    }

    /// Loopback port the server bound.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Server secret.
    pub fn secret(&self) -> &str {
        &self.secret
    }

    /// Optional path to the child log file.
    pub fn log_path(&self) -> Option<&Path> {
        self.log_path.as_deref()
    }

    /// OS pid of the child, when available.
    pub fn pid(&self) -> Option<u32> {
        self.child.id()
    }

    /// Gracefully kill and wait for the child (idempotent).
    pub async fn stop(mut self) -> Result<(), String> {
        self.kill_inner().await
    }

    async fn kill_inner(&mut self) -> Result<(), String> {
        // Try polite kill first; escalate if needed.
        let _ = self.child.start_kill();
        match timeout(Duration::from_secs(3), self.child.wait()).await {
            Ok(Ok(status)) => {
                if !status.success() {
                    // Non-zero exit is fine on forced stop.
                }
                Ok(())
            }
            Ok(Err(e)) => Err(format!("wait for grok serve child: {e}")),
            Err(_) => {
                let _ = self.child.start_kill();
                let _ = timeout(Duration::from_secs(2), self.child.wait()).await;
                Ok(())
            }
        }
    }
}

impl Drop for ManagedGrokServe {
    fn drop(&mut self) {
        // Best-effort sync kill if caller forgot stop().
        let _ = self.child.start_kill();
    }
}

async fn wait_until_listening(port: u16) -> Result<(), String> {
    loop {
        if port_is_listening(port).await {
            return Ok(());
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn port_is_listening(port: u16) -> bool {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    timeout(Duration::from_millis(100), TcpStream::connect(addr))
        .await
        .map(|r| r.is_ok())
        .unwrap_or(false)
}

fn free_loopback_port() -> Result<u16, String> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")
        .map_err(|e| format!("bind ephemeral port: {e}"))?;
    let port = listener
        .local_addr()
        .map_err(|e| format!("local_addr: {e}"))?
        .port();
    drop(listener);
    Ok(port)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_options_use_loopback_secret() {
        let o = GrokServeOptions::default();
        assert_eq!(o.port, Some(2419));
        assert_eq!(o.secret, "monoloop-live-test");
    }

    #[test]
    fn free_port_is_nonzero() {
        let p = free_loopback_port().expect("port");
        assert!(p > 0);
    }
}
