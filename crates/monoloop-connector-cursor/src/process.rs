//! Cursor `agent acp` process: NDJSON JSON-RPC over stdio.

use crate::config::CursorAgentConfig;
use crate::error::CursorConnectorError;
use crate::raw_dump::CursorRawDump;
use serde_json::Value;
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, warn};

/// Shared runtime for one Cursor ACP process (one or more sessions).
pub(crate) struct ProcessInner {
    pub config: CursorAgentConfig,
    next_id: AtomicU64,
    stdin: Mutex<Option<ChildStdin>>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Value, CursorConnectorError>>>>,
    /// Session update lines (complete JSON objects as bytes + newline).
    update_tx: Mutex<Option<mpsc::Sender<bytes::Bytes>>>,
    dump: Arc<CursorRawDump>,
    closed: AtomicBool,
    child: Mutex<Option<Child>>,
}

impl ProcessInner {
    pub async fn spawn(
        config: CursorAgentConfig,
        update_tx: mpsc::Sender<bytes::Bytes>,
        dump: Arc<CursorRawDump>,
    ) -> Result<Arc<Self>, CursorConnectorError> {
        let mut cmd = Command::new(&config.agent_bin);
        for a in &config.extra_args {
            cmd.arg(a);
        }
        cmd.arg("acp")
            .current_dir(&config.cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let mut child = cmd.spawn().map_err(|e| {
            CursorConnectorError::connection(format!(
                "failed to spawn cursor agent ({}): {e}",
                config.agent_bin.display()
            ))
        })?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| CursorConnectorError::connection("agent stdin missing"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| CursorConnectorError::connection("agent stdout missing"))?;
        let stderr = child.stderr.take();

        let inner = Arc::new(Self {
            config,
            next_id: AtomicU64::new(1),
            stdin: Mutex::new(Some(stdin)),
            pending: Mutex::new(HashMap::new()),
            update_tx: Mutex::new(Some(update_tx)),
            dump,
            closed: AtomicBool::new(false),
            child: Mutex::new(Some(child)),
        });

        // stdout reader (bounded per line — fail closed on oversize)
        {
            let this = Arc::clone(&inner);
            let max_line = this.config.max_line_bytes;
            tokio::spawn(async move {
                let mut reader = BufReader::new(stdout);
                loop {
                    match read_line_bounded(&mut reader, max_line).await {
                        Ok(None) => break,
                        Ok(Some(line)) => {
                            let trimmed = line.trim_end_matches(['\r', '\n']);
                            if trimmed.is_empty() {
                                continue;
                            }
                            this.dump.push_line(format!("<< {trimmed}"));
                            this.handle_inbound_line(trimmed).await;
                        }
                        Err(e) => {
                            warn!("cursor acp stdout bound/protocol failure: {e}");
                            this.fail_all_pending(e).await;
                            break;
                        }
                    }
                }
                this.fail_all_pending(CursorConnectorError::connection(
                    "cursor agent stdout closed",
                ))
                .await;
                this.closed.store(true, Ordering::SeqCst);
            });
        }

        // stderr drain (bounded diagnostics only)
        if let Some(err) = stderr {
            let max_line = inner.config.max_line_bytes.min(64 * 1024);
            tokio::spawn(async move {
                let mut reader = BufReader::new(err);
                loop {
                    match read_line_bounded(&mut reader, max_line).await {
                        Ok(None) => break,
                        Ok(Some(line)) => {
                            let t = line.trim();
                            if !t.is_empty() {
                                debug!(target: "cursor_agent", "stderr: {t}");
                            }
                        }
                        Err(_) => break,
                    }
                }
            });
        }

        Ok(inner)
    }

    async fn handle_inbound_line(&self, line: &str) {
        let value: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => {
                warn!("cursor acp invalid json: {e}");
                return;
            }
        };

        // Response to our request
        if let Some(id) = value.get("id").and_then(|i| i.as_u64()) {
            if value.get("result").is_some() || value.get("error").is_some() {
                let mut pending = self.pending.lock().await;
                if let Some(tx) = pending.remove(&id) {
                    if let Some(err) = value.get("error") {
                        let msg = err
                            .get("message")
                            .and_then(|m| m.as_str())
                            .unwrap_or("rpc error");
                        let _ = tx.send(Err(CursorConnectorError::protocol(msg.to_string())));
                    } else {
                        let result = value.get("result").cloned().unwrap_or(Value::Null);
                        let _ = tx.send(Ok(result));
                    }
                }
                return;
            }
        }

        // Server request that needs a response (permissions, extensions)
        if let Some(method) = value.get("method").and_then(|m| m.as_str()) {
            if let Some(id) = value.get("id") {
                if method == "session/request_permission" && self.config.auto_allow_permissions {
                    let resp = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": {
                            "outcome": {
                                "outcome": "selected",
                                "optionId": "allow-once"
                            }
                        }
                    });
                    let _ = self.write_value(&resp).await;
                    return;
                }
                // Unknown blocking request: reject safely so the agent does not hang forever.
                if method.starts_with("cursor/") || method.starts_with("session/") {
                    let resp = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": {
                            "code": -32601,
                            "message": format!("method not handled by monoloop connector: {method}")
                        }
                    });
                    let _ = self.write_value(&resp).await;
                    return;
                }
            }

            // Notifications: forward session/update (+ raw dump already) to interpreter output
            if method == "session/update" {
                let bytes = bytes::Bytes::from(format!("{line}\n"));
                let tx = self.update_tx.lock().await;
                if let Some(tx) = tx.as_ref() {
                    let _ = tx.send(bytes).await;
                }
            }
            // Other notifications (cursor/*) are ignored at connector layer;
            // hosts may later subscribe via a separate control path.
        }
    }

    async fn write_value(&self, value: &Value) -> Result<(), CursorConnectorError> {
        let line = serde_json::to_string(value)
            .map_err(|e| CursorConnectorError::protocol(format!("encode: {e}")))?;
        self.dump.push_line(format!(">> {line}"));
        let mut guard = self.stdin.lock().await;
        let stdin = guard
            .as_mut()
            .ok_or_else(|| CursorConnectorError::connection("stdin closed"))?;
        stdin
            .write_all(line.as_bytes())
            .await
            .map_err(|e| CursorConnectorError::connection(format!("write: {e}")))?;
        stdin
            .write_all(b"\n")
            .await
            .map_err(|e| CursorConnectorError::connection(format!("write nl: {e}")))?;
        stdin
            .flush()
            .await
            .map_err(|e| CursorConnectorError::connection(format!("flush: {e}")))?;
        Ok(())
    }

    /// Send a JSON-RPC request and wait for the matching response.
    pub async fn request(
        &self,
        method: &str,
        params: Value,
    ) -> Result<Value, CursorConnectorError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(CursorConnectorError::connection("agent process closed"));
        }
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending.lock().await;
            pending.insert(id, tx);
        }
        let req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        if let Err(e) = self.write_value(&req).await {
            let mut pending = self.pending.lock().await;
            pending.remove(&id);
            return Err(e);
        }
        match tokio::time::timeout(self.config.rpc_deadline, rx).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => Err(CursorConnectorError::connection("response channel dropped")),
            Err(_) => {
                let mut pending = self.pending.lock().await;
                pending.remove(&id);
                Err(CursorConnectorError::deadline(format!(
                    "rpc deadline exceeded for {method}"
                )))
            }
        }
    }

    async fn fail_all_pending(&self, err: CursorConnectorError) {
        let mut pending = self.pending.lock().await;
        for (_, tx) in pending.drain() {
            let _ = tx.send(Err(err.clone()));
        }
    }

    /// Kill the child process and close the update stream.
    pub async fn shutdown(&self) {
        self.closed.store(true, Ordering::SeqCst);
        {
            let mut g = self.stdin.lock().await;
            *g = None;
        }
        // Drop update sender so pump/interpreter drains can complete.
        {
            let mut g = self.update_tx.lock().await;
            *g = None;
        }
        let mut child = self.child.lock().await;
        if let Some(mut c) = child.take() {
            let _ = c.kill().await;
            let _ = c.wait().await;
        }
        self.fail_all_pending(CursorConnectorError::cancelled())
            .await;
    }
}

/// Read one line with a hard byte cap. Returns `Ok(None)` on EOF with empty buffer.
///
/// Unlike `BufRead::read_line`, this never grows beyond `max_bytes` for a single line.
pub(crate) async fn read_line_bounded<R: AsyncBufReadExt + Unpin>(
    reader: &mut R,
    max_bytes: usize,
) -> Result<Option<String>, CursorConnectorError> {
    let max_bytes = max_bytes.max(1);
    let mut buf = Vec::new();
    loop {
        let available = reader
            .fill_buf()
            .await
            .map_err(|e| CursorConnectorError::connection(format!("stdout read: {e}")))?;
        if available.is_empty() {
            if buf.is_empty() {
                return Ok(None);
            }
            return Ok(Some(String::from_utf8_lossy(&buf).into_owned()));
        }
        if let Some(pos) = available.iter().position(|&b| b == b'\n') {
            let take = pos + 1;
            if buf.len() + take > max_bytes {
                reader.consume(take);
                return Err(CursorConnectorError::protocol(
                    "ndjson line exceeds max_line_bytes",
                ));
            }
            buf.extend_from_slice(&available[..take]);
            reader.consume(take);
            return Ok(Some(String::from_utf8_lossy(&buf).into_owned()));
        }
        // No newline in available slice — append or fail if over budget.
        if buf.len() + available.len() > max_bytes {
            let n = available.len();
            reader.consume(n);
            return Err(CursorConnectorError::protocol(
                "ndjson line exceeds max_line_bytes",
            ));
        }
        let n = available.len();
        buf.extend_from_slice(available);
        reader.consume(n);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn exact_limit_line_ok() {
        let data = b"abcd\n";
        let mut r = BufReader::new(Cursor::new(&data[..]));
        let line = read_line_bounded(&mut r, 5).await.unwrap().unwrap();
        assert_eq!(line, "abcd\n");
    }

    #[tokio::test]
    async fn one_byte_over_fails() {
        let data = b"abcde\n"; // 6 bytes with newline
        let mut r = BufReader::new(Cursor::new(&data[..]));
        let err = read_line_bounded(&mut r, 5).await.unwrap_err();
        assert_eq!(
            err.kind,
            monoloop_contracts::ConnectorErrorKind::ProtocolFailed
        );
    }

    #[tokio::test]
    async fn no_newline_over_budget_fails() {
        let data = b"abcdefghij"; // no newline
        let mut r = BufReader::new(Cursor::new(&data[..]));
        let err = read_line_bounded(&mut r, 5).await.unwrap_err();
        assert_eq!(
            err.kind,
            monoloop_contracts::ConnectorErrorKind::ProtocolFailed
        );
    }
}
