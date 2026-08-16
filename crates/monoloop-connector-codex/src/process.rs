//! Codex ACP process: NDJSON JSON-RPC over stdio (`codex-acp` or native).

use crate::config::CodexAgentConfig;
use crate::error::CodexConnectorError;
use crate::raw_dump::CodexRawDump;
use serde_json::Value;
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, warn};

/// Shared runtime for one ACP process (one or more sessions).
pub(crate) struct ProcessInner {
    pub config: CodexAgentConfig,
    next_id: AtomicU64,
    stdin: Mutex<Option<ChildStdin>>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Value, CodexConnectorError>>>>,
    update_tx: Mutex<Option<mpsc::Sender<bytes::Bytes>>>,
    dump: Arc<CodexRawDump>,
    closed: AtomicBool,
    child: Mutex<Option<Child>>,
}

impl ProcessInner {
    pub async fn spawn(
        config: CodexAgentConfig,
        update_tx: mpsc::Sender<bytes::Bytes>,
        dump: Arc<CodexRawDump>,
    ) -> Result<Arc<Self>, CodexConnectorError> {
        let mut cmd = Command::new(&config.command);
        for a in &config.args {
            cmd.arg(a);
        }
        cmd.current_dir(&config.cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let mut child = cmd.spawn().map_err(|e| {
            CodexConnectorError::connection(format!(
                "failed to spawn codex ACP server ({} {:?}): {e}",
                config.command.display(),
                config.args
            ))
        })?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| CodexConnectorError::connection("agent stdin missing"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| CodexConnectorError::connection("agent stdout missing"))?;
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
                            warn!("codex acp stdout bound/protocol failure: {e}");
                            this.fail_all_pending(e).await;
                            break;
                        }
                    }
                }
                this.fail_all_pending(CodexConnectorError::connection(
                    "codex ACP server stdout closed",
                ))
                .await;
                this.closed.store(true, Ordering::SeqCst);
            });
        }

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
                                debug!(target: "codex_agent", "stderr: {t}");
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
                warn!("codex acp invalid json: {e}");
                return;
            }
        };

        if let Some(id) = value.get("id").and_then(|i| i.as_u64()) {
            if value.get("result").is_some() || value.get("error").is_some() {
                let mut pending = self.pending.lock().await;
                if let Some(tx) = pending.remove(&id) {
                    if let Some(err) = value.get("error") {
                        let msg = err
                            .get("message")
                            .and_then(|m| m.as_str())
                            .unwrap_or("rpc error");
                        let _ = tx.send(Err(CodexConnectorError::protocol(msg.to_string())));
                    } else {
                        let result = value.get("result").cloned().unwrap_or(Value::Null);
                        let _ = tx.send(Ok(result));
                    }
                }
                return;
            }
        }

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
                if method.starts_with("session/") || method.starts_with("cursor/") {
                    let resp = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": {
                            "code": -32601,
                            "message": format!("method not handled by monoloop codex connector: {method}")
                        }
                    });
                    let _ = self.write_value(&resp).await;
                    return;
                }
            }

            if method == "session/update" {
                let bytes = bytes::Bytes::from(format!("{line}\n"));
                let tx = self.update_tx.lock().await;
                if let Some(tx) = tx.as_ref() {
                    let _ = tx.send(bytes).await;
                }
            }
        }
    }

    async fn write_value(&self, value: &Value) -> Result<(), CodexConnectorError> {
        let line = serde_json::to_string(value)
            .map_err(|e| CodexConnectorError::protocol(format!("encode: {e}")))?;
        self.dump.push_line(format!(">> {line}"));
        let mut guard = self.stdin.lock().await;
        let stdin = guard
            .as_mut()
            .ok_or_else(|| CodexConnectorError::connection("stdin closed"))?;
        stdin
            .write_all(line.as_bytes())
            .await
            .map_err(|e| CodexConnectorError::connection(format!("write: {e}")))?;
        stdin
            .write_all(b"\n")
            .await
            .map_err(|e| CodexConnectorError::connection(format!("write nl: {e}")))?;
        stdin
            .flush()
            .await
            .map_err(|e| CodexConnectorError::connection(format!("flush: {e}")))?;
        Ok(())
    }

    pub async fn request(
        &self,
        method: &str,
        params: Value,
    ) -> Result<Value, CodexConnectorError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(CodexConnectorError::connection("agent process closed"));
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
            Ok(Err(_)) => Err(CodexConnectorError::connection("response channel dropped")),
            Err(_) => {
                let mut pending = self.pending.lock().await;
                pending.remove(&id);
                Err(CodexConnectorError::deadline(format!(
                    "rpc deadline exceeded for {method}"
                )))
            }
        }
    }

    async fn fail_all_pending(&self, err: CodexConnectorError) {
        let mut pending = self.pending.lock().await;
        for (_, tx) in pending.drain() {
            let _ = tx.send(Err(err.clone()));
        }
    }

    pub async fn shutdown(&self) {
        self.closed.store(true, Ordering::SeqCst);
        {
            let mut g = self.stdin.lock().await;
            *g = None;
        }
        {
            let mut g = self.update_tx.lock().await;
            *g = None;
        }
        let mut child = self.child.lock().await;
        if let Some(mut c) = child.take() {
            let _ = c.kill().await;
            let _ = c.wait().await;
        }
        self.fail_all_pending(CodexConnectorError::cancelled())
            .await;
    }
}

/// Read one line with a hard byte cap. Returns `Ok(None)` on EOF with empty buffer.
async fn read_line_bounded<R: AsyncBufReadExt + Unpin>(
    reader: &mut R,
    max_bytes: usize,
) -> Result<Option<String>, CodexConnectorError> {
    let max_bytes = max_bytes.max(1);
    let mut buf = Vec::new();
    loop {
        let available = reader
            .fill_buf()
            .await
            .map_err(|e| CodexConnectorError::connection(format!("stdout read: {e}")))?;
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
                return Err(CodexConnectorError::protocol(
                    "ndjson line exceeds max_line_bytes",
                ));
            }
            buf.extend_from_slice(&available[..take]);
            reader.consume(take);
            return Ok(Some(String::from_utf8_lossy(&buf).into_owned()));
        }
        if buf.len() + available.len() > max_bytes {
            let n = available.len();
            reader.consume(n);
            return Err(CodexConnectorError::protocol(
                "ndjson line exceeds max_line_bytes",
            ));
        }
        let n = available.len();
        buf.extend_from_slice(available);
        reader.consume(n);
    }
}
