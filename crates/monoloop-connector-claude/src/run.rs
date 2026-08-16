//! One-shot `claude -p --output-format stream-json` → NDJSON stdout stream.

use crate::config::ClaudeAgentConfig;
use crate::error::ClaudeConnectorError;
use crate::raw_dump::ClaudeRawDump;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use tracing::debug;

/// Result of one headless print run.
#[derive(Debug)]
pub struct ClaudeRunOutcome {
    /// Process exit code.
    pub exit_code: Option<i32>,
    /// Session id from stream `system` init when present, else synthetic.
    pub session_id: String,
    /// Raw dump text when configured.
    pub raw_dump_text: String,
}

/// Spawn Claude print mode; stream each stdout JSON line as bytes on `out_tx`.
pub async fn run_claude_print(
    config: &ClaudeAgentConfig,
    prompt: &str,
    out_tx: mpsc::Sender<bytes::Bytes>,
) -> Result<ClaudeRunOutcome, ClaudeConnectorError> {
    let dump = Arc::new(ClaudeRawDump::new(config.raw_dump_path.clone(), 50_000));
    dump.push_line(">>", &format!("PROMPT {}", redact_prompt_preview(prompt)));

    let args = config.argv_for_prompt(prompt);
    debug!(
        target: "claude_agent",
        "spawn {} {:?}",
        config.command.display(),
        args.iter()
            .map(|a| {
                if a.len() > 80 {
                    format!("{}…", &a[..40])
                } else {
                    a.clone()
                }
            })
            .collect::<Vec<_>>()
    );

    let mut child = Command::new(&config.command)
        .args(&args)
        .current_dir(&config.cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| ClaudeConnectorError::process(format!("spawn: {e}")))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| ClaudeConnectorError::process("missing stdout"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| ClaudeConnectorError::process("missing stderr"))?;

    let dump_out = Arc::clone(&dump);
    let session_holder = Arc::new(std::sync::Mutex::new(None::<String>));
    let session_out = Arc::clone(&session_holder);
    let out_pump = tokio::spawn(async move {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => break,
                Ok(_) => {
                    let trimmed = line.trim_end_matches(['\r', '\n']);
                    if trimmed.starts_with('{') {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(trimmed) {
                            if v.get("type").and_then(|t| t.as_str()) == Some("system")
                                && v.get("subtype").and_then(|s| s.as_str()) == Some("init")
                            {
                                if let Some(sid) = v.get("session_id").and_then(|s| s.as_str()) {
                                    *session_out.lock().unwrap_or_else(|e| e.into_inner()) =
                                        Some(sid.to_string());
                                }
                            }
                        }
                        dump_out.push_line("<<", trimmed);
                        let mut payload = trimmed.as_bytes().to_vec();
                        payload.push(b'\n');
                        if out_tx.send(bytes::Bytes::from(payload)).await.is_err() {
                            break;
                        }
                    } else if !trimmed.is_empty() {
                        dump_out.push_line("!!", trimmed);
                    }
                }
                Err(_) => break,
            }
        }
    });

    let dump_err = Arc::clone(&dump);
    let err_pump = tokio::spawn(async move {
        let mut reader = BufReader::new(stderr);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => break,
                Ok(_) => {
                    let t = line.trim();
                    if !t.is_empty() {
                        dump_err.push_line("E!", t);
                        debug!(target: "claude_agent", "stderr: {t}");
                    }
                }
                Err(_) => break,
            }
        }
    });

    let status = tokio::time::timeout(config.run_deadline, child.wait())
        .await
        .map_err(|_| ClaudeConnectorError::timeout("claude print exceeded run_deadline"))?
        .map_err(|e| ClaudeConnectorError::process(format!("wait: {e}")))?;

    let _ = out_pump.await;
    let _ = err_pump.await;
    dump.flush();

    if !status.success() {
        return Err(ClaudeConnectorError::run(format!(
            "claude exited with {status:?}"
        )));
    }

    let session_id = session_holder
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .unwrap_or_else(|| format!("claude-{}", uuid::Uuid::new_v4()));

    Ok(ClaudeRunOutcome {
        exit_code: status.code(),
        session_id,
        raw_dump_text: dump.text(),
    })
}

fn redact_prompt_preview(prompt: &str) -> String {
    let t = prompt.trim();
    if t.chars().count() <= 120 {
        t.to_string()
    } else {
        format!("{}…", t.chars().take(120).collect::<String>())
    }
}
