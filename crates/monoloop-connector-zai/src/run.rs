//! One-shot headless `zai -p` process → NDJSON stdout stream.

use crate::config::ZaiAgentConfig;
use crate::error::ZaiConnectorError;
use crate::raw_dump::ZaiRawDump;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use tracing::debug;

/// Result of one headless prompt run.
#[derive(Debug)]
pub struct ZaiRunOutcome {
    /// Process exit code.
    pub exit_code: Option<i32>,
    /// Synthetic session id for this run (no ambient zai session required).
    pub session_id: String,
    /// Raw dump text when configured.
    pub raw_dump_text: String,
}

/// Spawn `zai -p`, stream each stdout line as bytes (with trailing newline) on `out_tx`.
pub async fn run_headless_prompt(
    config: &ZaiAgentConfig,
    prompt: &str,
    out_tx: mpsc::Sender<bytes::Bytes>,
) -> Result<ZaiRunOutcome, ZaiConnectorError> {
    let dump = Arc::new(ZaiRawDump::new(config.raw_dump_path.clone(), 50_000));
    dump.push_line(">>", &format!("PROMPT {}", redact_prompt_preview(prompt)));

    let args = config.argv_for_prompt(prompt);
    debug!(
        target: "zai_agent",
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
        .map_err(|e| ZaiConnectorError::process(format!("spawn: {e}")))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| ZaiConnectorError::process("missing stdout"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| ZaiConnectorError::process("missing stderr"))?;

    let dump_out = Arc::clone(&dump);
    let mut pumps = tokio::task::JoinSet::new();
    pumps.spawn(async move {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => break,
                Ok(_) => {
                    let trimmed = line.trim_end_matches(['\r', '\n']);
                    if trimmed.starts_with('{') {
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
    pumps.spawn(async move {
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
                        debug!(target: "zai_agent", "stderr: {t}");
                    }
                }
                Err(_) => break,
            }
        }
    });

    let status = match tokio::time::timeout(config.run_deadline, child.wait()).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            pumps.abort_all();
            while pumps.join_next().await.is_some() {}
            return Err(ZaiConnectorError::process(format!("wait: {e}")));
        }
        Err(_) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            pumps.abort_all();
            while pumps.join_next().await.is_some() {}
            return Err(ZaiConnectorError::timeout(
                "zai headless exceeded run_deadline",
            ));
        }
    };

    while pumps.join_next().await.is_some() {}
    dump.flush();

    if !status.success() {
        return Err(ZaiConnectorError::run(format!(
            "zai exited with {status:?}"
        )));
    }

    Ok(ZaiRunOutcome {
        exit_code: status.code(),
        session_id: format!("zai-{}", uuid::Uuid::new_v4()),
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
