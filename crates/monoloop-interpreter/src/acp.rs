//! ACP / Grok Build dialect mapping (JSON-RPC messages → semantic fragments).
//!
//! Maps session/update and related notifications into text fragments and tool
//! lifecycle signals. Does not execute tools or interpret product UI.

use monoloop_contracts::{TextChannel, ToolActionId};
use serde_json::Value;

/// Semantic fragments produced by ACP framing (still may be incomplete text).
#[derive(Clone, Debug)]
pub enum AcpFragment {
    /// Text delta to assemble into sentences (channel-tagged).
    TextDelta {
        /// Channel.
        channel: TextChannel,
        /// Fragment text (not a canonical unit).
        text: String,
        /// Dialect `agentTimestampMs` when present on the message (observational).
        source_time_ms: Option<u64>,
        /// Dialect stream step (e.g. `_meta.stepIdx` / numeric `messageId`).
        source_step: Option<u64>,
    },
    /// Tool action declared / updated.
    Tool {
        /// Action id.
        action_id: ToolActionId,
        /// Kind of tool signal.
        signal: ToolSignal,
        /// Dialect `agentTimestampMs` when present on the message (observational).
        source_time_ms: Option<u64>,
        /// Dialect stream step (e.g. `_meta.stepIdx`).
        source_step: Option<u64>,
    },
    /// Dialect-level response finished.
    ResponseFinished,
    /// Safe diagnostic.
    Diagnostic {
        /// Message.
        message: String,
    },
}

/// Tool lifecycle signals from ACP (no partial arg escape as ready).
#[derive(Clone, Debug)]
pub enum ToolSignal {
    /// Waiting for complete request (identity known).
    Waiting {
        /// Optional tool name if already known without full args.
        tool_name: Option<String>,
        /// Why waiting.
        waiting_for: String,
    },
    /// Complete request ready (full name + JSON args).
    RequestReady {
        /// Tool name.
        tool_name: String,
        /// Complete args as JSON string.
        arguments_json: String,
    },
    /// Observed terminal result from dialect.
    Resolved {
        /// Outcome label.
        success: bool,
        /// Complete result JSON if present.
        result_json: Option<String>,
    },
}

/// ACP dialect helpers.
pub struct AcpDialect;

impl AcpDialect {
    /// Map one complete JSON-RPC message value into fragments.
    pub fn map_message(value: &Value) -> Vec<AcpFragment> {
        let mut out = Vec::new();
        let method = value.get("method").and_then(|m| m.as_str()).unwrap_or("");
        match method {
            "session/update" => {
                if let Some(params) = value.get("params") {
                    out.extend(map_session_update(params));
                }
            }
            // Cursor extension notifications — observational only (no effects).
            "cursor/update_todos" | "cursor/task" | "cursor/generate_image" => {
                out.push(AcpFragment::Diagnostic {
                    message: format!("cursor extension notification: {method}"),
                });
            }
            // Terminal prompt response often has result.stopReason
            _ => {
                if value.get("result").is_some() && value.get("id").is_some() {
                    if let Some(sr) = value
                        .pointer("/result/stopReason")
                        .and_then(|v| v.as_str())
                    {
                        if sr == "end_turn" || sr == "max_tokens" || sr == "cancelled" {
                            out.push(AcpFragment::ResponseFinished);
                        }
                    }
                }
            }
        }
        out
    }
}

fn map_session_update(params: &Value) -> Vec<AcpFragment> {
    let mut out = Vec::new();
    let source_time_ms = extract_agent_timestamp_ms(params);
    let update = params.get("update").unwrap_or(params);
    // Prefer update._meta.stepIdx (Antigravity), then params._meta, then numeric messageId.
    let source_step = extract_source_step(params, update);
    let kind = update
        .get("sessionUpdate")
        .or_else(|| update.get("type"))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    match kind {
        "agent_message_chunk" | "agent_message" | "message" => {
            if let Some(text) = extract_text_content(update) {
                if !text.is_empty() {
                    out.push(AcpFragment::TextDelta {
                        channel: TextChannel::PublicResponse,
                        text,
                        source_time_ms,
                        source_step,
                    });
                }
            }
        }
        "agent_thought_chunk" | "agent_thought" => {
            // Only publish as reasoning summary if field is explicitly public summary.
            // Private CoT is not mapped to PublicResponse.
            if update
                .get("public")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
                || update.get("summary").is_some()
            {
                if let Some(text) = extract_text_content(update) {
                    if !text.is_empty() {
                        out.push(AcpFragment::TextDelta {
                            channel: TextChannel::PublicReasoningSummary,
                            text,
                            source_time_ms,
                            source_step,
                        });
                    }
                }
            }
            // else: suppress private thought
        }
        "tool_call" | "tool_call_update" => {
            out.extend(map_tool_call(
                update,
                kind == "tool_call_update",
                source_time_ms,
                source_step,
            ));
        }
        // Known non-content / lifecycle updates — observe silently (no diagnostic noise).
        // Cursor ACP shares the same core sessionUpdate vocabulary as standard ACP.
        "available_commands_update"
        | "current_mode_update"
        | "plan"
        | "user_message_chunk"
        | "session_info_update"
        | "config_option_update"
        | "available_commands" => {}
        other if !other.is_empty() => {
            out.push(AcpFragment::Diagnostic {
                message: format!("unsupported sessionUpdate: {other}"),
            });
        }
        _ => {
            // Try generic content
            if let Some(text) = extract_text_content(update) {
                if !text.is_empty() {
                    out.push(AcpFragment::TextDelta {
                        channel: TextChannel::PublicResponse,
                        text,
                        source_time_ms,
                        source_step,
                    });
                }
            }
        }
    }
    out
}

/// Grok ACP places observational time on `params._meta.agentTimestampMs`.
/// Also accept the same key on the update object when present.
fn extract_agent_timestamp_ms(params: &Value) -> Option<u64> {
    let from_meta = |v: &Value| -> Option<u64> {
        v.get("_meta")
            .and_then(|m| m.get("agentTimestampMs"))
            .and_then(|t| t.as_u64().or_else(|| t.as_i64().map(|i| i as u64)))
    };
    from_meta(params)
        .or_else(|| params.get("update").and_then(from_meta))
        .or_else(|| {
            // Bare update object passed as params in some fixtures.
            params
                .get("agentTimestampMs")
                .and_then(|t| t.as_u64().or_else(|| t.as_i64().map(|i| i as u64)))
        })
}

/// Dialect stream step for human ordering when wall-clock times are absent.
///
/// Preference:
/// 1. `update._meta.stepIdx` (Antigravity)
/// 2. `params._meta.stepIdx`
/// 3. numeric `update.messageId` (Antigravity text chunks)
fn extract_source_step(params: &Value, update: &Value) -> Option<u64> {
    let step_from = |v: &Value| -> Option<u64> {
        v.get("_meta").and_then(|m| {
            m.get("stepIdx")
                .or_else(|| m.get("step_idx"))
                .and_then(|t| t.as_u64().or_else(|| t.as_i64().map(|i| i as u64)))
        })
    };
    step_from(update)
        .or_else(|| step_from(params))
        .or_else(|| {
            update
                .get("messageId")
                .and_then(|m| m.as_u64().or_else(|| m.as_i64().map(|i| i as u64)))
                .or_else(|| {
                    update
                        .get("messageId")
                        .and_then(|m| m.as_str())
                        .and_then(|s| s.parse().ok())
                })
        })
}

fn extract_text_content(update: &Value) -> Option<String> {
    if let Some(s) = update.get("text").and_then(|v| v.as_str()) {
        return Some(s.to_string());
    }
    if let Some(content) = update.get("content") {
        if let Some(s) = content.as_str() {
            return Some(s.to_string());
        }
        if let Some(s) = content.get("text").and_then(|v| v.as_str()) {
            return Some(s.to_string());
        }
        if let Some(arr) = content.as_array() {
            let mut acc = String::new();
            for item in arr {
                if item.get("type").and_then(|t| t.as_str()) == Some("text") {
                    if let Some(t) = item.get("text").and_then(|v| v.as_str()) {
                        acc.push_str(t);
                    }
                } else if let Some(t) = item.as_str() {
                    acc.push_str(t);
                }
            }
            if !acc.is_empty() {
                return Some(acc);
            }
        }
    }
    None
}

fn map_tool_call(
    update: &Value,
    is_update: bool,
    source_time_ms: Option<u64>,
    source_step: Option<u64>,
) -> Vec<AcpFragment> {
    let mut out = Vec::new();
    let id = update
        .get("toolCallId")
        .or_else(|| update.get("tool_call_id"))
        .or_else(|| update.get("id"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if id.is_empty() {
        out.push(AcpFragment::Diagnostic {
            message: "tool_call missing toolCallId".into(),
        });
        return out;
    }
    let action_id = ToolActionId::new(id);
    let name = update
        .get("title")
        .or_else(|| update.get("name"))
        .or_else(|| update.get("toolName"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // Grok Build often omits `status` (null). Prefer explicit status when present;
    // otherwise infer terminality from result payload on tool_call_update.
    let status = update
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    let args = update
        .get("rawInput")
        .or_else(|| update.get("arguments"))
        .or_else(|| update.get("input"));
    let args_complete = args.is_some()
        && args
            .map(|a| a.is_object() || a.is_array() || a.is_string())
            .unwrap_or(false);

    let result_value = update
        .get("rawOutput")
        .or_else(|| update.get("result"))
        // Grok Build: tool_call_update carries tool outcome as `content`
        // (e.g. diff array for write, text blocks for read) without status.
        .or_else(|| {
            if is_update {
                update.get("content")
            } else {
                None
            }
        });
    let has_result = result_value.is_some_and(is_tool_result_payload);

    let explicit_terminal = matches!(
        status.as_str(),
        "completed" | "complete" | "success" | "failed" | "failure" | "error" | "cancelled" | "canceled"
    );
    let explicit_failure = matches!(
        status.as_str(),
        "failed" | "failure" | "error" | "cancelled" | "canceled"
    );

    // Terminal: explicit status, or Grok-style update with result content and no
    // in-progress marker.
    let in_progress = matches!(
        status.as_str(),
        "pending" | "in_progress" | "in-progress" | "running" | "started"
    );
    if explicit_terminal || (has_result && !in_progress) {
        // Emit Ready first when this same message carries complete args, so
        // hosts still see a complete request before the terminal outcome.
        if args_complete {
            if let Some(a) = args {
                let tool_name = name.clone().unwrap_or_else(|| "unknown".into());
                let arguments_json = if a.is_string() {
                    a.as_str().unwrap_or("{}").to_string()
                } else {
                    a.to_string()
                };
                out.push(AcpFragment::Tool {
                    action_id: action_id.clone(),
                    signal: ToolSignal::RequestReady {
                        tool_name,
                        arguments_json,
                    },
                    source_time_ms,
                    source_step,
                });
            }
        }
        let success = !explicit_failure;
        let result_json = result_value.map(|v| v.to_string());
        out.push(AcpFragment::Tool {
            action_id,
            signal: ToolSignal::Resolved {
                success,
                result_json,
            },
            source_time_ms,
            source_step,
        });
        return out;
    }

    if args_complete {
        if let Some(a) = args {
            let tool_name = name.unwrap_or_else(|| "unknown".into());
            let arguments_json = if a.is_string() {
                a.as_str().unwrap_or("{}").to_string()
            } else {
                a.to_string()
            };
            out.push(AcpFragment::Tool {
                action_id,
                signal: ToolSignal::RequestReady {
                    tool_name,
                    arguments_json,
                },
                source_time_ms,
                source_step,
            });
            return out;
        }
    }

    // Waiting
    out.push(AcpFragment::Tool {
        action_id,
        signal: ToolSignal::Waiting {
            tool_name: name,
            waiting_for: if is_update {
                "tool_call_update incomplete".into()
            } else {
                "complete tool request".into()
            },
        },
        source_time_ms,
        source_step,
    });
    out
}

/// True when `content`/`rawOutput` looks like tool outcome material (not empty).
fn is_tool_result_payload(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(_) | Value::Number(_) => true,
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(m) => !m.is_empty(),
    }
}

/// Extract complete JSON values from a growing byte buffer (fragment-safe).
///
/// Supports concatenated JSON objects (WebSocket message bodies and partial chunks).
pub fn drain_json_values(buffer: &mut Vec<u8>) -> Result<Vec<Value>, String> {
    let mut out = Vec::new();
    loop {
        // skip leading whitespace
        let start = buffer
            .iter()
            .position(|b| !b.is_ascii_whitespace())
            .unwrap_or(buffer.len());
        if start > 0 {
            buffer.drain(..start);
        }
        if buffer.is_empty() {
            break;
        }
        match find_complete_json_end(buffer) {
            Some(end) => {
                let slice = &buffer[..end];
                let value: Value = serde_json::from_slice(slice)
                    .map_err(|e| format!("json parse: {e}"))?;
                out.push(value);
                buffer.drain(..end);
            }
            None => break,
        }
    }
    Ok(out)
}

fn find_complete_json_end(buf: &[u8]) -> Option<usize> {
    if buf.is_empty() {
        return None;
    }
    let first = buf[0];
    if first != b'{' && first != b'[' {
        // not a JSON value start — try line-based
        if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
            return Some(pos + 1);
        }
        return None;
    }
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escape = false;
    for (i, &b) in buf.iter().enumerate() {
        if in_string {
            if escape {
                escape = false;
                continue;
            }
            match b {
                b'\\' => escape = true,
                b'"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'{' | b'[' => depth += 1,
            b'}' | b']' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i + 1);
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fragment_json_reassembly() {
        let full = serde_json::json!({
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": { "type": "text", "text": "Hi. " }
                }
            }
        });
        let raw = serde_json::to_vec(&full).unwrap();
        let mid = raw.len() / 2;
        let mut buf = raw[..mid].to_vec();
        assert!(drain_json_values(&mut buf).unwrap().is_empty());
        buf.extend_from_slice(&raw[mid..]);
        let vals = drain_json_values(&mut buf).unwrap();
        assert_eq!(vals.len(), 1, "buf leftover={}", String::from_utf8_lossy(&buf));
        let frags = AcpDialect::map_message(&vals[0]);
        assert!(matches!(
            &frags[0],
            AcpFragment::TextDelta {
                text,
                source_time_ms: None,
                ..
            } if text == "Hi. "
        ));
    }

    #[test]
    fn extracts_agent_timestamp_ms_from_params_meta() {
        let msg = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "_meta": {
                    "agentTimestampMs": 1786859347289_u64,
                    "chunkId": 1
                },
                "sessionId": "s",
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": { "type": "text", "text": "I'll" }
                }
            }
        });
        let frags = AcpDialect::map_message(&msg);
        assert!(matches!(
            &frags[0],
            AcpFragment::TextDelta {
                text,
                source_time_ms: Some(1786859347289),
                ..
            } if text == "I'll"
        ));
    }

    #[test]
    fn tool_call_carries_source_time() {
        let msg = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "_meta": { "agentTimestampMs": 1001_u64 },
                "update": {
                    "sessionUpdate": "tool_call",
                    "toolCallId": "call-1",
                    "title": "write",
                    "rawInput": { "path": "/tmp/x" }
                }
            }
        });
        let frags = AcpDialect::map_message(&msg);
        assert!(
            frags.iter().any(|f| matches!(
                f,
                AcpFragment::Tool {
                    source_time_ms: Some(1001),
                    signal: ToolSignal::RequestReady { .. },
                    ..
                }
            )),
            "{frags:?}"
        );
    }

    /// Antigravity: tools carry `update._meta.stepIdx`; text uses numeric `messageId`.
    #[test]
    fn extracts_agy_step_idx_and_message_id() {
        let tool = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "s",
                "update": {
                    "sessionUpdate": "tool_call",
                    "toolCallId": "call_1",
                    "title": "Create file",
                    "status": "completed",
                    "rawInput": { "path": "/tmp/x" },
                    "content": [{ "type": "diff", "path": "/tmp/x" }],
                    "_meta": { "stepIdx": 3 }
                }
            }
        });
        let frags = AcpDialect::map_message(&tool);
        assert!(
            frags.iter().any(|f| matches!(
                f,
                AcpFragment::Tool {
                    source_step: Some(3),
                    source_time_ms: None,
                    ..
                }
            )),
            "tool stepIdx: {frags:?}"
        );

        let text = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "s",
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "messageId": "11",
                    "content": { "type": "text", "text": "Done." }
                }
            }
        });
        let frags = AcpDialect::map_message(&text);
        assert!(matches!(
            &frags[0],
            AcpFragment::TextDelta {
                text,
                source_step: Some(11),
                source_time_ms: None,
                ..
            } if text == "Done."
        ), "{frags:?}");
    }

    /// Grok Build live shape: null/absent status + rawInput on tool_call.
    #[test]
    fn grok_tool_call_without_status_is_ready_when_raw_input_present() {
        let msg = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "s",
                "update": {
                    "sessionUpdate": "tool_call",
                    "toolCallId": "call-1",
                    "title": "write",
                    "status": null,
                    "rawInput": {
                        "file_path": "/tmp/x.txt",
                        "content": "hello\n"
                    }
                }
            }
        });
        let frags = AcpDialect::map_message(&msg);
        assert!(
            frags.iter().any(|f| matches!(
                f,
                AcpFragment::Tool {
                    signal: ToolSignal::RequestReady { tool_name, .. },
                    ..
                } if tool_name == "write"
            )),
            "{frags:?}"
        );
        assert!(
            !frags.iter().any(|f| matches!(
                f,
                AcpFragment::Tool {
                    signal: ToolSignal::Resolved { .. },
                    ..
                }
            )),
            "initial call must not resolve: {frags:?}"
        );
    }

    /// Grok Build live shape: tool_call_update with content (diff) and no status
    /// means tool outcome observed → Resolved (and Ready if args present).
    #[test]
    fn grok_tool_update_content_without_status_resolves() {
        let msg = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "s",
                "update": {
                    "sessionUpdate": "tool_call_update",
                    "toolCallId": "call-1",
                    "title": "Write `/tmp/x.txt`",
                    "status": null,
                    "kind": "edit",
                    "rawInput": {
                        "file_path": "/tmp/x.txt",
                        "content": "hello\n"
                    },
                    "content": [{
                        "type": "diff",
                        "path": "/tmp/x.txt",
                        "oldText": "",
                        "newText": "hello\n"
                    }],
                    "locations": []
                }
            }
        });
        let frags = AcpDialect::map_message(&msg);
        assert!(
            frags.iter().any(|f| matches!(
                f,
                AcpFragment::Tool {
                    signal: ToolSignal::RequestReady { .. },
                    ..
                }
            )),
            "ready first: {frags:?}"
        );
        assert!(
            frags.iter().any(|f| matches!(
                f,
                AcpFragment::Tool {
                    signal: ToolSignal::Resolved {
                        success: true,
                        result_json: Some(j),
                    },
                    ..
                } if j.contains("diff")
            )),
            "resolved with result: {frags:?}"
        );
    }

    #[test]
    fn explicit_failed_status_still_fails() {
        let msg = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "tool_call_update",
                    "toolCallId": "call-2",
                    "title": "bash",
                    "status": "failed",
                    "rawOutput": { "error": "boom" }
                }
            }
        });
        let frags = AcpDialect::map_message(&msg);
        assert!(matches!(
            &frags[0],
            AcpFragment::Tool {
                signal: ToolSignal::Resolved {
                    success: false,
                    ..
                },
                ..
            }
        ));
    }
}
