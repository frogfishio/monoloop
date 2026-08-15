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
    },
    /// Tool action declared / updated.
    Tool {
        /// Action id.
        action_id: ToolActionId,
        /// Kind of tool signal.
        signal: ToolSignal,
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
    let update = params.get("update").unwrap_or(params);
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
                        });
                    }
                }
            }
            // else: suppress private thought
        }
        "tool_call" | "tool_call_update" => {
            out.extend(map_tool_call(update, kind == "tool_call_update"));
        }
        // Known non-content / lifecycle updates — observe silently (no diagnostic noise).
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
                    });
                }
            }
        }
    }
    out
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

fn map_tool_call(update: &Value, is_update: bool) -> Vec<AcpFragment> {
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

    let status = update
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    // Complete args?
    let args = update
        .get("rawInput")
        .or_else(|| update.get("arguments"))
        .or_else(|| update.get("input"));
    let args_complete = args.is_some()
        && args
            .map(|a| a.is_object() || a.is_array() || a.is_string())
            .unwrap_or(false);

    if matches!(status, "completed" | "failed" | "cancelled") {
        let success = status == "completed";
        let result_json = update
            .get("rawOutput")
            .or_else(|| update.get("content"))
            .or_else(|| update.get("result"))
            .map(|v| v.to_string());
        out.push(AcpFragment::Tool {
            action_id,
            signal: ToolSignal::Resolved {
                success,
                result_json,
            },
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
    });
    out
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
                ..
            } if text == "Hi. "
        ));
    }
}
