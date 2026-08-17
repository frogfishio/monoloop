//! Z.ai CLI headless dialect: OpenAI-compatible chat message NDJSON → fragments.
//!
//! `zai -p` prints one JSON object per line after the agent finishes a turn:
//! `user` / `assistant` (+ optional `tool_calls`) / `tool` results.
//! Tools already ran inside the CLI; we emit complete Ready+Resolved observations.

use crate::acp::{AcpFragment, ToolSignal};
use monoloop_contracts::{TextChannel, ToolActionId};
use serde_json::Value;

/// Map one complete NDJSON chat message line into ACP-shaped fragments.
pub fn map_chat_message_line(line: &str) -> Vec<AcpFragment> {
    let line = line.trim();
    if line.is_empty() || !line.starts_with('{') {
        return Vec::new();
    }
    let Ok(value) = serde_json::from_str::<Value>(line) else {
        return vec![AcpFragment::Diagnostic {
            message: "zai_cli: malformed chat JSON line".into(),
        }];
    };
    map_chat_message(&value)
}

/// Map a parsed OpenAI-style chat message object.
pub fn map_chat_message(value: &Value) -> Vec<AcpFragment> {
    let role = value.get("role").and_then(|r| r.as_str()).unwrap_or("");
    match role {
        "assistant" => map_assistant(value),
        "tool" => map_tool_result(value),
        "user" | "system" => Vec::new(), // observational skip; not public_response
        other if !other.is_empty() => vec![AcpFragment::Diagnostic {
            message: format!("zai_cli: unsupported role {other}"),
        }],
        _ => Vec::new(),
    }
}

fn map_assistant(value: &Value) -> Vec<AcpFragment> {
    let mut out = Vec::new();
    if let Some(text) = value.get("content").and_then(|c| c.as_str()) {
        let t = text.trim();
        // Skip placeholder chatter while tools run.
        if !t.is_empty() && t != "Using tools to help you..." {
            out.push(AcpFragment::TextDelta {
                channel: TextChannel::PublicResponse,
                text: text.to_string(),
                source_time_ms: None,
                source_step: None,
            });
        }
    }

    if let Some(calls) = value.get("tool_calls").and_then(|c| c.as_array()) {
        for call in calls {
            out.extend(map_tool_call(call));
        }
    }
    out
}

fn map_tool_call(call: &Value) -> Vec<AcpFragment> {
    let id = call.get("id").and_then(|v| v.as_str()).unwrap_or("");
    if id.is_empty() {
        return vec![AcpFragment::Diagnostic {
            message: "zai_cli: tool_call missing id".into(),
        }];
    }
    let action_id = ToolActionId::new(id);
    let func = call.get("function").unwrap_or(call);
    let name = func
        .get("name")
        .and_then(|n| n.as_str())
        .unwrap_or("unknown")
        .to_string();
    let arguments = func
        .get("arguments")
        .map(|a| {
            if a.is_string() {
                a.as_str().unwrap_or("{}").to_string()
            } else {
                a.to_string()
            }
        })
        .unwrap_or_else(|| "{}".into());

    // Tools already executed by zai headless (auto-approve). Emit Ready then leave
    // terminal for the following role=tool line when present; if none arrives,
    // Ready alone is still a complete request observation for EmptyToolRegistry.
    vec![AcpFragment::Tool {
        action_id,
        signal: ToolSignal::RequestReady {
            tool_name: name,
            arguments_json: arguments,
        },
        source_time_ms: None,
        source_step: None,
    }]
}

fn map_tool_result(value: &Value) -> Vec<AcpFragment> {
    let id = value
        .get("tool_call_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if id.is_empty() {
        return vec![AcpFragment::Diagnostic {
            message: "zai_cli: tool result missing tool_call_id".into(),
        }];
    }
    let content = value
        .get("content")
        .map(|c| {
            if c.is_string() {
                c.as_str().unwrap_or("").to_string()
            } else {
                c.to_string()
            }
        })
        .unwrap_or_default();
    let success = !content.to_ascii_lowercase().starts_with("error");
    vec![AcpFragment::Tool {
        action_id: ToolActionId::new(id),
        signal: ToolSignal::Resolved {
            success,
            result_json: Some(serde_json::json!({ "content": content }).to_string()),
        },
        source_time_ms: None,
        source_step: None,
    }]
}

/// Drain complete NDJSON lines from a growing buffer (UTF-8 safe at newlines).
pub fn drain_ndjson_lines(buffer: &mut Vec<u8>) -> Vec<String> {
    let mut out = Vec::new();
    loop {
        let Some(pos) = buffer.iter().position(|&b| b == b'\n') else {
            break;
        };
        let line_bytes: Vec<u8> = buffer.drain(..=pos).collect();
        let line = String::from_utf8_lossy(&line_bytes).into_owned();
        let trimmed = line.trim();
        if !trimmed.is_empty() && trimmed.starts_with('{') {
            out.push(trimmed.to_string());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assistant_text_becomes_public_response() {
        let frags = map_chat_message_line(
            r#"{"role":"assistant","content":"Hello, monoloop is a three-component async kernel."}"#,
        );
        assert!(matches!(
            &frags[0],
            AcpFragment::TextDelta {
                text,
                channel: TextChannel::PublicResponse,
                ..
            } if text.contains("three-component")
        ));
    }

    #[test]
    fn skips_using_tools_placeholder() {
        let frags = map_chat_message_line(
            r#"{"role":"assistant","content":"Using tools to help you...","tool_calls":[{"id":"c1","type":"function","function":{"name":"bash","arguments":"{\"command\":\"ls\"}"}}]}"#,
        );
        assert!(frags.iter().all(|f| !matches!(
            f,
            AcpFragment::TextDelta { text, .. } if text.contains("Using tools")
        )));
        assert!(frags.iter().any(|f| matches!(
            f,
            AcpFragment::Tool {
                signal: ToolSignal::RequestReady { tool_name, .. },
                ..
            } if tool_name == "bash"
        )));
    }

    #[test]
    fn tool_result_resolves() {
        let frags = map_chat_message_line(
            r#"{"role":"tool","tool_call_id":"c1","content":"hello monoloop zai crud"}"#,
        );
        assert!(matches!(
            &frags[0],
            AcpFragment::Tool {
                signal: ToolSignal::Resolved { success: true, .. },
                ..
            }
        ));
    }

    #[test]
    fn drain_lines_across_fragments() {
        let mut buf = b"{\"role\":\"assistant\",\"content\":\"Hi.\"}\n{\"role\"".to_vec();
        let lines = drain_ndjson_lines(&mut buf);
        assert_eq!(lines.len(), 1);
        buf.extend_from_slice(br#":"user","content":"x"}"#);
        buf.push(b'\n');
        let lines2 = drain_ndjson_lines(&mut buf);
        assert_eq!(lines2.len(), 1);
        assert!(buf.is_empty() || !buf.contains(&b'\n'));
    }
}
