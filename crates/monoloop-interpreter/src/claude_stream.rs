//! Claude Code headless dialect: `stream-json` NDJSON events → fragments.
//!
//! `claude -p --output-format stream-json --verbose` emits one JSON object per
//! line: `system`, `assistant` (text / tool_use / thinking), `user` (tool_result),
//! `result`. Tools already ran inside Claude Code; we observe Ready+Resolved.

use crate::acp::{AcpFragment, ToolSignal};
use monoloop_contracts::{TextChannel, ToolActionId};
use serde_json::Value;

/// Map one complete stream-json line into fragments.
pub fn map_stream_line(line: &str) -> Vec<AcpFragment> {
    let line = line.trim();
    if line.is_empty() || !line.starts_with('{') {
        return Vec::new();
    }
    let Ok(value) = serde_json::from_str::<Value>(line) else {
        return vec![AcpFragment::Diagnostic {
            message: "claude_code: malformed stream-json line".into(),
        }];
    };
    map_stream_event(&value)
}

/// Map a parsed stream-json event.
pub fn map_stream_event(value: &Value) -> Vec<AcpFragment> {
    let ty = value.get("type").and_then(|t| t.as_str()).unwrap_or("");
    let source_time_ms = extract_timestamp_ms(value);
    match ty {
        "assistant" => map_assistant(value, source_time_ms),
        "user" => map_user_tool_results(value, source_time_ms),
        "result" => {
            // Terminal marker for the print run.
            vec![AcpFragment::ResponseFinished]
        }
        "system" | "rate_limit_event" => Vec::new(),
        other if !other.is_empty() => Vec::new(), // ignore unknown observational types
        _ => Vec::new(),
    }
}

fn map_assistant(value: &Value, source_time_ms: Option<u64>) -> Vec<AcpFragment> {
    let mut out = Vec::new();
    let content = value
        .pointer("/message/content")
        .and_then(|c| c.as_array())
        .cloned()
        .unwrap_or_default();
    for block in content {
        let bty = block.get("type").and_then(|t| t.as_str()).unwrap_or("");
        match bty {
            "text" => {
                if let Some(text) = block.get("text").and_then(|t| t.as_str()) {
                    if !text.is_empty() {
                        out.push(AcpFragment::TextDelta {
                            channel: TextChannel::PublicResponse,
                            text: text.to_string(),
                            source_time_ms,
                            source_step: None,
                        });
                    }
                }
            }
            "tool_use" => {
                let id = block.get("id").and_then(|v| v.as_str()).unwrap_or("");
                if id.is_empty() {
                    out.push(AcpFragment::Diagnostic {
                        message: "claude_code: tool_use missing id".into(),
                    });
                    continue;
                }
                let name = block
                    .get("name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                let arguments = block
                    .get("input")
                    .map(|a| a.to_string())
                    .unwrap_or_else(|| "{}".into());
                out.push(AcpFragment::Tool {
                    action_id: ToolActionId::new(id),
                    signal: ToolSignal::RequestReady {
                        tool_name: name,
                        arguments_json: arguments,
                    },
                    source_time_ms,
                    source_step: None,
                });
            }
            "thinking" => {
                // Private CoT — do not promote to public reasoning summary.
            }
            _ => {}
        }
    }
    out
}

fn map_user_tool_results(value: &Value, source_time_ms: Option<u64>) -> Vec<AcpFragment> {
    let mut out = Vec::new();
    let content = value
        .pointer("/message/content")
        .and_then(|c| c.as_array())
        .cloned()
        .unwrap_or_default();
    for block in content {
        if block.get("type").and_then(|t| t.as_str()) != Some("tool_result") {
            continue;
        }
        let id = block
            .get("tool_use_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if id.is_empty() {
            continue;
        }
        let result_content = block.get("content").map(|c| {
            if c.is_string() {
                c.as_str().unwrap_or("").to_string()
            } else {
                c.to_string()
            }
        });
        let is_error = block
            .get("is_error")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let success = !is_error
            && result_content
                .as_deref()
                .map(|s| !s.to_ascii_lowercase().starts_with("error"))
                .unwrap_or(true);
        out.push(AcpFragment::Tool {
            action_id: ToolActionId::new(id),
            signal: ToolSignal::Resolved {
                success,
                result_json: result_content
                    .map(|c| serde_json::json!({ "content": c }).to_string()),
            },
            source_time_ms,
            source_step: None,
        });
    }
    out
}

fn extract_timestamp_ms(value: &Value) -> Option<u64> {
    let s = value.get("timestamp").and_then(|t| t.as_str())?;
    // RFC3339 / ISO-8601: 2026-08-16T09:12:22.325Z
    parse_rfc3339_ms(s)
}

fn parse_rfc3339_ms(s: &str) -> Option<u64> {
    // Minimal parse without chrono: DateTime::parse via time crate not in deps.
    // Accept `YYYY-MM-DDTHH:MM:SS(.mmm)Z` only.
    let s = s.trim();
    if !s.ends_with('Z') || s.len() < 20 {
        return None;
    }
    let core = &s[..s.len() - 1]; // strip Z
    let (date, time) = core.split_once('T')?;
    let mut d = date.split('-');
    let y: i64 = d.next()?.parse().ok()?;
    let mo: i64 = d.next()?.parse().ok()?;
    let day: i64 = d.next()?.parse().ok()?;
    let (hms, frac) = match time.split_once('.') {
        Some((a, b)) => (a, Some(b)),
        None => (time, None),
    };
    let mut t = hms.split(':');
    let h: i64 = t.next()?.parse().ok()?;
    let mi: i64 = t.next()?.parse().ok()?;
    let sec: i64 = t.next()?.parse().ok()?;
    let ms: i64 = match frac {
        Some(f) => {
            let digits: String = f.chars().take(3).collect();
            let padded = format!("{digits:0<3}");
            padded.parse().unwrap_or(0)
        }
        None => 0,
    };
    // Days from civil date (Howard Hinnant algorithm) → Unix ms.
    let days = days_from_civil(y, mo, day)?;
    let total_ms = days
        .checked_mul(86_400_000)?
        .checked_add(h.checked_mul(3_600_000)?)?
        .checked_add(mi.checked_mul(60_000)?)?
        .checked_add(sec.checked_mul(1000)?)?
        .checked_add(ms)?;
    if total_ms < 0 {
        None
    } else {
        Some(total_ms as u64)
    }
}

fn days_from_civil(y: i64, m: i64, d: i64) -> Option<i64> {
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(era * 146_097 + doe - 719_468)
}

/// Drain complete NDJSON lines from a growing buffer.
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
    fn assistant_text_and_timestamp() {
        let line = r#"{"type":"assistant","timestamp":"2026-08-16T09:12:22.325Z","message":{"content":[{"type":"text","text":"Hello monoloop."}]}}"#;
        let frags = map_stream_line(line);
        assert!(matches!(
            &frags[0],
            AcpFragment::TextDelta {
                text,
                source_time_ms: Some(_),
                ..
            } if text.contains("Hello monoloop")
        ));
    }

    #[test]
    fn tool_use_and_result() {
        let use_line = r#"{"type":"assistant","message":{"content":[{"type":"tool_use","id":"toolu_1","name":"Write","input":{"file_path":"/tmp/x"}}]}}"#;
        let frags = map_stream_line(use_line);
        assert!(matches!(
            &frags[0],
            AcpFragment::Tool {
                signal: ToolSignal::RequestReady { tool_name, .. },
                ..
            } if tool_name == "Write"
        ));
        let res_line = r#"{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"ok"}]}}"#;
        let frags = map_stream_line(res_line);
        assert!(matches!(
            &frags[0],
            AcpFragment::Tool {
                signal: ToolSignal::Resolved { success: true, .. },
                ..
            }
        ));
    }

    #[test]
    fn thinking_not_public() {
        let line = r#"{"type":"assistant","message":{"content":[{"type":"thinking","thinking":"secret"}]}}"#;
        assert!(map_stream_line(line).is_empty());
    }

    #[test]
    fn result_finishes() {
        let frags = map_stream_line(r#"{"type":"result","subtype":"success","result":"done"}"#);
        assert!(matches!(frags[0], AcpFragment::ResponseFinished));
    }

    #[test]
    fn rfc3339_ms_parse() {
        let ms = parse_rfc3339_ms("2026-08-16T09:12:22.325Z").unwrap();
        // Sanity: after 2020 and before 2030 in ms.
        assert!(ms > 1_577_836_800_000);
        assert!(ms < 1_893_456_000_000);
    }
}
