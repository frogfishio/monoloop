//! OpenAI Chat Completions v1 streaming SSE → canonical fragments.
//!
//! Frames `data: <json>` / `data: [DONE]` across arbitrary byte boundaries.
//! Emits complete text units via the shared segmenter path and
//! `ToolRequestReady` only when tool arguments are complete valid JSON.
//! OpenAI Responses and non-streaming JSON are unsupported.

use crate::acp::{AcpFragment, ToolSignal};
use monoloop_contracts::{InterpreterError, InterpreterErrorKind, TextChannel, ToolActionId};
use serde_json::Value;
use std::collections::HashMap;

/// Default accepted choice index for the first product path.
pub const DEFAULT_CHOICE_INDEX: u32 = 0;

/// Incremental SSE assembler for one interpretation.
#[derive(Debug, Default)]
pub struct OpenAiSseState {
    /// Incomplete trailing line bytes.
    line_carry: Vec<u8>,
    /// Data lines of the current SSE event.
    event_data_lines: Vec<String>,
    /// Partial tool calls keyed by provider tool-call index within the choice.
    tools: HashMap<u32, PartialToolCall>,
    /// Whether `data: [DONE]` was observed.
    saw_done: bool,
    /// Selected choice index.
    choice_index: u32,
    /// Maximum SSE line bytes.
    max_line_bytes: usize,
    /// Maximum single event data bytes.
    max_event_bytes: usize,
    /// Maximum tool argument accumulation bytes.
    max_tool_arg_bytes: usize,
}

#[derive(Debug, Default)]
struct PartialToolCall {
    id: String,
    name: String,
    arguments: String,
    emitted_waiting: bool,
    ready: bool,
}

impl OpenAiSseState {
    /// Construct with bounds from interpretation limits.
    pub fn new(max_line_bytes: usize, max_event_bytes: usize, max_tool_arg_bytes: usize) -> Self {
        Self {
            choice_index: DEFAULT_CHOICE_INDEX,
            max_line_bytes: max_line_bytes.max(64),
            max_event_bytes: max_event_bytes.max(64),
            max_tool_arg_bytes: max_tool_arg_bytes.max(64),
            ..Default::default()
        }
    }

    /// Whether a terminal `[DONE]` marker was observed.
    pub fn saw_done(&self) -> bool {
        self.saw_done
    }

    /// Ingest a raw transport chunk; return fragments to apply.
    pub fn push_bytes(&mut self, chunk: &[u8]) -> Result<Vec<AcpFragment>, InterpreterError> {
        if self.saw_done {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for &b in chunk {
            if b == b'\n' {
                let mut line = std::mem::take(&mut self.line_carry);
                // Strip CR for CRLF.
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                self.handle_line(&line, &mut out)?;
            } else {
                if self.line_carry.len() >= self.max_line_bytes {
                    return Err(InterpreterError::new(
                        InterpreterErrorKind::FrameLimitExceeded,
                        "SSE line exceeds bound",
                    ));
                }
                self.line_carry.push(b);
            }
        }
        Ok(out)
    }

    /// Flush on clean end: incomplete trailing event without [DONE] fails closed.
    pub fn seal_clean(&mut self) -> Result<Vec<AcpFragment>, InterpreterError> {
        if !self.line_carry.is_empty() {
            let line = std::mem::take(&mut self.line_carry);
            let mut out = Vec::new();
            self.handle_line(&line, &mut out)?;
            if !out.is_empty() {
                // fall through
            }
        }
        if !self.event_data_lines.is_empty() {
            // Incomplete event at EOF.
            return Err(InterpreterError::new(
                InterpreterErrorKind::MalformedFrame,
                "incomplete SSE event at end of stream",
            ));
        }
        if !self.saw_done {
            return Err(InterpreterError::new(
                InterpreterErrorKind::MalformedFrame,
                "missing [DONE] terminator",
            ));
        }
        // Incomplete tool args never become Ready.
        let mut frags = Vec::new();
        for partial in self.tools.values() {
            if !partial.ready && !partial.id.is_empty() {
                frags.push(AcpFragment::Tool {
                    action_id: ToolActionId::new(partial.id.clone()),
                    signal: ToolSignal::Waiting {
                        tool_name: if partial.name.is_empty() {
                            None
                        } else {
                            Some(partial.name.clone())
                        },
                        waiting_for: "incomplete tool arguments at stream end".into(),
                    },
                    source_time_ms: None,
                    source_step: None,
                });
            }
        }
        Ok(frags)
    }

    fn handle_line(
        &mut self,
        line: &[u8],
        out: &mut Vec<AcpFragment>,
    ) -> Result<(), InterpreterError> {
        if line.is_empty() {
            // Dispatch event.
            if self.event_data_lines.is_empty() {
                return Ok(());
            }
            let data = self.event_data_lines.join("\n");
            self.event_data_lines.clear();
            if data.len() > self.max_event_bytes {
                return Err(InterpreterError::new(
                    InterpreterErrorKind::FrameLimitExceeded,
                    "SSE event exceeds bound",
                ));
            }
            if data.trim() == "[DONE]" {
                self.saw_done = true;
                return Ok(());
            }
            self.map_data_json(&data, out)?;
            return Ok(());
        }

        // Comment / id / event / retry ignored; only data: matters for Chat Completions.
        let line_str = std::str::from_utf8(line).map_err(|_| {
            InterpreterError::new(
                InterpreterErrorKind::MalformedFrame,
                "SSE line is not valid UTF-8",
            )
        })?;
        if let Some(rest) = line_str.strip_prefix("data:") {
            let payload = rest.strip_prefix(' ').unwrap_or(rest);
            self.event_data_lines.push(payload.to_string());
        }
        Ok(())
    }

    fn map_data_json(
        &mut self,
        data: &str,
        out: &mut Vec<AcpFragment>,
    ) -> Result<(), InterpreterError> {
        let value: Value = serde_json::from_str(data).map_err(|_| {
            InterpreterError::new(
                InterpreterErrorKind::MalformedSemanticPayload,
                "SSE data is not valid JSON",
            )
        })?;
        let Some(choices) = value.get("choices").and_then(|c| c.as_array()) else {
            return Ok(());
        };
        for choice in choices {
            let index = choice.get("index").and_then(|i| i.as_u64()).unwrap_or(0) as u32;
            if index != self.choice_index {
                // Unsupported extra choices are ignored (not executed).
                continue;
            }
            if let Some(delta) = choice.get("delta") {
                if let Some(content) = delta.get("content").and_then(|c| c.as_str()) {
                    if !content.is_empty() {
                        out.push(AcpFragment::TextDelta {
                            channel: TextChannel::PublicResponse,
                            text: content.to_string(),
                            source_time_ms: None,
                            source_step: None,
                        });
                    }
                }
                if let Some(calls) = delta.get("tool_calls").and_then(|t| t.as_array()) {
                    for call in calls {
                        self.ingest_tool_delta(call, out)?;
                    }
                }
            }
            if let Some(reason) = choice.get("finish_reason").and_then(|r| r.as_str()) {
                self.on_finish_reason(reason, out)?;
            }
        }
        Ok(())
    }

    fn ingest_tool_delta(
        &mut self,
        call: &Value,
        out: &mut Vec<AcpFragment>,
    ) -> Result<(), InterpreterError> {
        let idx = call.get("index").and_then(|i| i.as_u64()).unwrap_or(0) as u32;
        let entry = self.tools.entry(idx).or_default();
        if let Some(id) = call.get("id").and_then(|v| v.as_str()) {
            if !id.is_empty() {
                entry.id = id.to_string();
            }
        }
        if let Some(func) = call.get("function") {
            if let Some(name) = func.get("name").and_then(|n| n.as_str()) {
                if !name.is_empty() {
                    entry.name.push_str(name);
                }
            }
            if let Some(args) = func.get("arguments").and_then(|a| a.as_str()) {
                if entry.arguments.len().saturating_add(args.len()) > self.max_tool_arg_bytes {
                    return Err(InterpreterError::new(
                        InterpreterErrorKind::ToolLimitExceeded,
                        "tool arguments exceed bound",
                    ));
                }
                entry.arguments.push_str(args);
            }
        }
        // D-016: accumulate deltas only. Waiting once we have an id; Ready only
        // on qualified finish_reason == "tool_calls" (never mid-stream).
        if !entry.id.is_empty() && !entry.emitted_waiting && !entry.ready {
            entry.emitted_waiting = true;
            out.push(AcpFragment::Tool {
                action_id: ToolActionId::new(entry.id.clone()),
                signal: ToolSignal::Waiting {
                    tool_name: if entry.name.is_empty() {
                        None
                    } else {
                        Some(entry.name.clone())
                    },
                    waiting_for: "tool arguments".into(),
                },
                source_time_ms: None,
                source_step: None,
            });
        }
        Ok(())
    }

    fn on_finish_reason(
        &mut self,
        reason: &str,
        out: &mut Vec<AcpFragment>,
    ) -> Result<(), InterpreterError> {
        match reason {
            // D-016: only tool_calls finish may promote Ready; length/content_filter
            // must not execute incomplete argument fragments.
            "tool_calls" => {
                let keys: Vec<u32> = self.tools.keys().copied().collect();
                for k in keys {
                    let Some(entry) = self.tools.get_mut(&k) else {
                        continue;
                    };
                    if entry.ready {
                        continue;
                    }
                    if entry.id.is_empty()
                        || entry.name.is_empty()
                        || !is_complete_json_value(&entry.arguments)
                    {
                        return Err(InterpreterError::new(
                            InterpreterErrorKind::MalformedSemanticPayload,
                            "incomplete tool call at tool_calls finish",
                        ));
                    }
                    entry.ready = true;
                    out.push(AcpFragment::Tool {
                        action_id: ToolActionId::new(entry.id.clone()),
                        signal: ToolSignal::RequestReady {
                            tool_name: entry.name.clone(),
                            arguments_json: entry.arguments.clone(),
                        },
                        source_time_ms: None,
                        source_step: None,
                    });
                }
                Ok(())
            }
            "stop" | "length" | "content_filter" | "null" => {
                // Do not promote incomplete tools (D-016).
                Ok(())
            }
            other => Err(InterpreterError::new(
                InterpreterErrorKind::MalformedSemanticPayload,
                format!("unsupported finish_reason: {other}"),
            )),
        }
    }
}

/// True when `s` parses as a complete JSON value (object/array/primitive).
fn is_complete_json_value(s: &str) -> bool {
    let t = s.trim();
    if t.is_empty() {
        return false;
    }
    serde_json::from_str::<Value>(t).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st() -> OpenAiSseState {
        OpenAiSseState::new(4096, 64 * 1024, 64 * 1024)
    }

    #[test]
    fn fragmented_sse_text() {
        let mut s = st();
        let mut all = Vec::new();
        for chunk in [
            b"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hel".as_slice(),
            b"lo\"}}]}\n\n".as_slice(),
            b"data: [DONE]\n\n".as_slice(),
        ] {
            all.extend(s.push_bytes(chunk).unwrap());
        }
        assert!(s.saw_done());
        let text: String = all
            .iter()
            .filter_map(|f| match f {
                AcpFragment::TextDelta { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(text, "Hello");
    }

    #[test]
    fn tool_args_fragmented_only_ready_when_complete() {
        let mut s = st();
        let c1 = br#"data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"search","arguments":"{\"q\":"}}]}}]}"#;
        let mut fr1 = s.push_bytes(c1).unwrap();
        fr1.extend(s.push_bytes(b"\n\n").unwrap());
        assert!(fr1.iter().any(|f| matches!(
            f,
            AcpFragment::Tool {
                signal: ToolSignal::Waiting { .. },
                ..
            }
        )));
        assert!(!fr1.iter().any(|f| matches!(
            f,
            AcpFragment::Tool {
                signal: ToolSignal::RequestReady { .. },
                ..
            }
        )));

        let c2 = br#"data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"hi\"}"}}]},"finish_reason":"tool_calls"}]}"#;
        let mut fr2 = s.push_bytes(c2).unwrap();
        fr2.extend(s.push_bytes(b"\n\n").unwrap());
        assert!(fr2.iter().any(|f| matches!(
            f,
            AcpFragment::Tool {
                signal: ToolSignal::RequestReady {
                    tool_name,
                    arguments_json,
                },
                ..
            } if tool_name == "search" && arguments_json.contains("hi")
        )));
        let _ = s.push_bytes(b"data: [DONE]\n\n").unwrap();
        assert!(s.saw_done());
    }

    #[test]
    fn invalid_json_args_never_ready() {
        let mut s = st();
        let c = br#"data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"c1","function":{"name":"x","arguments":"{not-json"}}]},"finish_reason":"tool_calls"}]}"#;
        let fr = s.push_bytes(c).unwrap();
        s.push_bytes(b"\n\n").unwrap();
        assert!(!fr.iter().any(|f| matches!(
            f,
            AcpFragment::Tool {
                signal: ToolSignal::RequestReady { .. },
                ..
            }
        )));
    }

    #[test]
    fn missing_done_fails_seal() {
        let mut s = st();
        let _ = s
            .push_bytes(br#"data: {"choices":[{"index":0,"delta":{"content":"x"}}]}"#)
            .unwrap();
        let _ = s.push_bytes(b"\n\n").unwrap();
        assert!(s.seal_clean().is_err());
    }

    #[test]
    fn other_choice_index_ignored() {
        let mut s = st();
        let c = br#"data: {"choices":[{"index":1,"delta":{"content":"nope"}}]}"#;
        let fr = s.push_bytes(c).unwrap();
        s.push_bytes(b"\n\n").unwrap();
        assert!(fr.is_empty());
    }
}
