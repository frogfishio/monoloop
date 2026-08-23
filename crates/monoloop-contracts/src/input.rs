//! Caller-owned canonical transaction input (provider-neutral).
//!
//! Monoloop validates and encodes; it never authors or rewrites messages.

use crate::id::{IdentityError, ToolName, MAX_IDENTITY_BYTES};
use crate::limits::InputLimits;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Ordered canonical messages for one transaction submission.
///
/// Monoloop does **not** own chat history. Hosts map their journal into
/// [`CanonicalMessage`] values (typically `User` / `Assistant`, plus `System` /
/// `Tool` when needed) and call [`CanonicalInput::try_new`]. For a single user
/// line only, see [`user_text_input`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CanonicalInput {
    messages: Vec<CanonicalMessage>,
}

impl CanonicalInput {
    /// Validate and construct input under the given limits.
    pub fn try_new(
        messages: Vec<CanonicalMessage>,
        limits: &InputLimits,
    ) -> Result<Self, InputValidationError> {
        if messages.is_empty() {
            return Err(InputValidationError::EmptyMessages);
        }
        if messages.len() > limits.max_messages {
            return Err(InputValidationError::TooManyMessages {
                count: messages.len(),
                max: limits.max_messages,
            });
        }

        let mut aggregate_text = 0usize;
        let mut seen_tool_call_ids: Vec<String> = Vec::new();

        for (index, msg) in messages.iter().enumerate() {
            msg.validate(limits, index, &mut aggregate_text, &mut seen_tool_call_ids)?;
        }

        if aggregate_text > limits.max_aggregate_text_bytes {
            return Err(InputValidationError::AggregateTextTooLarge {
                bytes: aggregate_text,
                max: limits.max_aggregate_text_bytes,
            });
        }

        Ok(Self { messages })
    }

    /// Borrow messages in caller order.
    pub fn messages(&self) -> &[CanonicalMessage] {
        &self.messages
    }

    /// Consume into messages.
    pub fn into_messages(self) -> Vec<CanonicalMessage> {
        self.messages
    }
}

/// Deterministic admission byte estimate covering every canonical field (D-035).
///
/// Counts UTF-8 text parts, optional names, tool-call ids, tool names, and
/// serialized tool-argument JSON. Encode failure fails closed (never counts as
/// zero). This is independent of [`InputLimits::max_aggregate_text_bytes`],
/// which only bounds text parts at construction.
pub fn estimate_canonical_input_bytes(
    input: &CanonicalInput,
) -> Result<usize, InputValidationError> {
    let mut total = 0usize;
    for msg in input.messages() {
        match msg {
            CanonicalMessage::System { content, name }
            | CanonicalMessage::User { content, name } => {
                if let Some(n) = name {
                    total = total.saturating_add(n.len());
                }
                for part in content {
                    total = total.saturating_add(part.text().len());
                }
            }
            CanonicalMessage::Assistant {
                content,
                tool_calls,
            } => {
                for part in content {
                    total = total.saturating_add(part.text().len());
                }
                for call in tool_calls {
                    total = total.saturating_add(call.tool_call_id.len());
                    total = total.saturating_add(call.tool_name.as_str().len());
                    let encoded = serde_json::to_vec(&call.arguments)
                        .map_err(|_| InputValidationError::JsonEncodeFailed)?;
                    total = total.saturating_add(encoded.len());
                }
            }
            CanonicalMessage::Tool {
                tool_call_id,
                content,
            } => {
                total = total.saturating_add(tool_call_id.len());
                for part in content {
                    total = total.saturating_add(part.text().len());
                }
            }
        }
    }
    Ok(total)
}

/// One typed canonical message.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CanonicalMessage {
    /// System message (text parts required).
    System {
        /// Text content parts (non-empty).
        content: Vec<TextPart>,
        /// Optional bounded name.
        name: Option<String>,
    },
    /// User message (text parts required).
    User {
        /// Text content parts (non-empty).
        content: Vec<TextPart>,
        /// Optional bounded name.
        name: Option<String>,
    },
    /// Assistant message (text and/or tool calls).
    Assistant {
        /// Text parts (may be empty when tool_calls is non-empty).
        content: Vec<TextPart>,
        /// Historical or current assistant tool calls.
        tool_calls: Vec<CanonicalAssistantToolCall>,
    },
    /// Tool result correlated to a preceding assistant tool call.
    Tool {
        /// Provider/tool-call id referenced by a prior assistant call.
        tool_call_id: String,
        /// Result text parts (non-empty).
        content: Vec<TextPart>,
    },
}

/// Non-empty text content part.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextPart {
    text: String,
}

impl TextPart {
    /// Construct a non-empty text part.
    pub fn try_new(
        text: impl Into<String>,
        max_bytes: usize,
    ) -> Result<Self, InputValidationError> {
        let text = text.into();
        if text.is_empty() {
            return Err(InputValidationError::EmptyTextPart);
        }
        if text.len() > max_bytes {
            return Err(InputValidationError::TextPartTooLarge {
                bytes: text.len(),
                max: max_bytes,
            });
        }
        Ok(Self { text })
    }

    /// Borrow text.
    pub fn text(&self) -> &str {
        &self.text
    }
}

/// Historical or live assistant tool call embedded in input.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CanonicalAssistantToolCall {
    /// Correlation id for a later [`CanonicalMessage::Tool`].
    pub tool_call_id: String,
    /// Tool name.
    pub tool_name: ToolName,
    /// JSON arguments object/value.
    pub arguments: serde_json::Value,
}

impl CanonicalMessage {
    fn validate(
        &self,
        limits: &InputLimits,
        index: usize,
        aggregate_text: &mut usize,
        seen_tool_call_ids: &mut Vec<String>,
    ) -> Result<(), InputValidationError> {
        match self {
            Self::System { content, name } | Self::User { content, name } => {
                validate_name(name, limits)?;
                validate_text_parts(content, limits, true, aggregate_text)?;
            }
            Self::Assistant {
                content,
                tool_calls,
            } => {
                if content.is_empty() && tool_calls.is_empty() {
                    return Err(InputValidationError::EmptyAssistant { index });
                }
                validate_text_parts(content, limits, false, aggregate_text)?;
                if tool_calls.len() > limits.max_tool_calls {
                    return Err(InputValidationError::TooManyToolCalls {
                        count: tool_calls.len(),
                        max: limits.max_tool_calls,
                    });
                }
                for call in tool_calls {
                    validate_tool_call_id(&call.tool_call_id, limits)?;
                    if seen_tool_call_ids.iter().any(|id| id == &call.tool_call_id) {
                        return Err(InputValidationError::DuplicateToolCallId {
                            id: call.tool_call_id.clone(),
                        });
                    }
                    seen_tool_call_ids.push(call.tool_call_id.clone());
                    validate_json_value(
                        &call.arguments,
                        limits.max_json_depth,
                        limits.max_tool_argument_bytes,
                    )?;
                    let _ = call.tool_name.as_str(); // already validated at ToolName construction
                }
            }
            Self::Tool {
                tool_call_id,
                content,
            } => {
                validate_tool_call_id(tool_call_id, limits)?;
                if !seen_tool_call_ids.iter().any(|id| id == tool_call_id) {
                    return Err(InputValidationError::UnknownToolCallId {
                        id: tool_call_id.clone(),
                    });
                }
                validate_text_parts(content, limits, true, aggregate_text)?;
            }
        }
        Ok(())
    }
}

fn validate_name(name: &Option<String>, limits: &InputLimits) -> Result<(), InputValidationError> {
    if let Some(n) = name {
        if n.is_empty() {
            return Err(InputValidationError::EmptyName);
        }
        if n.len() > limits.max_name_bytes {
            return Err(InputValidationError::NameTooLong {
                bytes: n.len(),
                max: limits.max_name_bytes,
            });
        }
        if n.chars().any(|c| c.is_control()) {
            return Err(InputValidationError::ControlCharacter);
        }
    }
    Ok(())
}

fn validate_tool_call_id(id: &str, limits: &InputLimits) -> Result<(), InputValidationError> {
    if id.is_empty() {
        return Err(InputValidationError::EmptyToolCallId);
    }
    if id.len() > limits.max_tool_call_id_bytes {
        return Err(InputValidationError::ToolCallIdTooLong {
            bytes: id.len(),
            max: limits.max_tool_call_id_bytes,
        });
    }
    if id.chars().any(|c| c.is_control()) {
        return Err(InputValidationError::ControlCharacter);
    }
    Ok(())
}

fn validate_text_parts(
    parts: &[TextPart],
    limits: &InputLimits,
    require_non_empty: bool,
    aggregate_text: &mut usize,
) -> Result<(), InputValidationError> {
    if require_non_empty && parts.is_empty() {
        return Err(InputValidationError::EmptyTextParts);
    }
    if parts.len() > limits.max_content_parts {
        return Err(InputValidationError::TooManyContentParts {
            count: parts.len(),
            max: limits.max_content_parts,
        });
    }
    for p in parts {
        if p.text.is_empty() {
            return Err(InputValidationError::EmptyTextPart);
        }
        if p.text.len() > limits.max_text_part_bytes {
            return Err(InputValidationError::TextPartTooLarge {
                bytes: p.text.len(),
                max: limits.max_text_part_bytes,
            });
        }
        *aggregate_text = aggregate_text.saturating_add(p.text.len());
    }
    Ok(())
}

fn validate_json_value(
    value: &serde_json::Value,
    max_depth: u32,
    max_bytes: usize,
) -> Result<(), InputValidationError> {
    let depth = json_depth(value);
    if depth > max_depth {
        return Err(InputValidationError::JsonTooDeep {
            depth,
            max: max_depth,
        });
    }
    let encoded = serde_json::to_vec(value).map_err(|_| InputValidationError::JsonEncodeFailed)?;
    if encoded.len() > max_bytes {
        return Err(InputValidationError::ToolArgumentsTooLarge {
            bytes: encoded.len(),
            max: max_bytes,
        });
    }
    Ok(())
}

fn json_depth(value: &serde_json::Value) -> u32 {
    match value {
        serde_json::Value::Array(items) => 1 + items.iter().map(json_depth).max().unwrap_or(0),
        serde_json::Value::Object(map) => 1 + map.values().map(json_depth).max().unwrap_or(0),
        _ => 1,
    }
}

/// Helper to build a single-user-text input under default limits.
pub fn user_text_input(text: impl Into<String>) -> Result<CanonicalInput, InputValidationError> {
    let limits = InputLimits::default();
    let part = TextPart::try_new(text, limits.max_text_part_bytes)?;
    CanonicalInput::try_new(
        vec![CanonicalMessage::User {
            content: vec![part],
            name: None,
        }],
        &limits,
    )
}

/// Canonical input validation failure.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum InputValidationError {
    /// No messages.
    #[error("canonical input requires at least one message")]
    EmptyMessages,
    /// Too many messages.
    #[error("message count {count} exceeds max {max}")]
    TooManyMessages {
        /// Actual count.
        count: usize,
        /// Configured max.
        max: usize,
    },
    /// Aggregate text too large.
    #[error("aggregate text bytes {bytes} exceeds max {max}")]
    AggregateTextTooLarge {
        /// Actual bytes.
        bytes: usize,
        /// Configured max.
        max: usize,
    },
    /// System/User/Tool without text parts.
    #[error("message requires at least one text part")]
    EmptyTextParts,
    /// Empty text part.
    #[error("text part must be non-empty")]
    EmptyTextPart,
    /// Text part too large.
    #[error("text part bytes {bytes} exceeds max {max}")]
    TextPartTooLarge {
        /// Actual bytes.
        bytes: usize,
        /// Configured max.
        max: usize,
    },
    /// Too many content parts.
    #[error("content part count {count} exceeds max {max}")]
    TooManyContentParts {
        /// Actual count.
        count: usize,
        /// Configured max.
        max: usize,
    },
    /// Assistant with neither text nor tool calls.
    #[error("assistant message at index {index} is empty")]
    EmptyAssistant {
        /// Message index.
        index: usize,
    },
    /// Too many tool calls.
    #[error("tool call count {count} exceeds max {max}")]
    TooManyToolCalls {
        /// Actual count.
        count: usize,
        /// Configured max.
        max: usize,
    },
    /// Duplicate tool_call_id in input.
    #[error("duplicate tool_call_id {id}")]
    DuplicateToolCallId {
        /// Offending id.
        id: String,
    },
    /// Tool message references unknown id.
    #[error("tool message references unknown tool_call_id {id}")]
    UnknownToolCallId {
        /// Offending id.
        id: String,
    },
    /// Empty tool_call_id.
    #[error("tool_call_id must be non-empty")]
    EmptyToolCallId,
    /// tool_call_id too long.
    #[error("tool_call_id bytes {bytes} exceeds max {max}")]
    ToolCallIdTooLong {
        /// Actual bytes.
        bytes: usize,
        /// Configured max.
        max: usize,
    },
    /// Empty name.
    #[error("message name must be non-empty when present")]
    EmptyName,
    /// Name too long.
    #[error("name bytes {bytes} exceeds max {max}")]
    NameTooLong {
        /// Actual bytes.
        bytes: usize,
        /// Configured max.
        max: usize,
    },
    /// Control character in a string field.
    #[error("input string must not contain control characters")]
    ControlCharacter,
    /// JSON nesting too deep.
    #[error("JSON depth {depth} exceeds max {max}")]
    JsonTooDeep {
        /// Actual depth.
        depth: u32,
        /// Configured max.
        max: u32,
    },
    /// Tool arguments JSON too large.
    #[error("tool argument bytes {bytes} exceeds max {max}")]
    ToolArgumentsTooLarge {
        /// Actual bytes.
        bytes: usize,
        /// Configured max.
        max: usize,
    },
    /// JSON encode failed (unexpected).
    #[error("JSON encode failed")]
    JsonEncodeFailed,
    /// Identity construction failed (tool name).
    #[error(transparent)]
    Identity(#[from] IdentityError),
}

// Silence unused constant import if only used in docs elsewhere.
const _: usize = MAX_IDENTITY_BYTES;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::ToolName;

    #[test]
    fn requires_messages_and_text() {
        let limits = InputLimits::default();
        assert!(CanonicalInput::try_new(vec![], &limits).is_err());
        let empty_user = CanonicalMessage::User {
            content: vec![],
            name: None,
        };
        assert!(CanonicalInput::try_new(vec![empty_user], &limits).is_err());
    }

    #[test]
    fn tool_must_reference_prior_assistant_call() {
        let limits = InputLimits::default();
        let part = TextPart::try_new("ok", limits.max_text_part_bytes).unwrap();
        let bad = CanonicalMessage::Tool {
            tool_call_id: "missing".into(),
            content: vec![part],
        };
        assert!(matches!(
            CanonicalInput::try_new(vec![bad], &limits),
            Err(InputValidationError::UnknownToolCallId { .. })
        ));
    }

    #[test]
    fn historical_tool_round_trip_ok() {
        let limits = InputLimits::default();
        let call = CanonicalAssistantToolCall {
            tool_call_id: "c1".into(),
            tool_name: ToolName::try_new("search").unwrap(),
            arguments: serde_json::json!({"q": "x"}),
        };
        let messages = vec![
            CanonicalMessage::User {
                content: vec![TextPart::try_new("hi", limits.max_text_part_bytes).unwrap()],
                name: None,
            },
            CanonicalMessage::Assistant {
                content: vec![],
                tool_calls: vec![call],
            },
            CanonicalMessage::Tool {
                tool_call_id: "c1".into(),
                content: vec![TextPart::try_new("result", limits.max_text_part_bytes).unwrap()],
            },
        ];
        let input = CanonicalInput::try_new(messages, &limits).unwrap();
        let json = serde_json::to_string(&input).unwrap();
        let back: CanonicalInput = serde_json::from_str(&json).unwrap();
        assert_eq!(input, back);
    }

    #[test]
    fn estimate_counts_names_ids_and_tool_arguments() {
        let limits = InputLimits::default();
        let args = serde_json::json!({"q": "abcdefghij"}); // larger than text-only path
        let encoded_args = serde_json::to_vec(&args).unwrap().len();
        let call = CanonicalAssistantToolCall {
            tool_call_id: "call-id-123".into(),
            tool_name: ToolName::try_new("search").unwrap(),
            arguments: args,
        };
        let input = CanonicalInput::try_new(
            vec![
                CanonicalMessage::User {
                    content: vec![TextPart::try_new("hi", limits.max_text_part_bytes).unwrap()],
                    name: Some("alice".into()),
                },
                CanonicalMessage::Assistant {
                    content: vec![],
                    tool_calls: vec![call],
                },
                CanonicalMessage::Tool {
                    tool_call_id: "call-id-123".into(),
                    content: vec![TextPart::try_new("ok", limits.max_text_part_bytes).unwrap()],
                },
            ],
            &limits,
        )
        .unwrap();

        let bytes = estimate_canonical_input_bytes(&input).unwrap();
        // text "hi" + name "alice" + id + tool name + args + id again + text "ok"
        let expected = 2 + 5 + "call-id-123".len() + "search".len() + encoded_args
            + "call-id-123".len()
            + 2;
        assert_eq!(bytes, expected);
        assert!(
            bytes > 2 + 2,
            "tool args/ids/names must increase estimate beyond text-only"
        );
    }
}
