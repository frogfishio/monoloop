//! Outbound encoder for ACP external-agent Channels (`session/prompt` params).
//!
//! Emits provider-neutral JSON params (or full method envelope). Connector only
//! transports bytes — no prompt on process argv.

use monoloop_contracts::{
    Bytes, CanonicalMessage, DialectDescriptor, EncodedExchange, EncodingError,
    ExchangeInputPolicy, InitialEncodeRequest, OutboundDialectEncoder,
    ToolContinuationEncodeRequest,
};
use serde_json::json;

/// How ACP prompt bytes are shaped for the Connector input path.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AcpPromptWireShape {
    /// JSON object `{ "prompt": [ { "type":"text", "text": ... } ] }` (Grok bridge).
    #[default]
    ParamsObject,
    /// Full JSON-RPC request without id: `{ "method":"session/prompt", "params": ... }`.
    MethodEnvelope,
    /// Plain UTF-8 user text only (Cursor/Codex/Agy prompt_text bridges).
    PlainText,
}

/// ACP session/prompt encoder for external-agent Channels.
#[derive(Clone, Debug)]
pub struct AcpPromptEncoder {
    /// Wire shape.
    pub shape: AcpPromptWireShape,
    /// Dialect stamp on encoded exchange.
    pub dialect: DialectDescriptor,
    /// Max encoded body bytes.
    pub max_encoded_bytes: usize,
}

impl Default for AcpPromptEncoder {
    fn default() -> Self {
        Self {
            shape: AcpPromptWireShape::ParamsObject,
            dialect: DialectDescriptor::acp_json_rpc("1"),
            max_encoded_bytes: 1024 * 1024,
        }
    }
}

impl AcpPromptEncoder {
    /// Grok Build wire shape (params object for session/prompt).
    pub fn grok() -> Self {
        Self {
            shape: AcpPromptWireShape::ParamsObject,
            dialect: DialectDescriptor::acp_json_rpc("1"),
            max_encoded_bytes: 1024 * 1024,
        }
    }

    /// Cursor ACP (plain text → `session/prompt` in connector bridge).
    pub fn cursor() -> Self {
        Self {
            shape: AcpPromptWireShape::PlainText,
            dialect: DialectDescriptor::cursor_acp("1"),
            max_encoded_bytes: 1024 * 1024,
        }
    }

    /// Codex ACP plain text.
    pub fn codex() -> Self {
        Self {
            shape: AcpPromptWireShape::PlainText,
            dialect: DialectDescriptor::codex_acp("1"),
            max_encoded_bytes: 1024 * 1024,
        }
    }

    /// Antigravity / agy plain text.
    pub fn agy() -> Self {
        Self {
            shape: AcpPromptWireShape::PlainText,
            dialect: DialectDescriptor::agy_acp("1"),
            max_encoded_bytes: 1024 * 1024,
        }
    }

    fn collect_user_text(messages: &[CanonicalMessage]) -> Result<String, EncodingError> {
        let mut text = String::new();
        for msg in messages {
            match msg {
                CanonicalMessage::User { content, .. }
                | CanonicalMessage::System { content, .. } => {
                    for part in content {
                        if !text.is_empty() {
                            text.push('\n');
                        }
                        text.push_str(part.text());
                    }
                }
                CanonicalMessage::Assistant { content, .. } => {
                    for part in content {
                        if !text.is_empty() {
                            text.push('\n');
                        }
                        text.push_str(part.text());
                    }
                }
                CanonicalMessage::Tool { content, .. } => {
                    for part in content {
                        if !text.is_empty() {
                            text.push('\n');
                        }
                        text.push_str(part.text());
                    }
                }
            }
        }
        if text.trim().is_empty() {
            return Err(EncodingError::UnrepresentableInput);
        }
        Ok(text)
    }
}

impl OutboundDialectEncoder for AcpPromptEncoder {
    fn encode_initial(
        &self,
        request: InitialEncodeRequest<'_>,
    ) -> Result<EncodedExchange, EncodingError> {
        let text = Self::collect_user_text(request.input.messages())?;
        // Tools for external agents must go through MCP, not model tool arrays.
        if !request.tools.is_empty() {
            return Err(EncodingError::Unsupported(
                "non-empty tools require MCP gateway for external-agent ACP profiles",
            ));
        }
        let bytes = match self.shape {
            AcpPromptWireShape::PlainText => Bytes::from(text.into_bytes()),
            AcpPromptWireShape::ParamsObject => {
                let v = json!({
                    "prompt": [{ "type": "text", "text": text }]
                });
                Bytes::from(
                    serde_json::to_vec(&v).map_err(|_| EncodingError::UnrepresentableInput)?,
                )
            }
            AcpPromptWireShape::MethodEnvelope => {
                let v = json!({
                    "method": "session/prompt",
                    "params": {
                        "prompt": [{ "type": "text", "text": text }]
                    }
                });
                Bytes::from(
                    serde_json::to_vec(&v).map_err(|_| EncodingError::UnrepresentableInput)?,
                )
            }
        };
        if bytes.len() > self.max_encoded_bytes {
            return Err(EncodingError::LimitExceeded);
        }
        Ok(EncodedExchange {
            bytes,
            required_input_dialect: self.dialect.clone(),
            input_policy: ExchangeInputPolicy::SendAndFinish,
        })
    }

    fn encode_tool_continuation(
        &self,
        _request: ToolContinuationEncodeRequest<'_>,
    ) -> Result<EncodedExchange, EncodingError> {
        // External-agent tool results return via MCP, not model-tool continuation.
        Err(EncodingError::Unsupported(
            "ACP external agents do not encode model tool continuations",
        ))
    }
}

/// Headless CLI encoder: plain text prompt body for Z.ai / Claude print mode.
///
/// Prompt on argv is a documented profile exception (CLI product contract);
/// the encoder still owns the text content (not secrets).
#[derive(Clone, Debug)]
pub struct HeadlessPromptEncoder {
    /// Dialect stamp.
    pub dialect: DialectDescriptor,
    /// Max bytes.
    pub max_encoded_bytes: usize,
}

impl HeadlessPromptEncoder {
    /// Z.ai CLI.
    pub fn zai() -> Self {
        Self {
            dialect: DialectDescriptor::zai_cli("1"),
            max_encoded_bytes: 1024 * 1024,
        }
    }

    /// Claude Code print mode.
    pub fn claude() -> Self {
        Self {
            dialect: DialectDescriptor::claude_code("1"),
            max_encoded_bytes: 1024 * 1024,
        }
    }
}

impl OutboundDialectEncoder for HeadlessPromptEncoder {
    fn encode_initial(
        &self,
        request: InitialEncodeRequest<'_>,
    ) -> Result<EncodedExchange, EncodingError> {
        if !request.tools.is_empty() {
            return Err(EncodingError::Unsupported(
                "headless CLI profiles reject Monoloop-linked tools (MCP None)",
            ));
        }
        let text = AcpPromptEncoder::collect_user_text(request.input.messages())?;
        let bytes = Bytes::from(text.into_bytes());
        if bytes.len() > self.max_encoded_bytes {
            return Err(EncodingError::LimitExceeded);
        }
        Ok(EncodedExchange {
            bytes,
            required_input_dialect: self.dialect.clone(),
            input_policy: ExchangeInputPolicy::SendAndFinish,
        })
    }

    fn encode_tool_continuation(
        &self,
        _request: ToolContinuationEncodeRequest<'_>,
    ) -> Result<EncodedExchange, EncodingError> {
        Err(EncodingError::Unsupported(
            "headless CLI has no tool continuation encoding",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use monoloop_contracts::{user_text_input, EffectiveConfig, ExchangeId, TransactionId};

    fn bare_cfg() -> EffectiveConfig {
        EffectiveConfig {
            model: None,
            temperature: None,
            reasoning_effort: None,
            max_output_tokens: None,
            stop: vec![],
            response_format: None,
            continuation_policy: Default::default(),
            deadline: None,
            extensions: Default::default(),
            session: Default::default(),
        }
    }

    #[test]
    fn grok_params_object_has_no_prompt_on_argv_shape() {
        let enc = AcpPromptEncoder::grok();
        let input = user_text_input("hello agent").unwrap();
        let tid = TransactionId::generate();
        let eid = ExchangeId::generate();
        let encoded = enc
            .encode_initial(InitialEncodeRequest {
                transaction_id: &tid,
                exchange_id: &eid,
                input: &input,
                config: &bare_cfg(),
                tools: &[],
            })
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&encoded.bytes).unwrap();
        assert!(v.get("prompt").is_some());
        assert!(v.get("method").is_none());
    }

    #[test]
    fn plain_text_cursor_shape() {
        let enc = AcpPromptEncoder::cursor();
        let input = user_text_input("hi").unwrap();
        let tid = TransactionId::generate();
        let eid = ExchangeId::generate();
        let encoded = enc
            .encode_initial(InitialEncodeRequest {
                transaction_id: &tid,
                exchange_id: &eid,
                input: &input,
                config: &bare_cfg(),
                tools: &[],
            })
            .unwrap();
        assert_eq!(&encoded.bytes[..], b"hi");
    }
}
