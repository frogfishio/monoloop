//! OpenAI Chat Completions v1 outbound encoder (streaming SSE dialect).
//!
//! Encodes provider-neutral canonical input into a Chat Completions request body.
//! Does not speak HTTP; Connector transports the bytes. OpenAI Responses is unsupported.

use monoloop_contracts::{
    Bytes, CanonicalMessage, CanonicalToolOutput, CanonicalToolResult, CanonicalToolResultOutcome,
    DialectDescriptor, EncodedExchange, EncodingError, ExchangeInputPolicy, InitialEncodeRequest,
    OutboundDialectEncoder, ReasoningEffort, ResponseFormat, ToolContinuationEncodeRequest,
    ToolSpec,
};
use serde_json::{json, Map, Value};

/// Options controlling Chat Completions field names and capability gates.
#[derive(Clone, Debug)]
pub struct OpenAiEncoderOptions {
    /// When true, emit `max_completion_tokens` instead of `max_tokens`.
    pub use_max_completion_tokens: bool,
    /// Whether `reasoning_effort` may be encoded.
    pub allow_reasoning_effort: bool,
    /// Maximum encoded body bytes.
    pub max_encoded_bytes: usize,
}

impl Default for OpenAiEncoderOptions {
    fn default() -> Self {
        Self {
            use_max_completion_tokens: false,
            allow_reasoning_effort: false,
            max_encoded_bytes: 4 * 1024 * 1024,
        }
    }
}

/// Chat Completions v1 encoder producing `stream: true` JSON bodies.
#[derive(Clone, Debug, Default)]
pub struct OpenAiChatCompletionsEncoder {
    /// Capability-dependent options.
    pub options: OpenAiEncoderOptions,
}

impl OpenAiChatCompletionsEncoder {
    /// Construct with options.
    pub fn new(options: OpenAiEncoderOptions) -> Self {
        Self { options }
    }

    fn dialect() -> DialectDescriptor {
        DialectDescriptor::openai_chat_completions("v1")
    }

    fn encode_body(
        &self,
        messages: Value,
        tools: &[ToolSpec],
        config: &monoloop_contracts::EffectiveConfig,
    ) -> Result<EncodedExchange, EncodingError> {
        let mut body = Map::new();
        let model = config
            .model
            .clone()
            .ok_or(EncodingError::InvalidConfiguration)?;
        body.insert("model".into(), Value::String(model));
        body.insert("messages".into(), messages);
        body.insert("stream".into(), Value::Bool(true));

        if let Some(t) = config.temperature {
            body.insert("temperature".into(), json!(t));
        }
        if let Some(max) = config.max_output_tokens {
            let key = if self.options.use_max_completion_tokens {
                "max_completion_tokens"
            } else {
                "max_tokens"
            };
            body.insert(key.into(), json!(max));
        }
        if !config.stop.is_empty() {
            body.insert("stop".into(), json!(config.stop));
        }
        if let Some(fmt) = &config.response_format {
            match fmt {
                ResponseFormat::Text => {
                    body.insert("response_format".into(), json!({ "type": "text" }));
                }
                ResponseFormat::JsonObject => {
                    body.insert("response_format".into(), json!({ "type": "json_object" }));
                }
            }
        }
        if let Some(effort) = config.reasoning_effort {
            if !self.options.allow_reasoning_effort {
                return Err(EncodingError::Unsupported("reasoning_effort"));
            }
            let label = match effort {
                ReasoningEffort::Low => "low",
                ReasoningEffort::Medium => "medium",
                ReasoningEffort::High => "high",
            };
            body.insert("reasoning_effort".into(), Value::String(label.into()));
        }

        // D-023: encode admitted openai.* extensions; never silently drop.
        encode_openai_extensions(&mut body, &config.extensions)?;

        if !tools.is_empty() {
            let tool_defs: Vec<Value> = tools
                .iter()
                .map(|t| {
                    json!({
                        "type": "function",
                        "function": {
                            "name": t.name.as_str(),
                            "description": t.description,
                            "parameters": t.input_schema.as_value(),
                        }
                    })
                })
                .collect();
            body.insert("tools".into(), Value::Array(tool_defs));
        }

        let bytes = serde_json::to_vec(&Value::Object(body))
            .map_err(|_| EncodingError::UnrepresentableInput)?;
        if bytes.len() > self.options.max_encoded_bytes {
            return Err(EncodingError::LimitExceeded);
        }
        Ok(EncodedExchange {
            bytes: Bytes::from(bytes),
            required_input_dialect: Self::dialect(),
            input_policy: ExchangeInputPolicy::SendAndFinish,
        })
    }
}

impl OutboundDialectEncoder for OpenAiChatCompletionsEncoder {
    fn encode_initial(
        &self,
        request: InitialEncodeRequest<'_>,
    ) -> Result<EncodedExchange, EncodingError> {
        let messages = encode_messages(request.input.messages())?;
        self.encode_body(messages, request.tools, request.config)
    }

    fn encode_tool_continuation(
        &self,
        request: ToolContinuationEncodeRequest<'_>,
    ) -> Result<EncodedExchange, EncodingError> {
        // D-031 residual: actor already appends tool results into ContinuationContext
        // via append_exchange_to_transcript — do not append `request.results` again.
        let _ = request.results;
        let msgs = encode_messages_vec(request.context.messages())?;
        self.encode_body(Value::Array(msgs), request.tools, request.config)
    }
}

/// Map admitted `openai.*` extensions into Chat Completions body fields (D-023).
///
/// Any extension that cannot be represented fails closed — never silently dropped.
fn encode_openai_extensions(
    body: &mut Map<String, Value>,
    extensions: &std::collections::BTreeMap<
        monoloop_contracts::ExtensionKey,
        monoloop_contracts::VersionedExtension,
    >,
) -> Result<(), EncodingError> {
    for (key, ext) in extensions {
        let Some(field) = key.as_str().strip_prefix("openai.") else {
            return Err(EncodingError::Unsupported("non-openai extension"));
        };
        match field {
            "seed" | "user" | "top_p" | "n" | "frequency_penalty" | "presence_penalty"
            | "logit_bias" | "logprobs" | "top_logprobs" | "metadata" => {
                if body.contains_key(field) {
                    return Err(EncodingError::Unsupported("extension overrides body field"));
                }
                body.insert(field.to_string(), ext.value.clone());
            }
            _ => return Err(EncodingError::Unsupported("openai extension field")),
        }
    }
    Ok(())
}

fn encode_messages(messages: &[CanonicalMessage]) -> Result<Value, EncodingError> {
    Ok(Value::Array(encode_messages_vec(messages)?))
}

fn encode_messages_vec(messages: &[CanonicalMessage]) -> Result<Vec<Value>, EncodingError> {
    let mut out = Vec::with_capacity(messages.len());
    for msg in messages {
        out.push(encode_message(msg)?);
    }
    Ok(out)
}

fn encode_message(msg: &CanonicalMessage) -> Result<Value, EncodingError> {
    match msg {
        CanonicalMessage::System { content, name } => {
            let mut m = Map::new();
            m.insert("role".into(), Value::String("system".into()));
            m.insert("content".into(), Value::String(join_text(content)));
            if let Some(n) = name {
                m.insert("name".into(), Value::String(n.clone()));
            }
            Ok(Value::Object(m))
        }
        CanonicalMessage::User { content, name } => {
            let mut m = Map::new();
            m.insert("role".into(), Value::String("user".into()));
            m.insert("content".into(), Value::String(join_text(content)));
            if let Some(n) = name {
                m.insert("name".into(), Value::String(n.clone()));
            }
            Ok(Value::Object(m))
        }
        CanonicalMessage::Assistant {
            content,
            tool_calls,
        } => {
            let mut m = Map::new();
            m.insert("role".into(), Value::String("assistant".into()));
            if content.is_empty() {
                m.insert("content".into(), Value::Null);
            } else {
                m.insert("content".into(), Value::String(join_text(content)));
            }
            if !tool_calls.is_empty() {
                let calls: Vec<Value> = tool_calls
                    .iter()
                    .map(|c| {
                        let args =
                            serde_json::to_string(&c.arguments).unwrap_or_else(|_| "{}".into());
                        json!({
                            "id": c.tool_call_id,
                            "type": "function",
                            "function": {
                                "name": c.tool_name.as_str(),
                                "arguments": args,
                            }
                        })
                    })
                    .collect();
                m.insert("tool_calls".into(), Value::Array(calls));
            }
            Ok(Value::Object(m))
        }
        CanonicalMessage::Tool {
            tool_call_id,
            content,
        } => Ok(json!({
            "role": "tool",
            "tool_call_id": tool_call_id,
            "content": join_text(content),
        })),
    }
}

#[allow(dead_code)] // retained for potential direct-result encoding; continuation uses transcript Tool messages
fn encode_tool_result_message(result: &CanonicalToolResult) -> Result<Value, EncodingError> {
    let content = match &result.outcome {
        CanonicalToolResultOutcome::Succeeded(CanonicalToolOutput::Json(v)) => {
            serde_json::to_string(v).map_err(|_| EncodingError::UnrepresentableInput)?
        }
        CanonicalToolResultOutcome::Succeeded(CanonicalToolOutput::Text(t)) => t.clone(),
        CanonicalToolResultOutcome::DomainFailed(err) => serde_json::to_string(&json!({
            "error": { "code": err.code, "message": err.message, "data": err.data },
        }))
        .map_err(|_| EncodingError::UnrepresentableInput)?,
    };
    // Preserve provider tool call id exactly.
    Ok(json!({
        "role": "tool",
        "tool_call_id": result.provider_tool_call_id,
        "content": content,
    }))
}

fn join_text(parts: &[monoloop_contracts::TextPart]) -> String {
    parts.iter().map(|p| p.text()).collect::<Vec<_>>().join("")
}

#[cfg(test)]
mod tests {
    use super::*;
    use monoloop_contracts::merge_effective_config;
    use monoloop_contracts::{
        user_text_input, CanonicalInput, ChannelDefaults, EffectiveConfig, ExchangeId,
        InvocationConfig, JsonSchema, ToolCancellationPolicy, ToolId, ToolLimits, ToolName,
        ToolOutputContract, ToolSuccessContract, TransactionId,
    };
    use monoloop_contracts::{ExtensionLimits, OptionPolicy};

    fn effective(model: &str) -> EffectiveConfig {
        let inv = InvocationConfig {
            model: Some(model.into()),
            temperature: Some(0.2),
            max_output_tokens: Some(128),
            ..Default::default()
        };
        merge_effective_config(
            &ChannelDefaults::default(),
            None,
            None,
            &inv,
            &OptionPolicy {
                supported_invocation: [
                    monoloop_contracts::ConfigOption::Model,
                    monoloop_contracts::ConfigOption::Temperature,
                    monoloop_contracts::ConfigOption::MaxOutputTokens,
                    monoloop_contracts::ConfigOption::ContinuationPolicy,
                ]
                .into_iter()
                .collect(),
                ..Default::default()
            },
            &ExtensionLimits::default(),
        )
        .unwrap()
    }

    #[test]
    fn initial_encode_golden_shape() {
        let enc = OpenAiChatCompletionsEncoder::default();
        let input = user_text_input("Hello").unwrap();
        let tid = TransactionId::generate();
        let eid = ExchangeId::generate();
        let cfg = effective("gpt-test");
        let encoded = enc
            .encode_initial(InitialEncodeRequest {
                transaction_id: &tid,
                exchange_id: &eid,
                input: &input,
                config: &cfg,
                tools: &[],
            })
            .unwrap();
        assert_eq!(
            encoded.required_input_dialect.family,
            monoloop_contracts::DialectFamily::OpenAiChatCompletions
        );
        assert_eq!(encoded.input_policy, ExchangeInputPolicy::SendAndFinish);
        let v: Value = serde_json::from_slice(&encoded.bytes).unwrap();
        assert_eq!(v["model"], "gpt-test");
        assert_eq!(v["stream"], true);
        assert_eq!(v["messages"][0]["role"], "user");
        assert_eq!(v["messages"][0]["content"], "Hello");
        assert_eq!(v["max_tokens"], 128);
        assert!((v["temperature"].as_f64().unwrap() - 0.2).abs() < 1e-5);
    }

    #[test]
    fn tools_and_max_completion_tokens() {
        let enc = OpenAiChatCompletionsEncoder::new(OpenAiEncoderOptions {
            use_max_completion_tokens: true,
            ..Default::default()
        });
        let schema = JsonSchema::try_new(json!({"type": "object"})).unwrap();
        let tool = ToolSpec::try_new(
            ToolId::try_new("search").unwrap(),
            ToolName::try_new("search").unwrap(),
            "Search",
            schema.clone(),
            ToolOutputContract {
                success: ToolSuccessContract::json(schema),
                error_data_schema: None,
            },
            ToolLimits::default(),
            ToolCancellationPolicy::Abortable,
        )
        .unwrap();
        let input = user_text_input("q").unwrap();
        let tid = TransactionId::generate();
        let eid = ExchangeId::generate();
        let cfg = effective("m");
        let encoded = enc
            .encode_initial(InitialEncodeRequest {
                transaction_id: &tid,
                exchange_id: &eid,
                input: &input,
                config: &cfg,
                tools: &[tool],
            })
            .unwrap();
        let v: Value = serde_json::from_slice(&encoded.bytes).unwrap();
        assert!(v.get("max_tokens").is_none());
        assert_eq!(v["max_completion_tokens"], 128);
        assert_eq!(v["tools"][0]["function"]["name"], "search");
    }

    #[test]
    fn reasoning_effort_rejected_without_capability() {
        let enc = OpenAiChatCompletionsEncoder::default();
        let inv = InvocationConfig {
            model: Some("m".into()),
            reasoning_effort: Some(ReasoningEffort::High),
            ..Default::default()
        };
        let cfg = merge_effective_config(
            &ChannelDefaults::default(),
            None,
            None,
            &inv,
            &OptionPolicy {
                supported_invocation: [
                    monoloop_contracts::ConfigOption::Model,
                    monoloop_contracts::ConfigOption::ReasoningEffort,
                    monoloop_contracts::ConfigOption::ContinuationPolicy,
                ]
                .into_iter()
                .collect(),
                ..Default::default()
            },
            &ExtensionLimits::default(),
        )
        .unwrap();
        let input = user_text_input("x").unwrap();
        let tid = TransactionId::generate();
        let eid = ExchangeId::generate();
        let err = enc
            .encode_initial(InitialEncodeRequest {
                transaction_id: &tid,
                exchange_id: &eid,
                input: &input,
                config: &cfg,
                tools: &[],
            })
            .unwrap_err();
        assert!(matches!(
            err,
            EncodingError::Unsupported("reasoning_effort")
        ));
    }

    #[test]
    fn missing_model_invalid() {
        let enc = OpenAiChatCompletionsEncoder::default();
        let input = user_text_input("x").unwrap();
        let tid = TransactionId::generate();
        let eid = ExchangeId::generate();
        let cfg = EffectiveConfig {
            model: None,
            ..effective("unused")
        };
        // override model to None
        let mut cfg = cfg;
        cfg.model = None;
        let err = enc
            .encode_initial(InitialEncodeRequest {
                transaction_id: &tid,
                exchange_id: &eid,
                input: &input,
                config: &cfg,
                tools: &[],
            })
            .unwrap_err();
        assert!(matches!(err, EncodingError::InvalidConfiguration));
        let _ = CanonicalInput::try_new(vec![], &Default::default());
    }

    #[test]
    fn openai_seed_extension_round_trip() {
        use monoloop_contracts::{ExtensionKey, VersionedExtension};
        let enc = OpenAiChatCompletionsEncoder::default();
        let input = user_text_input("Hi").unwrap();
        let tid = TransactionId::generate();
        let eid = ExchangeId::generate();
        let mut cfg = effective("gpt-test");
        let key = ExtensionKey::try_new("openai.seed", 64).unwrap();
        cfg.extensions.insert(
            key,
            VersionedExtension {
                version: 1,
                value: serde_json::json!(42),
            },
        );
        let encoded = enc
            .encode_initial(InitialEncodeRequest {
                transaction_id: &tid,
                exchange_id: &eid,
                input: &input,
                config: &cfg,
                tools: &[],
            })
            .unwrap();
        let v: Value = serde_json::from_slice(&encoded.bytes).unwrap();
        assert_eq!(v["seed"], 42);
    }

    #[test]
    fn unknown_openai_extension_fails_encode() {
        use monoloop_contracts::{ExtensionKey, VersionedExtension};
        let enc = OpenAiChatCompletionsEncoder::default();
        let input = user_text_input("Hi").unwrap();
        let tid = TransactionId::generate();
        let eid = ExchangeId::generate();
        let mut cfg = effective("gpt-test");
        let key = ExtensionKey::try_new("openai.not_a_real_field", 64).unwrap();
        cfg.extensions.insert(
            key,
            VersionedExtension {
                version: 1,
                value: serde_json::json!(1),
            },
        );
        let err = enc
            .encode_initial(InitialEncodeRequest {
                transaction_id: &tid,
                exchange_id: &eid,
                input: &input,
                config: &cfg,
                tools: &[],
            })
            .unwrap_err();
        assert!(matches!(err, EncodingError::Unsupported(_)));
    }

    #[test]
    fn non_openai_extension_fails_encode() {
        use monoloop_contracts::{ExtensionKey, VersionedExtension};
        let enc = OpenAiChatCompletionsEncoder::default();
        let input = user_text_input("Hi").unwrap();
        let tid = TransactionId::generate();
        let eid = ExchangeId::generate();
        let mut cfg = effective("gpt-test");
        let key = ExtensionKey::try_new("other.vendor", 64).unwrap();
        cfg.extensions.insert(
            key,
            VersionedExtension {
                version: 1,
                value: serde_json::json!(true),
            },
        );
        assert!(enc
            .encode_initial(InitialEncodeRequest {
                transaction_id: &tid,
                exchange_id: &eid,
                input: &input,
                config: &cfg,
                tools: &[],
            })
            .is_err());
    }
}
