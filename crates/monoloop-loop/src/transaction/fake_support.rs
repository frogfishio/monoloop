//! Test encoders used by startup and exchange tests.

use monoloop_contracts::{
    Bytes, CanonicalMessage, DialectDescriptor, EncodedExchange, EncodingError, ExchangeInputPolicy,
    InitialEncodeRequest, OutboundDialectEncoder, ToolContinuationEncodeRequest,
};

/// Encoder that rejects all encode calls.
#[derive(Debug, Default)]
pub struct RejectEncoder;

impl OutboundDialectEncoder for RejectEncoder {
    fn encode_initial(
        &self,
        _request: InitialEncodeRequest<'_>,
    ) -> Result<EncodedExchange, EncodingError> {
        Err(EncodingError::Unsupported("reject encoder"))
    }

    fn encode_tool_continuation(
        &self,
        _request: ToolContinuationEncodeRequest<'_>,
    ) -> Result<EncodedExchange, EncodingError> {
        Err(EncodingError::Unsupported("reject encoder"))
    }
}

/// Encoder returning empty bytes.
#[derive(Debug)]
pub struct EmptyBytesEncoder {
    /// Dialect stamped on the encoded exchange.
    pub dialect: DialectDescriptor,
}

impl EmptyBytesEncoder {
    /// Construct with an explicit dialect stamp.
    pub fn new(dialect: DialectDescriptor) -> Self {
        Self { dialect }
    }
}

impl OutboundDialectEncoder for EmptyBytesEncoder {
    fn encode_initial(
        &self,
        _request: InitialEncodeRequest<'_>,
    ) -> Result<EncodedExchange, EncodingError> {
        Ok(EncodedExchange {
            bytes: Bytes::new(),
            required_input_dialect: self.dialect.clone(),
            input_policy: ExchangeInputPolicy::SendAndFinish,
        })
    }

    fn encode_tool_continuation(
        &self,
        _request: ToolContinuationEncodeRequest<'_>,
    ) -> Result<EncodedExchange, EncodingError> {
        Ok(EncodedExchange {
            bytes: Bytes::new(),
            required_input_dialect: self.dialect.clone(),
            input_policy: ExchangeInputPolicy::SendAndFinish,
        })
    }
}

/// Encodes canonical text messages as UTF-8 for the Test dialect (FakeConnector echo).
///
/// Joins text parts and ensures a trailing sentence terminator so the segmenter emits.
#[derive(Debug, Default)]
pub struct TestTextEncoder;

impl OutboundDialectEncoder for TestTextEncoder {
    fn encode_initial(
        &self,
        request: InitialEncodeRequest<'_>,
    ) -> Result<EncodedExchange, EncodingError> {
        let mut text = String::new();
        for msg in request.input.messages() {
            match msg {
                CanonicalMessage::System { content, .. }
                | CanonicalMessage::User { content, .. }
                | CanonicalMessage::Tool { content, .. } => {
                    for part in content {
                        text.push_str(part.text());
                        text.push(' ');
                    }
                }
                CanonicalMessage::Assistant { content, .. } => {
                    for part in content {
                        text.push_str(part.text());
                        text.push(' ');
                    }
                }
            }
        }
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Err(EncodingError::UnrepresentableInput);
        }
        let mut body = trimmed.to_string();
        if !body.ends_with('.') && !body.ends_with('!') && !body.ends_with('?') {
            body.push('.');
        }
        body.push(' ');
        Ok(EncodedExchange {
            bytes: Bytes::from(body.into_bytes()),
            required_input_dialect: DialectDescriptor::test_raw(),
            input_policy: ExchangeInputPolicy::SendAndFinish,
        })
    }

    fn encode_tool_continuation(
        &self,
        _request: ToolContinuationEncodeRequest<'_>,
    ) -> Result<EncodedExchange, EncodingError> {
        Err(EncodingError::Unsupported(
            "TestTextEncoder has no tool continuation",
        ))
    }
}
