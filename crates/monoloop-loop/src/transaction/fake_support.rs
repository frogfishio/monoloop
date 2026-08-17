//! Test-only encoder stub used by runtime startup tests.

use monoloop_contracts::{
    Bytes, DialectDescriptor, EncodedExchange, EncodingError, ExchangeInputPolicy,
    InitialEncodeRequest, OutboundDialectEncoder, ToolContinuationEncodeRequest,
};

/// Encoder that rejects all encode calls (startup does not encode).
#[derive(Debug, Default)]
pub struct RejectEncoder;

impl OutboundDialectEncoder for RejectEncoder {
    fn encode_initial(
        &self,
        _request: InitialEncodeRequest<'_>,
    ) -> Result<EncodedExchange, EncodingError> {
        Err(EncodingError::Unsupported("WP-03 reject encoder"))
    }

    fn encode_tool_continuation(
        &self,
        _request: ToolContinuationEncodeRequest<'_>,
    ) -> Result<EncodedExchange, EncodingError> {
        Err(EncodingError::Unsupported("WP-03 reject encoder"))
    }
}

/// Encoder returning empty bytes for dialect smoke tests.
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
