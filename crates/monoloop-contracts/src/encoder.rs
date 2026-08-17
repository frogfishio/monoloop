//! Outbound dialect encoder contracts (provider-neutral request/result types).

use crate::config::EffectiveConfig;
use crate::dialect::DialectDescriptor;
use crate::id::{ExchangeId, TransactionId};
use crate::input::CanonicalInput;
use crate::input::CanonicalMessage;
use crate::tool::{CanonicalToolResult, ToolSpec};
use bytes::Bytes;
use thiserror::Error;

/// Whether the encoded exchange finishes the connection write path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExchangeInputPolicy {
    /// Send body and finish the request (HTTP request/response).
    SendAndFinish,
    /// Send while retaining the bidirectional session.
    SendAndRetain,
}

/// Encoded provider exchange body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncodedExchange {
    /// Bounded dialect bytes.
    pub bytes: Bytes,
    /// Dialect the Connector must accept for this write.
    pub required_input_dialect: DialectDescriptor,
    /// Send-and-finish vs retain.
    pub input_policy: ExchangeInputPolicy,
}

/// Initial outbound encode request (no live handler objects).
#[derive(Clone, Debug)]
pub struct InitialEncodeRequest<'a> {
    /// Transaction id.
    pub transaction_id: &'a TransactionId,
    /// Exchange id.
    pub exchange_id: &'a ExchangeId,
    /// Canonical caller input.
    pub input: &'a CanonicalInput,
    /// Effective configuration.
    pub config: &'a EffectiveConfig,
    /// Ordered tool specs for this transaction.
    pub tools: &'a [ToolSpec],
}

/// Immutable continuation context: original input plus required tool turns.
#[derive(Clone, Debug, PartialEq)]
pub struct ContinuationContext {
    messages: Vec<CanonicalMessage>,
}

impl ContinuationContext {
    /// Construct from a mechanical message sequence (caller/runtime-built).
    pub fn try_new(messages: Vec<CanonicalMessage>) -> Result<Self, EncodingError> {
        if messages.is_empty() {
            return Err(EncodingError::EmptyContinuationContext);
        }
        Ok(Self { messages })
    }

    /// Borrow messages.
    pub fn messages(&self) -> &[CanonicalMessage] {
        &self.messages
    }
}

/// Tool-result continuation encode request.
#[derive(Clone, Debug)]
pub struct ToolContinuationEncodeRequest<'a> {
    /// Transaction id.
    pub transaction_id: &'a TransactionId,
    /// Exchange id.
    pub exchange_id: &'a ExchangeId,
    /// Continuation context.
    pub context: &'a ContinuationContext,
    /// Canonical tool results for this continuation.
    pub results: &'a [CanonicalToolResult],
    /// Effective configuration.
    pub config: &'a EffectiveConfig,
    /// Ordered tool specs.
    pub tools: &'a [ToolSpec],
}

/// Outbound dialect encoder port (implemented in monoloop-loop adapters).
pub trait OutboundDialectEncoder: Send + Sync {
    /// Encode the first provider exchange for a transaction.
    fn encode_initial(
        &self,
        request: InitialEncodeRequest<'_>,
    ) -> Result<EncodedExchange, EncodingError>;

    /// Encode a tool-result continuation exchange.
    fn encode_tool_continuation(
        &self,
        request: ToolContinuationEncodeRequest<'_>,
    ) -> Result<EncodedExchange, EncodingError>;
}

/// Encoding failure (maps to `EncodingFailed` terminal).
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum EncodingError {
    /// Empty continuation context.
    #[error("continuation context must be non-empty")]
    EmptyContinuationContext,
    /// Unsupported dialect or option.
    #[error("unsupported encode option: {0}")]
    Unsupported(&'static str),
    /// Output would exceed Channel/runtime byte bound.
    #[error("encoded exchange exceeds bound")]
    LimitExceeded,
    /// Invalid configuration for this dialect.
    #[error("invalid configuration for encoder")]
    InvalidConfiguration,
    /// Input cannot be represented in this dialect.
    #[error("input not representable in dialect")]
    UnrepresentableInput,
}
