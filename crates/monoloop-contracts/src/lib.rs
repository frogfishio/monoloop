//! Shared contracts for Monoloop product components.
//!
//! Product components share identities, dialect descriptors, and closed error
//! families. This crate intentionally contains no transport, dialect decoder,
//! tool execution, UI, or host-agent logic.

#![deny(missing_docs)]

mod dialect;
mod error;
mod id;
mod limits;

pub use dialect::{DialectBinding, DialectDescriptor, DialectFamily, DialectNegotiation};
pub use error::{ConnectorError, ConnectorErrorKind};
pub use id::{
    ConnectionId, ExternalSessionId, GrokSessionId, MonoloopRunId, RequestId,
};
pub use limits::{ConnectorLimits, TransportBufferLimits};

pub use bytes::Bytes;
