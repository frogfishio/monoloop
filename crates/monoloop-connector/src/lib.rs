//! Component 01 — abstract Connector, fake transport, and connector proxy.
//!
//! See `doc/CONNECTOR.md`. This crate does not interpret semantic model content.

#![deny(missing_docs)]

mod control;
mod descriptor;
mod fake;
mod handles;
mod open;
mod proxy;
mod traits;

pub use control::{
    CancellationReason, ConnectionControlHandle, ControlDisposition, ControlState,
    TerminationReason,
};
pub use descriptor::{ConnectorDescriptor, ConnectorKind, ControlCapabilities, RawBoundary};
pub use fake::{FakeConnector, FakeConnectorConfig, FakeEndpoint};
pub use handles::{
    ConnectionCompletionHandle, ConnectionEnd, ConnectionEndKind, ConnectionOwner, EndInitiator,
    RawInputHandle, RawInputMessage, RawOutputHandle,
};
pub use open::{OpenConnection, OpenedRawConnection, PendingRawConnection};
pub use proxy::{ConnectorProxy, ConnectorProxyBuilder, ProxyRoute};
pub use traits::Connector;

pub use monoloop_contracts::{
    Bytes, ConnectionId, ConnectorError, ConnectorErrorKind, ConnectorLimits, DialectBinding,
    DialectDescriptor, DialectFamily, DialectNegotiation, ExternalSessionId, TransportBufferLimits,
};
