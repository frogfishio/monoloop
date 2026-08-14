//! Grok Build Network Connector Profile.
//!
//! One authenticated Grok Build server, many logical sessions correlated by
//! Grok's authoritative `sessionId`. See `doc/GROK_BUILD_CONNECTOR.md`.
//!
//! This crate moves ACP/JSON-RPC envelopes and routing state only. It does not
//! interpret agent messages, thoughts, plans, or tool semantics.

#![deny(missing_docs)]

mod config;
mod error;
mod jsonrpc;
mod secret;
mod server;
mod session;

pub use config::{
    GrokConnectorLimits, GrokServerConfig, GrokSessionConfig, GrokSessionLoadConfig,
};
pub use error::GrokConnectorError;
pub use secret::{EnvSecretResolver, InMemorySecretResolver, SecretRef, SecretResolver};
pub use server::{
    GrokConnector, GrokServerCompletion, GrokServerControl, GrokServerHandle, GrokServerHealth,
    PendingGrokServer,
};
pub use session::{
    EncodedAcpSessionMessage, GrokSessionControl, GrokSessionFactory, GrokSessionHandle,
    GrokSessionHealth, GrokSessionInput, PendingGrokExchange, PendingGrokSession,
};

pub use monoloop_contracts::{DialectBinding, GrokSessionId};
pub use monoloop_connector::{
    CancellationReason, ConnectionEnd, ConnectionEndKind, ConnectionId, Connector,
    ConnectorDescriptor, ControlDisposition, OpenConnection, OpenedRawConnection,
    PendingRawConnection, RawOutputHandle, TerminationReason,
};
