//! SPDX-License-Identifier: AGPL-3.0-or-later
//! Copyright (C) Alexander R. Croft
//!
//! Component 01 — abstract Connector, fake transport, and connector proxy.
//!
//! See `doc/CONNECTOR.md`. This crate does not interpret semantic model content.
//! Session factory/ownership seams implement
//! `doc/TRANSACTION_RUNTIME_IMPLEMENTATION.md` §5 (WP-02).

#![deny(missing_docs)]

mod control;
mod credential;
mod descriptor;
mod fake;
mod fake_session;
mod handles;
mod http;
mod instance;
mod open;
mod proxy;
mod session;
mod traits;

pub use control::{
    CancellationReason, ConnectionControlHandle, ControlDisposition, ControlState,
    TerminationReason,
};
pub use credential::{
    AnonymousCredentialResolver, ConnectorTargetResolver, CredentialResolver,
    MapCredentialResolver, ResolvedConnectorTarget, ResolvedCredential,
};
pub use descriptor::{ConnectorDescriptor, ConnectorKind, ControlCapabilities, RawBoundary};
pub use fake::{FakeConnector, FakeConnectorConfig, FakeEndpoint};
pub use fake_session::{
    pending_attach_with_dropped_completion, FakeConnectorFactory, FakeSessionAdapter,
    FakeSessionAdapterConfig, FakeSessionRoute,
};
pub use handles::{
    ConnectionCompletionHandle, ConnectionEnd, ConnectionEndKind, ConnectionOwner, EndInitiator,
    RawInputHandle, RawInputMessage, RawOutputHandle,
};
pub use http::{
    validate_endpoint_url, HttpMethod, StreamingHttpConfig, StreamingHttpConnector,
    StreamingHttpConnectorFactory,
};
pub use instance::{ConnectorBuildError, ConnectorFactory, ConnectorInstance, ConnectorInstanceId};
pub use open::{ConnectionOwnerWork, OpenConnection, OpenedRawConnection, PendingRawConnection};
pub use proxy::{ConnectorProxy, ConnectorProxyBuilder, ProxyRoute};
pub use session::{
    validate_open_attachment_owner, validate_session_id_match, McpServerDescriptor,
    PendingOperationControl, PendingSessionAttachment, PendingSessionConfiguration, SessionAdapter,
    SessionAttachError, SessionAttachRequest, SessionAttachment, SessionAttachmentCompletion,
    SessionConfigurationCompletion, SessionConfigurationError, SessionRoute,
};
pub use traits::Connector;

pub use monoloop_contracts::{
    Bytes, ConnectionId, ConnectorError, ConnectorErrorKind, ConnectorLimits, DialectBinding,
    DialectDescriptor, DialectFamily, DialectNegotiation, ExternalSessionId, TransportBufferLimits,
};
