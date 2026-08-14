//! Immutable connector descriptors.

/// Transport integration kind (not model intelligence).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConnectorKind {
    /// Deterministic in-memory fake for tests.
    Fake,
    /// LLM HTTP body transport.
    LlmHttp,
    /// Grok Build authenticated WebSocket ACP.
    GrokBuild,
    /// Cursor process/socket ACP.
    Cursor,
    /// Extension point.
    Other(String),
}

/// What bytes the raw boundary exposes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RawBoundary {
    /// In-process channel payload.
    InProcess,
    /// HTTP response body after TLS/headers.
    HttpBody,
    /// WebSocket message payloads.
    WebSocketMessage,
    /// Process stdout/stderr stream.
    ProcessPipe,
    /// Socket payload.
    Socket,
}

/// Control capabilities of a connector implementation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControlCapabilities {
    /// Cooperative cancel is supported.
    pub cancel: bool,
    /// Forced terminate is supported.
    pub terminate: bool,
    /// Input half-close via finish is supported.
    pub input_finish: bool,
}

impl Default for ControlCapabilities {
    fn default() -> Self {
        Self {
            cancel: true,
            terminate: true,
            input_finish: true,
        }
    }
}

/// Immutable connector implementation descriptor.
#[derive(Clone, Debug)]
pub struct ConnectorDescriptor {
    /// Integration kind.
    pub connector_kind: ConnectorKind,
    /// Implementation id (stable string).
    pub implementation_id: String,
    /// Implementation version.
    pub implementation_version: String,
    /// Transport kind label.
    pub transport_kind: String,
    /// Supported dialect families (descriptive).
    pub supported_dialects: Vec<String>,
    /// Raw byte boundary.
    pub raw_boundary: RawBoundary,
    /// Control capabilities.
    pub control_capabilities: ControlCapabilities,
}

impl ConnectorDescriptor {
    /// Descriptor for the in-memory fake connector.
    pub fn fake() -> Self {
        Self {
            connector_kind: ConnectorKind::Fake,
            implementation_id: "monoloop.fake".into(),
            implementation_version: env!("CARGO_PKG_VERSION").into(),
            transport_kind: "in_process".into(),
            supported_dialects: vec!["test/raw".into()],
            raw_boundary: RawBoundary::InProcess,
            control_capabilities: ControlCapabilities::default(),
        }
    }

    /// Descriptor for the Grok Build network connector.
    pub fn grok_build() -> Self {
        Self {
            connector_kind: ConnectorKind::GrokBuild,
            implementation_id: "monoloop.grok_build".into(),
            implementation_version: env!("CARGO_PKG_VERSION").into(),
            transport_kind: "websocket".into(),
            supported_dialects: vec!["acp/json_rpc".into()],
            raw_boundary: RawBoundary::WebSocketMessage,
            control_capabilities: ControlCapabilities::default(),
        }
    }
}
