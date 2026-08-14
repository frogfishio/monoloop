//! Dialect binding reported by a Connector after open/negotiation.

use serde::{Deserialize, Serialize};

/// High-level dialect family (not an encoder/decoder implementation).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DialectFamily {
    /// OpenAI Responses-style SSE.
    OpenAiResponses,
    /// Anthropic Messages SSE.
    AnthropicMessages,
    /// Agent Client Protocol / JSON-RPC.
    Acp,
    /// Cursor ACP profile.
    CursorAcp,
    /// Grok Build ACP/JSONL profile family tag.
    GrokBuild,
    /// Deterministic test dialect.
    Test,
    /// Extension point with bounded name.
    Other(String),
}

/// How the dialect was selected for a connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DialectNegotiation {
    /// Fixed by connector configuration / profile.
    Fixed,
    /// Negotiated during open/handshake and then frozen.
    Negotiated,
}

/// Stable, bounded, versioned dialect descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DialectDescriptor {
    /// Dialect family.
    pub family: DialectFamily,
    /// Version string (e.g. `"v1"`).
    pub version: String,
    /// Framing (e.g. `"sse"`, `"json_rpc"`, `"jsonl"`).
    pub framing: String,
    /// Optional profile qualifier (e.g. `"grok_build"`).
    pub profile: Option<String>,
}

impl DialectDescriptor {
    /// ACP / JSON-RPC dialect used by Grok Build.
    pub fn acp_json_rpc(version: impl Into<String>) -> Self {
        Self {
            family: DialectFamily::Acp,
            version: version.into(),
            framing: "json_rpc".into(),
            profile: Some("grok_build".into()),
        }
    }

    /// Deterministic in-memory test dialect.
    pub fn test_raw() -> Self {
        Self {
            family: DialectFamily::Test,
            version: "v1".into(),
            framing: "raw".into(),
            profile: Some("fake".into()),
        }
    }
}

/// Immutable input/output dialect pair for one opened connection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DialectBinding {
    /// Bytes written to the external system use this dialect.
    pub input: DialectDescriptor,
    /// Bytes read from the external system use this dialect.
    pub output: DialectDescriptor,
    /// Whether dialects were fixed or negotiated.
    pub negotiation: DialectNegotiation,
}

impl DialectBinding {
    /// Fixed identical input/output dialect.
    pub fn fixed(dialect: DialectDescriptor) -> Self {
        Self {
            input: dialect.clone(),
            output: dialect,
            negotiation: DialectNegotiation::Fixed,
        }
    }

    /// Negotiated identical input/output dialect (frozen at open).
    pub fn negotiated(dialect: DialectDescriptor) -> Self {
        Self {
            input: dialect.clone(),
            output: dialect,
            negotiation: DialectNegotiation::Negotiated,
        }
    }
}
