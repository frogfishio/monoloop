//! Dialect binding reported by a Connector after open/negotiation.

use serde::{Deserialize, Serialize};

/// High-level dialect family (not an encoder/decoder implementation).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DialectFamily {
    /// OpenAI Chat Completions v1 (streaming SSE) — first direct-LLM dialect.
    OpenAiChatCompletions,
    /// OpenAI Responses-style SSE (later dialect).
    OpenAiResponses,
    /// Anthropic Messages SSE.
    AnthropicMessages,
    /// Agent Client Protocol / JSON-RPC.
    Acp,
    /// Cursor ACP profile.
    CursorAcp,
    /// Google Antigravity (`agy`) ACP profile (stdio NDJSON / agy-acp bridge).
    AgyAcp,
    /// OpenAI Codex ACP profile (stdio NDJSON / codex-acp adapter).
    CodexAcp,
    /// Z.ai CLI headless profile: OpenAI-compatible chat message NDJSON on stdout.
    ZaiCli,
    /// Claude Code headless profile: `claude -p --output-format stream-json` NDJSON.
    ClaudeCode,
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
    /// OpenAI Chat Completions streaming SSE (direct-LLM).
    pub fn openai_chat_completions(version: impl Into<String>) -> Self {
        Self {
            family: DialectFamily::OpenAiChatCompletions,
            version: version.into(),
            framing: "sse".into(),
            profile: Some("openai_chat_completions".into()),
        }
    }

    /// ACP / JSON-RPC dialect used by Grok Build.
    pub fn acp_json_rpc(version: impl Into<String>) -> Self {
        Self {
            family: DialectFamily::Acp,
            version: version.into(),
            framing: "json_rpc".into(),
            profile: Some("grok_build".into()),
        }
    }

    /// Cursor Agent ACP over stdio (newline-delimited JSON-RPC).
    pub fn cursor_acp(version: impl Into<String>) -> Self {
        Self {
            family: DialectFamily::CursorAcp,
            version: version.into(),
            framing: "ndjson".into(),
            profile: Some("cursor".into()),
        }
    }

    /// Antigravity / agy ACP over stdio (native or `agy-acp` bridge).
    pub fn agy_acp(version: impl Into<String>) -> Self {
        Self {
            family: DialectFamily::AgyAcp,
            version: version.into(),
            framing: "ndjson".into(),
            profile: Some("antigravity".into()),
        }
    }

    /// OpenAI Codex ACP over stdio (`@agentclientprotocol/codex-acp` adapter).
    pub fn codex_acp(version: impl Into<String>) -> Self {
        Self {
            family: DialectFamily::CodexAcp,
            version: version.into(),
            framing: "ndjson".into(),
            profile: Some("codex".into()),
        }
    }

    /// Z.ai CLI headless (`zai -p`): OpenAI-compatible chat messages as NDJSON lines.
    pub fn zai_cli(version: impl Into<String>) -> Self {
        Self {
            family: DialectFamily::ZaiCli,
            version: version.into(),
            framing: "ndjson".into(),
            profile: Some("zai".into()),
        }
    }

    /// Claude Code headless (`claude -p --output-format stream-json --verbose`).
    pub fn claude_code(version: impl Into<String>) -> Self {
        Self {
            family: DialectFamily::ClaudeCode,
            version: version.into(),
            framing: "ndjson".into(),
            profile: Some("claude_code".into()),
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
