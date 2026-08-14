//! Minimal JSON-RPC 2.0 helpers for ACP (no semantic interpretation).

use crate::error::GrokConnectorError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// JSON-RPC request id (distinct from Grok sessionId).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RpcId {
    /// Numeric id.
    Number(u64),
    /// String id.
    String(String),
}

impl RpcId {
    /// Next numeric id.
    pub fn number(n: u64) -> Self {
        Self::Number(n)
    }
}

/// Outbound JSON-RPC request.
#[derive(Clone, Debug, Serialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: &'static str,
    pub id: RpcId,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

impl JsonRpcRequest {
    /// Build a request.
    pub fn new(id: RpcId, method: impl Into<String>, params: Option<Value>) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            method: method.into(),
            params,
        }
    }

    /// Serialize to bytes with size bound.
    pub fn to_bytes(&self, max_bytes: usize) -> Result<bytes::Bytes, GrokConnectorError> {
        let raw = serde_json::to_vec(self).map_err(|e| {
            GrokConnectorError::protocol(format!("serialize request failed: {e}"))
        })?;
        if raw.len() > max_bytes {
            return Err(GrokConnectorError::resource("json-rpc request exceeds max_message_bytes"));
        }
        Ok(bytes::Bytes::from(raw))
    }
}

/// Inbound JSON-RPC message (response, notification, or server request).
#[derive(Clone, Debug, Deserialize)]
pub struct JsonRpcMessage {
    #[serde(default)]
    #[allow(dead_code)]
    pub jsonrpc: Option<String>,
    #[serde(default)]
    pub id: Option<RpcId>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub params: Option<Value>,
    #[serde(default)]
    pub result: Option<Value>,
    #[serde(default)]
    pub error: Option<JsonRpcErrorObject>,
}

/// JSON-RPC error object (safe fields only retained).
#[derive(Clone, Debug, Deserialize)]
pub struct JsonRpcErrorObject {
    pub code: i64,
    pub message: String,
}

impl JsonRpcMessage {
    /// Parse from bytes with size bound already enforced by caller.
    pub fn parse(bytes: &[u8]) -> Result<Self, GrokConnectorError> {
        serde_json::from_slice(bytes)
            .map_err(|e| GrokConnectorError::protocol(format!("invalid json-rpc frame: {e}")))
    }

    /// True if this is a response (has id and result or error, no method).
    pub fn is_response(&self) -> bool {
        self.id.is_some() && self.method.is_none()
    }

    /// True if this is a notification or server request (has method).
    pub fn is_notification_or_request(&self) -> bool {
        self.method.is_some()
    }
}

/// Extract sessionId from notification/request params when present.
pub fn session_id_from_params(params: &Option<Value>) -> Option<String> {
    let params = params.as_ref()?;
    params
        .get("sessionId")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or_else(|| {
            // Some agents nest under update/session
            params
                .pointer("/sessionId")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
}
