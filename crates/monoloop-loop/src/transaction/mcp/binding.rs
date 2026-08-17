//! Capability tokens and transaction MCP route table.

use super::handler::TransactionMcpHandler;
use crate::transaction::dispatcher::TransactionToolDispatcher;
use crate::transaction::resolved_tools::ResolvedToolSet;
use monoloop_connector::McpServerDescriptor;
use monoloop_contracts::{ExchangeId, TransactionId};
use rand::TryRngCore;
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

const STATE_PENDING: u8 = 0;
const STATE_ACTIVE: u8 = 1;
const STATE_REVOKED: u8 = 2;

/// 256-bit unguessable capability token (hex in URLs; redacted in diagnostics).
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CapabilityToken {
    bytes: [u8; 32],
}

impl CapabilityToken {
    /// Generate via OS CSPRNG.
    pub fn generate() -> Result<Self, McpInstallError> {
        let mut bytes = [0u8; 32];
        rand::rngs::OsRng
            .try_fill_bytes(&mut bytes)
            .map_err(|_| McpInstallError::EntropyUnavailable)?;
        if bytes == [0u8; 32] {
            return Err(McpInstallError::EntropyUnavailable);
        }
        Ok(Self { bytes })
    }

    /// Parse a 64-char lowercase hex token (URL segment).
    pub fn from_hex(s: &str) -> Option<Self> {
        if s.len() != 64 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        let mut bytes = [0u8; 32];
        let chars = s.as_bytes();
        for (i, slot) in bytes.iter_mut().enumerate() {
            let hi = hex_nibble(chars[i * 2])?;
            let lo = hex_nibble(chars[i * 2 + 1])?;
            *slot = (hi << 4) | lo;
        }
        Some(Self { bytes })
    }

    /// Lowercase hex (64 chars) for URL path segments only.
    pub fn to_hex(&self) -> String {
        let mut out = String::with_capacity(64);
        for b in &self.bytes {
            out.push(hex_char(b >> 4));
            out.push(hex_char(b & 0x0f));
        }
        out
    }
}

impl fmt::Debug for CapabilityToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CapabilityToken(<redacted>)")
    }
}

fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn hex_char(n: u8) -> char {
    match n {
        0..=9 => (b'0' + n) as char,
        10..=15 => (b'a' + n - 10) as char,
        _ => '0',
    }
}

/// Public lifecycle state of a capability route.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum McpBindingState {
    /// Installed but not yet activated (SessionKey not claimed / refresh incomplete).
    Pending,
    /// Ready for tools/list and tools/call.
    Active,
    /// Revoked; route removed or rejects all traffic.
    Revoked,
}

/// Errors installing or mutating MCP routes.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum McpInstallError {
    /// OS CSPRNG failed.
    #[error("capability entropy unavailable")]
    EntropyUnavailable,
    /// Unknown token.
    #[error("unknown MCP capability")]
    UnknownCapability,
    /// Route not in expected state.
    #[error("MCP capability state conflict")]
    StateConflict,
    /// Route table capacity exceeded.
    #[error("MCP route table full")]
    CapacityExceeded,
    /// Invalid descriptor construction.
    #[error("invalid MCP descriptor")]
    InvalidDescriptor,
}

/// One transaction MCP binding (pending or active).
pub struct McpBinding {
    /// Capability token.
    pub token: CapabilityToken,
    /// Owning transaction.
    pub transaction_id: TransactionId,
    /// Shared state flag.
    state: Arc<AtomicU8>,
    /// Handler for this binding.
    pub handler: TransactionMcpHandler,
    /// Dispatcher (same instance as model path when both used).
    pub dispatcher: Arc<TransactionToolDispatcher>,
    /// Resolved tools projection.
    pub tools: ResolvedToolSet,
}

impl McpBinding {
    /// Current lifecycle state.
    pub fn state(&self) -> McpBindingState {
        match self.state.load(Ordering::SeqCst) {
            STATE_PENDING => McpBindingState::Pending,
            STATE_ACTIVE => McpBindingState::Active,
            _ => McpBindingState::Revoked,
        }
    }

    /// Whether tools/list and tools/call are allowed.
    pub fn is_active(&self) -> bool {
        self.state.load(Ordering::SeqCst) == STATE_ACTIVE
    }
}

impl fmt::Debug for McpBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpBinding")
            .field("token", &self.token)
            .field("transaction_id", &self.transaction_id)
            .field("state", &self.state())
            .field("tool_count", &self.tools.len())
            .finish()
    }
}

/// Handle returned when a pending binding is created.
pub struct PendingMcpBinding {
    /// Capability token (for activation/revoke; never log).
    pub token: CapabilityToken,
    /// Redacted descriptor for SessionAdapter install.
    pub descriptor: McpServerDescriptor,
    /// Transaction id.
    pub transaction_id: TransactionId,
}

impl fmt::Debug for PendingMcpBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingMcpBinding")
            .field("token", &self.token)
            .field("descriptor", &self.descriptor)
            .field("transaction_id", &self.transaction_id)
            .finish()
    }
}

/// Bounded in-memory capability route table.
pub struct McpRouteTable {
    max_routes: usize,
    routes: Mutex<HashMap<CapabilityToken, Arc<McpBinding>>>,
}

impl McpRouteTable {
    /// Create with a maximum concurrent route count.
    pub fn new(max_routes: usize) -> Arc<Self> {
        Arc::new(Self {
            max_routes: max_routes.max(1),
            routes: Mutex::new(HashMap::new()),
        })
    }

    /// Number of live (pending or active) routes.
    pub fn len(&self) -> usize {
        self.routes.lock().map(|m| m.len()).unwrap_or(0)
    }

    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Install a pending binding and return the redacted descriptor URL.
    pub fn install_pending(
        self: &Arc<Self>,
        transaction_id: TransactionId,
        tools: ResolvedToolSet,
        dispatcher: Arc<TransactionToolDispatcher>,
        exchange_id: ExchangeId,
        base_url: &str,
    ) -> Result<PendingMcpBinding, McpInstallError> {
        let token = CapabilityToken::generate()?;
        let state = Arc::new(AtomicU8::new(STATE_PENDING));
        let handler = TransactionMcpHandler::new(
            Arc::clone(&state),
            tools.clone(),
            Arc::clone(&dispatcher),
            transaction_id,
            exchange_id,
        );
        let binding = Arc::new(McpBinding {
            token: token.clone(),
            transaction_id,
            state,
            handler,
            dispatcher,
            tools,
        });

        {
            let mut map = self.routes.lock().unwrap_or_else(|e| e.into_inner());
            if map.len() >= self.max_routes {
                return Err(McpInstallError::CapacityExceeded);
            }
            map.insert(token.clone(), binding);
        }

        let url = format!("{}/mcp/{}", base_url.trim_end_matches('/'), token.to_hex());
        let descriptor = McpServerDescriptor::try_new("monoloop", "2024-11-05", url)
            .map_err(|_| McpInstallError::InvalidDescriptor)?;

        Ok(PendingMcpBinding {
            token,
            descriptor,
            transaction_id,
        })
    }

    /// Activate a pending route after SessionKey claim / MCP refresh.
    pub fn activate(&self, token: &CapabilityToken) -> Result<(), McpInstallError> {
        let map = self.routes.lock().unwrap_or_else(|e| e.into_inner());
        let binding = map.get(token).ok_or(McpInstallError::UnknownCapability)?;
        let prev = binding.state.compare_exchange(
            STATE_PENDING,
            STATE_ACTIVE,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        match prev {
            Ok(_) => Ok(()),
            Err(STATE_ACTIVE) => Ok(()), // idempotent activate
            Err(_) => Err(McpInstallError::StateConflict),
        }
    }

    /// Revoke and remove a route. Idempotent for unknown tokens.
    pub fn revoke(&self, token: &CapabilityToken) -> bool {
        let mut map = self.routes.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(binding) = map.remove(token) {
            binding.state.store(STATE_REVOKED, Ordering::SeqCst);
            true
        } else {
            false
        }
    }

    /// Revoke every route (shutdown).
    pub fn revoke_all(&self) {
        let mut map = self.routes.lock().unwrap_or_else(|e| e.into_inner());
        for (_, binding) in map.drain() {
            binding.state.store(STATE_REVOKED, Ordering::SeqCst);
        }
    }

    /// Lookup live binding by token hex (from URL).
    pub fn get_by_hex(&self, hex: &str) -> Option<Arc<McpBinding>> {
        let token = CapabilityToken::from_hex(hex)?;
        self.get(&token)
    }

    /// Lookup by token.
    pub fn get(&self, token: &CapabilityToken) -> Option<Arc<McpBinding>> {
        self.routes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(token)
            .cloned()
    }

    /// State for a token if present.
    pub fn state_of(&self, token: &CapabilityToken) -> Option<McpBindingState> {
        self.get(token).map(|b| b.state())
    }
}
