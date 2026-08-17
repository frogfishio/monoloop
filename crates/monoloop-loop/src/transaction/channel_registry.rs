//! Immutable Channel registry and live bindings.

use monoloop_connector::ConnectorFactory;
use monoloop_contracts::{
    ChannelCapabilities, ChannelDefaults, ChannelDescriptor, ChannelId, ChannelKind, ChannelLimits,
    OutboundDialectEncoder, ToolExecutionMode,
};
use std::collections::HashMap;
use std::sync::Arc;

/// One Channel's static binding (factories realized at runtime start).
pub struct ChannelBinding {
    /// Channel identity.
    pub id: ChannelId,
    /// External agent vs direct LLM.
    pub kind: ChannelKind,
    /// Tool execution mode.
    pub tool_mode: ToolExecutionMode,
    /// Matched Connector factory (one instance per Channel at start).
    pub connector_factory: Arc<dyn ConnectorFactory>,
    /// Outbound dialect encoder.
    pub encoder: Arc<dyn OutboundDialectEncoder>,
    /// Channel defaults for effective config merge.
    pub defaults: ChannelDefaults,
    /// Declared capabilities.
    pub capabilities: ChannelCapabilities,
    /// Per-Channel limits.
    pub limits: ChannelLimits,
}

impl ChannelBinding {
    /// View as a data-only descriptor for capability validation.
    pub fn descriptor(&self) -> ChannelDescriptor {
        ChannelDescriptor {
            kind: self.kind,
            tool_mode: self.tool_mode,
            capabilities: self.capabilities.clone(),
            limits: self.limits.clone(),
        }
    }
}

/// Immutable registry of Channel bindings (built before start).
pub struct ChannelRegistry {
    channels: HashMap<ChannelId, ChannelBinding>,
}

impl ChannelRegistry {
    /// Build from bindings; rejects duplicate IDs.
    pub fn build(bindings: Vec<ChannelBinding>) -> Result<Self, super::StartupError> {
        if bindings.is_empty() {
            return Err(super::StartupError::ChannelRegistry(
                "at least one Channel is required",
            ));
        }
        let mut channels = HashMap::with_capacity(bindings.len());
        for b in bindings {
            b.descriptor().validate()?;
            if channels.contains_key(&b.id) {
                return Err(super::StartupError::ChannelRegistry("duplicate ChannelId"));
            }
            // Dialect on encoder path is checked when encoding (WP-08); startup
            // requires input/output descriptors already match via ChannelDescriptor.
            channels.insert(b.id.clone(), b);
        }
        Ok(Self { channels })
    }

    /// Iterate bindings.
    pub fn iter(&self) -> impl Iterator<Item = (&ChannelId, &ChannelBinding)> {
        self.channels.iter()
    }

    /// Lookup by id.
    pub fn get(&self, id: &ChannelId) -> Option<&ChannelBinding> {
        self.channels.get(id)
    }

    /// Number of Channels.
    pub fn len(&self) -> usize {
        self.channels.len()
    }

    /// Whether empty (should not occur after successful build).
    pub fn is_empty(&self) -> bool {
        self.channels.is_empty()
    }
}
