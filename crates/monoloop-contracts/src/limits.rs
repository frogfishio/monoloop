//! Bounded transport and interpretation limits.

use std::time::Duration;

/// Input/output buffer bounds for one connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransportBufferLimits {
    /// Maximum queued input bytes awaiting send.
    pub max_queued_input_bytes: usize,
    /// Maximum queued output bytes awaiting receive.
    pub max_queued_output_bytes: usize,
    /// Maximum individual chunk accepted from the caller.
    pub max_chunk_bytes: usize,
}

impl Default for TransportBufferLimits {
    fn default() -> Self {
        Self {
            max_queued_input_bytes: 1024 * 1024,
            max_queued_output_bytes: 1024 * 1024,
            max_chunk_bytes: 256 * 1024,
        }
    }
}

/// Connector-level limits applied at open.
#[derive(Clone, Debug)]
pub struct ConnectorLimits {
    /// Connect / open deadline.
    pub connect_deadline: Duration,
    /// Buffer bounds.
    pub buffers: TransportBufferLimits,
    /// Cancellation grace before forced terminate (caller policy may override).
    pub cancel_grace: Duration,
    /// Cleanup deadline after terminal selection.
    pub cleanup_deadline: Duration,
}

impl Default for ConnectorLimits {
    fn default() -> Self {
        Self {
            connect_deadline: Duration::from_secs(30),
            buffers: TransportBufferLimits::default(),
            cancel_grace: Duration::from_secs(5),
            cleanup_deadline: Duration::from_secs(10),
        }
    }
}

/// Interpretation assembly and output bounds.
#[derive(Clone, Debug)]
pub struct InterpretationLimits {
    /// Maximum undecoded/raw buffer bytes.
    pub max_undecoded_bytes: usize,
    /// Maximum dialect frame bytes.
    pub max_frame_bytes: usize,
    /// Maximum sentence assembly buffer.
    pub max_sentence_assembly_bytes: usize,
    /// Maximum structural atom bytes.
    pub max_structural_atom_bytes: usize,
    /// Maximum pending tool actions.
    pub max_pending_tool_actions: usize,
    /// Maximum bytes per pending tool action.
    pub max_bytes_per_tool_action: usize,
    /// Maximum canonical output queue items.
    pub max_output_queue_items: usize,
    /// Maximum safe diagnostics retained.
    pub max_safe_diagnostics: usize,
}

impl Default for InterpretationLimits {
    fn default() -> Self {
        Self {
            max_undecoded_bytes: 4 * 1024 * 1024,
            max_frame_bytes: 4 * 1024 * 1024,
            max_sentence_assembly_bytes: 256 * 1024,
            max_structural_atom_bytes: 512 * 1024,
            max_pending_tool_actions: 256,
            max_bytes_per_tool_action: 256 * 1024,
            max_output_queue_items: 4096,
            max_safe_diagnostics: 64,
        }
    }
}
