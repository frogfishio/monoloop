//! Bounded transport and connector limits.

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
