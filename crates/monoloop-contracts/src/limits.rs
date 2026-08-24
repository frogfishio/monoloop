//! Bounded transport, interpretation, input, tool, and transaction limits.

use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;

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

/// Bounds for validating [`crate::input::CanonicalInput`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InputLimits {
    /// Maximum messages in one input.
    pub max_messages: usize,
    /// Maximum text parts per message.
    pub max_content_parts: usize,
    /// Maximum bytes of one text part.
    pub max_text_part_bytes: usize,
    /// Maximum aggregate UTF-8 bytes of all text parts.
    pub max_aggregate_text_bytes: usize,
    /// Maximum assistant tool calls in one input.
    pub max_tool_calls: usize,
    /// Maximum JSON argument bytes per tool call.
    pub max_tool_argument_bytes: usize,
    /// Maximum JSON nesting depth for tool arguments.
    pub max_json_depth: u32,
    /// Maximum optional `name` field bytes.
    pub max_name_bytes: usize,
    /// Maximum `tool_call_id` bytes.
    pub max_tool_call_id_bytes: usize,
}

impl Default for InputLimits {
    fn default() -> Self {
        Self {
            max_messages: 256,
            max_content_parts: 64,
            max_text_part_bytes: 256 * 1024,
            max_aggregate_text_bytes: 2 * 1024 * 1024,
            max_tool_calls: 64,
            max_tool_argument_bytes: 256 * 1024,
            max_json_depth: 16,
            max_name_bytes: 128,
            max_tool_call_id_bytes: 128,
        }
    }
}

/// Extension map bounds (invocation and session configuration).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExtensionLimits {
    /// Maximum number of extension keys.
    pub max_keys: usize,
    /// Maximum key string bytes.
    pub max_key_bytes: usize,
    /// Maximum nesting depth of extension values.
    pub max_value_depth: u32,
    /// Maximum total serialized JSON bytes of all extensions.
    pub max_serialized_bytes: usize,
}

impl Default for ExtensionLimits {
    fn default() -> Self {
        Self {
            max_keys: 32,
            max_key_bytes: 64,
            max_value_depth: 8,
            max_serialized_bytes: 16 * 1024,
        }
    }
}

/// Per-tool execution bounds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolLimits {
    /// Maximum concurrent executions of this tool.
    pub max_concurrent: usize,
    /// Maximum input payload bytes.
    pub max_input_bytes: usize,
    /// Maximum output payload bytes.
    pub max_output_bytes: usize,
    /// Maximum execution wall time.
    #[serde(with = "duration_secs")]
    pub execution_deadline: Duration,
}

impl Default for ToolLimits {
    fn default() -> Self {
        Self {
            max_concurrent: 8,
            max_input_bytes: 256 * 1024,
            max_output_bytes: 256 * 1024,
            execution_deadline: Duration::from_secs(60),
        }
    }
}

/// Per-Channel capacity and encoding bounds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelLimits {
    /// Maximum concurrent active transactions on this Channel.
    pub max_active_transactions: usize,
    /// Maximum concurrent distinct sessions.
    pub max_distinct_sessions: usize,
    /// Maximum encoded exchange bytes.
    pub max_encoded_exchange_bytes: usize,
}

impl Default for ChannelLimits {
    fn default() -> Self {
        Self {
            max_active_transactions: 64,
            max_distinct_sessions: 64,
            max_encoded_exchange_bytes: 4 * 1024 * 1024,
        }
    }
}

/// Runtime transaction bounds (see implementation §12).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionLimits {
    /// Global active transaction cap.
    pub max_active_transactions: usize,
    /// Per-Channel active cap (enforced with Channel limits).
    pub max_active_per_channel: usize,
    /// Supervisor control queue items (`ControlCommand` / D-015 remap).
    pub max_actor_commands: usize,
    /// Reserved; not a product-enforced byte bound (D-057 — closed-enum control messages).
    pub max_actor_command_bytes: usize,
    /// Runtime ceiling on caller [`crate::delivery::DeliveryLimits::max_event_items`].
    /// Admission rejects delivery ports that exceed this item capacity.
    pub max_event_queue: usize,
    /// Runtime ceiling on caller [`crate::delivery::DeliveryLimits::max_event_bytes`].
    /// Admission rejects delivery ports that exceed this byte capacity.
    pub max_event_queue_bytes: usize,
    /// Maximum input aggregate bytes (admission).
    pub max_input_bytes: usize,
    /// Maximum messages.
    pub max_messages: usize,
    /// Maximum content parts per message.
    pub max_content_parts: usize,
    /// Maximum tools selected on one transaction.
    pub max_tools_per_transaction: usize,
    /// Maximum tool input-schema JSON bytes (enforced at `StartedRuntime::start`).
    pub max_tool_schema_bytes: usize,
    /// Maximum tool payload bytes.
    pub max_tool_payload_bytes: usize,
    /// Maximum tool output bytes.
    pub max_tool_output_bytes: usize,
    /// Concurrent tools per transaction.
    pub max_concurrent_tools_per_transaction: usize,
    /// Queued tool starts per transaction.
    pub max_queued_tools_per_transaction: usize,
    /// Maximum inline continuations.
    pub max_continuations: usize,
    /// Maximum provider exchanges.
    pub max_provider_exchanges: usize,
    /// Maximum continuation-context encoded bytes.
    pub max_continuation_context_bytes: usize,
    /// Maximum total provider input bytes across exchanges.
    pub max_total_provider_input_bytes: usize,
    /// Maximum total provider output bytes across exchanges.
    pub max_total_provider_output_bytes: usize,
    /// Maximum retained diagnostics.
    pub max_diagnostic_count: usize,
    /// Maximum bytes per diagnostic message.
    pub max_diagnostic_bytes: usize,
    /// Default transaction deadline.
    pub transaction_deadline: Duration,
    /// Cleanup budget after terminal selection.
    pub cleanup_deadline: Duration,
    /// Terminal `Ended` delivery budget.
    pub terminal_event_delivery_deadline: Duration,
    /// Reserved callback budget (D-059 — no core wait site under M7 push completion).
    pub callback_deadline: Duration,
}

impl Default for TransactionLimits {
    fn default() -> Self {
        Self {
            max_active_transactions: 256,
            max_active_per_channel: 64,
            max_actor_commands: 256,
            max_actor_command_bytes: 1024 * 1024,
            max_event_queue: 1024,
            max_event_queue_bytes: 4 * 1024 * 1024,
            max_input_bytes: 2 * 1024 * 1024,
            max_messages: 256,
            max_content_parts: 64,
            max_tools_per_transaction: 64,
            max_tool_schema_bytes: 64 * 1024,
            max_tool_payload_bytes: 256 * 1024,
            max_tool_output_bytes: 256 * 1024,
            max_concurrent_tools_per_transaction: 16,
            max_queued_tools_per_transaction: 64,
            max_continuations: 32,
            max_provider_exchanges: 64,
            max_continuation_context_bytes: 2 * 1024 * 1024,
            max_total_provider_input_bytes: 16 * 1024 * 1024,
            max_total_provider_output_bytes: 16 * 1024 * 1024,
            max_diagnostic_count: 64,
            max_diagnostic_bytes: 1024,
            transaction_deadline: Duration::from_secs(600),
            cleanup_deadline: Duration::from_secs(30),
            terminal_event_delivery_deadline: Duration::from_secs(10),
            callback_deadline: Duration::from_secs(5),
        }
    }
}

/// Limit configuration rejected before runtime start.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum LimitsError {
    /// A capacity is zero where non-zero is required.
    #[error("limit must be non-zero: {0}")]
    ZeroCapacity(&'static str),
    /// Related limits are inconsistent.
    #[error("inconsistent limits: {0}")]
    Inconsistent(&'static str),
}

mod duration_secs {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(d: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        d.as_secs().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = u64::deserialize(deserializer)?;
        Ok(Duration::from_secs(secs))
    }
}

impl TransactionLimits {
    /// Validate non-zero capacities and basic consistency (D-015).
    pub fn validate(&self) -> Result<(), LimitsError> {
        for (name, v) in [
            ("max_active_transactions", self.max_active_transactions),
            ("max_active_per_channel", self.max_active_per_channel),
            ("max_actor_commands", self.max_actor_commands),
            ("max_actor_command_bytes", self.max_actor_command_bytes),
            ("max_event_queue", self.max_event_queue),
            ("max_event_queue_bytes", self.max_event_queue_bytes),
            ("max_input_bytes", self.max_input_bytes),
            ("max_messages", self.max_messages),
            ("max_content_parts", self.max_content_parts),
            ("max_tools_per_transaction", self.max_tools_per_transaction),
            ("max_tool_schema_bytes", self.max_tool_schema_bytes),
            ("max_tool_payload_bytes", self.max_tool_payload_bytes),
            ("max_tool_output_bytes", self.max_tool_output_bytes),
            (
                "max_concurrent_tools_per_transaction",
                self.max_concurrent_tools_per_transaction,
            ),
            (
                "max_queued_tools_per_transaction",
                self.max_queued_tools_per_transaction,
            ),
            ("max_continuations", self.max_continuations.max(1)), // 0 continuations allowed
            ("max_provider_exchanges", self.max_provider_exchanges),
            (
                "max_continuation_context_bytes",
                self.max_continuation_context_bytes,
            ),
            (
                "max_total_provider_input_bytes",
                self.max_total_provider_input_bytes,
            ),
            (
                "max_total_provider_output_bytes",
                self.max_total_provider_output_bytes,
            ),
            ("max_diagnostic_count", self.max_diagnostic_count),
            ("max_diagnostic_bytes", self.max_diagnostic_bytes),
        ] {
            // Continuations may be zero (CallerControlled-only channels).
            if name == "max_continuations" {
                continue;
            }
            if v == 0 {
                return Err(LimitsError::ZeroCapacity(name));
            }
        }
        if self.callback_deadline.is_zero() {
            return Err(LimitsError::ZeroCapacity("callback_deadline"));
        }
        if self.cleanup_deadline.is_zero() {
            return Err(LimitsError::ZeroCapacity("cleanup_deadline"));
        }
        if self.terminal_event_delivery_deadline.is_zero() {
            return Err(LimitsError::ZeroCapacity(
                "terminal_event_delivery_deadline",
            ));
        }
        if self.transaction_deadline.is_zero() {
            return Err(LimitsError::ZeroCapacity("transaction_deadline"));
        }
        if self.max_active_per_channel > self.max_active_transactions {
            return Err(LimitsError::Inconsistent(
                "max_active_per_channel exceeds max_active_transactions",
            ));
        }
        if self.max_event_queue_bytes < self.max_event_queue {
            return Err(LimitsError::Inconsistent(
                "max_event_queue_bytes smaller than max_event_queue items",
            ));
        }
        Ok(())
    }
}
