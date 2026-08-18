//! Channel capability data contracts (no live factory arcs).

use crate::config::{ContinuationPolicy, OptionPolicy};
use crate::dialect::DialectDescriptor;
use crate::limits::ChannelLimits;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use thiserror::Error;

/// Whether the Channel talks to an external agent or a direct LLM API.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ChannelKind {
    /// External coding agent (ACP family, etc.).
    ExternalAgent,
    /// Direct model API (e.g. OpenAI Chat Completions).
    DirectLlm,
}

/// How tools execute for transactions on this Channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ToolExecutionMode {
    /// External agent calls Monoloop MCP gateway.
    McpGateway,
    /// Model emits tool calls handled by LoopRuntime.
    ModelToolCalls,
    /// No host tools.
    None,
}

/// MCP configuration installability on the external agent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum McpConfigurationCapability {
    /// No Monoloop MCP attach.
    None,
    /// MCP only at external session creation.
    CreationOnly,
    /// MCP can be refreshed across transactions on one session.
    Refreshable,
}

/// Session topology for the Channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SessionMode {
    /// Ephemeral routing ids only (direct LLM).
    Stateless,
    /// External durable session owned outside Monoloop.
    External,
}

/// Whether the external agent can reach Monoloop's MCP listener.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum McpReachability {
    /// Not applicable.
    None,
    /// Agent and runtime share loopback namespace.
    SameLoopbackNamespace,
    /// Qualified remote transport (not initial product path).
    QualifiedRemoteTransport,
}

/// Provider exchange shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExchangeMode {
    /// HTTP-style one request/response cycle.
    RequestResponse,
    /// Retained session / bidirectional exchange.
    Bidirectional,
}

/// Declared Channel capabilities (immutable registry data).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelCapabilities {
    /// Session topology.
    pub session_mode: SessionMode,
    /// MCP install mode.
    pub mcp_configuration: McpConfigurationCapability,
    /// MCP reachability.
    pub mcp_reachability: McpReachability,
    /// Exchange shape.
    pub exchange_mode: ExchangeMode,
    /// Supported continuation policies.
    pub continuation_policies: BTreeSet<ContinuationPolicy>,
    /// Must be true for production Channels.
    pub supports_distinct_session_concurrency: bool,
    /// Input dialect declaration.
    pub input_dialect: DialectDescriptor,
    /// Output dialect declaration.
    pub output_dialect: DialectDescriptor,
    /// Immutable option/extension policy for this Channel (D-023).
    pub option_policy: OptionPolicy,
}

/// Data-only Channel descriptor used before live binding construction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelDescriptor {
    /// Kind.
    pub kind: ChannelKind,
    /// Tool mode.
    pub tool_mode: ToolExecutionMode,
    /// Capabilities.
    pub capabilities: ChannelCapabilities,
    /// Limits.
    pub limits: ChannelLimits,
}

impl ChannelDescriptor {
    /// Validate capability combinations (startup matrix).
    pub fn validate(&self) -> Result<(), ChannelCapabilityError> {
        if self.limits.max_active_transactions == 0
            || self.limits.max_distinct_sessions == 0
            || self.limits.max_encoded_exchange_bytes == 0
        {
            return Err(ChannelCapabilityError::ZeroLimit);
        }

        match self.kind {
            ChannelKind::DirectLlm => {
                if self.capabilities.session_mode != SessionMode::Stateless {
                    return Err(ChannelCapabilityError::DirectLlmRequiresStateless);
                }
                if self.capabilities.exchange_mode != ExchangeMode::RequestResponse {
                    return Err(ChannelCapabilityError::DirectLlmRequiresRequestResponse);
                }
            }
            ChannelKind::ExternalAgent => {
                if self.capabilities.session_mode != SessionMode::External {
                    return Err(ChannelCapabilityError::ExternalAgentRequiresExternalSession);
                }
            }
        }

        match self.tool_mode {
            ToolExecutionMode::McpGateway => {
                if self.capabilities.mcp_configuration == McpConfigurationCapability::None {
                    return Err(ChannelCapabilityError::McpGatewayRequiresConfiguration);
                }
                if self.capabilities.mcp_reachability == McpReachability::None {
                    return Err(ChannelCapabilityError::McpGatewayRequiresReachability);
                }
            }
            ToolExecutionMode::ModelToolCalls | ToolExecutionMode::None => {
                if self.capabilities.mcp_configuration != McpConfigurationCapability::None {
                    return Err(ChannelCapabilityError::NonMcpMustDisableConfiguration);
                }
                if self.capabilities.mcp_reachability != McpReachability::None {
                    return Err(ChannelCapabilityError::NonMcpMustDisableReachability);
                }
            }
        }

        if self
            .capabilities
            .continuation_policies
            .contains(&ContinuationPolicy::InlineToolContinuation)
            && self.tool_mode != ToolExecutionMode::ModelToolCalls
        {
            return Err(ChannelCapabilityError::InlineContinuationRequiresModelTools);
        }

        if !self.capabilities.supports_distinct_session_concurrency {
            return Err(ChannelCapabilityError::DistinctSessionConcurrencyRequired);
        }

        if self.capabilities.input_dialect != self.capabilities.output_dialect {
            // Allow asymmetric only when both families match for initial product?
            // Spec: encoder/Connector/Interpreter declarations must match exactly —
            // for descriptor data we require equal descriptors initially.
            return Err(ChannelCapabilityError::DialectMismatch);
        }

        Ok(())
    }
}

/// Whether `SendAndRetain` is legal for this Channel.
pub fn send_and_retain_allowed(caps: &ChannelCapabilities) -> bool {
    caps.exchange_mode == ExchangeMode::Bidirectional
}

/// Channel capability validation error.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ChannelCapabilityError {
    /// Zero capacity/limit.
    #[error("channel limits must be non-zero")]
    ZeroLimit,
    /// Direct LLM must be Stateless.
    #[error("DirectLlm requires Stateless session mode")]
    DirectLlmRequiresStateless,
    /// Direct LLM initially RequestResponse only.
    #[error("DirectLlm requires RequestResponse exchange mode")]
    DirectLlmRequiresRequestResponse,
    /// External agent must use External session mode.
    #[error("ExternalAgent requires External session mode")]
    ExternalAgentRequiresExternalSession,
    /// MCP gateway needs non-None configuration.
    #[error("McpGateway requires non-None mcp_configuration")]
    McpGatewayRequiresConfiguration,
    /// MCP gateway needs reachability.
    #[error("McpGateway requires declared mcp_reachability")]
    McpGatewayRequiresReachability,
    /// Non-MCP modes cannot declare MCP configuration.
    #[error("non-MCP tool mode requires mcp_configuration == None")]
    NonMcpMustDisableConfiguration,
    /// Non-MCP modes cannot declare MCP reachability.
    #[error("non-MCP tool mode requires mcp_reachability == None")]
    NonMcpMustDisableReachability,
    /// Inline continuation needs model tools.
    #[error("InlineToolContinuation requires ModelToolCalls")]
    InlineContinuationRequiresModelTools,
    /// Production concurrency flag.
    #[error("supports_distinct_session_concurrency must be true")]
    DistinctSessionConcurrencyRequired,
    /// Input/output dialect descriptors must match for the binding.
    #[error("input and output dialect descriptors must match")]
    DialectMismatch,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialect::DialectDescriptor;

    fn base_caps(mode: SessionMode, exchange: ExchangeMode) -> ChannelCapabilities {
        let d = DialectDescriptor::openai_chat_completions("v1");
        ChannelCapabilities {
            session_mode: mode,
            mcp_configuration: McpConfigurationCapability::None,
            mcp_reachability: McpReachability::None,
            exchange_mode: exchange,
            continuation_policies: BTreeSet::from([ContinuationPolicy::CallerControlled]),
            supports_distinct_session_concurrency: true,
            input_dialect: d.clone(),
            output_dialect: d,
            option_policy: crate::config::OptionPolicy::direct_llm(),
        }
    }

    #[test]
    fn direct_llm_matrix() {
        let d = ChannelDescriptor {
            kind: ChannelKind::DirectLlm,
            tool_mode: ToolExecutionMode::ModelToolCalls,
            capabilities: base_caps(SessionMode::Stateless, ExchangeMode::RequestResponse),
            limits: ChannelLimits::default(),
        };
        assert!(d.validate().is_ok());
    }

    #[test]
    fn mcp_gateway_requires_config() {
        let mut caps = base_caps(SessionMode::External, ExchangeMode::Bidirectional);
        let d = ChannelDescriptor {
            kind: ChannelKind::ExternalAgent,
            tool_mode: ToolExecutionMode::McpGateway,
            capabilities: caps.clone(),
            limits: ChannelLimits::default(),
        };
        assert_eq!(
            d.validate(),
            Err(ChannelCapabilityError::McpGatewayRequiresConfiguration)
        );
        caps.mcp_configuration = McpConfigurationCapability::CreationOnly;
        caps.mcp_reachability = McpReachability::SameLoopbackNamespace;
        let d = ChannelDescriptor {
            kind: ChannelKind::ExternalAgent,
            tool_mode: ToolExecutionMode::McpGateway,
            capabilities: caps,
            limits: ChannelLimits::default(),
        };
        assert!(d.validate().is_ok());
    }
}
