//! WP-11: Z.ai CLI ChannelBinding (SendAndFinish, MCP None, no durable load).

use crate::ZaiConnector;
use monoloop_connector::{
    Connector, ConnectorBuildError, ConnectorFactory, ConnectorInstance, ConnectorInstanceId,
};
use monoloop_contracts::{
    ChannelCapabilities, ChannelDefaults, ChannelId, ChannelKind, ChannelLimits,
    ContinuationPolicy, DialectDescriptor, ExchangeMode, McpConfigurationCapability,
    McpReachability, SessionMode, ToolExecutionMode,
};
use std::collections::BTreeSet;
use std::sync::Arc;

#[derive(Default)]
/// ConnectorFactory for this profile.
pub struct ZaiConnectorFactory;
impl ZaiConnectorFactory {
    /// Create a default factory.
    /// Build a ChannelBinding for TransactionRuntime composition.
    pub fn new() -> Self {
        Self
    }
}
impl ConnectorFactory for ZaiConnectorFactory {
    fn create(&self) -> Result<ConnectorInstance, ConnectorBuildError> {
        let instance_id = ConnectorInstanceId::generate();
        let connector = Arc::new(ZaiConnector::new());
        // DirectLlm-style: no SessionAdapter (synthetic session per run inside open).
        Ok(ConnectorInstance::new(
            instance_id,
            connector as Arc<dyn Connector>,
            None,
        ))
    }
}

/// Z.ai headless Channel (tools inside CLI; Monoloop tools rejected by encoder).
/// Build a ChannelBinding for TransactionRuntime composition.
pub fn zai_channel_binding(
    id: impl AsRef<str>,
    endpoint_ref: impl Into<String>,
    encoder: Arc<dyn monoloop_contracts::OutboundDialectEncoder>,
    interpreter: Arc<dyn monoloop_interpreter::InterpreterFactory>,
) -> monoloop_loop::ChannelBinding {
    let d = DialectDescriptor::zai_cli("1");
    monoloop_loop::ChannelBinding {
        id: ChannelId::try_new(id.as_ref()).expect("channel id"),
        kind: ChannelKind::DirectLlm,
        tool_mode: ToolExecutionMode::None,
        connector_factory: Arc::new(ZaiConnectorFactory::new()),
        encoder,
        interpreter,
        endpoint_ref: endpoint_ref.into(),
        credential_ref: None,
        defaults: ChannelDefaults::default(),
        capabilities: ChannelCapabilities {
            session_mode: SessionMode::Stateless,
            mcp_configuration: McpConfigurationCapability::None,
            mcp_reachability: McpReachability::None,
            exchange_mode: ExchangeMode::RequestResponse,
            continuation_policies: BTreeSet::from([ContinuationPolicy::CallerControlled]),
            supports_distinct_session_concurrency: true,
            input_dialect: d.clone(),
            output_dialect: d,
            option_policy: monoloop_contracts::OptionPolicy::direct_llm(),
        },
        limits: ChannelLimits::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn zai_binding_validates() {
        use monoloop_interpreter::DefaultInterpreterFactory;
        use monoloop_loop::HeadlessPromptEncoder;
        let b = zai_channel_binding(
            "zai-1",
            "zai:stdio",
            Arc::new(HeadlessPromptEncoder::zai()),
            Arc::new(DefaultInterpreterFactory::new()),
        );
        assert!(b.descriptor().validate().is_ok());
        assert!(b.connector_factory.create().unwrap().sessions.is_none());
    }
}
