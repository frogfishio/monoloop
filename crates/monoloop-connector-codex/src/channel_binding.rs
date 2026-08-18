//! WP-11: Codex ACP ChannelBinding + ConnectorFactory.

use crate::CodexConnector;
use monoloop_connector::{
    Connector, ConnectorBuildError, ConnectorFactory, ConnectorInstance, ConnectorInstanceId,
    ControlDisposition, McpServerDescriptor, PendingOperationControl, PendingSessionAttachment,
    PendingSessionConfiguration, SessionAdapter, SessionAttachError, SessionAttachRequest,
    SessionAttachment, SessionAttachmentCompletion, SessionConfigurationError, SessionRoute,
};
use monoloop_contracts::{
    ChannelCapabilities, ChannelDefaults, ChannelId, ChannelKind, ChannelLimits,
    ContinuationPolicy, DialectDescriptor, ExchangeMode, ExternalSessionId,
    McpConfigurationCapability, McpReachability, SessionId, SessionMode, ToolExecutionMode,
};
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

#[derive(Default)]
/// ConnectorFactory for this profile.
pub struct CodexConnectorFactory;
impl CodexConnectorFactory {
    /// Create a default factory.
    /// Build a ChannelBinding for TransactionRuntime composition.
    pub fn new() -> Self {
        Self
    }
}
impl ConnectorFactory for CodexConnectorFactory {
    fn create(&self) -> Result<ConnectorInstance, ConnectorBuildError> {
        let instance_id = ConnectorInstanceId::generate();
        let connector = Arc::new(CodexConnector::new());
        let sessions = Arc::new(ProfileSessionAdapter::new(instance_id.clone()));
        Ok(ConnectorInstance::new(
            instance_id,
            connector as Arc<dyn Connector>,
            Some(sessions as Arc<dyn SessionAdapter>),
        ))
    }
}

struct ProfileRoute {
    owner: ConnectorInstanceId,
}
impl SessionRoute for ProfileRoute {
    fn owner(&self) -> &ConnectorInstanceId {
        &self.owner
    }
}
struct PendingCtrl {
    cancel: std::sync::atomic::AtomicBool,
    terminate: std::sync::atomic::AtomicBool,
}
impl PendingOperationControl for PendingCtrl {
    fn cancel(&self) -> ControlDisposition {
        if self.cancel.swap(true, std::sync::atomic::Ordering::SeqCst) {
            ControlDisposition::AlreadyRequested
        } else {
            ControlDisposition::Accepted
        }
    }
    fn force_terminate(&self) -> ControlDisposition {
        if self
            .terminate
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            ControlDisposition::AlreadyRequested
        } else {
            ControlDisposition::Accepted
        }
    }
}
struct ProfileSessionAdapter {
    owner: ConnectorInstanceId,
    known: Arc<Mutex<HashMap<String, monoloop_contracts::SessionConfig>>>,
}
impl ProfileSessionAdapter {
    fn new(owner: ConnectorInstanceId) -> Self {
        Self {
            owner,
            known: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}
impl SessionAdapter for ProfileSessionAdapter {
    fn begin_attach(
        &self,
        request: SessionAttachRequest,
    ) -> Result<PendingSessionAttachment, SessionAttachError> {
        let control = Arc::new(PendingCtrl {
            cancel: std::sync::atomic::AtomicBool::new(false),
            terminate: std::sync::atomic::AtomicBool::new(false),
        });
        let control_api: Arc<dyn PendingOperationControl> = control.clone();
        let owner = self.owner.clone();
        let known = Arc::clone(&self.known);

        let completion: SessionAttachmentCompletion = Box::pin(async move {
            if control.terminate.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(SessionAttachError::Terminated);
            }
            if control.cancel.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(SessionAttachError::Cancelled);
            }
            let route = Arc::new(ProfileRoute {
                owner: owner.clone(),
            });
            if let Some(ref sid) = request.requested_session_id {
                let map = known
                    .lock()
                    .map_err(|_| SessionAttachError::SessionFailed)?;
                let cfg = map
                    .get(sid.as_str())
                    .cloned()
                    .unwrap_or_else(|| request.session_config.clone());
                let ext = ExternalSessionId::try_new(sid.as_str())
                    .map_err(|_| SessionAttachError::SessionFailed)?;
                monoloop_connector::validate_session_id_match(Some(sid), &ext)
                    .map_err(|_| SessionAttachError::SessionIdMismatch)?;
                Ok(Arc::new(SessionAttachment::new(owner, ext, cfg, route)))
            } else {
                let provisional = SessionId::generate();
                let ext = ExternalSessionId::try_new(provisional.as_str())
                    .map_err(|_| SessionAttachError::SessionFailed)?;
                known
                    .lock()
                    .map_err(|_| SessionAttachError::SessionFailed)?
                    .insert(
                        provisional.as_str().to_string(),
                        request.session_config.clone(),
                    );
                Ok(Arc::new(SessionAttachment::new_create(
                    owner,
                    ext,
                    request.session_config,
                    route,
                    request.initial_mcp,
                )))
            }
        });
        Ok(PendingSessionAttachment {
            control: control_api,
            completion,
        })
    }

    fn begin_refresh_mcp(
        &self,
        attachment: Arc<SessionAttachment>,
        _: Option<McpServerDescriptor>,
    ) -> Result<PendingSessionConfiguration, SessionConfigurationError> {
        if attachment.owner != self.owner {
            return Err(SessionConfigurationError::OwnerMismatch);
        }
        Err(SessionConfigurationError::Unsupported)
    }
}

/// Build a ChannelBinding for TransactionRuntime composition.
pub fn codex_channel_binding(
    id: impl AsRef<str>,
    endpoint_ref: impl Into<String>,
    encoder: Arc<dyn monoloop_contracts::OutboundDialectEncoder>,
    interpreter: Arc<dyn monoloop_interpreter::InterpreterFactory>,
) -> monoloop_loop::ChannelBinding {
    let d = DialectDescriptor::codex_acp("1");
    monoloop_loop::ChannelBinding {
        id: ChannelId::try_new(id.as_ref()).expect("channel id"),
        kind: ChannelKind::ExternalAgent,
        tool_mode: ToolExecutionMode::McpGateway,
        connector_factory: Arc::new(CodexConnectorFactory::new()),
        encoder,
        interpreter,
        endpoint_ref: endpoint_ref.into(),
        credential_ref: None,
        defaults: ChannelDefaults::default(),
        capabilities: ChannelCapabilities {
            session_mode: SessionMode::External,
            mcp_configuration: McpConfigurationCapability::CreationOnly,
            mcp_reachability: McpReachability::SameLoopbackNamespace,
            exchange_mode: ExchangeMode::Bidirectional,
            continuation_policies: BTreeSet::from([ContinuationPolicy::CallerControlled]),
            supports_distinct_session_concurrency: true,
            input_dialect: d.clone(),
            output_dialect: d,
            option_policy: monoloop_contracts::OptionPolicy::external_agent(),
        },
        limits: ChannelLimits::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn codex_binding_validates() {
        use monoloop_interpreter::DefaultInterpreterFactory;
        use monoloop_loop::AcpPromptEncoder;
        let b = codex_channel_binding(
            "codex-1",
            "codex:stdio",
            Arc::new(AcpPromptEncoder::codex()),
            Arc::new(DefaultInterpreterFactory::new()),
        );
        assert!(b.descriptor().validate().is_ok());
    }
}
