//! WP-11: Grok Build ChannelBinding factory and session ownership.
//!
//! External-agent Channel: create/load via explicit session ids, MCP CreationOnly,
//! Bidirectional exchange, loopback fail-closed. Prompts are encoder-owned
//! (`AcpPromptEncoder::grok`); `begin_open` never takes a prompt body.

use crate::config::GrokServerConfig;
use crate::secret::SecretResolver;
use crate::server::GrokConnector;
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
/// Factory: one Grok Connector instance + SessionAdapter per Channel.
pub struct GrokConnectorFactory {
    secrets: Arc<dyn SecretResolver>,
}

impl GrokConnectorFactory {
    /// Inject secret resolver (required for WebSocket auth).
    pub fn new(secrets: Arc<dyn SecretResolver>) -> Self {
        Self { secrets }
    }
}

impl ConnectorFactory for GrokConnectorFactory {
    fn create(&self) -> Result<ConnectorInstance, ConnectorBuildError> {
        let instance_id = ConnectorInstanceId::generate();
        let connector = Arc::new(GrokConnector::new(Arc::clone(&self.secrets)));
        let sessions = Arc::new(GrokSessionAdapter::new(instance_id.clone()));
        Ok(ConnectorInstance::new(
            instance_id,
            connector as Arc<dyn Connector>,
            Some(sessions as Arc<dyn SessionAdapter>),
        ))
    }
}

/// Opaque route for Grok session ownership checks.
struct GrokRoute {
    owner: ConnectorInstanceId,
}

impl SessionRoute for GrokRoute {
    fn owner(&self) -> &ConnectorInstanceId {
        &self.owner
    }
}

/// Identity SessionAdapter: reserves create/load session keys explicitly.
///
/// Provider `session/new` / `session/load` still run on Connector open using the
/// attachment's external id (load) or create path (`create_mode`).
/// Never selects a most-recent session.
pub struct GrokSessionAdapter {
    owner: ConnectorInstanceId,
    /// Known external session ids from prior create/load in this process (shared).
    known: Arc<Mutex<HashMap<String, monoloop_contracts::SessionConfig>>>,
}

impl GrokSessionAdapter {
    fn new(owner: ConnectorInstanceId) -> Self {
        Self {
            owner,
            known: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Record a provider-authoritative session id after successful create (runtime).
    #[allow(dead_code)]
    pub fn remember(&self, session_id: &str, config: monoloop_contracts::SessionConfig) {
        if let Ok(mut m) = self.known.lock() {
            m.insert(session_id.to_string(), config);
        }
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

impl SessionAdapter for GrokSessionAdapter {
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
            let route = Arc::new(GrokRoute {
                owner: owner.clone(),
            });
            if let Some(ref sid) = request.requested_session_id {
                // Explicit load — never invent most-recent; never create a replacement.
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
                // Create: provisional placeholder only; Connector open does session/new.
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
        _descriptor: Option<McpServerDescriptor>,
    ) -> Result<PendingSessionConfiguration, SessionConfigurationError> {
        if attachment.owner != self.owner {
            return Err(SessionConfigurationError::OwnerMismatch);
        }
        // CreationOnly: refresh unsupported (provisional until Refreshable proven).
        Err(SessionConfigurationError::Unsupported)
    }
}

/// Build a production-shaped Grok Channel binding.
///
/// - `endpoint_ref`: `ws://127.0.0.1:port` (loopback by default)
/// - `credential_ref`: secret resolver key name
/// - Tools: empty only unless MCP gateway is composed separately (CreationOnly)
pub fn grok_channel_binding(
    id: impl AsRef<str>,
    endpoint_ref: impl Into<String>,
    credential_ref: impl Into<String>,
    secrets: Arc<dyn SecretResolver>,
    encoder: Arc<dyn monoloop_contracts::OutboundDialectEncoder>,
    interpreter: Arc<dyn monoloop_interpreter::InterpreterFactory>,
) -> monoloop_loop::ChannelBinding {
    let d = DialectDescriptor::acp_json_rpc("1");
    monoloop_loop::ChannelBinding {
        id: ChannelId::try_new(id.as_ref()).expect("channel id"),
        kind: ChannelKind::ExternalAgent,
        // MCP CreationOnly: tool mode is McpGateway (empty tool sets remain valid).
        tool_mode: ToolExecutionMode::McpGateway,
        connector_factory: Arc::new(GrokConnectorFactory::new(secrets)),
        encoder,
        interpreter,
        endpoint_ref: endpoint_ref.into(),
        credential_ref: Some(credential_ref.into()),
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
        },
        limits: ChannelLimits {
            max_active_transactions: 32,
            max_distinct_sessions: 64,
            max_encoded_exchange_bytes: 4 * 1024 * 1024,
        },
    }
}

/// Validate Grok server config security at binding construction time (optional helper).
pub fn validate_grok_endpoint(config: &GrokServerConfig) -> Result<(), String> {
    config
        .validate_endpoint_security()
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use monoloop_contracts::ChannelDescriptor;

    #[test]
    fn grok_capability_matrix_validates() {
        let d = DialectDescriptor::acp_json_rpc("1");
        let desc = ChannelDescriptor {
            kind: ChannelKind::ExternalAgent,
            tool_mode: ToolExecutionMode::McpGateway,
            capabilities: ChannelCapabilities {
                session_mode: SessionMode::External,
                mcp_configuration: McpConfigurationCapability::CreationOnly,
                mcp_reachability: McpReachability::SameLoopbackNamespace,
                exchange_mode: ExchangeMode::Bidirectional,
                continuation_policies: BTreeSet::from([ContinuationPolicy::CallerControlled]),
                supports_distinct_session_concurrency: true,
                input_dialect: d.clone(),
                output_dialect: d,
            },
            limits: ChannelLimits::default(),
        };
        assert!(desc.validate().is_ok(), "{:?}", desc.validate());
    }

    #[test]
    fn factory_produces_session_adapter() {
        use crate::secret::InMemorySecretResolver;
        let secrets = Arc::new(InMemorySecretResolver::new());
        let f = GrokConnectorFactory::new(secrets);
        let inst = f.create().unwrap();
        assert!(inst.sessions.is_some());
    }

    #[test]
    fn binding_descriptor_validates() {
        use crate::secret::InMemorySecretResolver;
        use monoloop_interpreter::DefaultInterpreterFactory;
        use monoloop_loop::AcpPromptEncoder;
        let secrets = Arc::new(InMemorySecretResolver::new());
        let b = grok_channel_binding(
            "grok-main",
            "ws://127.0.0.1:9",
            "grok-secret",
            secrets,
            Arc::new(AcpPromptEncoder::grok()),
            Arc::new(DefaultInterpreterFactory::new()),
        );
        assert!(b.descriptor().validate().is_ok());
        assert_eq!(b.kind, ChannelKind::ExternalAgent);
        assert_eq!(b.tool_mode, ToolExecutionMode::McpGateway);
    }
}
