//! ConnectorProxy — routes `begin_open` to named backend connectors.
//!
//! The proxy is composition/routing only: it does not interpret semantic
//! payloads, invent sessions, or merge connection state across backends.

use crate::descriptor::{ConnectorDescriptor, ConnectorKind, ControlCapabilities, RawBoundary};
use crate::open::{OpenConnection, PendingRawConnection};
use crate::traits::Connector;
use monoloop_contracts::{ConnectorError, ConnectorErrorKind};
use std::collections::HashMap;
use std::sync::Arc;

/// How the proxy selects a backend for an open request.
#[derive(Clone, Debug)]
pub enum ProxyRoute {
    /// Use `OpenConnection.endpoint_ref` prefix `name:` → route `name`, remainder as endpoint.
    /// Example: `grok:ws://127.0.0.1:2419` → backend `grok`, endpoint `ws://127.0.0.1:2419`.
    EndpointPrefix,
    /// Always use this backend name (endpoint_ref passed through unchanged).
    Fixed(String),
}

/// Builder for [`ConnectorProxy`].
pub struct ConnectorProxyBuilder {
    routes: HashMap<String, Arc<dyn Connector>>,
    default_backend: Option<String>,
    route: ProxyRoute,
}

impl ConnectorProxyBuilder {
    /// Empty builder.
    pub fn new() -> Self {
        Self {
            routes: HashMap::new(),
            default_backend: None,
            route: ProxyRoute::EndpointPrefix,
        }
    }

    /// Register a named backend connector.
    pub fn register(mut self, name: impl Into<String>, connector: Arc<dyn Connector>) -> Self {
        self.routes.insert(name.into(), connector);
        self
    }

    /// Default backend when prefix is absent (EndpointPrefix mode).
    pub fn default_backend(mut self, name: impl Into<String>) -> Self {
        self.default_backend = Some(name.into());
        self
    }

    /// Selection policy.
    pub fn route(mut self, route: ProxyRoute) -> Self {
        self.route = route;
        self
    }

    /// Build the proxy.
    pub fn build(self) -> Result<ConnectorProxy, ConnectorError> {
        if self.routes.is_empty() {
            return Err(ConnectorError::configuration_invalid(
                "connector proxy requires at least one backend",
            ));
        }
        if let ProxyRoute::Fixed(ref name) = self.route {
            if !self.routes.contains_key(name) {
                return Err(ConnectorError::configuration_invalid(
                    "fixed proxy route names unknown backend",
                ));
            }
        }
        if let Some(ref name) = self.default_backend {
            if !self.routes.contains_key(name) {
                return Err(ConnectorError::configuration_invalid(
                    "default backend not registered",
                ));
            }
        }
        Ok(ConnectorProxy {
            descriptor: ConnectorDescriptor {
                connector_kind: ConnectorKind::Other("proxy".into()),
                implementation_id: "monoloop.connector_proxy".into(),
                implementation_version: env!("CARGO_PKG_VERSION").into(),
                transport_kind: "proxy".into(),
                supported_dialects: vec!["delegated".into()],
                raw_boundary: RawBoundary::InProcess,
                control_capabilities: ControlCapabilities::default(),
            },
            routes: self.routes,
            default_backend: self.default_backend,
            route: self.route,
        })
    }
}

impl Default for ConnectorProxyBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Routes opens to registered backend connectors by explicit name.
///
/// No ambient current backend. Missing/unknown routes fail before open.
pub struct ConnectorProxy {
    descriptor: ConnectorDescriptor,
    routes: HashMap<String, Arc<dyn Connector>>,
    default_backend: Option<String>,
    route: ProxyRoute,
}

impl ConnectorProxy {
    /// Start a builder.
    pub fn builder() -> ConnectorProxyBuilder {
        ConnectorProxyBuilder::new()
    }

    /// Resolve backend name and rewritten endpoint for a request.
    fn resolve(&self, request: &OpenConnection) -> Result<(String, String), ConnectorError> {
        match &self.route {
            ProxyRoute::Fixed(name) => Ok((name.clone(), request.endpoint_ref.clone())),
            ProxyRoute::EndpointPrefix => {
                if let Some((name, rest)) = request.endpoint_ref.split_once(':') {
                    // Avoid treating bare URLs (`ws://…`, `http://…`) as routes.
                    if !name.contains('/') && self.routes.contains_key(name) {
                        return Ok((name.to_string(), rest.to_string()));
                    }
                }
                if let Some(ref default) = self.default_backend {
                    return Ok((default.clone(), request.endpoint_ref.clone()));
                }
                Err(ConnectorError::configuration_invalid(
                    "no backend route in endpoint_ref and no default_backend",
                ))
            }
        }
    }
}

impl Connector for ConnectorProxy {
    fn descriptor(&self) -> &ConnectorDescriptor {
        &self.descriptor
    }

    fn begin_open(&self, mut request: OpenConnection) -> PendingRawConnection {
        match self.resolve(&request) {
            Ok((backend_name, endpoint)) => {
                let Some(backend) = self.routes.get(&backend_name) else {
                    return failed_pending(
                        request.connection_id.clone(),
                        ConnectorError::new(
                            ConnectorErrorKind::ConfigurationInvalid,
                            "unknown connector backend",
                        )
                        .with_connection_id(request.connection_id.as_str()),
                    );
                };
                request.endpoint_ref = endpoint;
                backend.begin_open(request)
            }
            Err(err) => failed_pending(
                request.connection_id.clone(),
                err.with_connection_id(request.connection_id.as_str()),
            ),
        }
    }
}

fn failed_pending(
    connection_id: monoloop_contracts::ConnectionId,
    err: ConnectorError,
) -> PendingRawConnection {
    use crate::control::{ConnectionControlHandle, ControlState};
    let state = ControlState::new();
    let control = ConnectionControlHandle::new(state);
    PendingRawConnection::failed(connection_id, control, err)
}
