//! Host-injected credential resolution (no ambient secret material).

use monoloop_contracts::{ConnectorError, ConnectorErrorKind};
use secrecy::{ExposeSecret, SecretString};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// Resolved transport credentials (never logged or Debug-printed).
pub struct ResolvedCredential {
    /// Full `Authorization` header value (e.g. `Bearer …`), if any.
    authorization: Option<SecretString>,
}

impl ResolvedCredential {
    /// Empty credential (no Authorization header).
    pub fn none() -> Self {
        Self {
            authorization: None,
        }
    }

    /// Authorization header value (secret).
    pub fn authorization(value: impl Into<String>) -> Self {
        Self {
            authorization: Some(SecretString::from(value.into())),
        }
    }

    /// Bearer token convenience.
    pub fn bearer(token: impl Into<String>) -> Self {
        Self::authorization(format!("Bearer {}", token.into()))
    }

    /// Expose authorization only at the HTTP boundary (not for logs).
    pub fn expose_authorization(&self) -> Option<&str> {
        self.authorization.as_ref().map(|s| s.expose_secret())
    }
}

impl fmt::Debug for ResolvedCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedCredential")
            .field(
                "authorization",
                &self.authorization.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// Host-injected resolver for `OpenConnection.credential_ref`.
pub trait CredentialResolver: Send + Sync {
    /// Resolve an opaque credential reference to transport credentials.
    ///
    /// Failures MUST NOT include secret material in the error message.
    fn resolve(&self, credential_ref: &str) -> Result<ResolvedCredential, ConnectorError>;
}

/// In-memory map of credential references (tests and simple hosts).
#[derive(Clone, Default)]
pub struct MapCredentialResolver {
    map: Arc<HashMap<String, SecretString>>,
}

impl MapCredentialResolver {
    /// Build from (ref → authorization header value) pairs.
    pub fn new(entries: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>) -> Self {
        let mut map = HashMap::new();
        for (k, v) in entries {
            map.insert(k.into(), SecretString::from(v.into()));
        }
        Self { map: Arc::new(map) }
    }

    /// Empty resolver (any ref fails).
    pub fn empty() -> Self {
        Self::default()
    }
}

impl fmt::Debug for MapCredentialResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MapCredentialResolver")
            .field("entries", &self.map.len())
            .finish()
    }
}

impl CredentialResolver for MapCredentialResolver {
    fn resolve(&self, credential_ref: &str) -> Result<ResolvedCredential, ConnectorError> {
        if credential_ref.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CredentialUnavailable,
                "empty credential reference",
            ));
        }
        match self.map.get(credential_ref) {
            Some(secret) => Ok(ResolvedCredential::authorization(secret.expose_secret())),
            None => Err(ConnectorError::new(
                ConnectorErrorKind::CredentialUnavailable,
                "credential reference not found",
            )),
        }
    }
}

/// Resolver that always returns no credentials (anonymous HTTP).
#[derive(Clone, Debug, Default)]
pub struct AnonymousCredentialResolver;

impl CredentialResolver for AnonymousCredentialResolver {
    fn resolve(&self, _credential_ref: &str) -> Result<ResolvedCredential, ConnectorError> {
        Ok(ResolvedCredential::none())
    }
}

/// One resolved destination: where to send the request, and how to
/// authenticate to it.
pub struct ResolvedConnectorTarget {
    /// Full URL this connection should POST/PUT to, overriding the
    /// Channel's own `endpoint_ref` for this transaction.
    pub endpoint: String,
    /// Transport credentials for that endpoint (see [`ResolvedCredential`]).
    pub credential: ResolvedCredential,
}

/// Host-injected resolver for `OpenConnection.session_config.connector_ref`
/// (see [`monoloop_contracts::SessionConfig::connector_ref`]).
///
/// Lets one Channel serve many otherwise-equivalent backends — same wire
/// protocol, different endpoint/credential per transaction — instead of
/// needing one Channel per backend. A Channel built with
/// [`crate::StreamingHttpConnectorFactory::new_dynamic`] consults this
/// *instead of* its fixed `endpoint_ref`/`CredentialResolver` whenever the
/// submitting transaction set a `connector_ref`; Channels that never set one
/// keep behaving exactly as before (fixed endpoint, `CredentialResolver` by
/// `credential_ref`).
pub trait ConnectorTargetResolver: Send + Sync {
    /// Resolve an opaque `connector_ref` to a concrete endpoint + credential.
    ///
    /// Failures MUST NOT include secret material in the error message.
    fn resolve(&self, connector_ref: &str) -> Result<ResolvedConnectorTarget, ConnectorError>;
}
