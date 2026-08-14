//! Injected secret boundary — secrets never appear in descriptors or errors.

use crate::error::GrokConnectorError;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Opaque credential reference resolved only inside the connector boundary.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SecretRef(String);

impl SecretRef {
    /// Create a secret reference name (not the secret itself).
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Reference name for routing only.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SecretRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never display the secret; only the ref name is non-sensitive if chosen carefully.
        write!(f, "secret-ref:{}", self.0)
    }
}

/// Resolves secret references to secret material for transport auth only.
pub trait SecretResolver: Send + Sync {
    /// Resolve a reference. Implementations must not log the returned value.
    fn resolve(&self, secret_ref: &SecretRef) -> Result<String, GrokConnectorError>;
}

/// In-memory resolver for tests.
#[derive(Clone, Default)]
pub struct InMemorySecretResolver {
    map: Arc<Mutex<HashMap<String, String>>>,
}

impl InMemorySecretResolver {
    /// Empty resolver.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a secret for a reference name.
    pub fn insert(&self, secret_ref: impl Into<String>, secret: impl Into<String>) {
        self.map
            .lock()
            .expect("secret map")
            .insert(secret_ref.into(), secret.into());
    }
}

impl SecretResolver for InMemorySecretResolver {
    fn resolve(&self, secret_ref: &SecretRef) -> Result<String, GrokConnectorError> {
        self.map
            .lock()
            .expect("secret map")
            .get(secret_ref.as_str())
            .cloned()
            .ok_or_else(GrokConnectorError::credential_unavailable)
    }
}

/// Resolve secrets from environment variables (value = env var name is the ref).
pub struct EnvSecretResolver;

impl SecretResolver for EnvSecretResolver {
    fn resolve(&self, secret_ref: &SecretRef) -> Result<String, GrokConnectorError> {
        std::env::var(secret_ref.as_str()).map_err(|_| GrokConnectorError::credential_unavailable())
    }
}
