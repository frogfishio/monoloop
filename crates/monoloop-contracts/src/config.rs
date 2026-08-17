//! Invocation, session, and effective configuration contracts.

use crate::limits::ExtensionLimits;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use thiserror::Error;

/// How the runtime continues after model tool calls.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub enum ContinuationPolicy {
    /// Runtime encodes tool results and continues the provider exchange.
    InlineToolContinuation,
    /// Runtime ends with `ContinuationRequired`; caller submits next transaction.
    #[default]
    CallerControlled,
}

/// Optional reasoning effort hint (provider-neutral label).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ReasoningEffort {
    /// Minimal effort.
    Low,
    /// Default effort.
    Medium,
    /// Higher effort.
    High,
}

/// Optional response format constraint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResponseFormat {
    /// Unstructured text.
    Text,
    /// JSON object mode.
    JsonObject,
}

/// Namespaced extension key (e.g. `openai.seed`).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ExtensionKey(String);

impl ExtensionKey {
    /// Fallible constructor: non-empty, bounded, must contain a `.` namespace separator.
    pub fn try_new(value: impl Into<String>, max_bytes: usize) -> Result<Self, ConfigError> {
        let s = value.into();
        if s.is_empty() {
            return Err(ConfigError::EmptyExtensionKey);
        }
        if s.len() > max_bytes {
            return Err(ConfigError::ExtensionKeyTooLong {
                bytes: s.len(),
                max: max_bytes,
            });
        }
        if s.chars().any(|c| c.is_control()) {
            return Err(ConfigError::ControlCharacter);
        }
        if !s.contains('.') {
            return Err(ConfigError::ExtensionKeyMissingNamespace);
        }
        Ok(Self(s))
    }

    /// Borrow the key.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Versioned extension payload.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VersionedExtension {
    /// Schema version for this key.
    pub version: u16,
    /// JSON value (bounded at admission).
    pub value: serde_json::Value,
}

/// Per-request invocation overrides (never secrets or endpoints).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InvocationConfig {
    /// Optional model id override.
    pub model: Option<String>,
    /// Optional temperature.
    pub temperature: Option<f32>,
    /// Optional reasoning effort.
    pub reasoning_effort: Option<ReasoningEffort>,
    /// Optional max output tokens.
    pub max_output_tokens: Option<u32>,
    /// Stop sequences.
    pub stop: Vec<String>,
    /// Optional response format.
    pub response_format: Option<ResponseFormat>,
    /// Continuation policy (required).
    pub continuation_policy: ContinuationPolicy,
    /// Optional deadline override.
    pub deadline: Option<Duration>,
    /// Namespaced extensions.
    pub extensions: BTreeMap<ExtensionKey, VersionedExtension>,
}

impl Default for InvocationConfig {
    fn default() -> Self {
        Self {
            model: None,
            temperature: None,
            reasoning_effort: None,
            max_output_tokens: None,
            stop: Vec::new(),
            response_format: None,
            continuation_policy: ContinuationPolicy::CallerControlled,
            deadline: None,
            extensions: BTreeMap::new(),
        }
    }
}

/// External-agent session configuration (no prompt, MCP URL, or secrets).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct SessionConfig {
    /// Optional specialist profile label.
    pub specialist_profile: Option<String>,
    /// Optional mode label (agent/plan/ask…).
    pub mode: Option<String>,
    /// Optional permission profile label.
    pub permission_profile: Option<String>,
    /// Namespaced extensions.
    pub extensions: BTreeMap<ExtensionKey, VersionedExtension>,
}

/// Channel default invocation values.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct ChannelDefaults {
    /// Default model.
    pub model: Option<String>,
    /// Default temperature.
    pub temperature: Option<f32>,
    /// Default reasoning effort.
    pub reasoning_effort: Option<ReasoningEffort>,
    /// Default max output tokens.
    pub max_output_tokens: Option<u32>,
    /// Default stop sequences.
    pub stop: Vec<String>,
    /// Default response format.
    pub response_format: Option<ResponseFormat>,
    /// Default continuation policy.
    pub continuation_policy: ContinuationPolicy,
    /// Default extensions.
    pub extensions: BTreeMap<ExtensionKey, VersionedExtension>,
}

/// Which options a Channel accepts and which are immutable once a session exists.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct OptionPolicy {
    /// Options callers may set on invocation.
    pub supported_invocation: BTreeSet<ConfigOption>,
    /// Options frozen for an existing external session (must match or fail).
    pub session_immutable: BTreeSet<ConfigOption>,
    /// Allowed extension key namespaces (exact keys).
    /// Empty means **no extensions permitted** (D-023).
    pub allowed_extension_keys: BTreeSet<ExtensionKey>,
}

/// Named configuration option for policy checks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConfigOption {
    /// Model id.
    Model,
    /// Temperature.
    Temperature,
    /// Reasoning effort.
    ReasoningEffort,
    /// Max output tokens.
    MaxOutputTokens,
    /// Stop sequences.
    Stop,
    /// Response format.
    ResponseFormat,
    /// Continuation policy.
    ContinuationPolicy,
    /// Deadline.
    Deadline,
    /// Extensions map.
    Extensions,
}

/// Immutable effective configuration after merge.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EffectiveConfig {
    /// Model id.
    pub model: Option<String>,
    /// Temperature.
    pub temperature: Option<f32>,
    /// Reasoning effort.
    pub reasoning_effort: Option<ReasoningEffort>,
    /// Max output tokens.
    pub max_output_tokens: Option<u32>,
    /// Stop sequences.
    pub stop: Vec<String>,
    /// Response format.
    pub response_format: Option<ResponseFormat>,
    /// Continuation policy.
    pub continuation_policy: ContinuationPolicy,
    /// Deadline.
    pub deadline: Option<Duration>,
    /// Merged extensions.
    pub extensions: BTreeMap<ExtensionKey, VersionedExtension>,
    /// Effective session config when external-agent (else empty defaults).
    pub session: SessionConfig,
}

/// Merge: Channel defaults <- session configuration <- permitted invocation overrides.
pub fn merge_effective_config(
    defaults: &ChannelDefaults,
    session: Option<&SessionConfig>,
    attached_session: Option<&SessionConfig>,
    invocation: &InvocationConfig,
    policy: &OptionPolicy,
    extension_limits: &ExtensionLimits,
) -> Result<EffectiveConfig, ConfigError> {
    validate_extensions(&defaults.extensions, extension_limits, policy)?;
    if let Some(s) = session {
        validate_session_labels(s)?;
        validate_extensions(&s.extensions, extension_limits, policy)?;
    }
    if let Some(s) = attached_session {
        validate_session_labels(s)?;
        validate_extensions(&s.extensions, extension_limits, policy)?;
    }
    validate_extensions(&invocation.extensions, extension_limits, policy)?;
    validate_invocation_strings(invocation)?;

    // Session immutability: requested session must match attached when present.
    if let (Some(requested), Some(attached)) = (session, attached_session) {
        check_session_match(requested, attached, policy)?;
    }

    let mut effective = EffectiveConfig {
        model: defaults.model.clone(),
        temperature: defaults.temperature,
        reasoning_effort: defaults.reasoning_effort,
        max_output_tokens: defaults.max_output_tokens,
        stop: defaults.stop.clone(),
        response_format: defaults.response_format.clone(),
        continuation_policy: defaults.continuation_policy,
        deadline: None,
        extensions: defaults.extensions.clone(),
        session: session.cloned().unwrap_or_default(),
    };

    // Session extensions layer on defaults (non-secret labels only).
    if let Some(s) = session {
        for (k, v) in &s.extensions {
            effective.extensions.insert(k.clone(), v.clone());
        }
    }

    apply_invocation(&mut effective, invocation, policy)?;

    // Re-check extension serialized size after merge.
    validate_extensions(&effective.extensions, extension_limits, policy)?;
    Ok(effective)
}

fn apply_invocation(
    effective: &mut EffectiveConfig,
    invocation: &InvocationConfig,
    policy: &OptionPolicy,
) -> Result<(), ConfigError> {
    if invocation.model.is_some() {
        require_supported(policy, ConfigOption::Model)?;
        effective.model = invocation.model.clone();
    }
    if invocation.temperature.is_some() {
        require_supported(policy, ConfigOption::Temperature)?;
        if let Some(t) = invocation.temperature {
            if !(0.0..=2.0).contains(&t) {
                return Err(ConfigError::InvalidNumeric("temperature"));
            }
        }
        effective.temperature = invocation.temperature;
    }
    if invocation.reasoning_effort.is_some() {
        require_supported(policy, ConfigOption::ReasoningEffort)?;
        effective.reasoning_effort = invocation.reasoning_effort;
    }
    if invocation.max_output_tokens.is_some() {
        require_supported(policy, ConfigOption::MaxOutputTokens)?;
        effective.max_output_tokens = invocation.max_output_tokens;
    }
    if !invocation.stop.is_empty() {
        require_supported(policy, ConfigOption::Stop)?;
        effective.stop = invocation.stop.clone();
    }
    if invocation.response_format.is_some() {
        require_supported(policy, ConfigOption::ResponseFormat)?;
        effective.response_format = invocation.response_format.clone();
    }
    // Continuation policy always set on invocation; must be supported.
    require_supported(policy, ConfigOption::ContinuationPolicy)?;
    effective.continuation_policy = invocation.continuation_policy;

    if invocation.deadline.is_some() {
        require_supported(policy, ConfigOption::Deadline)?;
        effective.deadline = invocation.deadline;
    }
    if !invocation.extensions.is_empty() {
        require_supported(policy, ConfigOption::Extensions)?;
        for (k, v) in &invocation.extensions {
            effective.extensions.insert(k.clone(), v.clone());
        }
    }
    Ok(())
}

fn require_supported(policy: &OptionPolicy, opt: ConfigOption) -> Result<(), ConfigError> {
    if policy.supported_invocation.contains(&opt) {
        Ok(())
    } else {
        Err(ConfigError::UnsupportedOption(opt))
    }
}

fn check_session_match(
    requested: &SessionConfig,
    attached: &SessionConfig,
    policy: &OptionPolicy,
) -> Result<(), ConfigError> {
    if policy.session_immutable.contains(&ConfigOption::Model) {
        // Session labels treated as immutable fields when listed.
    }
    // Compare specialist/mode/permission when either side sets them.
    if requested.specialist_profile != attached.specialist_profile
        && (requested.specialist_profile.is_some() || attached.specialist_profile.is_some())
    {
        return Err(ConfigError::ImmutableSessionMismatch("specialist_profile"));
    }
    if requested.mode != attached.mode && (requested.mode.is_some() || attached.mode.is_some()) {
        return Err(ConfigError::ImmutableSessionMismatch("mode"));
    }
    if requested.permission_profile != attached.permission_profile
        && (requested.permission_profile.is_some() || attached.permission_profile.is_some())
    {
        return Err(ConfigError::ImmutableSessionMismatch("permission_profile"));
    }
    for (k, v) in &requested.extensions {
        if let Some(existing) = attached.extensions.get(k) {
            if existing != v {
                return Err(ConfigError::ImmutableSessionMismatch("extension"));
            }
        }
    }
    Ok(())
}

fn validate_session_labels(session: &SessionConfig) -> Result<(), ConfigError> {
    for label in [
        &session.specialist_profile,
        &session.mode,
        &session.permission_profile,
    ]
    .into_iter()
    .flatten()
    {
        if label.is_empty() || label.len() > 128 || label.chars().any(|c| c.is_control()) {
            return Err(ConfigError::InvalidSessionLabel);
        }
    }
    Ok(())
}

fn validate_invocation_strings(invocation: &InvocationConfig) -> Result<(), ConfigError> {
    if let Some(m) = &invocation.model {
        if m.is_empty() || m.len() > 256 || m.chars().any(|c| c.is_control()) {
            return Err(ConfigError::InvalidModel);
        }
    }
    for s in &invocation.stop {
        if s.is_empty() || s.len() > 64 || s.chars().any(|c| c.is_control()) {
            return Err(ConfigError::InvalidStop);
        }
    }
    Ok(())
}

fn validate_extensions(
    map: &BTreeMap<ExtensionKey, VersionedExtension>,
    limits: &ExtensionLimits,
    policy: &OptionPolicy,
) -> Result<(), ConfigError> {
    if map.len() > limits.max_keys {
        return Err(ConfigError::TooManyExtensions {
            count: map.len(),
            max: limits.max_keys,
        });
    }
    // D-023: empty allowlist denies all extensions (not unrestricted).
    if !map.is_empty() && policy.allowed_extension_keys.is_empty() {
        let first = map
            .keys()
            .next()
            .map(|k| k.as_str().to_string())
            .unwrap_or_default();
        return Err(ConfigError::UnknownExtension(first));
    }
    let mut total = 0usize;
    for (k, v) in map {
        if k.as_str().len() > limits.max_key_bytes {
            return Err(ConfigError::ExtensionKeyTooLong {
                bytes: k.as_str().len(),
                max: limits.max_key_bytes,
            });
        }
        if !policy.allowed_extension_keys.contains(k) {
            return Err(ConfigError::UnknownExtension(k.as_str().to_string()));
        }
        let depth = json_depth(&v.value);
        if depth > limits.max_value_depth {
            return Err(ConfigError::ExtensionTooDeep {
                depth,
                max: limits.max_value_depth,
            });
        }
        let encoded =
            serde_json::to_vec(&v.value).map_err(|_| ConfigError::ExtensionEncodeFailed)?;
        total = total
            .saturating_add(encoded.len())
            .saturating_add(k.as_str().len());
    }
    if total > limits.max_serialized_bytes {
        return Err(ConfigError::ExtensionsTooLarge {
            bytes: total,
            max: limits.max_serialized_bytes,
        });
    }
    Ok(())
}

fn json_depth(value: &serde_json::Value) -> u32 {
    match value {
        serde_json::Value::Array(items) => 1 + items.iter().map(json_depth).max().unwrap_or(0),
        serde_json::Value::Object(map) => 1 + map.values().map(json_depth).max().unwrap_or(0),
        _ => 1,
    }
}

/// Configuration construction / merge error.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum ConfigError {
    /// Empty extension key.
    #[error("extension key must be non-empty")]
    EmptyExtensionKey,
    /// Extension key missing namespace separator.
    #[error("extension key must be namespaced (contain '.')")]
    ExtensionKeyMissingNamespace,
    /// Extension key too long.
    #[error("extension key bytes {bytes} exceeds max {max}")]
    ExtensionKeyTooLong {
        /// Actual.
        bytes: usize,
        /// Max.
        max: usize,
    },
    /// Control character.
    #[error("configuration string must not contain control characters")]
    ControlCharacter,
    /// Too many extensions.
    #[error("extension count {count} exceeds max {max}")]
    TooManyExtensions {
        /// Actual.
        count: usize,
        /// Max.
        max: usize,
    },
    /// Extension JSON too deep.
    #[error("extension depth {depth} exceeds max {max}")]
    ExtensionTooDeep {
        /// Actual.
        depth: u32,
        /// Max.
        max: u32,
    },
    /// Extensions aggregate too large.
    #[error("extensions serialized bytes {bytes} exceed max {max}")]
    ExtensionsTooLarge {
        /// Actual.
        bytes: usize,
        /// Max.
        max: usize,
    },
    /// Unknown extension key for policy.
    #[error("unknown extension key {0}")]
    UnknownExtension(String),
    /// Extension encode failed.
    #[error("extension JSON encode failed")]
    ExtensionEncodeFailed,
    /// Unsupported option for Channel.
    #[error("unsupported configuration option: {0:?}")]
    UnsupportedOption(ConfigOption),
    /// Invalid numeric.
    #[error("invalid numeric value for {0}")]
    InvalidNumeric(&'static str),
    /// Invalid model string.
    #[error("invalid model string")]
    InvalidModel,
    /// Invalid stop sequence.
    #[error("invalid stop sequence")]
    InvalidStop,
    /// Invalid session label.
    #[error("invalid session configuration label")]
    InvalidSessionLabel,
    /// Immutable session setting mismatch.
    #[error("immutable session setting mismatch: {0}")]
    ImmutableSessionMismatch(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_policy() -> OptionPolicy {
        let mut p = OptionPolicy::default();
        p.supported_invocation.extend([
            ConfigOption::Model,
            ConfigOption::Temperature,
            ConfigOption::ContinuationPolicy,
            ConfigOption::Deadline,
            ConfigOption::Extensions,
        ]);
        p
    }

    #[test]
    fn merge_precedence_invocation_over_defaults() {
        let defaults = ChannelDefaults {
            model: Some("base".into()),
            temperature: Some(0.2),
            continuation_policy: ContinuationPolicy::CallerControlled,
            ..Default::default()
        };
        let inv = InvocationConfig {
            model: Some("override".into()),
            temperature: Some(0.7),
            continuation_policy: ContinuationPolicy::InlineToolContinuation,
            ..Default::default()
        };
        let eff = merge_effective_config(
            &defaults,
            None,
            None,
            &inv,
            &open_policy(),
            &ExtensionLimits::default(),
        )
        .unwrap();
        assert_eq!(eff.model.as_deref(), Some("override"));
        assert_eq!(eff.temperature, Some(0.7));
        assert_eq!(
            eff.continuation_policy,
            ContinuationPolicy::InlineToolContinuation
        );
    }

    #[test]
    fn immutable_session_mismatch_fails() {
        let requested = SessionConfig {
            mode: Some("agent".into()),
            ..Default::default()
        };
        let attached = SessionConfig {
            mode: Some("ask".into()),
            ..Default::default()
        };
        let err = merge_effective_config(
            &ChannelDefaults::default(),
            Some(&requested),
            Some(&attached),
            &InvocationConfig {
                continuation_policy: ContinuationPolicy::CallerControlled,
                ..Default::default()
            },
            &open_policy(),
            &ExtensionLimits::default(),
        )
        .unwrap_err();
        assert!(matches!(err, ConfigError::ImmutableSessionMismatch("mode")));
    }

    #[test]
    fn extension_bounds() {
        let limits = ExtensionLimits {
            max_keys: 1,
            max_key_bytes: 32,
            max_value_depth: 2,
            max_serialized_bytes: 64,
        };
        let k = ExtensionKey::try_new("ns.a", limits.max_key_bytes).unwrap();
        let k2 = ExtensionKey::try_new("ns.b", limits.max_key_bytes).unwrap();
        let mut inv = InvocationConfig::default();
        inv.extensions.insert(
            k,
            VersionedExtension {
                version: 1,
                value: serde_json::json!(1),
            },
        );
        inv.extensions.insert(
            k2,
            VersionedExtension {
                version: 1,
                value: serde_json::json!(2),
            },
        );
        let mut policy = open_policy();
        policy.allowed_extension_keys.clear(); // empty means unrestricted in our validate when empty
                                               // With empty allowed set we currently allow all — re-check validate_extensions
        let err = merge_effective_config(
            &ChannelDefaults::default(),
            None,
            None,
            &inv,
            &policy,
            &limits,
        )
        .unwrap_err();
        assert!(matches!(err, ConfigError::TooManyExtensions { .. }));
    }
}
