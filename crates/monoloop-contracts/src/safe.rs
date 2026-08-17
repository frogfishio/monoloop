//! Bounded, pre-redacted diagnostics (no secrets, prompts, or raw bodies).

use crate::limits::TransactionLimits;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Safe diagnostic code (closed vocabulary preferred; free-form codes are bounded).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DiagnosticCode(String);

impl DiagnosticCode {
    /// Maximum code bytes.
    pub const MAX_BYTES: usize = 64;

    /// Fallible constructor.
    pub fn try_new(value: impl Into<String>) -> Result<Self, SafeDiagnosticError> {
        let s = value.into();
        if s.is_empty() {
            return Err(SafeDiagnosticError::EmptyCode);
        }
        if s.len() > Self::MAX_BYTES {
            return Err(SafeDiagnosticError::CodeTooLong);
        }
        if s.chars().any(|c| c.is_control()) {
            return Err(SafeDiagnosticError::ControlCharacter);
        }
        Ok(Self(s))
    }

    /// Borrow the code.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Bounded safe diagnostic attached to terminals, cancellations, and events.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SafeDiagnostic {
    /// Classification code.
    pub code: DiagnosticCode,
    /// Optional pre-redacted detail (never prompts, secrets, or raw bodies).
    pub message: Option<String>,
}

impl SafeDiagnostic {
    /// Construct with optional message truncated to limits.
    pub fn try_new(
        code: impl Into<String>,
        message: Option<impl Into<String>>,
        max_message_bytes: usize,
    ) -> Result<Self, SafeDiagnosticError> {
        let code = DiagnosticCode::try_new(code)?;
        let message = match message {
            None => None,
            Some(m) => {
                let mut s = m.into();
                if s.chars().any(|c| c.is_control()) {
                    return Err(SafeDiagnosticError::ControlCharacter);
                }
                if s.len() > max_message_bytes {
                    s.truncate(max_message_bytes);
                    // Avoid splitting a UTF-8 scalar.
                    while !s.is_char_boundary(s.len()) {
                        s.pop();
                    }
                }
                if s.is_empty() {
                    None
                } else {
                    Some(s)
                }
            }
        };
        Ok(Self { code, message })
    }

    /// Construct using default transaction diagnostic byte budget.
    pub fn try_new_default(
        code: impl Into<String>,
        message: Option<impl Into<String>>,
    ) -> Result<Self, SafeDiagnosticError> {
        let limits = TransactionLimits::default();
        Self::try_new(code, message, limits.max_diagnostic_bytes)
    }
}

/// Safe diagnostic construction error.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum SafeDiagnosticError {
    /// Empty code.
    #[error("diagnostic code must be non-empty")]
    EmptyCode,
    /// Code too long.
    #[error("diagnostic code exceeds maximum length")]
    CodeTooLong,
    /// Control characters rejected.
    #[error("diagnostic must not contain control characters")]
    ControlCharacter,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_by_truncation_not_secret_content() {
        let long = "x".repeat(2000);
        let d = SafeDiagnostic::try_new("ok", Some(long), 32).unwrap();
        assert!(d.message.as_ref().unwrap().len() <= 32);
    }
}
