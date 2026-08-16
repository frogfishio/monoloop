//! Interpreter factory and dialect support levels.

use crate::engine::{spawn_interpretation, Interpretation, StartInterpretation};
use monoloop_contracts::{DialectBinding, DialectFamily, InterpreterError};

/// How well a factory supports a dialect binding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SupportLevel {
    /// Full support.
    Full,
    /// Partial (best-effort mapping).
    Partial,
    /// Unsupported.
    None,
}

/// Creates Interpretation instances for supported dialects.
pub trait InterpreterFactory: Send + Sync {
    /// Report support for a dialect binding.
    fn supports(&self, dialect: &DialectBinding) -> SupportLevel;

    /// Start an interpretation. Returns immediately with live handles.
    fn start(&self, request: StartInterpretation) -> Result<Interpretation, InterpreterError>;
}

/// Default factory supporting Test + ACP/GrokBuild dialects.
#[derive(Clone, Debug, Default)]
pub struct DefaultInterpreterFactory;

impl DefaultInterpreterFactory {
    /// Create the default factory.
    pub fn new() -> Self {
        Self
    }
}

impl InterpreterFactory for DefaultInterpreterFactory {
    fn supports(&self, dialect: &DialectBinding) -> SupportLevel {
        match &dialect.output.family {
            DialectFamily::Test
            | DialectFamily::Acp
            | DialectFamily::GrokBuild
            | DialectFamily::CursorAcp
            | DialectFamily::AgyAcp
            | DialectFamily::CodexAcp
            | DialectFamily::ZaiCli
            | DialectFamily::ClaudeCode => SupportLevel::Full,
            _ => SupportLevel::None,
        }
    }

    fn start(&self, request: StartInterpretation) -> Result<Interpretation, InterpreterError> {
        if self.supports(&request.dialect) == SupportLevel::None {
            return Err(InterpreterError::unsupported_dialect(
                "dialect not supported by DefaultInterpreterFactory",
            ));
        }
        spawn_interpretation(request)
    }
}
