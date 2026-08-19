//! Synchronous admission algorithm surface (M2 scaffold).
//!
//! `submit` MUST perform no spawn and no executor wait (v2 §9).

use monoloop_contracts::{AdmissionError, AdmissionErrorKind};

/// Helper for rejected admission messages during the scaffold phase.
pub fn rejecting(kind: AdmissionErrorKind, message: impl Into<String>) -> AdmissionError {
    AdmissionError::new(kind, message)
}
