//! Lifecycle unit tests — composed modules (LOC gate: prefer <3000 per file).
//!
//! Shared helpers live in [`common`]; thematic proofs are split by concern so
//! `tests.rs` is no longer a 6k+ monolith.

#![allow(clippy::unwrap_used)]
#![allow(unused_imports)]

mod admission_capacity;
mod admission_limits;
mod common;
mod exchange_shutdown;
mod mcp_external;
mod public_limits;
mod race_load;
mod s22_finalization;
mod s22_isolation_loop;
