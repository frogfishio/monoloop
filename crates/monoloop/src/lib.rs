//! SPDX-License-Identifier: AGPL-3.0-or-later
//! Copyright (C) Alexander R. Croft
//!
//! # Monoloop
//!
//! Product façade for the three Monoloop components:
//!
//! - [`monoloop_contracts`] — shared identities and ports
//! - [`monoloop_connector`] — abstract Connector + FakeConnector
//! - [`monoloop_interpreter`] — dialect → **complete** canonical units (no token stream)
//! - [`monoloop_loop`] — transaction runtime + inner tool loop
//!
//! Host assembly: see the crate README and examples `fake_echo` /
//! `host_grok_wiring` (`--features grok`). docs.rs: <https://docs.rs/monoloop>.
//! Normative specs: <https://github.com/frogfishio/monoloop> (`doc/`).
//!
//! Licensed under **AGPL-3.0-or-later**. A commercial license is available at
//! <https://frogfish.io>. See the repository `LICENSING.md` and
//! `LICENSE-COMMERCIAL.md`.

#![forbid(unsafe_code)]

pub use monoloop_connector as connector;
pub use monoloop_contracts as contracts;
pub use monoloop_interpreter as interpreter;
pub use monoloop_loop as loop_runtime;

#[cfg(feature = "agy")]
pub use monoloop_connector_agy as connector_agy;
#[cfg(feature = "claude")]
pub use monoloop_connector_claude as connector_claude;
#[cfg(feature = "codex")]
pub use monoloop_connector_codex as connector_codex;
#[cfg(feature = "cursor")]
pub use monoloop_connector_cursor as connector_cursor;
#[cfg(feature = "grok")]
pub use monoloop_connector_grok as connector_grok;
#[cfg(feature = "zai")]
pub use monoloop_connector_zai as connector_zai;

/// Crate version from Cargo (`MAJOR.MINOR.PATCH`).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Build number from this crate’s `BUILD` file (synced from the workspace root).
///
/// Packaged with the crate so `cargo publish` / crates.io builds resolve without
/// needing the workspace root.
pub const BUILD: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/BUILD"));

/// `{version}+build-{build}` as required by `LICENSING.md`.
pub fn version_string() -> String {
    let build = BUILD.trim();
    format!("{VERSION}+build-{build}")
}

/// Two-line copyright / license notice (SPDX equivalent).
pub fn copyright_notice() -> &'static str {
    "Copyright (C) Alexander R. Croft\n\
     SPDX-License-Identifier: AGPL-3.0-or-later (commercial: https://frogfish.io)"
}
