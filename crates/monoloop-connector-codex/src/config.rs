//! OpenAI Codex ACP connector configuration.
//!
//! Native Codex exposes `app-server` / MCP / exec surfaces, but not ACP directly.
//! The practical ACP path for Monoloop is the official adapter
//! `@agentclientprotocol/codex-acp` (stdio NDJSON), which starts Codex App Server
//! and speaks Agent Client Protocol. Pin via `CODEX_ACP_BIN` or rely on discovery.

use std::path::PathBuf;
use std::time::Duration;

/// How to launch the ACP server process for Codex.
#[derive(Clone, Debug)]
pub struct CodexAgentConfig {
    /// Command to run (default: discover `codex-acp`, else `npx`).
    pub command: PathBuf,
    /// Args after command (default: empty for `codex-acp`, or
    /// `["--yes","@agentclientprotocol/codex-acp"]` for npx).
    pub args: Vec<String>,
    /// Working directory for the process and default session `cwd`.
    pub cwd: PathBuf,
    /// Auth method id for ACP `authenticate` when `authenticate` is true.
    ///
    /// codex-acp advertises ChatGPT login and API-key methods; default leaves
    /// auth to existing `codex login` / `OPENAI_API_KEY` / `CODEX_API_KEY`.
    pub auth_method_id: String,
    /// When true, send `authenticate` after initialize (may open a login flow).
    pub authenticate: bool,
    /// Client name reported in `initialize`.
    pub client_name: String,
    /// Client version reported in `initialize`.
    pub client_version: String,
    /// Handshake / RPC deadline.
    pub rpc_deadline: Duration,
    /// Max bytes per NDJSON line (fail-closed).
    pub max_line_bytes: usize,
    /// Bounded output queue for dialect bytes (session/update lines).
    pub max_output_queue: usize,
    /// Auto-answer `session/request_permission` with allow-once.
    pub auto_allow_permissions: bool,
    /// When true, advertise client fs read/write capabilities.
    pub advertise_fs: bool,
    /// Optional path to append raw NDJSON lines (test diagnostics only).
    pub raw_dump_path: Option<PathBuf>,
}

impl Default for CodexAgentConfig {
    fn default() -> Self {
        let (command, args) = discover_acp_command();
        Self {
            command,
            args,
            cwd: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            // Prefer env/login already on the host; explicit authenticate can hang.
            auth_method_id: "openai-api-key".into(),
            authenticate: false,
            client_name: "monoloop-codex".into(),
            client_version: env!("CARGO_PKG_VERSION").into(),
            rpc_deadline: Duration::from_secs(90),
            max_line_bytes: 8 * 1024 * 1024,
            max_output_queue: 256,
            // Fail closed: hosts must opt in to auto-approve tool permissions.
            auto_allow_permissions: false,
            advertise_fs: false,
            raw_dump_path: None,
        }
    }
}

impl CodexAgentConfig {
    /// Config for a project directory.
    pub fn for_project(cwd: impl Into<PathBuf>) -> Self {
        Self {
            cwd: cwd.into(),
            ..Default::default()
        }
    }

    /// Attach a raw NDJSON dump path.
    pub fn with_raw_dump(mut self, path: impl Into<PathBuf>) -> Self {
        self.raw_dump_path = Some(path.into());
        self
    }

    /// Point at a specific Codex binary via `CODEX_PATH` for the adapter.
    ///
    /// The ACP adapter reads `CODEX_PATH` from the process environment; this
    /// only records intent for callers (env is set by the host before spawn).
    pub fn with_codex_path_env_hint(self) -> Self {
        self
    }

    /// Prefer a globally installed `codex-acp` binary when present.
    pub fn with_global_codex_acp(mut self) -> Self {
        self.command = PathBuf::from("codex-acp");
        self.args.clear();
        self
    }

    /// Force unattended permission auto-allow on the ACP client side.
    ///
    /// Only for trusted test sandboxes / unattended qualification.
    pub fn with_auto_allow_permissions(mut self) -> Self {
        self.auto_allow_permissions = true;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_denies_auto_permissions() {
        assert!(!CodexAgentConfig::default().auto_allow_permissions);
    }

    #[test]
    fn opt_in_enables_auto_permissions() {
        assert!(
            CodexAgentConfig::default()
                .with_auto_allow_permissions()
                .auto_allow_permissions
        );
    }
}

/// Resolve ACP server argv:
/// `CODEX_ACP_BIN` → `codex-acp` on PATH → `npx --yes @agentclientprotocol/codex-acp`.
fn discover_acp_command() -> (PathBuf, Vec<String>) {
    if let Some(bin) = std::env::var_os("CODEX_ACP_BIN") {
        return (PathBuf::from(bin), Vec::new());
    }
    if which("codex-acp").is_some() {
        return (PathBuf::from("codex-acp"), Vec::new());
    }
    (
        PathBuf::from(std::env::var_os("NPX_BIN").unwrap_or_else(|| "npx".into())),
        vec!["--yes".into(), "@agentclientprotocol/codex-acp".into()],
    )
}

fn which(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

/// Session create parameters (`session/new` + optional mode).
///
/// codex-acp modes (see adapter docs): `read-only` | `agent` | `agent-full-access`.
#[derive(Clone, Debug)]
pub struct CodexSessionConfig {
    /// Session working directory.
    pub cwd: PathBuf,
    /// MCP servers (opaque JSON; usually empty).
    pub mcp_servers: serde_json::Value,
    /// Mode after create via `session/set_mode`.
    pub mode_id: Option<String>,
}

impl CodexSessionConfig {
    /// Session in the given cwd.
    pub fn new(cwd: impl Into<PathBuf>) -> Self {
        Self {
            cwd: cwd.into(),
            mcp_servers: serde_json::json!([]),
            mode_id: None,
        }
    }

    /// Read-only mode (plan / review style).
    pub fn with_read_only_mode(mut self) -> Self {
        self.mode_id = Some("read-only".into());
        self
    }

    /// Default agent mode (workspace write sandbox typical).
    pub fn with_agent_mode(mut self) -> Self {
        self.mode_id = Some("agent".into());
        self
    }

    /// Full-access agent mode (elevated sandbox; test sandboxes only).
    pub fn with_agent_full_access_mode(mut self) -> Self {
        self.mode_id = Some("agent-full-access".into());
        self
    }
}
