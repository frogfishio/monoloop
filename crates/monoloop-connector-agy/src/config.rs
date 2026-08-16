//! Antigravity (agy) ACP connector configuration.
//!
//! Native `agy` does not yet ship `--acp` (tracked upstream). The practical ACP
//! surface is the community `agy-acp` stdio bridge, which spawns `agy` and
//! speaks Agent Client Protocol over NDJSON. When Google ships native
//! `agy --acp`, set `command` / `args` accordingly.

use std::path::PathBuf;
use std::time::Duration;

/// How to launch the ACP server process for Antigravity.
#[derive(Clone, Debug)]
pub struct AgyAgentConfig {
    /// Command to run (default: discover `agy-acp`, else `npx`).
    pub command: PathBuf,
    /// Args after command (default: empty for `agy-acp`, or `["--yes","agy-acp"]` for npx).
    pub args: Vec<String>,
    /// Working directory for the process and default session `cwd`.
    pub cwd: PathBuf,
    /// Auth method id advertised by agy-acp (`agy-login`).
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

impl Default for AgyAgentConfig {
    fn default() -> Self {
        let (command, args) = discover_acp_command();
        Self {
            command,
            args,
            cwd: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            auth_method_id: "agy-login".into(),
            // Existing Google login is usually enough; explicit auth can hang on TTY.
            authenticate: false,
            client_name: "monoloop-agy".into(),
            client_version: env!("CARGO_PKG_VERSION").into(),
            rpc_deadline: Duration::from_secs(60),
            max_line_bytes: 8 * 1024 * 1024,
            max_output_queue: 256,
            auto_allow_permissions: true,
            advertise_fs: false,
            raw_dump_path: None,
        }
    }
}

impl AgyAgentConfig {
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

    /// Prefer native `agy --acp` when/if available (still experimental).
    pub fn with_native_agy_acp(mut self) -> Self {
        let bin = std::env::var_os("AGY_BIN")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("agy"));
        self.command = bin;
        self.args = vec!["--acp".into()];
        self
    }

    /// Pass `--dangerously-skip-permissions` to the ACP bridge (unattended tools).
    ///
    /// Only for trusted test sandboxes; maps to the agy-acp opt-in flag.
    pub fn with_skip_permissions(mut self) -> Self {
        if !self
            .args
            .iter()
            .any(|a| a == "--dangerously-skip-permissions")
        {
            self.args.push("--dangerously-skip-permissions".into());
        }
        self.auto_allow_permissions = true;
        self
    }
}

/// Resolve ACP server argv: `AGY_ACP_BIN`, `agy-acp` on PATH, else `npx --yes agy-acp`.
fn discover_acp_command() -> (PathBuf, Vec<String>) {
    if let Some(bin) = std::env::var_os("AGY_ACP_BIN") {
        return (PathBuf::from(bin), Vec::new());
    }
    if which("agy-acp").is_some() {
        return (PathBuf::from("agy-acp"), Vec::new());
    }
    // npx downloads/runs the community ACP bridge (stdio → agy).
    (
        PathBuf::from(std::env::var_os("NPX_BIN").unwrap_or_else(|| "npx".into())),
        vec!["--yes".into(), "agy-acp".into()],
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
#[derive(Clone, Debug)]
pub struct AgySessionConfig {
    /// Session working directory.
    pub cwd: PathBuf,
    /// MCP servers (opaque JSON; usually empty).
    pub mcp_servers: serde_json::Value,
    /// Mode after create: `default` | `accept-edits` | `plan` (via `session/set_mode`).
    pub mode_id: Option<String>,
}

impl AgySessionConfig {
    /// Session in the given cwd.
    pub fn new(cwd: impl Into<PathBuf>) -> Self {
        Self {
            cwd: cwd.into(),
            mcp_servers: serde_json::json!([]),
            mode_id: None,
        }
    }

    /// Plan mode (read-only style).
    pub fn with_plan_mode(mut self) -> Self {
        self.mode_id = Some("plan".into());
        self
    }

    /// Accept-edits mode (auto-approve writes when the agent supports it).
    pub fn with_accept_edits_mode(mut self) -> Self {
        self.mode_id = Some("accept-edits".into());
        self
    }

    /// Default mode (review before writes).
    pub fn with_default_mode(mut self) -> Self {
        self.mode_id = Some("default".into());
        self
    }
}
