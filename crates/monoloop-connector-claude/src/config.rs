//! Claude Code CLI connector configuration.
//!
//! Headless surface:
//! `claude -p <prompt> --output-format stream-json --verbose [--dangerously-skip-permissions]`
//! emits NDJSON events on stdout (assistant / tool_use / tool_result / result).
//! Tools execute inside Claude Code; Monoloop observes the stream.

use std::path::PathBuf;
use std::time::Duration;

/// How to launch Claude Code for one headless print run.
#[derive(Clone, Debug)]
pub struct ClaudeAgentConfig {
    /// Command to run (default: `CLAUDE_BIN` or `claude` on PATH).
    pub command: PathBuf,
    /// Extra args before the standard headless flags.
    pub extra_args: Vec<String>,
    /// Working directory for the process (`cwd` of the agent).
    pub cwd: PathBuf,
    /// Optional model override (`--model`).
    pub model: Option<String>,
    /// When true, pass `--dangerously-skip-permissions` (test sandboxes only).
    pub skip_permissions: bool,
    /// Wall-clock deadline for the whole headless process.
    pub run_deadline: Duration,
    /// Max bytes of captured stdout (fail-closed).
    pub max_stdout_bytes: usize,
    /// Optional path to append raw stdout (test diagnostics only).
    pub raw_dump_path: Option<PathBuf>,
}

impl Default for ClaudeAgentConfig {
    fn default() -> Self {
        Self {
            command: discover_claude_bin(),
            extra_args: Vec::new(),
            cwd: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            model: std::env::var("CLAUDE_MODEL").ok().filter(|s| !s.is_empty()),
            skip_permissions: false,
            run_deadline: Duration::from_secs(15 * 60),
            max_stdout_bytes: 16 * 1024 * 1024,
            raw_dump_path: None,
        }
    }
}

impl ClaudeAgentConfig {
    /// Config for a project directory.
    pub fn for_project(cwd: impl Into<PathBuf>) -> Self {
        Self {
            cwd: cwd.into(),
            ..Default::default()
        }
    }

    /// Attach a raw stdout dump path.
    pub fn with_raw_dump(mut self, path: impl Into<PathBuf>) -> Self {
        self.raw_dump_path = Some(path.into());
        self
    }

    /// Explicit model id.
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    /// Enable permission bypass for unattended tool runs (test sandboxes only).
    pub fn with_skip_permissions(mut self) -> Self {
        self.skip_permissions = true;
        self
    }

    /// Build argv for one headless print prompt.
    ///
    /// Prompt is the positional argument after flags (Claude CLI contract).
    pub fn argv_for_prompt(&self, prompt: &str) -> Vec<String> {
        let mut args = self.extra_args.clone();
        args.push("-p".into());
        args.push("--output-format".into());
        args.push("stream-json".into());
        args.push("--verbose".into());
        if self.skip_permissions {
            args.push("--dangerously-skip-permissions".into());
        }
        if let Some(ref m) = self.model {
            args.push("--model".into());
            args.push(m.clone());
        }
        // Positional prompt last.
        args.push(prompt.to_string());
        args
    }
}

fn discover_claude_bin() -> PathBuf {
    if let Some(bin) = std::env::var_os("CLAUDE_BIN") {
        return PathBuf::from(bin);
    }
    PathBuf::from("claude")
}
