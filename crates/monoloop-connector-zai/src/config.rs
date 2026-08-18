//! Z.ai CLI connector configuration.
//!
//! Headless surface: `zai -p <prompt> --no-color -d <cwd>` prints OpenAI-compatible
//! chat messages as NDJSON on stdout after the agent turn completes. Tools execute
//! inside the CLI (auto-approved in headless mode); Monoloop observes the transcript.

use std::path::PathBuf;
use std::time::Duration;

/// How to launch the Z.ai CLI for one headless prompt.
#[derive(Clone, Debug)]
pub struct ZaiAgentConfig {
    /// Command to run (default: `ZAI_BIN` or `zai` on PATH).
    pub command: PathBuf,
    /// Extra args before the standard headless flags.
    pub extra_args: Vec<String>,
    /// Working directory (`-d`).
    pub cwd: PathBuf,
    /// Optional model (`-m` / `ZAI_MODEL`).
    pub model: Option<String>,
    /// Optional base URL (`-u` / `ZAI_BASE_URL`). Not logged.
    pub base_url: Option<String>,
    /// Max tool rounds (`--max-tool-rounds`).
    pub max_tool_rounds: u32,
    /// Wall-clock deadline for the whole headless process.
    pub run_deadline: Duration,
    /// Max bytes of captured stdout (fail-closed).
    pub max_stdout_bytes: usize,
    /// Optional path to append raw stdout (test diagnostics only).
    pub raw_dump_path: Option<PathBuf>,
}

impl Default for ZaiAgentConfig {
    fn default() -> Self {
        Self {
            command: discover_zai_bin(),
            extra_args: Vec::new(),
            cwd: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            model: std::env::var("ZAI_MODEL").ok().filter(|s| !s.is_empty()),
            base_url: std::env::var("ZAI_BASE_URL").ok().filter(|s| !s.is_empty()),
            max_tool_rounds: 50,
            run_deadline: Duration::from_secs(10 * 60),
            max_stdout_bytes: 8 * 1024 * 1024,
            raw_dump_path: None,
        }
    }
}

impl ZaiAgentConfig {
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

    /// Explicit model id (e.g. `glm-4.6`).
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    /// Build argv for one headless prompt.
    ///
    /// **LAW 16 note:** Z.ai CLI requires `-p <prompt>` (vendor contract). Secrets
    /// MUST NOT appear on argv; API keys stay in the process environment only.
    /// See `DECISIONS.md` (headless CLI prompt surface).
    pub fn argv_for_prompt(&self, prompt: &str) -> Vec<String> {
        let mut args = self.extra_args.clone();
        args.push("-d".into());
        args.push(self.cwd.display().to_string());
        args.push("--no-color".into());
        args.push("--max-tool-rounds".into());
        args.push(self.max_tool_rounds.to_string());
        if let Some(ref m) = self.model {
            args.push("-m".into());
            args.push(m.clone());
        }
        if let Some(ref u) = self.base_url {
            args.push("-u".into());
            args.push(u.clone());
        }
        args.push("-p".into());
        args.push(prompt.to_string());
        args
    }
}

fn discover_zai_bin() -> PathBuf {
    if let Some(bin) = std::env::var_os("ZAI_BIN") {
        return PathBuf::from(bin);
    }
    PathBuf::from("zai")
}
