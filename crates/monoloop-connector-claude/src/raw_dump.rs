//! Optional raw stdout capture (test diagnostics only).

use std::path::PathBuf;
use std::sync::Mutex;

/// Bounded append-only capture of headless stdout lines.
#[derive(Debug, Default)]
pub struct ClaudeRawDump {
    path: Option<PathBuf>,
    lines: Mutex<Vec<String>>,
    max_lines: usize,
}

impl ClaudeRawDump {
    /// Create dump sink; writes to path when finished if set.
    pub fn new(path: Option<PathBuf>, max_lines: usize) -> Self {
        Self {
            path,
            lines: Mutex::new(Vec::new()),
            max_lines: max_lines.max(1),
        }
    }

    /// Record one logical line (direction-tagged for parity with ACP dumps).
    pub fn push_line(&self, direction: &str, line: &str) {
        let mut g = self.lines.lock().unwrap_or_else(|e| e.into_inner());
        if g.len() >= self.max_lines {
            return;
        }
        g.push(format!("{direction} {line}"));
    }

    /// Snapshot as text.
    pub fn text(&self) -> String {
        let g = self.lines.lock().unwrap_or_else(|e| e.into_inner());
        g.join("\n")
    }

    /// Flush to path if configured.
    pub fn flush(&self) {
        if let Some(ref path) = self.path {
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let _ = std::fs::write(path, self.text());
        }
    }
}
