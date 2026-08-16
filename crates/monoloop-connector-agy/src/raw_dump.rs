//! Optional raw NDJSON capture (test diagnostics only).

use std::path::PathBuf;
use std::sync::Mutex;

/// Bounded collector for exact inbound/outbound NDJSON lines.
#[derive(Debug, Default)]
pub struct AgyRawDump {
    lines: Mutex<Vec<String>>,
    path: Option<PathBuf>,
    max_lines: usize,
}

impl AgyRawDump {
    /// Create an in-memory collector; optionally mirror to a file.
    pub fn new(path: Option<PathBuf>, max_lines: usize) -> Self {
        Self {
            lines: Mutex::new(Vec::new()),
            path,
            max_lines: max_lines.max(1),
        }
    }

    /// Record one complete NDJSON line.
    pub fn push_line(&self, line: impl Into<String>) {
        let line = line.into();
        if let Ok(mut g) = self.lines.lock() {
            if g.len() < self.max_lines {
                g.push(line.clone());
            }
        }
        if let Some(path) = &self.path {
            use std::io::Write;
            if let Ok(mut f) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
            {
                let _ = writeln!(f, "{line}");
            }
        }
    }

    /// Snapshot of captured lines.
    pub fn snapshot(&self) -> Vec<String> {
        self.lines.lock().map(|g| g.clone()).unwrap_or_default()
    }

    /// Join captured lines for file export.
    pub fn as_text(&self) -> String {
        self.snapshot().join("\n")
    }
}
