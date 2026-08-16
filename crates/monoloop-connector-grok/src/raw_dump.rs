//! Opt-in raw wire dump of what the remote (Grok Build) sends.
//!
//! Captures **exact** inbound WebSocket payload bytes before demux/interpretation.
//! Disabled by default. Never holds transport secrets (auth is outbound-only).
//!
//! Bound every buffer so a dump cannot become an unbounded memory path.

use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Parameters controlling an optional raw inbound dump.
#[derive(Clone, Debug)]
pub struct RawDumpParams {
    /// When false, collector records nothing.
    pub enabled: bool,
    /// Maximum number of frames retained.
    pub max_frames: usize,
    /// Maximum bytes retained per frame (exact prefix if truncated).
    pub max_bytes_per_frame: usize,
    /// Maximum total payload bytes across all frames.
    pub max_total_bytes: usize,
}

impl Default for RawDumpParams {
    fn default() -> Self {
        Self {
            enabled: true,
            max_frames: 10_000,
            max_bytes_per_frame: 1024 * 1024,
            max_total_bytes: 16 * 1024 * 1024,
        }
    }
}

impl RawDumpParams {
    /// Enabled dump with defaults.
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            ..Default::default()
        }
    }

    /// Disabled (no-op collector if used).
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }
}

/// One captured inbound wire frame (exactly as received on the WebSocket, subject to bounds).
#[derive(Clone, Debug)]
pub struct RawDumpFrame {
    /// Monotonic index (0-based order of accepted frames).
    pub index: u64,
    /// Exact retained bytes (may be truncated to `max_bytes_per_frame`).
    pub bytes: Bytes,
    /// True if the frame was truncated to fit bounds.
    pub truncated: bool,
    /// Original wire length before truncation.
    pub original_len: usize,
}

impl RawDumpFrame {
    /// Lossy UTF-8 view for human dump (not a semantic decode).
    pub fn utf8_lossy(&self) -> String {
        String::from_utf8_lossy(&self.bytes).into_owned()
    }

    /// Pretty JSON if the retained bytes are a single JSON value; else `None`.
    pub fn try_pretty_json(&self) -> Option<String> {
        let v: serde_json::Value = serde_json::from_slice(&self.bytes).ok()?;
        serde_json::to_string_pretty(&v).ok()
    }
}

/// Snapshot of a dump for tests / reporting.
#[derive(Clone, Debug, Default)]
pub struct RawDumpSnapshot {
    /// Frames in receive order.
    pub frames: Vec<RawDumpFrame>,
    /// Total original wire bytes observed (including dropped/truncated).
    pub total_original_bytes: u64,
    /// Frames dropped because limits were hit.
    pub frames_dropped: u64,
    /// Whether the dump was enabled.
    pub enabled: bool,
}

impl RawDumpSnapshot {
    /// Concatenate all retained frame bytes (exact retained payloads, in order).
    pub fn concat_bytes(&self) -> Bytes {
        let mut out = Vec::new();
        for f in &self.frames {
            out.extend_from_slice(&f.bytes);
        }
        Bytes::from(out)
    }

    /// Human-readable dump of every frame (exact retained body + metadata).
    pub fn format_text(&self) -> String {
        let mut s = String::new();
        s.push_str(&format!(
            "=== RAW DUMP (enabled={}, frames={}, original_bytes={}, dropped={}) ===\n",
            self.enabled,
            self.frames.len(),
            self.total_original_bytes,
            self.frames_dropped
        ));
        for f in &self.frames {
            s.push_str(&format!(
                "--- frame #{} len={} truncated={} ---\n",
                f.index, f.original_len, f.truncated
            ));
            if let Some(pretty) = f.try_pretty_json() {
                s.push_str(&pretty);
                s.push('\n');
            } else {
                s.push_str(&f.utf8_lossy());
                if !s.ends_with('\n') {
                    s.push('\n');
                }
            }
        }
        s.push_str("=== END RAW DUMP ===\n");
        s
    }

    /// True if any retained frame contains the needle as raw bytes.
    pub fn contains_bytes(&self, needle: &[u8]) -> bool {
        if needle.is_empty() {
            return true;
        }
        self.frames.iter().any(|f| {
            f.bytes.windows(needle.len()).any(|w| w == needle)
        })
    }

    /// True if any frame's UTF-8 lossy text contains `needle`.
    pub fn contains_str(&self, needle: &str) -> bool {
        self.frames
            .iter()
            .any(|f| f.utf8_lossy().contains(needle))
    }

    /// Parse each frame as JSON where possible.
    pub fn json_values(&self) -> Vec<serde_json::Value> {
        self.frames
            .iter()
            .filter_map(|f| serde_json::from_slice(&f.bytes).ok())
            .collect()
    }
}

/// Thread-safe collector of inbound raw frames.
#[derive(Clone)]
pub struct RawDumpCollector {
    params: RawDumpParams,
    frames: Arc<Mutex<Vec<RawDumpFrame>>>,
    total_original: Arc<AtomicU64>,
    total_retained: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
    next_index: Arc<AtomicU64>,
}

impl std::fmt::Debug for RawDumpCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawDumpCollector")
            .field("enabled", &self.params.enabled)
            .field("frames", &self.frames.lock().map(|g| g.len()).unwrap_or(0))
            .finish()
    }
}

impl RawDumpCollector {
    /// Create a collector from params.
    pub fn new(params: RawDumpParams) -> Self {
        Self {
            params,
            frames: Arc::new(Mutex::new(Vec::new())),
            total_original: Arc::new(AtomicU64::new(0)),
            total_retained: Arc::new(AtomicU64::new(0)),
            dropped: Arc::new(AtomicU64::new(0)),
            next_index: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Enabled collector with default bounds.
    pub fn enabled() -> Self {
        Self::new(RawDumpParams::enabled())
    }

    /// Whether recording is active.
    pub fn is_enabled(&self) -> bool {
        self.params.enabled
    }

    /// Record one inbound wire payload **exactly** as received (subject to bounds).
    pub fn record_inbound(&self, payload: &[u8]) {
        if !self.params.enabled {
            return;
        }
        let original_len = payload.len();
        self.total_original
            .fetch_add(original_len as u64, Ordering::Relaxed);

        let mut truncated = false;
        let take = original_len.min(self.params.max_bytes_per_frame);
        if take < original_len {
            truncated = true;
        }

        let mut guard = match self.frames.lock() {
            Ok(g) => g,
            Err(_) => return,
        };

        if guard.len() >= self.params.max_frames {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }

        let retained_so_far = self.total_retained.load(Ordering::Relaxed) as usize;
        let room = self
            .params
            .max_total_bytes
            .saturating_sub(retained_so_far);
        if room == 0 {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let take = take.min(room);
        if take < original_len.min(self.params.max_bytes_per_frame) {
            truncated = true;
        }
        if take == 0 {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }

        let index = self.next_index.fetch_add(1, Ordering::Relaxed);
        let bytes = Bytes::copy_from_slice(&payload[..take]);
        self.total_retained
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        guard.push(RawDumpFrame {
            index,
            bytes,
            truncated,
            original_len,
        });
    }

    /// Snapshot for assertions / printing.
    pub fn snapshot(&self) -> RawDumpSnapshot {
        let frames = self
            .frames
            .lock()
            .map(|g| g.clone())
            .unwrap_or_default();
        RawDumpSnapshot {
            frames,
            total_original_bytes: self.total_original.load(Ordering::Relaxed),
            frames_dropped: self.dropped.load(Ordering::Relaxed),
            enabled: self.params.enabled,
        }
    }

    /// Clear retained frames (does not reset counters unless requested).
    pub fn clear(&self) {
        if let Ok(mut g) = self.frames.lock() {
            g.clear();
        }
    }
}

/// Optional dump handle stored on a live server connection.
#[allow(dead_code)]
pub type SharedRawDump = Option<Arc<RawDumpCollector>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_exact_bytes() {
        let c = RawDumpCollector::enabled();
        let payload = br#"{"method":"session/update","params":{"x":1}}"#;
        c.record_inbound(payload);
        let snap = c.snapshot();
        assert_eq!(snap.frames.len(), 1);
        assert_eq!(&snap.frames[0].bytes[..], payload);
        assert!(!snap.frames[0].truncated);
        assert!(snap.contains_str("session/update"));
    }

    #[test]
    fn respects_frame_cap() {
        let c = RawDumpCollector::new(RawDumpParams {
            enabled: true,
            max_frames: 2,
            max_bytes_per_frame: 1024,
            max_total_bytes: 1024 * 1024,
        });
        c.record_inbound(b"a");
        c.record_inbound(b"b");
        c.record_inbound(b"c");
        let snap = c.snapshot();
        assert_eq!(snap.frames.len(), 2);
        assert_eq!(snap.frames_dropped, 1);
    }

    #[test]
    fn disabled_is_noop() {
        let c = RawDumpCollector::new(RawDumpParams::disabled());
        c.record_inbound(b"hello");
        assert!(c.snapshot().frames.is_empty());
    }
}
