//! §23 forbidden-pattern search for production lifecycle / tool / MCP paths.
//!
//! Spec: `doc/TRANSACTION_RUNTIME_V2_SPEC.md` §21 / §23 — ambient `tokio::spawn`
//! is forbidden in lifecycle, exchange, tool, MCP, and Connector owner paths
//! without a documented exception.

use std::fs;
use std::path::{Path, PathBuf};

fn crate_src() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src")
}

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

/// True when the line is documentation / attribute prose, not a call site.
fn is_prose_mention(line: &str) -> bool {
    let t = line.trim_start();
    t.starts_with("//")
        || t.starts_with("///")
        || t.starts_with("//!")
        || t.starts_with("#[")
        || t.starts_with("note =")
        || t.contains("\"") && !t.contains("tokio::spawn(") && !t.contains("spawn_blocking(")
}

/// Paths under `src/` that may contain ambient spawn with a documented exception.
fn is_documented_exception(rel: &str, line: &str) -> bool {
    if is_prose_mention(line) {
        return true;
    }
    // Deprecated ambient Loop start (cfg/test or deprecated API).
    if rel.contains("runtime.rs") && line.contains("tokio::spawn(fut)") {
        return true;
    }
    // Deprecated HostToolRuntime::new unit-test path.
    if rel.contains("loop_adapters.rs") && line.contains("tokio::spawn(work)") {
        return true;
    }
    // Standalone McpGateway::bind_loopback (tests); RuntimeOwner uses TaskSupervisor.
    if rel.contains("mcp/gateway.rs") && line.contains("tokio::spawn(prepared.serve())") {
        return true;
    }
    // sticky_cancel unit tests only.
    if rel.contains("sticky_cancel.rs") {
        return true;
    }
    // Test-only JoinOnlySpillInject parks a JoinOnly on the spill (harness).
    if rel.contains("lifecycle/supervisor.rs") && line.contains("tokio::spawn(async move") {
        return true;
    }
    false
}

#[test]
fn s23_no_undocumented_ambient_tokio_spawn_in_production_src() {
    let root = crate_src();
    let mut files = Vec::new();
    collect_rs_files(&root, &mut files);
    assert!(!files.is_empty(), "expected monoloop-loop src files");

    let mut hits = Vec::new();
    for path in &files {
        let rel = path
            .strip_prefix(&root)
            .map(|p| p.to_string_lossy().replace('\\', "/"))
            .unwrap_or_else(|_| path.display().to_string());
        // Lifecycle unit-test module is compiled into the lib; allow its harness spawns.
        if rel.contains("lifecycle/tests.rs") {
            continue;
        }
        let Ok(text) = fs::read_to_string(path) else {
            continue;
        };
        for (idx, line) in text.lines().enumerate() {
            if !(line.contains("tokio::spawn") || line.contains("spawn_blocking")) {
                continue;
            }
            if is_documented_exception(&rel, line) {
                continue;
            }
            hits.push(format!("{rel}:{}: {}", idx + 1, line.trim()));
        }
    }

    assert!(
        hits.is_empty(),
        "undocumented ambient spawn in production src (§21 / §23):\n{}",
        hits.join("\n")
    );
}

#[test]
fn s23_adversarial_host_adapter_suite_present() {
    // §22.7 / §23: host-adapter adversarial proofs exist as an isolated suite.
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/s22_7_host_adapters.rs");
    assert!(
        path.is_file(),
        "expected tests/s22_7_host_adapters.rs for §22.7 adversarial host proofs"
    );
    let text = fs::read_to_string(&path).expect("read s22_7");
    for needle in [
        "s22_7_completion_callback_blocks_before_future",
        "s22_7_completion_future_never_yields",
        "s22_7_event_consumer_stops_draining",
        "s22_7_receivers_dropped_immediately",
        "s22_7_host_adapter_task_destroyed",
    ] {
        assert!(
            text.contains(needle),
            "s22_7 suite missing proof `{needle}`"
        );
    }
}
