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
    // sticky_cancel unit tests only (`#[cfg(test)]` module).
    if rel.contains("sticky_cancel.rs") {
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

/// §23 exact-limit / plus-one inventory (documentation gate — not exhaustive codegen).
///
/// Lists high-value public limits that already have exact/plus-one proofs in-tree.
/// Remaining gaps are documented in DEFECTS (optional exhaustive D-035 variant
/// matrix). This test fails if a listed proof file disappears.
#[test]
fn s23_exact_limit_plus_one_inventory_present() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let required = [
        ("tests/linked_tools.rs", "capacity_limit_plus_one_rejects"),
        ("tests/mcp_gateway.rs", "http_oversized_body_fails_closed"),
        (
            "tests/mcp_gateway.rs",
            "mcp_per_capability_concurrency_plus_one_rejects",
        ),
        (
            "tests/mcp_gateway.rs",
            "mcp_global_concurrency_plus_one_rejects",
        ),
        (
            "tests/mcp_gateway.rs",
            "mcp_request_duration_plus_one_fails_closed",
        ),
        (
            "src/transaction/lifecycle/tests.rs",
            "s22_6_event_byte_plus_one_fails_closed",
        ),
        (
            "src/transaction/lifecycle/tests.rs",
            "s22_6_event_item_plus_one_fails_closed",
        ),
        (
            "src/transaction/lifecycle/tests.rs",
            "d047_full_queue_seal_reports_deadline_not_published",
        ),
        (
            "src/transaction/lifecycle/tests.rs",
            "capacity_plus_one_rejects",
        ),
        (
            "src/transaction/lifecycle/tests.rs",
            "max_distinct_sessions_exact_admits_plus_one_rejects",
        ),
        (
            "src/transaction/lifecycle/tests.rs",
            "external_agent_claim_time_distinct_sessions_plus_one_limit_exceeded",
        ),
        (
            "src/transaction/lifecycle/tests.rs",
            "concurrent_global_capacity_exhaustion_admits_exactly_max",
        ),
        (
            "src/transaction/lifecycle/tests.rs",
            "concurrent_per_channel_capacity_exhaustion_admits_exactly_channel_max",
        ),
        (
            "src/transaction/lifecycle/tests.rs",
            "start_queue_full_rolls_back_all_permits",
        ),
        // D-033 lives in monoloop-connector (sibling crate).
        (
            "../monoloop-connector/tests/streaming_http.rs",
            "absolute_request_deadline_covers_header_and_body_delay",
        ),
        (
            "../monoloop-connector/tests/streaming_http.rs",
            "full_output_queue_terminates_at_overall_deadline",
        ),
        (
            "../monoloop-connector/tests/streaming_http.rs",
            "max_queued_output_bytes_plus_one_fails_closed",
        ),
        (
            "../monoloop-connector/tests/streaming_http.rs",
            "blocked_enqueue_honors_idle_before_overall_deadline",
        ),
        (
            "src/transaction/lifecycle/tests.rs",
            "max_messages_plus_one_rejected_at_admit",
        ),
        (
            "src/transaction/lifecycle/tests.rs",
            "max_messages_exact_admits_plus_one_rejects",
        ),
        (
            "src/transaction/lifecycle/tests.rs",
            "submit_versus_shutdown_barrier_race_two_outcomes",
        ),
        (
            "src/transaction/lifecycle/tests.rs",
            "submit_versus_shutdown_hang_barrier_both_outcomes",
        ),
        (
            "src/transaction/lifecycle/tests.rs",
            "max_input_bytes_plus_one_rejected_at_admit",
        ),
        (
            "src/transaction/lifecycle/tests.rs",
            "max_content_parts_plus_one_rejected_at_admit",
        ),
        (
            "src/transaction/lifecycle/tests.rs",
            "max_content_parts_exact_admits_plus_one_rejects",
        ),
        (
            "src/transaction/lifecycle/tests.rs",
            "max_tools_per_transaction_exact_admits_plus_one_rejects",
        ),
        (
            "src/transaction/lifecycle/tests.rs",
            "max_input_bytes_exact_admits_plus_one_rejects",
        ),
        (
            "src/transaction/lifecycle/tests.rs",
            "multi_channel_multi_session_concurrent_load",
        ),
        (
            "../monoloop-connector-grok/tests/grok_connector.rs",
            "concurrent_session_new_and_explicit_load",
        ),
        (
            "src/transaction/lifecycle/tests.rs",
            "large_tool_arguments_counted_toward_max_input_bytes",
        ),
        (
            "../monoloop-contracts/src/input.rs",
            "estimate_counts_names_ids_and_tool_arguments",
        ),
        (
            "src/transaction/lifecycle/tests.rs",
            "runtime_owner_drop_joins_executor_thread_reaches_stopped",
        ),
        (
            "../monoloop-connector/tests/streaming_http.rs",
            "cancel_interrupts_blocked_output_enqueue",
        ),
        (
            "src/transaction/lifecycle/tests.rs",
            "s22_6_concurrent_producers_contiguous_sequence",
        ),
        // D-042: Refreshable deferred — profile gate must keep asserting it.
        (
            "../monoloop-testkit/tests/profile_bindings.rs",
            "MUST NOT declare Refreshable",
        ),
    ];
    for (rel, needle) in required {
        let path = root.join(rel);
        assert!(path.is_file(), "missing limit-proof file {rel}");
        let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {rel}: {e}"));
        assert!(
            text.contains(needle),
            "limit-proof `{needle}` missing from {rel}"
        );
    }
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

/// §23: adversarial lifecycle tests run in isolated subprocesses with an outer
/// harness timeout (never shape a missing proof into a green pass).
#[test]
fn s23_adversarial_lifecycle_subprocess_harness_inventory() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // JoinOnly harness must assert TaskSupervisor ownership (not spill_pending).
    {
        let path = root.join("tests/s22_4_join_only_spill_sacrificial.rs");
        let text = fs::read_to_string(&path).expect("read join_only sacrificial");
        assert!(
            text.contains("owned_tasks"),
            "JoinOnly sacrificial must assert TaskSupervisor owned_tasks"
        );
        assert!(
            !text.contains("spill_pending="),
            "JoinOnly sacrificial must not require spill_pending (M5.4 delete-vaults)"
        );
    }
    let harnesses = [
        (
            "tests/s22_3_non_yielding_sacrificial.rs",
            "s22_3_non_yielding_sacrificial_never_false_stopped",
            "MONOLOOP_S22_3_NON_YIELDING_CHILD",
            "recv_timeout",
        ),
        (
            "tests/s22_4_join_only_spill_sacrificial.rs",
            "s22_4_join_only_spill_sacrificial_never_false_stopped",
            "MONOLOOP_S22_4_JOIN_ONLY_SPILL_CHILD",
            "recv_timeout",
        ),
    ];
    for (rel, test_fn, child_env, timeout_api) in harnesses {
        let path = root.join(rel);
        assert!(path.is_file(), "missing subprocess harness file {rel}");
        let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {rel}: {e}"));
        assert!(
            text.contains(test_fn),
            "harness `{rel}` missing test `{test_fn}`"
        );
        assert!(
            text.contains(child_env),
            "harness `{rel}` missing child env `{child_env}`"
        );
        assert!(
            text.contains(timeout_api),
            "harness `{rel}` must bound the parent wait with `{timeout_api}`"
        );
        assert!(
            text.contains("child.kill()") || text.contains("child.kill();"),
            "harness `{rel}` must kill the sacrificial child"
        );
        assert!(
            text.contains("never false") || text.contains("false Stopped"),
            "harness `{rel}` must document fail-closed / never-false-Stopped intent"
        );
    }
}
