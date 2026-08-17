# WP-00 — Baseline and dependency qualification evidence

**Work package:** WP-00 (`doc/TRANSACTION_RUNTIME_DELIVERY_PLAN.md`)  
**Date:** 2026-08-17  
**Host toolchain:** `rustc 1.92.0`, `cargo 1.92.0`  
**Workspace MSRV after WP-00:** `1.88` (see `DECISIONS.md` D-001)

## 1. Baseline suite (pre-feature)

Recorded before product behavior changes. Commands run at workspace root.

| Check | Command | Result |
|---|---|---|
| Tests | `cargo test --workspace --all-targets` | **PASS** (all crates; no failures) |
| Clippy | `cargo clippy --workspace --all-targets -- -D warnings` | **PASS** (exit 0) |
| Docs | `cargo doc --workspace --no-deps` | **PASS** (exit 0) |

Pre-existing failures: **none**. Suite green; no failure assignment needed.

Approximate product/test counts observed in baseline run: connector Fake (11),
Grok integration (7), interpreter unit+fixtures (~41), empty loop (4), testkit
pipeline/projection (~39), plus small connector unit suites. Example targets
compile with 0 tests each.

### Post-WP-00 suite (after deps + spike)

Same three commands re-run after MSRV raise, workspace deps, and
`rmcp_loopback_spike` tests: **all PASS** (includes +2 spike tests).

## 2. Selected dependencies

Declared under `[workspace.dependencies]` in root `Cargo.toml`, pinned by
`Cargo.lock` after first resolve.

| Crate | Locked version | Features (enabled) | Default features | License (crate metadata) | Declared MSRV |
|---|---|---|---|---|---|
| `reqwest` | 0.13.4 | `rustls`, `json`, `stream`, `http2` | **off** (no native-tls default) | MIT OR Apache-2.0 | 1.64 |
| `rmcp` | 3.1.2 | `server`, `macros`, `transport-streamable-http-server` | **off** | Apache-2.0 | **1.88** |
| `axum` | 0.8.9 | `http1`, `tokio` | **off** | MIT | 1.80 |
| `tower` | 0.5.3 | (workspace pin; available if integration needs) | **off** | MIT | (crate default) |
| `jsonschema` | 0.49.9 | none extra | **off** (no HTTP schema fetch) | MIT | 1.85 |
| `secrecy` | 0.10.3 | `serde` | default | Apache-2.0 OR MIT | 1.60 |
| `rand` | 0.9.5 | `std`, `std_rng`, `os_rng` | **off** then those | MIT OR Apache-2.0 | 1.63 |
| `getrandom` | (workspace pin 0.3; also 0.2/0.4 via tree) | default | default | MIT OR Apache-2.0 | (CSPRNG) |

### Streamable HTTP / Rustls

- MCP server path: `rmcp` feature `transport-streamable-http-server` (includes
  `server-side-http` + session transport). Spike binds via `axum::serve` on
  loopback; `StreamableHttpServerConfig` default `allowed_hosts` is loopback-only.
- HTTP client path (future generic connector): `reqwest` with **Rustls only**
  (`default-features = false` + `rustls`). No `native-tls` feature enabled at
  workspace pin.

### Duplicate dependency impact

Expect multiple `getrandom`/`rand` major lines via transitive crates (Tokio
ecosystem + `rmcp` optional `rand` 0.10). Acceptable for WP-00; re-check after
full TransactionRuntime feature graph in WP-07/09. No second TLS stack intended
from workspace pins (reqwest rustls-only).

### MSRV decision

Maintained `rmcp` 3.1.x requires Rust **1.88**. Per implementation contract,
raise workspace MSRV rather than hand-roll MCP. Documented as **D-001**.

No unresolved license or security blocker identified for the pinned set at
qualification time (standard permissive licenses; no GPL in selected direct deps).

## 3. `rmcp` loopback spike

| Item | Detail |
|---|---|
| Location | `crates/monoloop-loop/tests/rmcp_loopback_spike.rs` |
| Scope | Construct `StreamableHttpService`, nest under `/mcp`, bind `127.0.0.1:0`, cancel, join |
| Product status | **Spike only** — not production `McpGateway` (WP-07) |
| Empty handler | `EmptySpikeHandler` implements `ServerHandler` with no tools |
| Extra compile check | `wp00_selected_deps_typecheck` exercises reqwest/secrecy/jsonschema/rand |

Promote or delete this spike when WP-07 lands a tested production seam.

## 4. Profile capability worksheet

See `doc/WP00_PROFILE_CAPABILITY_WORKSHEET.md`:

- Six profiles with session create/load, MCP mode, loopback reachability,
  exchange mode, continuation candidates.
- Bidirectional profiles assigned SendAndRetain qualification (WP-05 + WP-11).
- Prompt-shortcut inventory PS-01…PS-07.
- Component acceptance gaps listed without relabelling complete.

## 5. Exit gate checklist

| Gate | Status |
|---|---|
| Selected dependencies compile at workspace MSRV | **PASS** (MSRV 1.88; spike + typecheck tests) |
| No unresolved license/security/MSRV blocker | **PASS** (MSRV raised via D-001) |
| Every profile has evidence-backed capability declarations | **PASS** (worksheet) |
| Bidirectional profiles assigned SendAndRetain work | **PASS** (worksheet table) |
| Baseline suite green or failures recorded | **PASS** (green) |

## 6. Recommended PR packaging

PR 01: dependency qualification, capability evidence, MSRV decision, spike test —
**no TransactionRuntime product behavior**.
