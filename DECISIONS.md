# Decisions

Explicit project decisions that change contracts, MSRV, or delivery assumptions.
Normative behavior still lives under `doc/`; this file records *why* a deliberate
change was made.

## D-001 — Raise workspace MSRV to 1.88 (WP-00)

**Date:** 2026-08-17

**Context:** TransactionRuntime MCP gateway is specified to use the maintained
`rmcp` Streamable HTTP SDK (`doc/TRANSACTION_RUNTIME_IMPLEMENTATION.md` §10,
`doc/TRANSACTION_RUNTIME_DELIVERY_PLAN.md` WP-00 / WP-07). `rmcp` 3.1.x declares
`rust-version = "1.88"` and uses edition 2024 internally.

**Decision:** Raise workspace `rust-version` from `1.75` to `1.88`. Do **not**
replace `rmcp` with a partial hand-rolled MCP protocol stack.

**Consequences:**

- CI and developer toolchains must be ≥ 1.88 (current verification host: 1.92).
- Other WP-00 deps (`jsonschema` 0.49, `axum` 0.8, `reqwest` 0.13) fit under 1.88.
- Product crates remain `edition = "2021"`; only the dependency crate uses 2024.

**Evidence:** `doc/WP00_BASELINE_EVIDENCE.md`.
