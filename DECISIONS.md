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

## D-002 — Headless CLI prompt argv exception (LAW 16 clarification)

**Date:** 2026-08-18

**Context:** Advisor review found Law 16 (“Prompts never go on process argv”)
colliding with Z.ai (`zai -p <prompt>`) and Claude Code (`claude -p … <prompt>`)
vendor CLI contracts. Those profiles are WP-11 deliverables and default workspace
members. Passing secrets via `-k` was also possible in Z.ai (`pass_api_key_flag`).

**Decision:**

1. Clarify Law 16: Grok Build path remains **absolute** no-argv for prompts and
   secrets. Headless CLI profiles MAY put the **prompt only** on argv when the
   vendor CLI requires it, recorded here.
2. **Remove** Z.ai `pass_api_key_flag` / `-k` argv secret injection. API keys stay
   in process environment for the child CLI only.
3. In-CLI tool execution (Z.ai/Claude auto-approve headless) remains a documented
   non-responsibility leak: Monoloop EmptyToolRegistry is zero-effect inside the
   kernel; the spawned CLI may still perform tools. Callers must treat those
   Channels as observational streams, not Monoloop-authorized tool runtimes.
4. Non-loopback Grok endpoints require **`wss`** even when `allow_non_loopback`
   is true (authenticated transport policy, not a boolean alone).

**Consequences:**

- Silver Fake/OpenAI path unchanged.
- Six-profile “release candidate” is not Golden until CLI profiles gain
  non-argv prompt transport or hosts accept the documented exception.
- Architecture still allows profile crates to depend on Loop for
  `ChannelBinding` construction (accepted coupling; not independently
  testable Connector-only packages).

**Evidence:** Law 16 text in `rules/LAWS.md`; Z.ai/Claude config docs;
`GrokServerConfig::validate_endpoint_security`.

## D-00x: AGPL-3.0-or-later + commercial dual licensing

**Date:** 2026-08-18

Workspace crates publish under SPDX `AGPL-3.0-or-later` (see root `LICENSE`).
A commercial license is offered separately at https://frogfish.io
(`LICENSE-COMMERCIAL.md`, `LICENSING.md`). Cargo.toml no longer uses
`MIT OR Apache-2.0`. External contributions are not accepted (`CONTRIBUTING.md`)
so ownership for commercial licensing stays clear.

Versioning uses root `VERSION` + `BUILD` with `make bump` / `make dist`
(see `LICENSING.md`, `PUBLISHING.md`).

