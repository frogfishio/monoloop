# Decisions

Explicit project decisions that change contracts, MSRV, or delivery assumptions.
Normative behavior still lives under `doc/`; this file records *why* a deliberate
change was made.

## D-004 — Sessionless DirectLlm tool envelope SessionKey (D-044)

**Date:** 2026-08-20

**Context:** Empty-tool Loop dispatch under Transaction Runtime v2 must emit
`CanonicalToolResult` / tool lifecycle events that require a `SessionKey`.
DirectLlm admissions often have no external session (no Grok `sessionId`).
Inventing ambient “current session” would violate LAWS 5–7; omitting the field
requires a contracts change to make `SessionKey` optional on tool results.

**Decision:** For **sessionless DirectLlm** (and similar sessionless channels),
tool envelopes MAY use an explicit **transaction-scoped** `SessionKey`:

- `SessionId` = `tx-{transaction_id}` when that forms a valid id, else `direct`
- `ChannelId` = the admitted channel

This key is **not** an external resume identity and MUST NOT be used for
`session/load` or most-recent heuristics. Grok Build and other sessionful
profiles continue to use the authoritative external session id when claimed.

Making `CanonicalToolResult.session_key` optional remains a future option if
hosts prefer absence over a synthetic key; until then the transaction-scoped
key is normative for sessionless paths.

**Consequences:**

- `loop_dispatch::session_key_for` is the intentional implementation of this
  policy (DEFECTS D-044 Fixed).
- Laws 5–7 remain: no ambient current session; no most-recent heuristic; Grok
  correlation id unchanged when a real session exists.

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


## D-003 — Transaction Runtime v2 lifecycle replacement

**Date:** 2026-08-19

**Context:** The v1 transaction lifecycle implementation (`runtime`, `admission`,
`actor`, `finalization`, `callback_service`, `executor_spawn`, `tool_join_vault`)
repeatedly failed adversarial ownership review: non-blocking admission versus
first-poll confirm, fabricated completion waiters mistaken for worker ownership,
capacity leaks on deferred finalization, and shutdown deadlines treated as proof
that arbitrary Rust futures had stopped. Those guarantees cannot all be satisfied
together for in-process futures.

**Decision:**

1. Accept `doc/TRANSACTION_RUNTIME_V2_SPEC.md` as the normative replacement for
   lifecycle, admission, callback, task-ownership, finalization, and shutdown.
2. Do **not** recreate the seven deleted files individually. Replace them with
   `transaction/lifecycle/`.
3. Mark corresponding sections of `TRANSACTION_RUNTIME_DESIGN.md` and
   `TRANSACTION_RUNTIME_IMPLEMENTATION.md` as superseded for those topics.
4. Preserve Connector → Interpreter → Loop, canonical types, Channel identity,
   bounded resources, and provider-neutral tool semantics.
5. Migrate in stages M1–M7 from the v2 spec. Remaining on-disk modules that still
   depend on deleted symbols are deferred (not deleted) until their stage.

**Consequences:**

- Core runtime publishes to concrete mailboxes; arbitrary sinks/callbacks move
  outside the ownership boundary.
- Production bootstrap owns its executor; bare external `Handle` is removed from
  the production constructor (M2).
- Shutdown timeout yields `Quiescing`, never false `Stopped`.
- Only process-isolated work is described as hard-killable.
- M4: connectors return `ConnectionOwnerWork` on `Connector::begin_open`
  (Fake, HTTP, Claude, Z.ai, Codex, Cursor, Agy, Grok). Lifecycle exchange
  registers Connector/Interpreter owners through `TransactionTaskSpawner`.
  ACP ProcessInner pumps use `Weak`+JoinSet; Grok pending
  connect/session/exchange workers abort on Drop; `GrokServerHandle::shutdown`
  joins `run_connection` (D-042 Fixed).
