# Decisions

Explicit project decisions that change contracts, MSRV, or delivery assumptions.
Normative behavior still lives under `doc/`; this file records *why* a deliberate
change was made.

## D-064 — `DirectLlm` Channels can resolve endpoint + credential per transaction

**Date:** 2026-08-25

**Context:** Found while wiring real LLM-provider support into Tinker
(frogfish.io product). Tinker needed one Channel per OpenAI-compatible
provider (OpenAI, xAI, Groq, …) purely because `ChannelBinding.endpoint_ref`
and `credential_ref` are fixed at Channel construction — `StartedRuntime`
takes its Channel list once, at process startup, and there is no way to add
one later. For 13+ providers that meant either restarting the process to
pick up every newly configured provider, or registering all known presets
speculatively up front as a workaround. Neither is what "Direct" should
require: unlike `ExternalAgent` Channels (Grok, Cursor, Codex, Claude — each
a genuinely distinct stateful protocol, where "one Channel = one fixed
backend" is forced by the protocol), `DirectLlm`/HTTP backends speaking the
*same* wire format differ only in endpoint + credential — plain
configuration, not protocol identity. `ChannelBinding`'s shape was inherited
from the stateful connectors and never given a more flexible variant for the
genuinely stateless HTTP case.

Investigated whether the existing `SessionConfig.extensions` mechanism (used
for e.g. Grok's per-turn `cwd`, D-063 era work) could carry a "which
backend" selector instead. It cannot: `OpenAiChatCompletionsEncoder`'s
`encode_openai_extensions` (D-023: encode-or-reject) validates every
extension against a closed allowlist of actual Chat Completions body fields
(`seed`, `user`, `top_p`, …) — a routing selector has no representation
there and would hard-fail the encode. Extensions are for wire-visible
per-turn data; connector routing needed a separate, connector-only channel
that never reaches the encoder.

Also found: `DirectLlm` transactions never receive a `SessionAttachment` at
all — `coordinator.rs`'s DirectLlm branch calls `run_direct_llm_exchange`
with `session_attachment: None` unconditionally (only the `ExternalAgent`
branch, via a claimed SessionKey, ever populates one). So even
connector-only data had no channel-agnostic path to `Connector::begin_open`
before this decision — `EffectiveConfig` (which does carry the merged
`SessionConfig` in its own `.session` field) was never forwarded into
`OpenConnection` at all.

**Decision:** Add a genuinely new, connector-only, channel-agnostic path:

- `SessionConfig` gains `connector_ref: Option<String>` — opaque, never
  merged into `EffectiveConfig.extensions`, never wire-visible, never
  subject to D-023 encode-or-reject.
- `OpenConnection` gains `session_config: SessionConfig` — always present
  (default when unset), populated in `run_inner`/`run_encoded_exchange`
  from `EffectiveConfig.session` for *every* Channel kind, not gated behind
  `SessionAttachment`. This is the fix for the "DirectLlm never gets
  per-transaction data" gap above.
- New `monoloop_connector::ConnectorTargetResolver` trait (sibling to
  `CredentialResolver`): `resolve(connector_ref) -> {endpoint, credential}`.
- `StreamingHttpConnectorFactory::new_dynamic` / `StreamingHttpConnector::
  try_new_dynamic`: when the submitting transaction sets
  `session_config.connector_ref`, `open_http` resolves endpoint +
  credential together through the new resolver, overriding the Channel's
  fixed `endpoint_ref`/`credential_ref` for that connection only. A
  transaction that leaves `connector_ref` unset gets the original fixed
  behavior exactly — `new`/`try_new` (no resolver) are untouched and still
  the right choice for a Channel that only ever needs one backend.

**Consequences:**

- `crates/monoloop-contracts/src/config.rs`: `SessionConfig::connector_ref`.
- `crates/monoloop-connector/src/open.rs`: `OpenConnection::session_config`
  + `with_session_config`.
- `crates/monoloop-connector/src/credential.rs`: `ConnectorTargetResolver`,
  `ResolvedConnectorTarget`.
- `crates/monoloop-connector/src/http.rs`: `StreamingHttpConnector::
  try_new_dynamic`, `StreamingHttpConnectorFactory::new_dynamic`, `open_http`
  resolves dynamically when both a resolver and a `connector_ref` are present.
- `crates/monoloop-loop/src/transaction/lifecycle/exchange.rs`:
  `run_encoded_exchange` takes `session_config: SessionConfig` explicitly
  (it has no `EffectiveConfig` in scope — only pre-encoded bytes); both
  callers (`run_inner`, `run_direct_llm_continuation`) pass
  `config.session.clone()`.
- New proof: `crates/monoloop-loop/tests/direct_llm_openai_e2e.rs`
  `one_dynamic_channel_routes_by_connector_ref_per_transaction` — one
  Channel, two independent mock servers, each turn's `connector_ref`
  deterministically reaches the right one.
- Fully backward compatible: no existing `OpenConnection`/`SessionConfig`
  construction site is exhaustive (all use `::new`/`..Default::default()`),
  and `new`/`try_new` callers are unaffected — this is additive.
- Verified: `make gates` green (fmt / clippy `-D warnings` / test
  `--all-targets --all-features` / rustdoc `-D warnings`), full suite 62/62
  binaries, including the new test.
- Does not change `ExternalAgent` Channels (Grok, Cursor, Codex, Claude) at
  all — `SessionAttachment` and the fixed-backend model remain exactly
  right for those; this decision only gives `DirectLlm` an escape hatch it
  was missing.

## D-063 — `bind_session` rejected a transaction's own admission-time SessionKey

**Date:** 2026-08-25

**Context:** Discovered downstream of D-061, in a Tinker (frogfish.io product)
debugging session against real Grok Build 1.0.5. After the D-061-adjacent
`session/load` field fix landed product-side (matching `session/new`'s
`mcpServers` + `_meta.yoloMode`), `session/load` started returning `ok`, but
the *second* turn of any resumed ExternalAgent conversation still ended
`TransactionEndKind::InvariantFailed`, with no `session/prompt` ever sent.
Confirmed via an isolated `LoopHost`-only spike (no product integration code)
that the failure is instant and does **not** clear with a real 10s delay
between turns — ruling out a teardown race.

Root cause: `admission::admit` (`insert_queued`) already reserves the
`SessionKey` in `LifecycleLedger::by_session` for any resumed submission
(`TransactionSubmitRequest.session_id: Some(..)`), bound to the *new*
transaction's own id, before the coordinator ever runs. The coordinator's
claim-time `LifecycleLedger::bind_session` call — once the external session
is actually established — found that same key already present and rejected
with `SessionAlreadyActive` unconditionally, never checking whether the
existing holder was itself. Every resumed ExternalAgent transaction rejected
its own admission-time reservation, deterministically, every time.

**Decision:** Fix `bind_session` to treat "already bound to this same
transaction" as a no-op success; only reject when the existing holder is a
*different* transaction. This preserves every existing protection
(concurrent-duplicate-session admission rejection, claim-time
`DistinctSessionsExceeded`, cross-transaction `SessionAlreadyActive`) — none
of those paths involve a transaction re-confirming its own reservation.

**Consequences:**

- `crates/monoloop-loop/src/transaction/lifecycle/ledger.rs`: `bind_session`
  now compares the existing holder against `id` before rejecting.
- New regression test:
  `transaction::lifecycle::tests::mcp_external::external_agent_resume_of_known_session_completes`.
  Uses a new `FakeSessionAdapterConfig::pre_registered_sessions` hook rather
  than a live create-then-resume pair — the Fake adapter's create path only
  ever registers a *provisional* placeholder id (see `run_attach`'s create
  branch in `monoloop-connector/src/fake_session.rs`) and has no step that
  re-registers the real provider-assigned id afterward, so a literal
  create-then-resume through the Fake harness fails at `begin_attach`'s
  "known" lookup for an unrelated reason (untouched by this fix — see the gap
  noted below).
- Verified: full `make test` (`cargo test --workspace --all-targets
  --all-features`) green (62/62 binaries) after the fix; the new test fails
  with `InvariantFailed` against the pre-fix code and passes after.
- Also live-verified against real Grok Build 1.0.5 through Tinker's own
  `LoopHost` (product wiring, not this repo) once this fix was applied
  locally.
- **Adjacent gap noted, not fixed here:** the Fake `SessionAdapter`'s create
  path (`monoloop-connector/src/fake_session.rs`, `run_attach`) never
  re-registers the real provider-assigned session id after a successful
  create — only a provisional placeholder ever lands in the session table.
  This mirrors a separate, also-unfixed gap in
  `monoloop-connector-grok`'s `GrokSessionAdapter::remember()` (dead code,
  `#[allow(dead_code)]`, never called): the "known" map for resumed loads is
  never actually populated from a real create either. Neither gap caused
  this defect (this fix does not depend on either), but both are honest
  residuals worth a follow-up decision.

## D-062 — crates.io 0.1.2 Silver / Golden-ready package

**Date:** 2026-08-24

**Context:** Transaction Runtime Golden-ready Silver residuals were accepted as
Silver (Expert+Advisor PASS; Sign-off / §25 / D-025 still unsigned). Workspace
was already at crates.io `0.1.1`; a new registry version is required to ship the
accepted tree.

**Decision:** Publish workspace **0.1.2** as the Silver / Golden-ready package.
Quality tier remains **Silver / Golden-ready — Not Golden**. Do not treat this
publish as D-025 Sign-off or §25 complete.

**Consequences:**

- `VERSION` / workspace Cargo.toml → `0.1.2`; `BUILD` incremented by `make dist`.
- Open residuals unchanged: D-025 Sign-off, D-058, D-059, `cleanup_deadline`
  Partial, D-061 live `session/load`.

## D-061 — Live Grok `session/load` after short sessions (agent residual)

**Date:** 2026-08-24

**Context:** `live_grok_multi_session` proves concurrent `session/new` + marker
isolation on preauthorized hosts (default secret `monoloop-live-test`). Explicit
`session/load` of a just-finished short session returns ACP `-32602 Invalid
params` on current Grok Build, even with `cwd` set. Mock
`concurrent_session_new_and_explicit_load` remains green.

**Decision:** Treat live `session/load` after ephemeral short prompts as an
**agent precondition residual**, not a monoloop encoding defect, until a
durable-session load fixture succeeds against live Grok. Concurrent new +
isolation remains the landed live multi-session qualification. Do not claim
live load Golden.

**Consequences:**

- Evidence pack / checklist name the residual honestly.
- Follow-up: longer-lived session + load, or agent-side param clarification.

## D-060 — D-054 compatibility-alias breaking cut (deprecated surfaces removed)

**Date:** 2026-08-24

**Context:** D-054 Silver retained an explicit M7.3 compatibility phase:
deprecated `TransactionRequest` / `TransactionRuntime` / `RuntimeToolSpill`,
plus host `adapt_*` bridges. Inventory
`doc/D054_COMPATIBILITY_ALIAS_INVENTORY.md` confirmed no live `impl` of
`TransactionRuntime` and no production submit of `TransactionRequest`.

**Decision:** Execute the **breaking cut** for deprecated-only surfaces:

- **Removed:** `TransactionRequest`, `TransactionRuntime` trait,
  `RuntimeToolSpill` type alias, empty `HostCompletionAdapter` /
  `HostEventAdapter` markers, public `reap_vault` / orphan `reap_finished`
  no-ops (M5.4 vault-name leftovers).
- **Retained (documented host helpers, not deprecated):**
  `adapt_event_sink` / `adapt_completion_callback`, and host traits
  `TransactionEventSink` / `CompletionCallback` / `Fn*` adapters — still
  outside the kernel executor (M1 / §22.7).
- Production submit remains `StartedRuntime` /
  `TransactionRuntimeHandle::submit(TransactionSubmitRequest)`.

**Consequences:**

- Public API break for any external crate still naming the removed symbols
  (workspace had none). Prefer a crate major bump if published.
- D-054 Golden residual “breaking cut” is **closed** for the declared
  deprecated set; `adapt_*` stay until a later decision moves them out of
  `monoloop-loop`.
- Inventory file updated to “cut executed (D-060)”.

## D-059 — `callback_deadline` deferred (no core callback wait under M7 push completion)

**Date:** 2026-08-24

**Context:** `TransactionLimits.callback_deadline` validates nonzero only. Core
completion is push oneshot (`TransactionCompletionSender`); M7 removed
host `CompletionCallback` from the core submit API. There is no production
wait/join on a host callback that would honor this duration. Inventing a
callback wait solely to green a Covered cell would be shaped-done.

**Decision:** **Defer** `callback_deadline` as a product-enforced bound:

- Field retained for validate nonzero / ABI / future host-adapter budgets.
- Matrix row stays **Open** with this deferral note (Golden residual).
- A superseding decision is required before Covered: a real core or
  documented host-adapter wait site that reads the runtime field.

**Consequences:**

- Same honesty class as D-058 (diagnostics) — Open + DECISIONS, not Covered.
- Host adapters outside the kernel that impose their own callback budgets
  MUST document them separately; they are not this field until wired.

## D-058 — `max_diagnostic_*` deferred until production `TransactionDiagnostic` emission

**Date:** 2026-08-24

**Context:** `TransactionLimits.max_diagnostic_count` / `max_diagnostic_bytes`
exist and validate nonzero. Ledger carries `diagnostic_count` (always 0).
`build_completion` / `end_event` always pass `diagnostics: Vec::new()`.
Production publishes interpreter units as
`TransactionEventPayload::CanonicalUnit` (including
`CanonicalUnit::Diagnostic`); the separate
`TransactionEventPayload::Diagnostic(TransactionDiagnostic)` path is
test-injected only. `SafeDiagnostic::try_new_default` uses
`TransactionLimits::default().max_diagnostic_bytes`, not the runtime field.

**Decision:** **Defer** wiring these fields as Covered product bounds:

- Do **not** invent a shaped emission path solely to green a matrix cell.
- When Loop gains a real `TransactionDiagnostic` emission / retention path,
  enforce count + message bytes from the runtime `TransactionLimits` and add
  exact/plus-one Covered needles in a superseding decision.
- Matrix rows stay **Open** with this deferral note (still Golden residuals).

**Consequences:**

- §23 honesty: Open ≠ Covered; D-058 records deliberate non-invention.
- Hosts must not assume runtime truncation/count today beyond
  `SafeDiagnostic` constructors they call themselves.

## D-057 — `max_actor_command_bytes` retired (closed-enum control channel)

**Date:** 2026-08-24

**Context:** Spec text historically paired `max_actor_commands` with
`max_actor_command_bytes` for an actor `command_rx`. Production maps
`max_actor_commands` to the supervisor control `mpsc` (D-015). Control
messages are the closed enum `ControlCommand` (`Cancel` / `ForceTerminate` /
`BeginShutdown` / `StopSupervisor`) — fixed-size identities, no payload bytes.
A byte capacity on that queue has no honest use site; inventing byte
accounting would be shaped-done.

**Decision:** **Retire** `TransactionLimits.max_actor_command_bytes` as a
product-enforced bound:

- Item capacity **`max_actor_commands`** remains the control-queue product limit
  (Covered).
- `max_actor_command_bytes` stays on the struct for validate nonzero / ABI
  stability but **MUST NOT** be treated as an enqueue byte budget until a
  future decision introduces payload-bearing control messages.
- §23 matrix status **Retired** (D-057); no Covered needle required.

**Consequences:**

- Golden residual list treats Retired as decision-closed, not Open.
- Reintroducing a byte bound requires a superseding DECISIONS entry and a
  real payload-bearing control/command channel.

## D-056 — `TransactionLimits.max_tool_schema_bytes` enforced at `StartedRuntime::start`

**Date:** 2026-08-24

**Context:** `HostToolRegistry::build` rejected schemas over a hardcoded
`64 * 1024`, matching the `TransactionLimits` default but not reading the
runtime field. Matrix correctly listed `max_tool_schema_bytes` as Open.

**Decision:** Keep a construction hygiene check in `HostToolRegistry::build`
against `TransactionLimits::default().max_tool_schema_bytes`. Additionally,
`StartedRuntime::start` re-validates every registered tool schema against the
bootstrap `transaction_limits.max_tool_schema_bytes` and fails closed with
`StartupError::InvalidConfig("tool schema exceeds max_tool_schema_bytes")`.

**Consequences:**

- Tighter runtime ceilings cannot be bypassed by building the registry under
  the default 64 KiB construction limit.
- Covered needle:
  `transaction_limits_max_tool_schema_bytes_exact_admits_plus_one_rejects`.

## D-055 — `TransactionLimits.max_event_queue*` are runtime ceilings over caller `DeliveryLimits`

**Date:** 2026-08-24

**Context:** Push delivery builds the event mailbox in
`transaction_delivery(DeliveryLimits)` before `submit`. `TransactionLimits`
also named `max_event_queue` / `max_event_queue_bytes`, but those fields were
validate-only while enqueue enforcement lived solely on the caller-built
ports (`s22_6_event_*`). Matrix honesty correctly marked them Open (unwired).

**Decision:** Keep caller-built mailboxes. Wire the TransactionLimits fields as
**admission ceilings**: reject `InvalidConfiguration` when
`delivery.event_tx.max_event_items() > max_event_queue` or
`max_event_bytes() > max_event_queue_bytes`. Enqueue fail-closed remains on
`DeliveryLimits` (unchanged).

**Consequences:**

- Hosts may choose any DeliveryLimits up to the runtime ceiling.
- Exact/plus-one Covered cells:
  `transaction_limits_max_event_queue_exact_admits_plus_one_rejects`,
  `transaction_limits_max_event_queue_bytes_exact_admits_plus_one_rejects`.
- Does not replace or retire `DeliveryLimits`; both layers remain.

## D-042 — Refreshable MCP deferred; CreationOnly is the declared initial posture

**Date:** 2026-08-23

**Context:** Contracts and delivery plans include
`McpConfigurationCapability::Refreshable` (rotate tools across transactions on
one retained external session). WP-12 acceptance still lists Refreshable as an
open item. Initial ExternalAgent profiles (Grok, Cursor, Codex, agy) ship
`CreationOnly`; headless CLI profiles ship `None`. Implementing Refreshable
without a vendor-proven session refresh path would invent authority and expand
scope past the empty-tool / CreationOnly qualification bar.

**Decision:** For the **initial** shipped profile set, Refreshable MCP is
**explicitly deferred**:

- ExternalAgent MCP profiles **MUST** declare `CreationOnly` (or `None`).
- No shipped profile **MAY** declare `Refreshable` until a dedicated decision
  revises this entry with vendor evidence and proofs.
- `Refreshable` remains a valid contracts enum variant for future profiles.
- Tool-enabled reuse of an existing external session on CreationOnly continues
  to fail closed at admission (existing D-014 / CreationOnly gate).

**Consequences:**

- WP-12 “Refreshable MCP” is a **declared limitation**, not an accidental gap
  (`doc/WP12_CURRENT_LIMITATIONS.md`, capability report).
- Qualification proof: `six_profile_bindings_register_and_validate` asserts no
  profile uses `Refreshable`.
- Promoting Refreshable requires: vendor session refresh contract, Loop
  install/refresh ownership under TaskSupervisor, exact-limit proofs, and a
  new DECISIONS entry superseding this one.

## D-041 — Never-attempted terminal delivery is `NotAttempted`

**Date:** 2026-08-20

**Context:** Parked-Start / shutdown-before-Start never starts an event publisher.
Recording `TerminalEventDelivery::Published` (or a failed-enqueue variant) when
`publisher_cmd_tx` is `None` fabricates an `Ended` attempt that did not occur.
Spec §6.4 requires honest recording of terminal-event delivery; §19 previously
omitted a never-attempted variant.

**Decision:** Add `TerminalEventDelivery::NotAttempted`. When no publisher
exists, skip `Seal` and record `NotAttempted`. Do not map never-attempted to
`Published`, `QueueClosed`, `DeadlineExceeded`, or `LimitExceeded`.

**Consequences:**

- Completions still fire once (§6.3). The event mailbox may close without
  `Ended`; the completion field is the honest record.
- Host adapters that only speak v1 `EventDeliveryOutcome` MAY collapse
  `NotAttempted` to `Failed` (not `Accepted`).
- Transaction admission capacity remains `ReservationPool` only; the former
  public `CapacityManagers` counter API stays deleted.

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
5. Migrate in stages M1–M7 from the v2 spec. Obsolete uncompiled v1 modules
   (`active_registry`, `events`, `exchange`, `spawn_gate`) were **deleted** under
   D-054 (do not restore). Deprecated sink-shaped aliases
   (`TransactionRequest`, `TransactionRuntime`, `RuntimeToolSpill`) were
   **removed** under **D-060**. Host helpers `adapt_event_sink` /
   `adapt_completion_callback` remain outside the kernel (optional later move
   out of `monoloop-loop`) — that is not “deferred on-disk source.”

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
