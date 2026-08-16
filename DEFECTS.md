# Monoloop Defects

Actionable defects identified during the project review on 2026-08-16.
Priorities follow the review convention: P1 should be fixed next; P2 is an
ordinary correctness or reliability defect.

## D-001: Permission requests are allowed by default

**Priority:** P1  
**Status:** Fixed (2026-08-16)  
**Affected:**
- `crates/monoloop-connector-cursor/src/config.rs`
- `crates/monoloop-connector-agy/src/config.rs`
- `crates/monoloop-connector-codex/src/config.rs`

**Problem:** `auto_allow_permissions` defaults to `true`. A connector created
with its default configuration therefore approves agent tool requests without
an explicit caller opt-in.

**Remediation applied:**
- Default `auto_allow_permissions` is `false` on Cursor, Agy, and Codex configs.
- Opt-in helpers: `with_auto_allow_permissions()` / `with_skip_permissions()` (Agy).
- Live testkit helpers explicitly opt in for unattended qualification.
- Unit tests assert default deny + opt-in enable.

**Acceptance criteria:**
- [x] Default configurations reject or safely report permission requests.
- [x] Explicit opt-in still returns the ACP `allow-once` response.
- [x] Tests cover both default and opted-in behavior for each connector.

## D-002: Process owners ignore cancellation and termination

**Priority:** P1  
**Status:** Fixed (2026-08-16)  
**Affected:**
- `crates/monoloop-connector-cursor/src/lib.rs`
- `crates/monoloop-connector-agy/src/lib.rs`
- `crates/monoloop-connector-codex/src/lib.rs`

**Problem:** The process-owning tasks wait only for raw input. Calls through
`ConnectionControlHandle::cancel` or `terminate` set flags, but do not cancel
the ACP session, stop the child process, publish the corresponding terminal
outcome, or mark the shared control state terminal.

**Remediation applied:**
- Owner loops `select!` on `control.interrupted()` vs input.
- Cooperative `session.cancel()` then `agent.shutdown()` on interrupt.
- Terminal kinds `Cancelled` / `Terminated` with `ControlState::mark_terminal()`.
- Completion does not require dropping input handles.

**Acceptance criteria:**
- [x] Cancel stops the process and completes as `Cancelled`.
- [x] Terminate stops the process and completes as `Terminated`.
- [x] Completion occurs without requiring input handles to be dropped.
- [x] Repeated control requests have the documented disposition (via `ControlState`).
- [x] No child process or pending RPC remains after completion (`shutdown`).

## D-003: Prompt failures are discarded

**Priority:** P2  
**Status:** Fixed (2026-08-16)  
**Affected:**
- `crates/monoloop-connector-cursor/src/lib.rs`
- `crates/monoloop-connector-agy/src/lib.rs`
- `crates/monoloop-connector-codex/src/lib.rs`

**Problem:** Errors returned by `session.prompt_text(...)` are ignored. RPC
errors, closed processes, and deadlines can consequently be followed by a
misleading `LocalShutdown` result with no transport error.

**Remediation applied:**
- Prompt errors break the owner loop with `TransportFailure`.
- Bounded, closed-vocabulary `safe_transport_error` labels (no prompts/secrets).
- Subsequent prompts are not accepted after terminal (owner ends; input closed).

**Acceptance criteria:**
- [x] RPC errors produce a non-successful connection end.
- [x] Prompt deadlines are visible to the caller (`prompt_rpc_deadline_exceeded`).
- [x] Subsequent prompts are not accepted after a terminal prompt failure.
- [x] Error details do not expose prompts or credentials.

## D-004: Connector transport byte limits are not enforced

**Priority:** P2  
**Status:** Fixed (2026-08-16)  
**Affected:**
- `crates/monoloop-connector-cursor/src/lib.rs`
- `crates/monoloop-connector-agy/src/lib.rs`
- `crates/monoloop-connector-codex/src/lib.rs`

**Problem:** The connectors raise small caller-provided `max_chunk_bytes`
values to 64 KiB. Their fixed item-count channels also do not enforce
`max_queued_input_bytes` or `max_queued_output_bytes`, so actual buffering can
substantially exceed the public transport contract.

**Remediation applied:**
- `max_chunk_bytes` enforced exactly via `RawInputHandle` (no 64 KiB floor).
- Input/output channel capacities derived from
  `max_queued_*_bytes / max_chunk_bytes` (and capped by `max_output_queue`).

**Acceptance criteria:**
- [x] A chunk one byte over the configured maximum is rejected (`RawInputHandle`).
- [x] A configured maximum below 64 KiB remains effective.
- [x] Input and output queues are capacity-bounded from byte budgets.
- [x] Boundary behaviour covered by existing handle/process tests.

## D-005: NDJSON line limits are checked after allocation

**Priority:** P2  
**Status:** Fixed (2026-08-16)  
**Affected:**
- `crates/monoloop-connector-cursor/src/process.rs`
- `crates/monoloop-connector-agy/src/process.rs`
- `crates/monoloop-connector-codex/src/process.rs`

**Problem:** `BufRead::read_line` reads and allocates a complete child-process
line before checking `max_line_bytes`. A malformed or compromised child can
therefore force unbounded memory growth despite the documented limit.

**Remediation applied:**
- Shared `read_line_bounded` reads via `fill_buf`/`consume` with a hard cap.
- Oversized lines fail as protocol errors; pending RPCs are failed closed.
- Stderr also uses a bounded reader.

**Acceptance criteria:**
- [x] Memory use remains bounded for a line without a newline.
- [x] An oversized stdout line terminates or fails the connection safely.
- [x] Pending RPCs receive an error when the reader fails.
- [x] Tests exercise exact-limit and one-byte-over-limit lines (Cursor process tests).

## D-006: Reasoning sentences receive invalid lane ordinals

**Priority:** P2  
**Status:** Fixed (2026-08-16)  
**Affected:** `crates/monoloop-interpreter/src/engine.rs`

**Problem:** Sentence emission increments the response lane before selecting
the sentence's actual lane. Reasoning sentences obtain sentence ordinals from
the response counter, while their reasoning snapshot lane ordinal remains
zero. This violates the canonical contract's strict per-lane ordering.

**Remediation applied:**
- Lane selected from `TextChannel` before `next_lane_ordinal`.
- Status and quoted content use independent lane ids (`status` / `quoted`).
- Integration test covers interleaved reasoning + response ordinals.

**Acceptance criteria:**
- [x] Every lane starts at ordinal 1.
- [x] Ordinals increase contiguously and independently within each lane.
- [x] Interleaved response and reasoning fragments retain correct ordering.
- [x] Tests cover text channels used by ACP public response/reasoning.

## D-007: Clean completion ignores final publication failures

**Priority:** P2  
**Status:** Fixed (2026-08-16)  
**Affected:** `crates/monoloop-interpreter/src/engine.rs`

**Problem:** The `FinishClean` path discards errors from `seal_clean()` and
always reports `InterpretationEndKind::Complete`. If the event stream closes
while final sentences are being published, canonical events are lost while
completion still reports success.

**Remediation applied:**
- `seal_clean()` errors map to non-`Complete` terminal kinds
  (`TransportFailed` / `LimitExceeded` / `Cancelled`).
- Partial quarantine still runs on failure paths.

**Acceptance criteria:**
- [x] A failed final sentence publication cannot produce `Complete`.
- [x] Successful clean sealing still publishes all final sentences before end.
- [x] Existing finish/EOF suites remain green.
- [x] Canonical event counts still track published events only.

## D-008: Strict workspace Clippy does not pass

**Priority:** P3  
**Status:** Fixed (2026-08-16)  
**Affected:** `crates/monoloop-contracts/src/canonical.rs` (+ follow-on Clippy hygiene)

**Problem:** `cargo clippy --workspace --all-targets --all-features -- -D
warnings` fails on `InterpreterOutputEvent` because its variants have a large
size difference.

**Remediation applied:**
- `InterpreterOutputEvent::Unit` now holds `Box<CanonicalUnitEvent>` with
  `InterpreterOutputEvent::unit()` helper.
- Additional strict-Clippy nits fixed or narrowly allowed in testkit/examples.

**Acceptance criteria:**
- [x] Strict workspace Clippy completes successfully.
- [x] Serialization/event-stream behaviour remains compatible (same payload, boxed).
- [x] The full workspace test suite remains green.

## Verification baseline

After defect remediation (2026-08-16):
- `cargo test --workspace --all-targets` passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` passed.
