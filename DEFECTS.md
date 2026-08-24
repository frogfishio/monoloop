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

---

# WP-00 through WP-12 Acceptance Review

Actionable defects found during the implementation acceptance review on
2026-08-18. These findings review commits `c1a8a60..bc70297` against
`REQUIREMENTS.md`, `TRANSACTION_RUNTIME_IMPLEMENTATION.md`, and
`TRANSACTION_RUNTIME_DELIVERY_PLAN.md`.

The delivery is **not accepted**. P1 and P2 findings remain in delivered scope,
and `doc/WP12_REQUIREMENTS_ACCEPTANCE.md` still labels required behavior Partial
or Open.

Verification performed:

- `cargo test --workspace --all-targets --all-features`: passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: passed.
- `cargo doc --workspace --no-deps`: passed.
- `cargo fmt --all -- --check`: failed across delivered production and test
  files.

Priorities:

- P0: universal release blocker or critical failure.
- P1: urgent correctness, security, or lifecycle defect; fix before acceptance.
- P2: ordinary correctness/reliability defect; fix before acceptance.
- P3: lower-impact issue worth fixing.

## D-009: Actor work starts before the active transaction is installed

**Priority:** P1
**Status:** Fixed (2026-08-18) — install-before-start + registry under Accepting lock
**Affected:**
- `crates/monoloop-loop/src/transaction/admission.rs:123-194`
- `crates/monoloop-loop/src/transaction/actor.rs:746-818`

**Problem:** Admission spawns the delivery task and actor at lines 137-171, then
wraps the actor in a reaper, and only installs `ActiveTransaction` in the
registry at lines 188-194. The actor can complete and run
`finalize_and_cleanup` before insertion. Its registry removal then finds
nothing, after which admission inserts an already-terminal entry that will
remain active indefinitely. A concurrent duplicate-session insertion can also
make `submit` return an admission error after the first actor has already
started; dropping its join handle detaches it, so it can perform I/O and invoke a
callback for a request the caller was told was rejected.

This reverses normative admission steps 9-11, which require registry
installation before actor spawn and rollback on spawn failure.

**Required remediation:**

- Reserve and install a non-running active entry in the same registry critical
  section that checks `SessionKey`.
- Create all bounded resources before making the entry visible.
- Start actor/delivery work only after installation succeeds.
- Add an explicit start gate or install actor handles atomically so an actor
  cannot finalize before its entry is complete.
- On any spawn/start failure, remove the entry and release every reservation
  without invoking events or callback.

**Acceptance criteria:**

- [ ] A deliberately immediate actor cannot complete before registry install.
- [ ] A forced duplicate-insert race produces one admitted actor and no work,
      event, or callback for the rejected request.
- [ ] `active_count`, global capacity, and per-Channel capacity return to zero.
- [ ] No detached reaper, actor, or delivery task remains.

## D-010: Submission can race shutdown and install work after the drain snapshot

**Priority:** P1
**Status:** Fixed (2026-08-18) — Accepting re-check under registry lock;
§18.2 lock coupling + late-Start drain aligned 2026-08-23 (v2 live path)
**Affected:**
- `crates/monoloop-loop/src/transaction/lifecycle/admission.rs` (install re-check under ledger lock)
- `crates/monoloop-loop/src/transaction/lifecycle/owner.rs` (`begin_shutdown` CAS under ledger lock)
- `crates/monoloop-loop/src/transaction/lifecycle/supervisor.rs` (`begin_shutdown_inner` CAS+snapshot; late Start while `stopping`)

**Problem:** `submit` checks the atomic runtime state before calling `admit`, but
that check is not synchronized with registry installation. Shutdown can change
the state to `Draining` and drain the registry after the check but before
admission inserts its entry. The new transaction is then absent from the
shutdown snapshot and can remain active after the runtime stores `Stopped` and
closes MCP.

**Required remediation:**

- Serialize the `Accepting` check and registry installation with shutdown's
  transition/snapshot, or use an admission generation/read-write gate.
- Re-check runtime state inside the same non-async critical section that
  installs the entry.
- Make rollback complete if shutdown wins after capacity reservation.

**Acceptance criteria:**

- [x] A barrier-controlled `submit` versus `shutdown` race has only two legal
      outcomes: rejected admission with no callback, or admitted and included in
      shutdown finalization
      (`submit_versus_shutdown_barrier_race_two_outcomes`;
      Hang both-outcomes:
      `submit_versus_shutdown_hang_barrier_both_outcomes`).
- [x] No ledger entry remains after `Stopped` (same proof: ledger_len == 0).
- [x] Runtime `Stopped` implies zero held capacity (same proof:
      `global_reservations == 0`; MCP clear covered by separate MCP Stopped
      proofs).

## D-011: Canonical events are buffered until exchange completion instead of streamed

**Priority:** P1
**Status:** Fixed (via D-027/D-036)
**Affected:**
- `crates/monoloop-loop/src/transaction/exchange.rs:262-316`
- `crates/monoloop-loop/src/transaction/actor.rs:328-343`

**Problem:** The exchange task collects every Interpreter unit in a
`Vec<CanonicalUnitEvent>` and returns it only after both Connector and
Interpreter are terminal. The actor then publishes the accumulated units. A
caller therefore receives no live model events while an exchange is running,
contrary to R-004 and the ExchangeDriver fan-out design. The vector also grows
for the entire response with no transaction byte/item bound.

**Required remediation:**

- Fan Interpreter units into the transaction EventSequencer as they are
  produced.
- Fan ModelToolCalls units to the inner Loop concurrently through the same
  canonical distributor.
- Keep only the bounded continuation material required for a later exchange;
  do not retain all canonical output.
- Include ExchangeId/child identity on actor commands and reject stale output.

**Acceptance criteria:**

- [ ] A provider blocked before terminal still causes already-produced
      canonical units to reach the event sink.
- [ ] Fragmented text arrives incrementally and in order.
- [ ] Response output exceeding configured item or byte limits terminates as
      `LimitExceeded` without proportional retained memory.
- [ ] No event is emitted after `Ended`.

## D-012: Cancellation drops exchange futures without terminating and joining their children

**Priority:** P1
**Status:** Fixed (via D-028)
**Affected:**
- `crates/monoloop-loop/src/transaction/actor.rs`
- `crates/monoloop-loop/src/transaction/exchange.rs`

**Problem:** Actor control wins a `select!` by dropping `run_exchange` or
`run_encoded_exchange`. The exchange does not have a drop guard that calls the
Connector and Interpreter controls. Its separately spawned `units_task` is a
Tokio task whose join handle is detached when the future is dropped. Connector
owner work can consequently continue after the transaction callback and
capacity release. The actor also does not apply `cleanup_deadline` to
concurrent child cancellation/join.

**Required remediation:**

- Represent each exchange as an owned child containing Connector,
  Interpreter, pump, distributor, and join handles.
- On every terminal selection, request cancellation/termination through those
  controls, close inputs, and join children within `cleanup_deadline`.
- Abort only explicitly abortable tasks after grace and record bounded
  diagnostics.
- Never detach `units_task` or provider transport work.

**Acceptance criteria:**

- [x] Exchange drop/cancel terminates connector and aborts units/pump
      (`ExchangeGuard`); actor joins live fan-out/claim within
      `cleanup_deadline`.
- [x] Force/timeout path invokes Connector terminate controls.
- [x] Normal completion joins children within configured `cleanup_deadline`
      (not a hard-coded grace).
- [x] Non-default `cleanup_deadline` path tested
      (`cleanup_deadline_non_default_completes`).
- [x] Cancel during delayed open leaves zero capacity
      (`cancel_during_slow_open_releases_capacity`).
- [x] Cancel during hang (no provider body) after open/send
      (`cancel_during_response_wait_releases_capacity`, `FakeEndpoint::Hang`).

## D-013: External session create and reuse do not attach authoritative provider sessions

**Priority:** P1
**Status:** Fixed (via D-026)
**Affected:**
- `crates/monoloop-loop/src/transaction/actor.rs`
- profile SessionAdapter / FakeSessionAdapter paths

**Problem:** The actor calls `SessionAdapter::begin_attach` only for a new
provisional external session. When the caller supplies an existing SessionId,
the actor skips attachment entirely and opens the Connector without an external
session ID, causing process connectors to create a new session while the
transaction remains indexed under the caller's old ID.

For new sessions, the profile adapters generate a synthetic SessionId and return
it as `ExternalSessionId` before contacting the provider. `OpenConnection` then
treats that fabricated ID as an existing session to load. The adapters also
clone their `known` map into a per-call map, so successful creates are not
persisted in the adapter's in-memory routing registry. No path returns and
claims the provider's authoritative created session ID.

**Required remediation:**

- Run `begin_attach` for both explicit load and new-session creation.
- Make the adapter perform or own the real provider create/load operation before
  returning `SessionAttachment`.
- Return the provider's authoritative ID; never fabricate it as an external ID.
- Validate byte equality for caller-supplied SessionId before registry claim or
  prompt send.
- Keep bounded adapter routing state needed for reuse for the runtime lifetime.

**Acceptance criteria:**

- [x] Existing SessionId causes explicit provider load (`begin_attach` always).
- [x] Missing SessionId uses create_mode and claims authoritative id after open.
- [x] Provider ID mismatch fails closed before continuing.
- [x] Deterministic FakeSessionAdapter create/reuse path.
- [ ] Residual: live multi-exchange proof per external profile is qualification
      (see `WP12_CURRENT_LIMITATIONS.md`), not a Fake gate.

## D-014: CreationOnly MCP capabilities are installed through the unsupported refresh path

**Priority:** P1
**Status:** Fixed (via D-026)
**Affected:**
- `crates/monoloop-loop/src/transaction/actor.rs`
- `crates/monoloop-loop/src/transaction/admission.rs`
- profile MCP capability declarations

**Problem:** All four external profiles declare `McpGateway +
CreationOnly`. The actor creates the session with `initial_mcp: None`, then
calls `begin_refresh_mcp(Some(descriptor))`. Every CreationOnly adapter correctly
returns `Unsupported`, so a new external transaction terminates
`InvariantFailed` before opening the provider. The actor does this even for an
empty resolved tool set. Existing sessions are also rejected asynchronously by
the actor rather than by admission when the requested tool combination is
incompatible.

**Required remediation:**

- Create the pending MCP binding before session attachment.
- Pass its descriptor in `SessionAttachRequest.initial_mcp` for CreationOnly
  creation.
- Claim the authoritative SessionKey and activate the route only after attach
  succeeds.
- Skip MCP installation entirely for an empty tool set when no capability is
  required.
- Reject tool-enabled reuse on CreationOnly profiles synchronously with
  `CapabilityMismatch`.

**Acceptance criteria:**

- [x] CreationOnly create path installs pending MCP before attach / initial_mcp.
- [x] CreationOnly does not call refresh; Refreshable may refresh after claim.
- [x] Empty-tool transactions skip unnecessary MCP activation.
- [x] Tool-enabled existing-session reuse rejected at admission (CreationOnly).
- [x] Real HTTP MCP initialize/list/call via gateway
      (`http_mcp_initialize_list_call_sequence`).

## D-015: Most configured transaction and Channel limits are inert

**Priority:** P1
**Status:** Fixed (via D-027/D-031/D-035)
**Affected:**
- `crates/monoloop-contracts/src/limits.rs`
- `crates/monoloop-loop/src/transaction/admission.rs`
- `crates/monoloop-loop/src/transaction/actor.rs`
- `crates/monoloop-loop/src/transaction/dispatcher.rs`
- `crates/monoloop-loop/src/transaction/events.rs`

**Problem:** Runtime validation checks only global active capacity, event item
capacity, callback deadline, and one relationship. Searches of production code
show no enforcement for actor command bytes, event queue bytes, runtime input
bytes/messages/parts, tool schema aggregate bytes, transaction tool
payload/output limits, configured transaction tool concurrency/queue limits,
continuation context bytes, aggregate provider input/output bytes, diagnostic
counts/bytes, cleanup deadline, terminal-event deadline, Channel distinct
sessions, or Channel encoded-exchange bytes.

The actor uses `InterpretationLimits::default()`,
`SharedToolCapacity::unlimited()`, and hard-coded tool limits `16/64`, bypassing
runtime configuration. Event queues are item-bounded only.

**Required remediation:**

- Validate every zero/contradictory limit at startup.
- Thread effective runtime/Channel limits into admission, actor, exchange,
  Interpreter, event queue, dispatcher, MCP, callback, and cleanup.
- Add byte permits to every queue carrying variable-sized values.
- Track aggregate provider and continuation usage across exchanges.
- Remove silent `.max(1)`/`.max(...)` substitutions for invalid configured
  values.

**Acceptance criteria:**

- [x] High-value public limits have production use sites and plus-one tests
      (tools, messages, input bytes, event bytes, tool payload, schema,
      provider aggregates, continuations/exchanges, concurrency/queue).
- [x] Zero or contradictory values fail startup (`TransactionLimits::validate`).
- [x] Event queues enforce item and byte capacity (`BoundedEventSender`).
- [x] Tool and provider aggregate limits select `LimitExceeded` / reject paths.
- [x] Tests demonstrate configured non-default values, not only defaults.
- [x] Channel `max_distinct_sessions` plus-one at admission
      (`distinct_sessions_plus_one_rejected`).
- [x] Channel `max_encoded_exchange_bytes` fails closed
      (`encoded_exchange_bytes_plus_one_fails`).
- [x] `bound_diagnostics` enforces count + message bytes.
- [x] Control channel capacity taken from `max_actor_commands`.
- [x] Actor-command byte budget retired (D-057 — closed-enum `ControlCommand`).
- [ ] Residual: deeper non-responsive provider matrix.

## D-016: OpenAI tool calls can execute before the provider finishes declaring them

**Priority:** P1
**Status:** Fixed (via D-030)
**Affected:**
- `crates/monoloop-interpreter/src/openai_chat.rs:224-324`
- `crates/monoloop-loop/src/transaction/actor.rs:544-570`

**Problem:** The Interpreter promotes a tool call to `RequestReady` whenever the
currently accumulated argument string happens to parse as JSON. A fragmented
argument such as `"1"` followed by `"23"` can therefore be executed as `1`
before the provider finishes the intended `123`. The same promotion is attempted
for `stop`, `length`, and `content_filter`, rather than requiring the qualified
`tool_calls` finish condition.

The provider tool-call ID is also used directly as `ToolActionId`; no
`CanonicalToolActionKey { ExchangeId, provider_id }` is constructed. Reusing a
provider ID in a later exchange therefore collides in canonical lifecycle
identity.

**Required remediation:**

- Accumulate tool deltas without emitting Ready.
- On the qualified tool-call finish condition, validate complete JSON, name, ID,
  count, and bounds, then emit exactly one Ready event.
- Generate a distinct internal ToolActionId and retain provider ID separately.
- Key deduplication/correlation by ExchangeId plus provider call ID.

**Acceptance criteria:**

- [ ] A parseable prefix followed by more argument fragments is executed once
      with the final payload.
- [ ] `length` and `content_filter` cannot promote incomplete tool calls.
- [ ] Invalid/incomplete arguments produce a truthful non-success outcome.
- [ ] Repeated provider IDs in separate exchanges yield distinct internal action
      IDs and correct continuation IDs.

## D-017: Continuation encoding uses a different ExchangeId from the exchange it opens

**Priority:** P1
**Status:** Fixed (2026-08-18) — shared ExchangeId
**Affected:**
- `crates/monoloop-loop/src/transaction/actor.rs:424-461`
- `crates/monoloop-loop/src/transaction/exchange.rs:129-145`

**Problem:** The actor generates an ExchangeId and passes it to
`encode_tool_continuation`, then discards it. `run_encoded_exchange` generates a
second ExchangeId for the actual exchange. Encoder diagnostics/correlation and
the resulting canonical/tool events therefore refer to different exchange
identities.

**Required remediation:**

- Allocate one ExchangeId in the actor for each exchange.
- Pass that ID to both encoder and ExchangeDriver.
- Remove ExchangeId generation from `run_encoded_exchange`.

**Acceptance criteria:**

- [ ] Initial and continuation encoder, Connector, Interpreter, canonical units,
      tool keys, diagnostics, and terminal reconciliation share one ExchangeId
      per cycle.
- [ ] A deterministic two-continuation test asserts exact identity propagation.

## D-018: The MCP HTTP endpoint recreates protocol session state for every request

**Priority:** P1
**Status:** Fixed (via D-034)
**Affected:**
- `crates/monoloop-loop/src/transaction/mcp/gateway.rs`
- `crates/monoloop-loop/tests/mcp_gateway.rs`

**Problem:** `forward_mcp` constructs a new `StreamableHttpService` and a new
`LocalSessionManager::default()` for every HTTP request. MCP Streamable HTTP
session state established by initialize cannot be reused by subsequent
notifications/list/call requests. Existing tests exercise handlers directly and
only test unknown capability over HTTP; they never perform the required real
initialize → initialized → tools/list → tools/call protocol sequence.

The gateway also has no configured request-body byte limit, request duration,
per-route concurrency limit, or global in-flight request bound.

**Required remediation:**

- Retain one bounded service/session manager per active capability or one shared
  manager with capability-safe routing.
- Apply request body, duration, per-capability, and global concurrency limits
  before protocol dispatch.
- Revoke and terminate active MCP sessions/calls during transaction cleanup.

**Acceptance criteria:**

- [x] A real MCP client completes initialize, initialized notification,
      tools/list, and tools/call over HTTP
      (`http_mcp_initialize_list_call_sequence`).
- [x] Pending rejects tools/list; revoked/unknown 404
      (`http_mcp_pending_token_rejects_tools_list`,
      `http_mcp_revoked_token_is_404`, `http_unknown_capability_is_404`).
- [x] Maximum body plus one fails closed (`http_oversized_body_fails_closed`).
- [x] Revoke/shutdown cancel only that gateway's per-token services (no
      process-wide drain).
- [x] Explicit per-capability/global concurrency + request duration
      exact-limit/plus-one proofs (via D-034 `McpGatewayLimits`:
      `mcp_per_capability_concurrency_plus_one_rejects`,
      `mcp_global_concurrency_plus_one_rejects`,
      `mcp_request_duration_plus_one_fails_closed`).

## D-019: HTTP failure and backpressure paths bypass resource and cancellation bounds

**Priority:** P1
**Status:** Fixed (via D-033)
**Affected:**
- `crates/monoloop-connector/src/http.rs:469-478`
- `crates/monoloop-connector/src/http.rs:481-574`

**Problem:** On non-success status, the Connector calls `response.bytes().await`,
which buffers the entire untrusted provider body despite the comment claiming a
bounded drain. During successful streaming, `out_tx.send(chunk).await` is not
selected against control, idle timeout, or remaining request deadline. A full
output queue can therefore make cancellation and timeout unresponsive.

The send/header phase uses
`connect_deadline.max(config.request_timeout)`, allowing the shorter configured
deadline to be ignored, and the response deadline starts only after headers, so
the advertised overall request timeout can be consumed twice.

**Required remediation:**

- Do not read non-success response bodies, or drain only through a strict byte
  and time bound without retaining them.
- Select output enqueue against cancellation and remaining overall deadline.
- Use one absolute overall deadline and independent smaller connect/header/idle
  bounds.
- Enforce the configured output byte queue, not the input-derived item count.

**Acceptance criteria:**

- [x] Non-success responses drop the body without unbounded retain (D-033 path:
      `drop(response)` on non-success).
- [x] Cancellation interrupts a blocked output enqueue
      (`cancel_interrupts_blocked_output_enqueue`).
- [x] Connect/headers/body/idle/overall use one absolute deadline with
      independent smaller bounds (D-033 proofs:
      `absolute_request_deadline_covers_header_and_body_delay`,
      `blocked_enqueue_honors_idle_before_overall_deadline`,
      `full_output_queue_terminates_at_overall_deadline`).
- [ ] Residual: barrier-controlled multi-phase elapsed-sum matrix still
      desirable as an exhaustive timing harness (not required to keep the
      named cancel/backpressure close).

## D-020: Shutdown does not obey its global deadline or join aborted actors

**Priority:** P1
**Status:** Fixed (via D-029)
**Affected:**
- `crates/monoloop-loop/src/transaction/runtime.rs:201-320`

**Problem:** Shutdown gives every active entry `slice / 4` sequentially, then
waits separately for callbacks and MCP shutdown. With N entries, total duration
can exceed the supplied deadline by approximately N times the per-entry budget.
Callback branches also use hard-coded 100/200 ms values rather than the smaller
of configured callback deadline and remaining global time.

When an actor times out, shutdown calls `abort.abort()` but does not await the
aborted join before claiming the guard and continuing. This violates the
required supervisor rule that the actor be aborted and joined before supervisor
finalization. Concurrent shutdown calls are not coordinated; each can swap the
state and independently drain/stop services.

**Required remediation:**

- Compute one absolute shutdown deadline and use its remaining time for every
  phase.
- Signal actors as a group, join concurrently, then abort and join stragglers.
- Claim supervisor guards only after the corresponding actor join is terminal.
- Coordinate concurrent shutdown callers through one shared shutdown future or
  state machine.

**Acceptance criteria:**

- [ ] Shutdown of many blocked actors completes within one global deadline.
- [ ] Every aborted actor is joined before supervisor callback invocation.
- [ ] Concurrent shutdown calls return one consistent disposition.
- [ ] Disposition counts account for every admitted transaction exactly once.

## D-021: Event-sink and completion-callback panics escape their runtime boundaries

**Priority:** P1
**Status:** Fixed (via D-029)
**Affected:**
- `crates/monoloop-loop/src/transaction/events.rs`
- `crates/monoloop-loop/src/transaction/actor.rs`
- `crates/monoloop-loop/src/transaction/runtime.rs`
- `crates/monoloop-loop/src/transaction/callback_service.rs`

**Problem:** The synchronous call that creates `sink.deliver(...)` and the
synchronous call that creates `callback.call(...)` are not protected with
`catch_unwind`. A host implementation panic can kill the delivery task or actor
and bypass normal delivery-failure/callback accounting. Callback futures are
also awaited inside actor/supervisor paths rather than through a separately
bounded callback executor/reservation.

**Required remediation:**

- Catch panics both while invoking sink/callback methods and while polling their
  returned futures.
- Convert sink panic to `EventDeliveryFailed`.
- Record callback panic/failure in shutdown/accounting without changing the
  selected transaction terminal cause.
- Execute callbacks in a bounded runtime-owned service independent of actor
  liveness.

**Acceptance criteria:**

- [x] Sink panic (invoke or future poll) produces one callback with
      `EventDeliveryFailed` (`sink_panic_on_invoke_*`, `sink_panic_in_future_*`).
- [x] Callback panic does not panic actor or runtime; capacity released;
      subsequent admits work (`callback_panic_does_not_kill_runtime`).
- [x] Shutdown supervisor callbacks also use isolated invoke/poll
      (`run_callback_isolated`).
- [x] Runtime-owned `CallbackService`: bounded concurrent slots, schedule
      without holding actor capacity; shutdown drains inflight
      (`slow_callback_does_not_block_capacity_release`).

## D-022: Rejected direct-model tool calls produce no canonical result

**Priority:** P1
**Status:** Fixed (via D-030)
**Affected:**
- `crates/monoloop-loop/src/transaction/dispatcher.rs:118-181`
- `crates/monoloop-loop/src/transaction/actor.rs:373-417`

**Problem:** Invalid/disallowed/capacity-rejected tool dispatch returns
`DispatchOutcome::Rejected` without a `CanonicalToolResult`. The actor collects
only `DispatchOutcome::Canonical` results. If every call is rejected, it breaks
the continuation loop and can report the transaction Completed; the provider
never receives a correlated tool error. A model-requested tool with an empty
resolved set is also silently ignored and the transaction completes.

This contradicts the specification that invalid tool arguments are canonical
tool outcomes rather than transaction failures.

**Required remediation:**

- Produce a bounded correlated domain-error `CanonicalToolResult` for ordinary
  tool rejection.
- Preserve provider call ID and request ordinal.
- Include those results in CallerControlled output or inline continuation.
- Reserve `ToolExchangeFailed` for handler/runtime failures.

**Acceptance criteria:**

- [ ] Invalid JSON, schema failure, unknown tool, disallowed tool, queue full,
      and empty allowlist each produce a correlated canonical tool error.
- [ ] Inline continuation sends that result to the model.
- [ ] CallerControlled terminates `ContinuationRequired` with the result
      available in events.
- [ ] No rejected tool request is reported as a successful Completed
      transaction with no result.

## D-023: Admission accepts a liberal configuration policy and encoders drop extensions

**Priority:** P2
**Status:** Fixed (2026-08-18) — ChannelCapabilities.option_policy + encoder round-trip
**Affected:**
- `crates/monoloop-contracts/src/config.rs`
- `crates/monoloop-loop/src/transaction/admission.rs`

**Problem:** Admission constructs one hard-coded liberal `OptionPolicy` for
every Channel instead of using Channel-declared supported options and extension
keys. `OptionPolicy` documentation says an empty allowed-extension set means no
extensions, while validation treats it as unrestricted. Effective extensions
are then ignored by the OpenAI and ACP/headless encoders, so accepted
provider-specific configuration is silently dropped rather than passed down or
rejected.

**Required remediation:**

- Add immutable option/extension policy to ChannelBinding.
- Make empty extension allowlist deny all extensions.
- Validate version and key support per selected dialect/profile at admission.
- Encode supported extensions through the selected dialect and reject all
  unsupported fields synchronously.

**Acceptance criteria:**

- [x] Empty allowlist denies extensions
      (`empty_extension_allowlist_denies`,
      `unknown_extension_rejected_at_admission`).
- [x] Channel defaults seed `allowed_extension_keys` at admission.
- [x] Per-Channel distinct option matrices (`direct_llm` vs `external_agent`)
      and OpenAI/ACP encoder round-trip / fail-closed for extensions.

## D-024: Declared tool cancellation policy is not enforced by handler registration or cleanup

**Priority:** P2
**Status:** Fixed (via D-028)
**Affected:**
- `crates/monoloop-loop/src/transaction/host_tools.rs`
- `crates/monoloop-loop/src/transaction/dispatcher.rs`
- `crates/monoloop-loop/src/transaction/tool_handler.rs`

**Problem:** `RegisteredTool::new` accepts any `ToolHandler` for any declared
`ToolCancellationPolicy`; the registry cannot verify that an Abortable handler
has an abort handle or that an IsolatedKillable handler owns a killable worker.
At deadline the dispatcher only calls cooperative `control.cancel()` and
returns immediately. A custom handler can ignore it and continue external
effects after transaction terminalization.

**Required remediation:**

- Make termination mechanics part of the execution handle contract.
- Validate policy/handle compatibility before start or at registration.
- Apply cooperative grace, then abort/kill according to policy.
- Join cleanup before releasing execution capacity.

**Acceptance criteria:**

- [x] Unstoppable handler cannot register as Abortable
      (`abortable_requires_supports_abort_handler`).
- [x] Handler trait exposes `supports_abort` / `supports_isolated_kill`;
      `ToolKillHandle` on execution handles.
- [x] IsolatedKillable registration requires kill support
      (`isolated_killable_requires_supports_isolated_kill`).
- [x] Escalate after grace + join
      (`isolated_killable_escalates_after_grace_and_stops_work`,
      `cancel_running_async_tool`).

## D-025: WP-12 does not meet its own acceptance and formatting gates

**Priority:** P2
**Status:** Fixed (via D-037) — mandatory formatting and test
gates fail; see D-037
**Affected:**
- `doc/WP12_REQUIREMENTS_ACCEPTANCE.md`
- `doc/WP12_CURRENT_LIMITATIONS.md`
- formatting / clippy / workspace test gates

**Problem:** The acceptance checklist still marks advertised end-to-end paths,
terminal races, fail-closed security, production placeholders, external
create/reuse, and tool cancellation as Partial, and explicitly lists open
acceptance items. The current-limitations report likewise says live external
multi-exchange, MCP refresh, full race coverage, paused-time deadlines, and
forced-abort proof are not release-proven. In addition, the mandatory formatting
gate fails. Under R-000 and the delivery definition of done, WP-12 cannot be
considered delivered while these remain.

**Required remediation:**

- Fix D-009 through D-024.
- Add direct deterministic evidence for every applicable acceptance item.
- Keep unsupported optional capabilities honestly disabled, but do not mark
  required paths complete through descriptor-only tests.
- Run formatting and update acceptance documents only after all gates pass.

**Acceptance criteria:**

- [x] Required deterministic Fake/scripted paths have direct tests (hardening,
      mcp_gateway, linked_tools, admission, exchange, profiles).
- [x] Remaining open items are documented as qualification / out-of-scope in
      `WP12_CURRENT_LIMITATIONS.md` and the acceptance Open items list.
- [x] Six profile bindings register/validate (`profile_bindings`).
- [x] `cargo fmt --check` and `clippy -D warnings` for product crates.
- [ ] Residual: independent security audit sign-off (process, not code gate);
      checklist: `doc/SECURITY_REVIEW_CHECKLIST.md` (unsigned until filled)
      live SendAndRetain multi-exchange per agent remains qualification.


### Remediation progress (2026-08-18, continued)

| ID | Status | Notes |
|---|---|---|
| D-009 | Fixed | start_gate; install under Accepting+registry lock |
| D-010 | Fixed | shared Arc state; re-check under lock |
| D-011 | Fixed (via D-027/D-036) | ordered publisher + retention ceiling + live capacity from output budget |
| D-012 | Fixed (via D-028) | PendingOpenGuard + units join + mid-dispatch cancel/join |
| D-013 | Fixed (via D-026) | claim-before-activate + SessionEstablished-first for create |
| D-014 | Fixed (via D-026) | Grok/Cursor/Codex/Agy create serialize `initial_mcp` into mcpServers |
| D-015 | Fixed (via D-027/D-031/D-035) | input estimate + cumulative continuation + retention bound |
| D-016 | Fixed (via D-030) | internal action id scoped by ExchangeId |
| D-017 | Fixed | single ExchangeId |
| D-018 | Fixed (via D-034) | token hex canonicalize; concurrency/duration exact-limit proofs closed (D-034) |
| D-019 | Fixed (via D-033) | absolute HTTP deadline + output capacity from output budget |
| D-020 | Fixed (via D-029) | shared shutdown disposition + abort-then-join under remaining budget |
| D-021 | Fixed (via D-029) | callback reservation at admission + drain abort/join |
| D-022 | Fixed (via D-030) | empty allowlist rejects; rejection Completed published |
| D-023 | Fixed | ChannelCapabilities.option_policy; openai.*/acp.meta.* encode or fail closed |
| D-024 | Fixed (via D-028) | actor cancel notifies dispatch; worker terminate+join; fail-closed capability defaults |
| D-025 | Fixed (via D-037) | fmt + invalid_json test + gates |

### Remediation progress (2026-08-18, D-026–D-037)

| ID | Status | Notes |
|---|---|---|
| D-026 | Fixed (residual closed) | Create: claim+`SessionEstablished`+MCP activate **before** prompt send (`prompt_ready` gate); ACP encode uses empty tools for `McpGateway` |
| D-027 | Fixed (residual closed) | Per-exchange remaining output budget; limits before live publish; immediate `LimitExceeded` on retention exceed |
| D-028 | Fixed (residual closed) | Per-runtime `ToolJoinVault` with normal-op `reap_finished`; missing-kill orphans permit (no fabricated waiter); vault parks real worker joins only |
| D-029 | Fixed (residual closed) | Deferred restore miss owns a watcher (always releases capacity; late restore still schedulable); supervisor joins re-parked when budget is zero (never detached) |
| D-030 | Fixed | ExchangeId-scoped ToolActionId; empty allowlist → rejection Completed; CallerControlled after observe |
| D-031 | Fixed (residual closed) | OpenAI continuation encodes transcript only (no duplicate `results` append) |
| D-032 | Fixed (residual closed) | Non-blocking `try_spawn` (no first-poll wait); multi-thread executor required at bootstrap; async `try_spawn_confirmed` for shutdown callbacks |
| D-033 | Fixed | absolute request deadline; enqueue selects deadline; output queue from output budget |
| D-034 | Fixed | Canonical hex + global/per-cap permits before body; body+dispatch share duration budget; concurrency/duration plus-one proofs closed (2026-08-23) |
| D-035 | Fixed | estimate covers names, args JSON, tool_call_id; serialize fail closed |
| D-036 | Fixed | OrderedEventPublisher serialize allocate+enqueue; live waits for claim |
| D-037 | Fixed | fmt + invalid_json asserts MalformedSemanticPayload; gates re-run |

---

# Post-remediation Acceptance Re-review

Actionable findings from the 2026-08-18 review of commits
`cf5615b..f6af016`. These are defects beyond the qualification residuals
reported by the developers. They also explain why several earlier defects have
been reopened above.

Verification performed (original review):

- `cargo fmt --all -- --check`: failed.
- `cargo test --workspace --all-targets --all-features`: failed in
  `openai_chat::tests::invalid_json_args_never_ready`.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`:
  passed when run independently.
- `cargo doc --workspace --no-deps`: passed when run independently.

Re-verification after D-026–D-037 remediation (2026-08-18):

- `cargo fmt --all -- --check`: passed.
- `cargo test --workspace --all-targets --all-features`: passed (after D-029
  hardening test update for admission-reserved callbacks).
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`:
  passed.
- `cargo doc --workspace --no-deps`: passed.

## D-026: External-session MCP activation and event identity precede authoritative session claim

**Priority:** P1
**Status:** Fixed
**Affected:**
- `crates/monoloop-loop/src/transaction/actor.rs:247-408`
- `crates/monoloop-loop/src/transaction/actor.rs:456-588`
- `crates/monoloop-connector-{agy,codex,cursor,grok}/src/channel_binding.rs`
- corresponding process Connector create paths

**Problem:** CreationOnly adapters copy `initial_mcp` into
`SessionAttachment`, but no production process Connector reads that field when
creating the provider session. The actor nevertheless activates the route
immediately after the adapter returns a provisional attachment, before provider
open returns the authoritative session ID and before `SessionKey` is claimed.

The MCP dispatcher is constructed with a random placeholder `SessionKey` and is
never rebound. The live event task also captures the pre-claim `None`
`session_key`; `emit_unit_or_session` then generates a new random `SessionId`
for each event. Provider units may race ahead of `SessionEstablished`, and a
create that returns no external ID is accepted instead of failing invariant.

**Required remediation:**

- Serialize `initial_mcp` into each profile's real provider create operation.
- Keep the route pending until provider create returns an authoritative ID,
  atomically claim the real `SessionKey`, bind the dispatcher to it, emit
  `SessionEstablished`, and only then activate MCP and permit prompt output.
- Treat a successful external create without an authoritative ID as
  `InvariantFailed`.
- Ensure every event in the transaction uses the same authoritative session
  identity and that `SessionEstablished` is the first ordinary event.

**Acceptance criteria:**

- [x] A production-profile create request contains the transaction MCP
      descriptor. (profile `initial_mcp` → `mcpServers` on create)
- [x] No MCP call or prompt send is possible before authoritative claim.
      (`prompt_ready` gate; activate in claim task before send)
- [x] MCP tool context/results and all events carry the claimed `SessionKey`.
      (`TransactionToolDispatcher::rebind_session` before activate)
- [x] Missing/mismatched provider ID fails before prompt transmission —
      proven by `tests/claim_gate.rs::create_without_provider_session_id_ends_invariant_failed`
      (`FakeConnectorConfig::omit_created_session_id` → `InvariantFailed`, not Cancelled).
      Claim/MCP errors awaited via `claim_join` on exchange fail.
- [x] Live fan-out waits for claim watch before publishing units
      (`session_watch.wait_for(Some)`). Dedicated multi-event barrier e2e still
      desirable for create ordering vs. first CanonicalUnit.

## D-027: Live exchange fan-out still retains unbounded output and incompletely accounts limits

**Priority:** P1
**Status:** Fixed
**Affected:**
- `crates/monoloop-loop/src/transaction/exchange.rs:303-370`
- `crates/monoloop-loop/src/transaction/actor.rs:446-588`
- `crates/monoloop-loop/src/transaction/events.rs:42-83`

**Problem:** `units_task` appends every canonical unit to a `Vec` while also
streaming it. The output limit is checked only after the exchange is terminal,
so an arbitrarily long response retains proportional memory. The initial
encoded request is not included in aggregate provider-input accounting, and
continuation responses are not added to aggregate provider-output accounting.
The intermediate live-unit channels are item-bounded only.

`BoundedEventSender` reserves bytes before awaiting item capacity but has no
drop guard. Cancelling a blocked send leaks the byte reservation and can prevent
the terminal event from being queued.

**Required remediation:**

- Stream through one bounded distributor and retain only bounded continuation
  state.
- Enforce output bytes incrementally before retention/enqueue.
- Include initial and every continuation request/response in aggregate limits.
- Add byte permits to intermediate variable-sized queues.
- Make byte reservations cancellation-safe.

**Acceptance criteria:**

- [x] A provider that never terminates cannot grow retained output beyond the
      configured bound. (byte-bounded retention in `run_opened_exchange`)
- [x] Initial and continuation requests counted into provider input before/at
      send (`max_remaining_provider_input_bytes`); continuation units into output.
      (dedicated exact-limit/plus-one matrix still desirable)
- [x] Cancelling a blocked event enqueue restores the byte counter.
      (cancel-safe reservation on `BoundedEventSender`)
- [x] Terminal delivery remains possible after cancelled backpressure.
- [x] Unit size checked before live publish; retention exceed returns
      `LimitExceeded` immediately (does not wait for provider terminal).
- [x] Estimator includes structure, diagnostics, tool names/results, and
      envelope identifiers (not text/request-payload only).

## D-028: Cancelling an exchange or tool dispatch can leave owned work detached

**Priority:** P1
**Status:** Fixed
**Affected:**
- `crates/monoloop-loop/src/transaction/exchange.rs:202-251`
- `crates/monoloop-loop/src/transaction/exchange.rs:288-403`
- `crates/monoloop-loop/src/transaction/actor.rs:627-646`
- `crates/monoloop-loop/src/transaction/dispatcher.rs:311-450`
- `crates/monoloop-loop/src/transaction/tool_handler.rs`

**Problem:** `ExchangeGuard` is created only after provider open,
Interpreter start, and input send/finish. Cancellation or failure in those
earlier phases drops pending/opened handles without requesting termination.
On normal cleanup, the units-task abort handle is discarded before a timed
join; timeout therefore detaches the task.

Likewise, when actor control wins while awaiting `dispatch_ready_tool`, dropping
the dispatch future drops the completion receiver but does not cancel/kill or
join the tool worker. `ToolHandler::supports_abort` also defaults to `true`, so
a custom handler can self-assert support without a structurally enforced
termination handle.

**Required remediation:**

- Own pending Connector control from `begin_open` onward and install a child
  guard before the first await.
- Terminate and join all opened/pump/interpreter/distributor work on every exit.
- Give dispatched tools an actor-owned execution guard whose drop path
  cancel/escalates and whose cleanup is joined before terminal callback.
- Make termination support structural; capability booleans must default
  fail-closed and the returned handle must match the declared policy.

**Acceptance criteria:**

- [x] Cancel during open / pre-guard window: `PendingOpenGuard` + `EarlyOpenedGuard`
      terminate; `ExchangeGuard` owns pump/units after install.
- [x] Transaction cancel during tool dispatch uses sticky cancel + terminate/join
      (`StickyCancel`); missing kill aborts started work.
- [x] A custom handler cannot claim Abortable/IsolatedKillable without the
      required execution handle. (`linked_tools` + registration checks)
- [x] Deadline / event-delivery failure cancels shared `StickyCancel` and joins
      in-flight dispatch within `cleanup_deadline` (does not drop mid-dispatch).
- [x] `supports_abort` / `supports_isolated_kill` checked before `handler.start`.

## D-029: Callback scheduling is unbounded and shutdown still violates its global contract

**Priority:** P1
**Status:** Fixed
**Affected:**
- `crates/monoloop-loop/src/transaction/callback_service.rs`
- `crates/monoloop-loop/src/transaction/runtime.rs:210-401`

**Problem:** Callback permits are acquired inside newly spawned tasks after
actor capacity is released. Slow callbacks therefore allow completed
transactions to enqueue an unbounded number of callback tasks; no callback
reservation is made at admission. `drain` only polls a counter and does not
abort owned callbacks when its budget expires.

Shutdown still exceeds its supplied deadline by applying `.max(50ms)` to MCP
shutdown and callback drain after the deadline. The timeout branch aborts an
actor through `AbortHandle`, yields once, and claims finalization without
joining it. Concurrent shutdown callers receive an empty default disposition
rather than the same shared result.

**Required remediation:**

- Reserve bounded callback capacity at admission and retain it through callback
  terminal state.
- Own queued/running callback joins and abort+join them at deadline.
- Use only remaining global shutdown time; never add a minimum after expiry.
- Share one shutdown future/result across concurrent callers.
- Join each aborted actor before supervisor finalization.

**Acceptance criteria:**

- [x] Repeated completions against blocked callbacks cannot exceed configured
      callback task capacity (admission `try_reserve` + semaphore).
- [x] Callback joins always registered; timed-out children abort+join.
- [x] Shutdown per-actor join uses remaining global budget only (no 20 ms pad).
- [x] Concurrent shutdown callers wait for / share one disposition
      (never return fabricated `Default` when local wait expires early).
- [x] After global deadline reaches zero, aborted actor joins are not awaited
      unboundedly (deadline wins over non-yielding host code).
- [x] Completed callback joins are reaped (no runtime-lifetime `joins` growth).
- [x] Configured `cleanup_deadline` honored exactly (no silent 50 ms floor).

## D-030: OpenAI tool correlation and rejection handling remain incomplete

**Priority:** P1
**Status:** Fixed
**Affected:**
- `crates/monoloop-interpreter/src/openai_chat.rs:114-316`
- `crates/monoloop-loop/src/transaction/actor.rs:597-703`
- `crates/monoloop-loop/src/transaction/actor.rs:880-946`

**Problem:** The provider call ID is still used directly as internal
`ToolActionId`; `collect_ready_tools` then reconstructs the provider ID from
that internal ID. Reusing a provider ID in a later exchange therefore does not
produce the required distinct internal identity.

If the resolved tool set is empty, a model-requested tool call still exits the
loop as `Completed`. Rejected calls create a local result for inline encoding,
but emit no `Completed { result }` lifecycle event, so CallerControlled callers
cannot observe the promised canonical result.

**Required remediation:**

- Preserve provider ID separately and allocate internal action identity scoped
  by `ExchangeId`.
- Produce and publish a correlated canonical domain-error result for empty
  allowlist and every ordinary rejection.
- Make CallerControlled return `ContinuationRequired` only after those results
  are observable.

**Acceptance criteria:**

- [ ] The same provider ID in two exchanges yields distinct internal action IDs.
- [ ] Empty allowlist never reports a tool request as `Completed`.
- [ ] Every rejection is present in canonical lifecycle events and inline
      continuation with the original provider ID.

## D-031: Multi-continuation context loses prior exchanges

**Priority:** P1
**Status:** Fixed
**Affected:**
- `crates/monoloop-loop/src/transaction/actor.rs:590-784`
- `crates/monoloop-loop/src/transaction/actor.rs:905-946`

**Problem:** Each continuation context is rebuilt from the original input plus
only the most recent assistant tool-call units. Prior assistant calls and tool
results are not carried into the next cycle. The existing end-to-end test
exercises one continuation only. This makes a second tool continuation
semantically incomplete and also avoids cumulative context-limit accounting.

**Required remediation:**

- Maintain one cumulative, bounded canonical continuation transcript.
- Append each assistant tool-call group and ordered tool results exactly once.
- Enforce `max_continuation_context_bytes` on the whole cumulative context, not
  only the newest encoded body.

**Acceptance criteria:**

- [x] Continuation encode uses cumulative transcript once (no duplicate
      `results` append in OpenAI encoder). Three-exchange golden test still
      desirable.
- [x] Repeated provider IDs remain ExchangeId-scoped (`ToolActionId`).
- [x] Cumulative context byte check on whole transcript before encode.

## D-032: The injected runtime executor is ignored

**Priority:** P1
**Status:** Fixed
**Affected:**
- `crates/monoloop-loop/src/transaction/runtime.rs:71-73`
- transaction runtime spawn sites

**Problem:** Startup discards `RuntimeBootstrap.executor`; all actors, delivery
tasks, callbacks, tools, and exchange children use ambient `tokio::spawn`.
Because `TransactionRuntime::submit` is synchronous, calling it from an ordinary
host thread can panic with “no reactor running” instead of returning an
admission result. Work is also not guaranteed to run on the runtime selected by
the host.

**Required remediation:**

- Store the injected `tokio::runtime::Handle` and route every runtime-owned
  spawn through it.
- Make synchronous `submit` safe from threads without an entered Tokio context.
- Convert unavailable/shutting-down executor conditions into typed admission or
  startup failure with complete reservation rollback.

**Acceptance criteria:**

- [x] Runtime-owned spawns (actors, exchange children, callbacks, shutdown
      supervisor callbacks) use injected `Handle` via `try_spawn`.
- [x] Synchronous `submit` does not require ambient reactor for spawn path.
- [x] Spawn on an already shut-down Handle fails closed (cancelled join /
      never-started task), not only when `spawn` panics.
- [ ] Dedicated “start on A / submit from OS thread” e2e still desirable.

## D-033: Streaming HTTP still resets the overall deadline and can block past it

**Priority:** P1
**Status:** Fixed
**Affected:**
- `crates/monoloop-connector/src/http.rs:432-590`

**Problem:** The response deadline is created only after headers arrive, giving
the send/header phase and body phase separate full `request_timeout` budgets.
While enqueueing a response chunk to a full output channel, the inner select
observes cancellation but not the remaining overall or idle deadline. Output
channel capacity is also derived from input-buffer limits rather than the
configured output-byte budget.

**Required remediation:**

- Create one absolute request deadline before send and use remaining time in
  every phase.
- Select blocked output enqueue against control, idle, and overall deadlines.
- Enforce a byte-bounded output queue from output limits.

**Acceptance criteria:**

- [x] Header delay plus body delay cannot exceed one request timeout
      (`absolute_request_deadline_covers_header_and_body_delay`).
- [x] A full output queue terminates at overall deadline without host receive
      (`full_output_queue_terminates_at_overall_deadline`).
- [x] Exact output-byte capacity plus one fails closed
      (`max_queued_output_bytes_plus_one_fails_closed`).

## D-034: MCP services are not fully bounded and non-canonical token spelling can leak them

**Priority:** P2
**Status:** Fixed
**Affected:**
- `crates/monoloop-loop/src/transaction/mcp/gateway.rs:198-275`

**Problem:** Per-capability services are stored in a process-wide map under the
raw URL token string. Equivalent uppercase hexadecimal tokens can resolve the
same route but create a differently keyed service; revoke removes only the
canonical lowercase key, leaving the alternate service/session uncancelled.
The gateway still has no configured per-capability/global request concurrency
or request-duration bound.

**Required remediation:**

- Parse and canonicalize the token before route lookup and service-map access.
- Prefer gateway-owned service state over a process-global map.
- Apply configured per-route/global concurrency and duration limits.
- Revoke, cancel, and join all service sessions/requests on transaction and
  runtime teardown.

**Acceptance criteria:**

- [x] Alternate hex spelling cannot create a second service (canonical hex).
- [x] Revoke removes capability route; global + per-capability permits acquired
      before body buffering; body read and dispatch share one duration budget.
- [x] Concurrency and duration exact-limit/plus-one tests
      (`mcp_per_capability_concurrency_plus_one_rejects`,
      `mcp_global_concurrency_plus_one_rejects`,
      `mcp_request_duration_plus_one_fails_closed` via injectable
      `McpGatewayLimits`).

## D-035: Runtime canonical-input byte accounting omits bounded fields

**Priority:** P2
**Status:** Fixed (admission enforcement restored 2026-08-23)
**Affected:**
- `crates/monoloop-contracts/src/input.rs` (`estimate_canonical_input_bytes`)
- `crates/monoloop-loop/src/transaction/lifecycle/admission.rs`
- `crates/monoloop-loop/src/transaction/lifecycle/supervisor.rs` (`RuntimeShared.transaction_limits`)

**Problem:** `estimate_input_bytes` omitted message names, assistant tool argument
JSON, and Tool-message correlation IDs. A request could therefore exceed the
runtime's `max_input_bytes` while passing admission, especially through large
historical assistant tool arguments. After the v2 admit rewrite, TransactionLimits
`max_input_bytes` / `max_messages` were not checked at all (construction used
roomy `InputLimits` only).

**Required remediation:**

- Define one canonical deterministic byte-size function covering every field.
- Use it in admission and continuation accounting.
- Avoid serialization-error fallbacks that count malformed values as zero.

**Acceptance criteria:**

- [x] Canonical estimate covers text, names, tool-call ids, tool names, and
      serialized tool arguments (`estimate_counts_names_ids_and_tool_arguments`).
- [x] Admission enforces `TransactionLimits.max_messages` /
      `max_content_parts` / `max_input_bytes` (`max_messages_plus_one_rejected_at_admit`,
      `max_input_bytes_plus_one_rejected_at_admit`).
- [x] Large historical tool arguments cannot bypass `max_input_bytes`
      (`large_tool_arguments_counted_toward_max_input_bytes`).
- [ ] Residual: every optional field / message-variant matrix still desirable
      as exhaustive plus-one codegen (not required to keep Status Fixed for
      the named bypass). Continuation-context re-estimate remains on encoded
      provider bytes (`max_remaining_provider_input_bytes`), not a second
      canonical estimate pass.

## D-036: Concurrent event producers can deliver sequence numbers out of order

**Priority:** P1
**Status:** Fixed (v2 confirm 2026-08-23)
**Affected:**
- v1 `events.rs` / `OrderedEventPublisher` not compiled (`transaction/mod.rs`)
- Live v2: `lifecycle/event_publisher.rs` (`run_event_publisher`)
- Proofs: `lifecycle/tests.rs`
  (`s22_6_concurrent_producers_contiguous_sequence`,
  `s22_6_session_established_is_sequence_one`,
  `s22_2_failed_enqueue_consumes_no_sequence`,
  `s22_6_establish_external_capacity_fail_does_not_steal_seq1`)

**Problem:** `EventSequencer::allocate` is atomic, but the session-claim task and
live-unit task independently allocate and then asynchronously enqueue events.
A producer with sequence N can be preempted before send while sequence N+1 is
queued first. The delivery task preserves queue order, not sequence order, so
the public stream can be non-contiguous and `SessionEstablished` can lose its
required first-event position.

**Required remediation:**

- Route all event production through one actor-owned sequencer/distributor, or
  atomically combine sequence allocation with ordered enqueue.
- Do not let child tasks allocate public sequence numbers directly.

**Acceptance criteria:**

- [x] Concurrent producers through one publisher deliver contiguous `1..N` in
      allocation/delivery order
      (`s22_6_concurrent_producers_contiguous_sequence`).
- [x] Ordinary allocate+enqueue is serialized on the single
      `run_event_publisher` task (child tasks MUST NOT allocate public
      sequences; they send `EventPublisherCommand`s).
- [x] Failed enqueue does not consume sequence
      (`s22_2_failed_enqueue_consumes_no_sequence`;
      `s22_6_establish_external_capacity_fail_does_not_steal_seq1`).
- [x] Publisher unit path: `SessionEstablished` is sequence 1
      (`s22_6_session_established_is_sequence_one`).
- [ ] Residual: create-path e2e that first CanonicalUnit cannot precede
      `SessionEstablished` at sequence 1 under concurrent fan-out (existing
      e2e asserts established-before-Ended only). Not a sequencer reopen.

## D-037: Mandatory acceptance gates currently fail

**Priority:** P2
**Status:** Fixed
**Affected:**
- `crates/monoloop-loop/src/transaction/active_registry.rs`
- `crates/monoloop-loop/src/transaction/actor.rs`
- `crates/monoloop-loop/src/transaction/runtime.rs`
- `crates/monoloop-interpreter/src/openai_chat.rs`
- WP-12 acceptance status

**Problem:** `cargo fmt --all -- --check` reports formatting diffs in delivered
transaction files. The all-features workspace test gate fails because
`openai_chat::tests::invalid_json_args_never_ready` unwraps the expected
`MalformedSemanticPayload` error. Therefore the claim that mandatory gates pass
is false even before the reopened behavioral defects are exercised.

**Required remediation:**

- Format the workspace.
- Correct the invalid-JSON test to assert the truthful terminal error.
- Run every mandatory gate from a clean tree and record exact commands/results
  only after all behavioral findings above are resolved.

**Acceptance criteria:**

- [ ] Formatting, all-target/all-feature tests, strict Clippy, and docs all pass.
- [ ] Independent re-review finds no unresolved P0, P1, or P2 defect.

---

# Runtime v2 M2 review (2026-08-19)

Advisor review of milestone **M2 — Owner, task supervisor, and ledger** against
`doc/TRANSACTION_RUNTIME_V2_SPEC.md` §7–§9 / §18 / §21 / §22.1 / §24, Laws 1–9
and 21–25, and D-003 (do not resurrect the seven deleted v1 lifecycle files).

**Verdict:** M2 is **not accepted**. Isolated `monoloop-loop --lib` tests pass
(19). The workspace does not compile under the spec §23 gates. Mandatory §22.1
admission tests are incomplete. Claiming “M0–M2 landed” is a shaped
qualification. **Do not start M3 coordinator/event work until D-038–D-040 are
closed.**

Verification performed:

- `cargo test -p monoloop-loop --lib`: passed (includes five M2 lifecycle tests).
- `cargo test -p monoloop-contracts`: architecture gates passed.
- `cargo test --workspace --all-targets`: failed (`DefaultTransactionRuntime`
  / `RuntimeBootstrap.executor` in façade example and testkit).
- `cargo test -p monoloop-loop --features legacy_runtime_tests --no-run`:
  failed (deleted v1 types).
- Deleted v1 files (`transaction/{runtime,admission,actor,finalization,
  callback_service,executor_spawn,tool_join_vault}.rs`) are **absent**.
- Replacement lives under `transaction/lifecycle/` as required by D-003.

## D-038: Workspace gates still compile deleted v1 runtime types

**Priority:** P1
**Status:** Fixed
**Affected:**
- `crates/monoloop/examples/fake_echo.rs`
- `crates/monoloop/examples/host_grok_wiring.rs`
- `crates/monoloop-testkit/tests/canonical_event_presentation.rs`
- `crates/monoloop-loop` v1 suites (unregistered on disk)
- crate READMEs / contracts assembler recipe

**Problem:** M2 correctly removed `DefaultTransactionRuntime` and
`RuntimeBootstrap.executor`. Downstream examples, the testkit suite, and the
`legacy_runtime_tests` feature still imported those symbols. Spec §23 requires
`cargo test --workspace --all-targets --all-features`.

**Remediation (M7 façade landed):**

- Ported façade examples + `canonical_event_presentation` to
  `StartedRuntime::start` + push `transaction_delivery` (no external `Handle`).
- Removed `legacy_runtime_tests` / `legacy_runtime_examples` features so
  `--all-features` cannot compile deleted v1 symbols.
- Deprecated sink-shaped `TransactionRequest` / `TransactionRuntime` trait as
  core submit APIs; assembler docs point at `TransactionSubmitRequest`.
- Host adapters `adapt_event_sink` / `adapt_completion_callback` retained
  (outside the kernel). Unregistered v1 `.rs` suites remain on disk.
- Spec header: M7 façade landed; **not** M0–M7 / Golden / §25.

**Acceptance criteria:**

- [x] Façade examples use `StartedRuntime::start` (no `executor` field).
- [x] `canonical_event_presentation` ported to v2 push delivery.
- [x] No `legacy_runtime_tests` feature that compiles deleted v1 symbols.
- [x] `cargo test --workspace --all-targets --all-features --no-run` compiles.
- [x] Host-facing READMEs updated off `DefaultTransactionRuntime` / bare `Handle`.
- [x] Core v1 `TransactionRequest` / `TransactionRuntime` retired as assembler
      recipe (deprecated); contracts README + v2 spec header + loop README aligned.

**Advisor (2026-08-20, bar check):** Finish M7 remainder without claiming Golden.
§22 remainder stays open (D-039, D-040, D-041, D-045 M6 partial).

**Advisor (2026-08-20, façade stop):** **Yes — stop façade cutover.** D-038
acceptance is met. Remaining M7 spec items are **not** this cutover: callback
deletion on a breaking-version boundary (deprecated + host adapters retained),
and uncompiled-module deletion after Loop-machine consolidation. Do not rewrite
unregistered v1 integration suites as further façade work. **Not** Golden / §25.
Next is D-039 (kernel), not host.

## D-039: Shutdown and control share the start queue at exact capacity

**Priority:** P1
**Status:** Fixed
**Affected:**
- `crates/monoloop-loop/src/transaction/lifecycle/owner.rs`
- `crates/monoloop-loop/src/transaction/lifecycle/supervisor.rs`
- `crates/monoloop-contracts` `TerminationDisposition`

**Problem (filed):** The supervisor `mpsc` was sized `max_active_transactions`
and carried `Start`, `Cancel`, `ForceTerminate`, `BeginShutdown`, and
`StopSupervisor`. Admission could fill that queue with `Start` commands before
the supervisor polled. `begin_shutdown` then `try_send`s `BeginShutdown` and
**drops the command on `Full`**, after already CAS-ing state to `Quiescing`.

`wait_stopped` only re-sends shutdown when state is still `Accepting`. After a
lost `BeginShutdown` the owner stays `Quiescing` with parked coordinators and
never retries. `terminate` reports `AlreadyTerminal` on the same `Full` path
(a lie). This is the D-010 class of defect in v2 clothing: admission-closed
does not imply the supervisor will drain.

Spec §9.2 requires start-queue capacity ≥ `max_active_transactions` for
**start** commands. Control and shutdown must not be starved by that bound.

**Current code (do not close on this):** start vs control vs worker queues are
split; `begin_shutdown` CAS-es `Quiescing` and `wake`s; supervisor observes
`Quiescing` on wake. Residual P1:

- `terminate` still maps `control_tx.try_send` `Full`/`Closed` →
  `AlreadyTerminal` (lie; Law 22 fail-closed must be explicit).
- `wait_stopped` still does not re-announce `BeginShutdown` while `Quiescing`.
- Biased `control_rx` can delay shutdown observation under a terminate flood.
- `shutdown_control_not_starved_when_start_queue_full` is admit-at-capacity +
  shutdown, not parked unprocessed `Start`s.

**Advisor (2026-08-20):** **Pick D-039 next. Do not leave for host.** Shutdown
and control delivery are kernel (Laws 22–25, §9.2 / §10). Host adapters
(`adapt_event_sink` / `adapt_completion_callback`) drain push receivers outside
the runtime; they cannot substitute for control-queue integrity. Pair the fix
with D-040 §22.1 tests; do not treat the existing happy-path test as proof.

**Required remediation:**

- Separate control/shutdown from the start queue, or size the shared queue
  with dedicated control slack that admission cannot consume. *(done: split queues)*
- Retry `BeginShutdown` / `StopSupervisor` from `wait_stopped` while
  `Quiescing`.
- Do not map `try_send` failure to `AlreadyTerminal`.
- Preferential control drain so a Cancel flood cannot delay Quiescing observation.

**Remediation (2026-08-20):**

- `terminate`: ledger-first `NotFound` / `AlreadyTerminal`; `try_send` `Full` →
  `ControlCapacityExceeded`, `Closed` → `RuntimeClosed` (never Full→AlreadyTerminal).
- `wait_until_stopped` while `Quiescing` re-sends `BeginShutdown` + `wake`.
- Supervisor loop preferentially `try_recv`s the control queue each lap.

**Acceptance criteria:**

- [x] Start vs control queues split; admit-at-capacity shutdown still reaches
      `Stopped` (`shutdown_control_not_starved_when_start_queue_full`).
- [x] `wait_stopped` re-announces while `Quiescing`
      (`wait_stopped_reannounce_while_quiescing_then_stopped`).
- [x] `terminate` never reports `AlreadyTerminal` solely because the control
      queue was full (`ControlCapacityExceeded` / ledger-honest paths).
- [x] Explicit proof with parked unprocessed `Start`s
      (`parked_starts_reach_stopped_on_shutdown`, D-040).

## D-040: Mandatory §22.1 admission tests are not implemented

**Priority:** P1
**Status:** Fixed
**Affected:**
- `crates/monoloop-loop/src/transaction/lifecycle/tests.rs`
- `RuntimeConfig::{hold_start, start_queue_capacity}`, `StartHoldGate`

**Problem:** Present tests covered OS-thread submit, duplicate session, capacity
plus-one, one completion per shutdown, and a best-effort zero-deadline wait.
Spec §22.1 also required parked-worker admission, start-queue rollback,
submit-vs-shutdown two outcomes, rejected silent delivery, and non-conditional
short-wait TimedOut.

**Remediation:**

- `StartHoldGate` + `start_queue_capacity` test overrides (production defaults
  unchanged).
- `FakeEndpoint::Hang` parked-worker admission proof.
- Start-queue-full rollback proves global/channel/ledger permits + silent
  delivery.
- Parked Starts still complete on shutdown.
- `short_wait_may_timeout_while_quiescing_then_complete` forces `TimedOut` via
  `StoppedGate` (no contradictory Stopped acceptance).
- Duplicate-session race admits exactly one; submit-vs-shutdown is reject or
  fully admitted + one completion; rejected admits are silent.

**Acceptance criteria:**

- [x] Every §22.1 item has a direct deterministic test.
- [x] No M2 test is green for both of two contradictory outcomes.

**Advisor (2026-08-20):** D-040 closed. Façade cutover stays stopped. **Not**
Golden / §25. D-041 (`.max(1)` / fabricated `Published`) and D-045 (M6 §22
remainder) stay open.

**Advisor (2026-08-20, bar check after D-040):** D-040 **Fixed** stands. D-039
parked-Start **closed** via `parked_starts_reach_stopped_on_shutdown`. Next
kernel bar is **D-041 honesty** — not the §22.2–22.7 matrix. D-045 stays
**open**. **Not** Golden / §25.

## D-041: M2 still substitutes limits and fabricates terminal-event delivery

**Priority:** P2
**Status:** Fixed
**Affected:**
- `crates/monoloop-loop/src/transaction/lifecycle/supervisor.rs`
- `crates/monoloop-contracts` `TerminalEventDelivery::NotAttempted`
- deleted `crates/monoloop-loop/src/transaction/capacity.rs` (`CapacityManagers`)

**Already honest (do not re-open as D-041):**
- `lifecycle/capacity.rs` `ReservationPool::try_new` fail-closes on zero
  (no silent `.max(1)`).
- `lifecycle/owner.rs` start queue is `unwrap_or(max_active)` after a
  nonzero startup check; no start-queue `.max(1)`.
- Spec header is no longer “M0–M2 landed”; it already records D-045 open
  and **not** M0–M7 / Golden / §25.

**Remediation:**
- `finalize_after_terminal` defaults `TerminalEventDelivery::NotAttempted`
  when `publisher_cmd_tx` is `None` (no Seal / `Ended` enqueue).
- Removed public `CapacityManagers` dual API; v2 uses `ReservationPool` only.
- Parked-Start shutdown completions assert `NotAttempted`.

**Out of D-041 close-out:**
- `task_spawner` mailbox `.max(1)` is a derived spawn channel (≥ 32
  from validated `max_active`), not a caller-configured capacity clamp.
- Uncompiled v1 `dispatcher` / `events` / `tool_capacity` `.max(1)`
  stays deferred (D-015 residual / later migration), not this defect.

**Acceptance criteria:**

- [x] No production lifecycle `.max(1)` on caller-configured capacities
      (`ReservationPool` + start queue).
- [x] Completion does not claim `Published` for an event that was not sent.
- [x] `CapacityManagers` is not a public dual capacity API.
- [x] Spec status matches review acceptance (D-045 open; not Golden).

**Advisor (2026-08-20):** D-041 honesty closed. Keep §22.2–22.7 under D-045.
**Not** Golden / §25.

**Expert (2026-08-20):** No remaining fabricated `Published` path or dual
`CapacityManagers` API. `finalize_after_terminal` defaults `NotAttempted` when
`pub_cmd` is `None`; parked-Start asserts it. Spec §6.4 / §19 / §13.2 now
include `NotAttempted` (was stale vs code). Residual: Seal-reply wait is a
hardcoded 200ms, not `terminal_event_delivery_deadline`; uncompiled
`SharedToolCapacity` `.max(1)` stays out of this close-out.

**Advisor (2026-08-20, honesty stop):** **Yes — stop the honesty cutover.**
D-041 **Fixed** stands. Do not reopen for derived spawn-mailbox `.max(1)`,
uncompiled v1 `.max(1)`, or the 200ms Seal wait. Next kernel pick is
**D-045 §22.2**, not host adapters. **Not** M6 done / Golden / §25.

## M2 boundary notes (not separate defects)

- **No v1 file resurrection.** The seven files named in D-003 were not
  recreated. Uncompiled leftovers (`active_registry.rs`, `spawn_gate.rs`,
  `dispatcher.rs`, `exchange.rs`, `events.rs`, `mcp/`, `loop_adapters.rs`)
  match D-003 “deferred until their stage.” `spawn_gate.rs` still comments on
  deleted `executor_spawn` — delete at M7, do not revive.
- **Three components hold.** Connector/Interpreter crates are untouched by M2.
  Loop still composes them. Product crates do not depend on testkit.
  Architecture import gates pass.
- **Identity.** Duplicate `SessionKey` is rejected; no most-recent-session
  heuristic. Grok `sessionId` is not replaced.
- **Core does not invoke host sinks/callbacks.** Host adapters live in the
  Loop crate and run only when the host calls them (spec-allowed). They are
  not a fourth component.
- **Inner `DefaultLoopRuntime`** still uses ambient `tokio::spawn`. That is
  the preserved complete-unit machine (M3/M7 consolidation), not an M2
  resurrection of `DefaultTransactionRuntime`.
- **Accepted structural gap until M4:** realized Connector instances live on
  the cloneable handle `Arc`, not on `RuntimeOwner` (spec §7.1). Do not
  deepen this in M3.

## D-042: M4 ACP owner fusion and process-core join residuals

**Priority:** P1
**Status:** Fixed
**Affected:**
- `crates/monoloop-connector-codex/src/lib.rs` (and cursor/agy twins)
- `crates/monoloop-connector-*/src/process.rs`, `run.rs`
- `crates/monoloop-connector-grok/src/server.rs`, `session.rs`
- `doc/TRANSACTION_RUNTIME_V2_SPEC.md` (M4 status wording)

**Problem:** The first M4 ACP `ConnectionOwnerWork` cut fused the session
update pump into the same `select!` as `prompt_text().await` (deadlock under
`SendAndFinish`). ProcessInner pumps held `Arc<ProcessInner>` so
`kill_on_drop` never ran. Grok `run_connection` / pending connect / session
new/load/send workers were ambient if pending handles were dropped.

**Remediation:**

- Connection-owner update pumps JoinSet-owned (Codex/Cursor/Agy); Claude/Z.ai
  observe control; Grok open path cancel-aware during prompt RPC.
- ProcessInner: `Weak` pumps + JoinSet joined on shutdown; Drop aborts pumps /
  `start_kill`s child.
- Claude/Z.ai `run_*`: JoinSet pumps joined/aborted on timeout.
- Grok: `PendingGrokServer` / `PendingGrokSession` / `PendingGrokExchange`
  expose `wait()` and abort worker joins on Drop; `ServerInner.connection_join`
  + `GrokServerHandle::shutdown`.

**Acceptance criteria:**

- [x] Codex/Cursor/Agy connection owners do not fuse update drain with
      `prompt_text` await.
- [x] Claude/Z.ai owners observe cancel/terminate during input wait.
- [x] ProcessInner pumps do not pin `Arc` after handle drop.
- [x] Grok `run_connection` join retained; pending connect abort-on-drop.
- [x] Grok session pending RPC/new/load spawns join-owned or fail-closed on drop.
- [x] Spec status matches fixed D-042 (optional shared process-core helper remains
      a non-blocking cleanup, not a defect).

## D-043: M5 residuals — process isolation, MCP ownership, join vaults

**Priority:** P2  
**Status:** Fixed  
**Affected:**
- `process_tool.rs` / `ToolKillHandle` process variant
- `lifecycle/mcp_listener.rs` + `enable_mcp_listener`
- Deferred `dispatcher.rs` (local vault stub; no `tool_join_vault` revival)
- `DefaultLoopRuntime::start` / `start_empty` removed (prepare* only; see M5.4.4)
- Busy Loop spawn retries via `spawn_with_busy_retry`

**Problem:** M5 EmptyToolRegistry under `TaskClass::LoopRuntime` still left
Tokio-abort “isolated kill”, ambient Loop `start`, Busy inline-first spawn,
join-vault revival risk, and MCP deferred at startup.

**Remediation:**
- `ProcessIsolatedToolHandler` owns an OS child (`sleep` direct, no `sh -c`
  grandchild); kill/join share one mutex with non-blocking `try_wait` (OS errors
  fail closed; wait bounded by call deadline). Registration requires
  `os_process_isolated()` **and** `supports_isolated_kill()` (structural, not
  boolean-only). Tokio `IsolatedKillableToolHandler` cannot register as
  ProcessIsolated.
- MCP loopback bind at startup (fail-closed); listener as `RuntimeService`;
  shutdown wakes + abort/joins before `Stopped`.
- Join vault module stays deleted; deferred dispatcher uses a local no-op stub.
- Ambient `start`/`start_empty` deleted (M5.4.4); testkit Driver uses
  `prepare_empty` + explicit spawn.
- Busy: bounded supervisor retries, then coordinator-owned last resort.

**Honesty residuals (not reopen):**
- Full MCP gateway / non-empty dispatcher still deferred (empty-tool listener only).
- `IsolatedKillableToolHandler` remains as AbortableAtYield/legacy fixture (not
  ProcessIsolated).
- `spawn_blocking` wait for process tools is not yet a `TaskClass::ToolWorker`
  (handlers not on empty-tool M5 composition path).

**Acceptance criteria:**
- [x] `ProcessIsolated` uses a real OS process kill boundary
- [x] MCP listener tasks registered under `TaskSupervisor` (empty-tool placeholder;
      full MCP gateway/dispatcher still deferred)
- [x] Join vaults not revived; supervisor retains joins
- [x] Production builds do not compile ambient Loop `start`
- [x] Busy Loop spawn prefers supervisor (retry) before inline last resort
- [x] ProcessIsolated registration requires typed
      `RegisteredTool::try_new_process_isolated(ProcessIsolatedToolHandler)`
      (dyn boolean path rejected)

## D-044: Sessionless DirectLlm tool envelopes invent `SessionId`

**Priority:** P2  
**Status:** Fixed  
**Affected:**
- `crates/monoloop-loop/src/transaction/lifecycle/loop_dispatch.rs` (`session_key_for`)
- `DECISIONS.md` D-004

**Problem:** When admission has no session, empty-tool lifecycle events need a
`SessionKey` on `CanonicalToolResult`. Without a policy, this looked like ambient
identity (LAWS 5–7).

**Remediation:** Recorded **DECISIONS.md D-004**: transaction-scoped
`SessionKey` (`tx-{transaction_id}` / `direct` + admitted `ChannelId`) is
**normative** for sessionless DirectLlm tool envelopes; not a resume identity.
Grok / claimed sessions still use the authoritative external id.

**Acceptance criteria:**
- [x] Spec/decision: transaction-scoped key normative for sessionless DirectLlm
- [x] Code and docs match that decision (`session_key_for` + D-004)

## D-045: M6 harden2 — SessionKey post-grace clear + honest partial

**Priority:** P2  
**Status:** Accepted (SessionKey clear) / **M6 §22 closed enough** (2026-08-20)  
**Affected:**
- `lifecycle/supervisor.rs` post-grace `force_remove_tombstone`
- `lifecycle/tests.rs` Seal payload sync + `owned_tasks` TimedOut
- `doc/TRANSACTION_RUNTIME_V2_SPEC.md` M6/M7 / §22
- façade examples still on v1 (`D-038`)

**Advisor (2026-08-20):**

Post-grace `SessionKey` release while admission is already `Quiescing` is
**acceptable fail-closed**. It is not a LAW 7 violation (LAW 7 is “do not invent
a competing correlation id”). It is Law 8/9 + one-active-`SessionKey`: no new
admit can take the key, no most-recent recovery, residual work keeps its
envelope copies, and `Stopped` still requires empty joins.

**Condition:** hard-grace must not abort `Finalizer`/`EventPublisher` then
remove the ledger row *before* the one completion attempt. Prefer
`abort_transaction_residuals` until Seal/completion is recorded; only then
`force_remove_tombstone`. Clearing the index ≠ `Stopped`.

Seal envelope/payload `SessionId` sync and TimedOut residual
`ledger_entries || owned_tasks` under `StoppedGate` are **real** assertions
(not shaped). `block_stopped` stays test-only; production default remains
`None`.

**M6 is partial.** Remaining: full §22 matrix (22.2 races, 22.3 non-yielding
subprocess, 22.4 process-kill, 22.6, 22.7 host adapters), MCP/non-empty tools
(deferred). Do not mark M6 done.

**Preferred next was M7 façade cutover** (close D-038). That cutover is done
(D-038 Fixed). Next is **D-039** (kernel shutdown/control), not the full
adversarial matrix and not host adapters. M7 does **not** satisfy Definition
of Done / Golden; §22 remains the gate before v2 is complete.

**Advisor (2026-08-20, bar check):** Yes — finish remaining M7 (callback
alias / v1 submit-shape deletion + README/spec status) **without** claiming
Golden. Track §22 remainder as **open** (D-039, D-040, D-041, this M6
partial). Host adapters `adapt_completion_callback` / `adapt_event_sink`
stay (M1 / §22.7); they are not core callback APIs. Do not treat uncompiled
`active_registry` / `spawn_gate` / duplicate Loop deletion as Loop-machine
consolidation. Spec header must not read M0–M7 landed.

**Advisor (2026-08-20, façade stop):** Façade cutover **stops**. Do not claim
M0–M7 / Golden / §25. Pick **D-039** in-kernel; do not leave start-queue /
control starve for the host. D-040 is the proof suite for that fix. D-041
and this M6 remainder stay open behind that.

**Advisor (2026-08-20, D-040 closed):** D-039 parked-Start proof + §22.1 suite
landed. Next kernel bar is **D-041** (no `.max(1)`, honest terminal-event
delivery). This M6 remainder stays **open**. **Not** Golden.

**Advisor (2026-08-20, D-041 next):** Keep **§22.2–22.7 open under this
defect.** Partial §22.5 (TimedOut/`StoppedGate`, CAS generation, later
Stopped) is not the mandatory matrix. Do not treat D-041 close-out as
§22.2 finalization races, §22.3 non-yielding subprocess, §22.4
process-kill, §22.6 identity, or §22.7 host-adapter proofs. Host
adapters stay (M1 / §22.7); they are not the next task.

**Advisor (2026-08-20, D-041 Fixed):** Honesty items closed (`NotAttempted`,
`CapacityManagers` removed). **§22.2–22.7 remain open under this defect.**
**Not** Golden / §25 / M6 done.

**Advisor (2026-08-20, honesty stop):** **Yes — stop honesty cutover here.**
Leave the next pick as **this defect’s §22 matrix**, first slice **§22.2
finalization** (exactly-one completion, coordinator panic, cancel/force
races, no event after terminal, failed enqueue consumes no sequence).
**Not host** as an equal alternative: `adapt_event_sink` /
`adapt_completion_callback` stay (M1 / §22.7) and are outside the kernel;
they do not substitute for §22.2–22.6. Partial §22.5 is not M6 item 2
(“all adversarial acceptance tests”). **Do not** promote M6 / Golden / §25.

**§22.2 first slice (2026-08-20):** Landed proofs —
`s22_2_one_completion_per_admission`, dropped-receiver accounting,
coordinator panic → `InvariantFailed`, cancel→force `Terminated` (ledger
re-read at Seal + terminate upgrade path), completion/cancel race one cause,
no event after Seal, failed enqueue consumes no sequence. **M6 still partial**
(§22.3–22.7 + MCP/non-empty tools open). **Not** Golden / §25.

**Acceptance criteria:**
- [x] Post-grace SessionKey clear under closed admission accepted as fail-closed
- [x] Hard-grace path preserves one completion attempt (residuals, not full abort)
- [x] M6 not labelled done / Golden (README + spec remain “partial”)
- [x] M7 cutover started (examples + presentation ported; D-038 Fixed-in-progress)
- [x] M7 façade remainder landed without promoting M6 / Golden / §25 (D-038 Fixed)
- [x] D-041 honesty closed without claiming §22.2–22.7 / Golden
- [x] Honesty cutover stopped; next pick §22.2 (not host, not M6 done)
- [x] §22.2 first-slice proofs landed; §22.3–22.7 remain open
- [x] §22.2 remainder: shutdown between Seal and completion cannot lose
      ledger/completion (`s22_2_shutdown_between_seal_and_completion_keeps_completion`
      + `FinalizerHoldGate` + take `completion_tx` before Seal; hard-grace
      keeps Finalizer, aborts EventPublisher after grace)
- [x] §22.3 in-process ownership proofs: register-before-poll, abort-then-join,
      yielding abortable, `abort_and_drain` → 0, runtime normal/cancel/failure
      counts → 0, Hang exchange pumps joined not detached (`s22_3_*`)
- [x] §22.3 non-yielding future: sacrificial process + outer harness kill;
      `inject_non_yielding_service` parks a never-awaiting `RuntimeService`;
      child short `wait_stopped` → `TimedOut` + `Quiescing` + `owned_tasks>0`;
      never false `Stopped`; missing proof line within outer bound fails
      (`tests/s22_3_non_yielding_sacrificial.rs`)
- [x] §22.4 tools: cooperative ack/non-ack, abortable permit-until-join,
      capacity while owned, process-isolated kill+reap, structural class claim
      (`tests/s22_4_tools.rs`; dispatcher + ToolJoinVault restored)
- [x] §22.6 events/identity: `EstablishExternal` → `SessionEstablished` seq 1;
      concurrent producers contiguous; same session string / different Channels
      isolated; provider tool-call id reuse distinct via exchange-scoped action id;
      item + byte plus-one fail closed (`s22_6_*`)
- [ ] §22.7 host-adapter adversarial proofs (outside core; not next)

**Advisor (2026-08-20, §22.3 vs pause):** **Pause the sacrificial
non-yielding subprocess.** Do **not** pause the M6 kernel remainder.
Honesty cutover stays stopped; D-041 Fixed; §22.2 first slice stands.
**Not** M6 done / Golden / §25. Host adapters stay out.

**§22.2 closed (2026-08-20):** Seal→completion vs shutdown proof landed.
`FinalizerHoldGate` holds Finalizer after Seal; shutdown while held must
`TimedOut`/`Quiescing` (not false `Stopped`); release yields one completion.
Join cancel path maps Tokio task id → `TaskId` so aborted joins cannot
leak meta and strand ledger rows. Hard-grace uses
`abort_transaction_except_finalizer`. **§22.2 matrix complete.**

**§22.3 in-process (2026-08-20):** Landed `s22_3_spawn_registers_before_first_poll`,
`s22_3_abort_then_observed_join`, `s22_3_yielding_abortable_aborted_and_joined`,
`s22_3_abort_and_drain_counts_to_zero`, runtime normal/cancel/failure
counts→0, Hang exchange pumps joined not detached.

**§22.3 sacrificial (2026-08-20):** Landed fail-closed non-yielding proof.
`RuntimeConfig::inject_non_yielding_service` parks a never-awaiting
`RuntimeService` (signals before park so abort-before-poll cannot fake
`Stopped`). Child short `wait_stopped` → `TimedOut` + `Quiescing` +
`owned_tasks>0`; parent asserts the proof line then kills the child; missing
line within outer bound fails. **§22.3 matrix complete.**

**§22.4 tools (2026-08-20):** Re-enabled `dispatcher` + `loop_adapters`.
Restored `ToolJoinVault` (park join+permit; `ToolKillHandle::join_only` for
cooperative ownership; vault Drop transfers pending work to a process-scoped
set — no `mem::forget`, no JoinOnly abort, no false capacity free). Landed
`tests/s22_4_tools.rs` (6) + registered `linked_tools` (14). **§22.4 matrix
complete** with residual: process-scoped pending is over-hold until process
exit / future supervisor drain (not false free; not Stopped-linked yet).

**§22.6 events/identity (2026-08-20):** `EventPublisherCommand::EstablishExternal`
publishes `SessionEstablished` at sequence 1 (identity commit only after
enqueue success; capacity-fail retry proof). Coordinator sends
`EstablishExternal` when DirectLlm exchange returns `external_session_id`.
Concurrent Publish contiguous 1..N; same session string / different Channels
isolated; `tool_action_id_for_exchange` helper + proof; item/byte plus-one
fail closed. Residual: Fake DirectLlm often returns no external id (no e2e
SessionEstablished on echo path); helper not yet adopted by interpreter feed.
**§22.6 closed enough for bar with residuals.** M6 still partial (§22.7 +
MCP/non-empty tools). **Not** Golden / §25.

**Non-empty tools Loop path (2026-08-20):** `run_supervised_tool_loop` +
coordinator uses `HostToolRegistry` selected tools via `ResolvedToolRegistry` /
`HostToolRuntime::with_spawner` (TaskSupervisor-owned tool workers, no ambient
spawn on production path). Empty path unchanged. Proof:
`supervised_non_empty_loop_dispatches_registered_tool`. Residual: Fake echo
does not emit Ready tools end-to-end.

**MCP gateway re-enable (2026-08-20):** Compiled `mod mcp`; capability HTTP
services + request semaphore are **gateway-instance-owned** (no process-global
service map — §17). Registered `tests/mcp_gateway.rs` (15): bind/shutdown,
pending→active list/call, HTTP initialize/list/call, isolation, revoke 404,
oversized body fail-closed.

**RuntimeOwner MCP RuntimeService wiring (2026-08-20):** When
`enable_mcp_listener`, `StartedRuntime` binds/prepares `McpGateway` before
Accepting (fail-closed), **publishes** `mcp_local_addr` / `mcp_gateway` before
the start ready handshake (§7.1 — gate residual closed), and serves as
`TaskClass::RuntimeService` via `PreparedMcpGateway` (no ambient serve spawn
on the production path). Quiesce revokes routes, cancels axum serve, clears
published handle; Stopped waits for join. Proof:
`mcp_listener_owned_shutdown_reaches_stopped` — handle/addr present
immediately after `start`; **live** unknown-capability HTTP 404 (not a
post-shutdown claim). Standalone `McpGateway::bind_loopback` still
JoinHandle-owned for unit tests only.

**ExternalAgent + CreationOnly MCP consumption (2026-08-20):** Coordinator
accepts `ExternalAgent`: SessionAdapter attach → open with attachment →
`PromptReadyGate` (ledger `bind_session` + EstablishExternal + `rebind_session`
+ MCP activate **before** prompt send, D-026 / LAW 7) → exchange. Empty tools
skip MCP install. Non-empty `McpGateway` + `CreationOnly` uses published
`McpGatewayHandle` for `install_pending` → `initial_mcp` → rebind → activate →
revoke after terminal. One shared `ExchangeId` for install + exchange. Attach
failure after install revokes the route (no leak). Missing open external
session id fails closed (no prompt). `McpGateway` skips Loop dual-dispatch.
Admission rejects tool-enabled existing-session reuse on CreationOnly
(`CapabilityMismatch`, D-014). Proofs:
`external_agent_empty_tools_establishes_session_and_completes`,
`creation_only_mcp_install_activate_revoke_round_trip`,
`creation_only_tool_reuse_rejected_at_admission`,
`mcp_route_revoked_when_attach_fails_after_install`,
`mcp_dispatcher_rebind_session_before_activate`.

**MCP `TaskClass::McpRequest` ownership (2026-08-20):** RuntimeOwner gateway
prepare injects `SupervisedMcpRequestOwner` so each HTTP MCP request is
registered as `TaskClass::McpRequest(transaction_id)` via TaskSupervisor.
Concurrency permits + body buffering run **inside** the owned task (Law 22).
Spawn uses non-blocking `try_send`; Busy/Rejected/Orphaned fail closed with 503
(work undriven). Standalone `bind_loopback` remains inline (no supervisor).
Proofs: `mcp_http_request_registers_task_class_mcp_request` (TaskClass via pump),
`supervised_mcp_owner_returns_503_when_spawn_rejected`,
`runtime_owner_mcp_http_uses_supervised_request_owner` (StartedRuntime:
RuntimeService live + HTTP non-503 under injected owner). TaskClass
observation is the instrumented-pump proof, not the RuntimeOwner smoke.
**Bronze** for this residual. Refreshable MCP not declared by current profiles
(WP12).

**§22.7 host-adapter proofs (2026-08-20):** Outside-core proofs in
`tests/s22_7_host_adapters.rs` (5): blocking completion callback before
future; never-yielding completion future; event consumer stops draining;
receivers dropped immediately; host adapter task destroyed. In all cases
`wait_stopped` reaches `Stopped` — adapters run on caller tasks and cannot
stall the supervisor. **M6 §22 matrix closed enough** (Refreshable undeclared).
**Not** Golden / §25 DoD (independent review + additional §23 bullets still
open).

**§23 verification hygiene (2026-08-20):** Core §23 commands run green on
this tree: `cargo fmt --all -- --check`, `cargo clippy --workspace
--all-targets --all-features -- -D warnings`, `cargo test --workspace
--all-targets --all-features`, `RUSTDOCFLAGS="-D warnings" cargo doc
--workspace --no-deps` (rustdoc private/broken link fixes in loop crate).
Loop README aligned to **M6 §22 closed enough**. §22.5 compatible TimedOut
snapshots: `m6_wait_stopped_timed_out_snapshots_compatible` (`wait_stopped`
is `&mut self` — concurrent joiner not an API surface). Remaining §23
extras: forbidden-pattern search, isolated adversarial subprocess harness
inventory, exact-limit plus-one audit completeness, independent P0–P2
review. Refreshable MCP undeclared (WP12). **Not** Golden / §25.

**Tool spill runtime-scoped (2026-08-20):** Removed process-global
`OnceLock` pending transfer. Unfinished joins/permits park on
runtime-scoped `RuntimeToolSpill` (`RuntimeShared` → coordinator
`with_runtime_spill`). Supervisor quiesce runs `shutdown_progress`
(abort AbortableAtYield; release join-less orphans; reap finished);
**JoinOnly blocks `Stopped`** until joined (Law 8 / 23 / §21). Spill Drop
last-resort aborts when the runtime Arc is gone — no cross-runtime bleed.
`ready_to_stop` requires spill empty. Proof:
`s22_4_tool_spill_is_runtime_scoped_not_process_global`.
`HostToolRuntime::new` deprecated (ambient spawn; use `with_spawner`).

**Known residuals (honest — block Golden, not M6 §22 closed-enough):**
- Ambient `tokio::spawn` remains on deprecated `DefaultLoopRuntime::start*`,
  deprecated `HostToolRuntime::new`, sticky_cancel unit helper, and standalone
  `McpGateway::bind_loopback`. Production handlers + §22.4 fixtures drive
  inline; JoinOnly Stopped inject is TaskSupervisor-owned; join vault
  retired to orphan-permit set only. Golden still wants remaining exact-limit
  gaps / independent P0–P2 review / deprecated ambient API retirement.

**Advisor (2026-08-20, independent review — honesty residual slice):**

**Bar (at review time):** **M6 §22 closed enough** held; **Not** Golden /
§25. Review found two P1s (vault vs Stopped; ambient handler spawn).

**Remediation note (same day):** P1 #1 (process-global vault / Stopped
blind to parked tool work) **addressed** by `RuntimeToolSpill` +
`ready_to_stop` spill-empty gate (see “Tool spill runtime-scoped” above).
P1 #2 (handler-level `tokio::spawn`) **still open** for Golden —
JoinHandles are retained into the spill, but §21 / M5.4 still ask for
TaskSupervisor ownership of tool worker bodies. Re-gate before claiming
§23 “no unresolved P0/P1/P2”.

Unlisted residuals (block Golden; do not demote the named M6 bar):
- `RuntimeOwner` Drop abandons the OS-thread join after grace (§18.4
  MUST NOT detach).
- Spec M5.4 / §20 still lists “delete tool join vaults” as the end state;
  spill is the interim honesty fix until JoinOnly fixtures are supervisor-owned.
  (`owned_processes` snapshot honesty landed 2026-08-20 — see below.)

§23 extras still open: forbidden-pattern search, isolated adversarial
harness inventory, exact-limit plus-one audit. Refreshable MCP undeclared
(WP12).

**Expert + Advisor (2026-08-20, post-spill gate):** **PASS — Silver.**
Prior vault/Stopped/process-global **P1 closed**. M6 §22 closed-enough
**still holds**. Handler-level `tokio::spawn` + M5.4 delete-vaults end state
remain Golden blockers. Do **not** promote Golden / §25.

**JoinOnly↔Stopped RuntimeOwner proof (2026-08-20):** Closed the P2 coverage
gap from the spill gate. Test-only `JoinOnlySpillInject` parks a cooperative
JoinOnly join on `RuntimeShared.tool_spill` at supervisor start.
`join_only_spill_blocks_stopped_until_released` proves short `wait_stopped` →
`TimedOut` + `Quiescing` with `tool_spill_pending() >= 1`, then release →
`Stopped` with spill empty. Production leaves inject `None`.

**Expert + Advisor (2026-08-20, JoinOnly proof gate):** **PASS — Silver.**
Spill-gate P2 coverage closed. M6 §22 closed-enough **still holds**. Handler
ambient spawn + M5.4 remain Golden blockers. Do **not** promote Golden / §25.

**M5.4 inline handler drive (2026-08-20):** `AsyncToolHandler` /
`IsolatedKillableToolHandler` no longer `tokio::spawn`. Bodies return as
`LinkedToolExecutionHandle::drive` and are polled on the dispatcher task
(supervised `ToolWorker` under RuntimeOwner / `with_spawner`). Kill is
`ToolKillHandle::cancel_only`. Proof:
`s22_4_async_handler_drives_inline_no_ambient_join`. Cooperative JoinOnly
fixtures and `ProcessIsolated` wait tasks may still spawn (test / OS wait).
Spill remains for JoinOnly park; M5.4 “delete vaults” end state not claimed.
Deprecated `HostToolRuntime::new` / `bind_loopback` residuals unchanged.

**Expert + Advisor (2026-08-20, M5.4 inline drive gate):** **PASS — Silver.**
Production Abortable-handler ambient-spawn residual closed. M6 §22
closed-enough **still holds**. Delete-vaults / JoinOnly fixtures /
ProcessIsolated wait / §23 extras remain Golden blockers. Do **not** promote
Golden / §25.

**ProcessIsolated drive + §23 forbidden-pattern (2026-08-20):**
`ProcessIsolatedToolHandler` wait loop is now
`LinkedToolExecutionHandle::drive` (async `try_wait` + `tokio::time::sleep`;
no `spawn_blocking`). Dispatcher `await_tool_termination_driven` escalates
OS kill after ProcessIsolated grace. `has_join` / DispatchGuard Drop hold
capacity (orphan park) until the child is observed exited. §23 gate:
`tests/s23_forbidden_patterns.rs` (undocumented ambient spawn search with
documented exceptions; §22.7 suite inventory). Still open: exact-limit
plus-one audit completeness, isolated adversarial subprocess harness,
`owned_processes` always 0, JoinOnly fixture spawn, delete-vaults.
**Not** Golden / §25.

**Expert + Advisor (2026-08-20, ProcessIsolated/§23 gate):** **PASS — Silver.**
M6 §22 closed-enough **still holds**. Capacity-on-Drop honesty for driven
ProcessIsolated addressed in-slice (orphan park while child alive). Do **not**
promote Golden / §25.

**owned_processes + exact-limit inventory (2026-08-20):**
`ShutdownSnapshot.owned_processes` reads `RuntimeShared.owned_processes`
(`AtomicU32`). ProcessIsolated registers via
`ToolKillHandle::register_owned_process` at dispatch; lease releases on reap
(drive / join_timeout / has_join) or spill orphan Drop. Proof:
`process_isolated_owned_processes_counter_tracks_live_child`. §23 inventory
gate: `s23_exact_limit_plus_one_inventory_present` (high-value proofs present;
MCP concurrency/duration + some canonical variants still open in DEFECTS).
Still open: adversarial subprocess harness, JoinOnly fixture spawn,
delete-vaults, remaining exact-limit gaps, `CleanupStatus.owned_processes`
hardcode. **Not** Golden / §25.

**Expert + Advisor (2026-08-20, owned_processes gate):** **PASS — Silver.**
M6 §22 closed-enough **still holds**. Do **not** promote Golden / §25.

**§23 adversarial lifecycle subprocess harness (2026-08-20):** Added
`tests/s22_4_join_only_spill_sacrificial.rs` — isolated child proves JoinOnly
spill → short `wait_stopped` → `TimedOut` + `Quiescing` + `spill_pending>=1`,
never false `Stopped`; parent bounds with `recv_timeout` and always kills the
child. Inventory gate:
`s23_adversarial_lifecycle_subprocess_harness_inventory` covers both
§22.3 non-yielding and §22.4 JoinOnly spill harnesses. Still open: JoinOnly
*fixture* ambient spawn (s22_4_tools handlers), delete-vaults end state,
remaining exact-limit gaps, independent P0–P2 review,
`CleanupStatus.owned_processes` hardcode. **Not** Golden / §25.

**Expert + Advisor (2026-08-20, sacrificial harness gate):** **PASS — Silver.**
Named §23 adversarial lifecycle subprocess residual closed. M6 §22
closed-enough **still holds**. Do **not** promote Golden / §25.

**JoinOnly fixture inline + CleanupStatus honesty (2026-08-20):**
`AckCancelCooperative` / `IgnoreCancelCooperative` in `s22_4_tools` no longer
`tokio::spawn` — inline `drive` + `cancel_only`. Dispatcher parks orphan
permit on cooperative deadline for cancel_only non-ack (§22.4 capacity).
`CleanupStatus::Pending` uses live `owned_tasks` / `owned_processes` /
`tool_spill.pending_count()` (no hardcodes). Spill / `JoinOnlySpillInject`
remain interim (delete-vaults end state not claimed). **Not** Golden / §25.

**Expert + Advisor (2026-08-20, JoinOnly fixture gate):** **PASS — Silver.**
Named fixture ambient-spawn residual closed. M6 §22 closed-enough **still
holds**. Do **not** promote Golden / §25. Caveat: cancel_only non-ack proves
capacity hold after drive-stop, not unforceable JoinOnly work (that remains
spill-inject / sacrificial).

**JoinOnly inject under TaskSupervisor (2026-08-20, M5.4 delete-vaults step):**
`JoinOnlySpillInject` no longer ambient-`tokio::spawn`s or parks on
`RuntimeToolSpill`. Supervisor registers `TaskClass::RuntimeService` that
`thread::park`s until `release()` unparks (abort-resistant). Proofs:
`join_only_owned_task_blocks_stopped_until_released` + sacrificial harness
assert `owned_tasks` (not `spill_pending`). Spill remains for orphan permits
only. Full vault deletion still open. **Not** Golden / §25.

**Expert + Advisor (2026-08-20, TaskSupervisor JoinOnly gate):** **PASS —
Silver.** M5.4 JoinOnly ownership step sound. M6 §22 closed-enough **still
holds**. Do **not** claim delete-vaults complete / Golden / §25.

**Join vault retired → orphan-permit set (2026-08-20, M5.4):**
`RuntimeToolSpill` is now `OrphanToolPermitSet` (type alias retained). No
JoinHandle parking; `Stopped` gated on TaskSupervisor emptiness only (orphans
released at quiesce). Deprecated `ToolKillHandle::new` / `join_only`. Proof:
`s22_4_orphan_permits_are_runtime_scoped_not_process_global`. **Not** Golden /
§25 (exact-limit gaps / independent review / deprecated ambient APIs remain).

**Expert + Advisor (2026-08-20, orphan-permit vault gate):** **PASS — Silver.**
Join-vault retirement sound for production path. M6 §22 closed-enough **still
holds**. Do **not** claim M5.4 fully complete / Golden / §25 (ambient APIs +
deprecated join constructors remain).

**Deprecated ambient API retirement (2026-08-23, M5.4.4):**
- Removed `HostToolRuntime::new` (ambient spawn branch gone); only
  `with_spawner` remains.
- Removed `DefaultLoopRuntime::start` / `start_empty` (prepare* only).
- Removed `ToolKillHandle::new` / `join_only` and dead `KillInner::Tokio` /
  `JoinOnly` variants; production kill surface is `cancel_only` + Process.
- Removed production `McpGateway::bind_loopback` owned-join wrapper; `McpGateway`
  is a prepare-only namespace. Integration tests own serve via local
  `BoundGateway` / prepare+spawn. §23 production exceptions reduced to
  `sticky_cancel` unit-test module only.
Proof: `s23_no_undocumented_ambient_tokio_spawn_in_production_src`,
`loop_adapters_available_not_dispatch_rejected_placeholder`, mcp_gateway suite.
**Not** Golden / §25 (exact-limit gaps / independent P0–P2 review remain;
sticky_cancel cfg(test) spawn is documented exception).

**Advisor (2026-08-23, M5.4.4 ambient-API retirement):** **PASS — Silver.**
Named ambient constructors are gone from production `src` (`HostToolRuntime`
is `with_spawner` only; Loop is `prepare*` only; kill surface is
`cancel_only` + Process; `McpGateway` is prepare-only). Architecture gates
hold (product crates still do not depend on testkit; three-component
boundary intact). §23 production spawn exceptions reduced to `sticky_cancel`
`cfg(test)`. Do **not** promote Golden / §25 / full M5.4 completion.
Chronological “Known residuals” bullets above that still name
`HostToolRuntime::new` / `start*` / `bind_loopback` are historical; live
residuals are exact-limit plus-one gaps, independent P0–P2 review, and the
documented sticky_cancel test exception.

**Next pick:** remaining exact-limit plus-one gaps **or** independent P0–P2
review. Do not promote Golden / §25.

**MCP concurrency/duration exact-limit proofs (2026-08-23, D-034 residual):**
Injectable `McpGatewayLimits` (production defaults unchanged) + three proofs:
per-capability concurrency plus-one → 429, global concurrency plus-one → 429,
request-duration plus-one (hanging body) → 504. Inventory gated in
`s23_exact_limit_plus_one_inventory_present`. D-034 concurrency/duration
checkbox closed. Remaining exact-limit gaps (e.g. D-033 output-byte plus-one,
D-035 every canonical message variant) and independent P0–P2 review still
block Golden / §25. **Not** Golden / §25.

**Next pick:** remaining exact-limit gaps (admission/output-byte / canonical
variants) **or** independent P0–P2 review. Do not promote Golden / §25.

**D-033 HTTP absolute-deadline / output-queue proofs (2026-08-23):**
Closed the three open D-033 acceptance checkboxes with
`crates/monoloop-connector/tests/streaming_http.rs`:
`absolute_request_deadline_covers_header_and_body_delay`,
`full_output_queue_terminates_at_overall_deadline`,
`max_queued_output_bytes_plus_one_fails_closed`. D-018/D-034 honesty
leftovers from the prior Advisor gate also cleared (checkbox / progress row /
s23 prose); MCP concurrency holds use a body-poll barrier. Remaining
exact-limit gap called out for Golden: D-035 every canonical message variant.
Independent P0–P2 review still open. **Not** Golden / §25.

**Next pick:** D-035 canonical-variant exact-limit matrix **or** independent
P0–P2 review. Do not promote Golden / §25.

**Advisor (2026-08-23, D-034 concurrency/duration residual):** **PASS — Silver.**
Scoped close is sound: injectable `McpGatewayLimits` (defaults 64 / 8 / 30s
unchanged on the RuntimeOwner path) + fail-closed plus-one proofs (429 / 429 /
504) + inventory needles. Permits are acquired before body read; body and
dispatch share one deadline. Services stay gateway-owned (not process-global).
MCP remains Component 3; product crates still do not depend on testkit.
Do **not** promote Golden / §25.

Honesty leftovers from the gate (closed same day — do not reopen D-034):
D-018 residual checkbox, 2026-08-18 progress “known residual” row, and
`s23` prose naming MCP concurrency/duration as a gap were updated to match
the closed proofs. Concurrency hold uses an incomplete HTTP/1.1 chunked POST
(server-side body wait; no client body-stream poll race) + probe-until-429.
Remaining coverage caveats (not reopen): duration plus-one is hanging-body
only; global plus-one is one capability with `max_global=1` (semaphore
proven; cross-capability isolation not).

**Advisor (2026-08-23, D-033 acceptance + D-034 honesty leftovers):**
**PASS — Silver.** Named D-033 proofs pass
(`absolute_request_deadline_covers_header_and_body_delay`,
`full_output_queue_terminates_at_overall_deadline`,
`max_queued_output_bytes_plus_one_fails_closed`) and are inventory-gated.
Connector uses one absolute request deadline before send; blocked enqueue
selects control + remaining overall deadline; output queue capacity is
taken from `max_queued_output_bytes` (not input buffers). D-018/D-034
checkboxes and MCP plus-one proofs still hold. Product crates still do
not depend on testkit. HTTP stays Connector; MCP stays Component 3.
Do **not** promote Golden / §25.

Coverage caveats at gate time (idle residual closed same day — see below):
blocked enqueue previously slept only on remaining overall (comment
overclaimed idle); output plus-one fail-closes via deadline on a full
queue rather than a distinct limit error; D-034 duration is hanging-body
only and global plus-one is one capability with `max_global=1`.

**Expert (2026-08-23, D-033 + D-034 honesty/hold-fix):** **PASS — Silver
with residual.** Absolute deadline + output-queue plus-one + MCP hold
proofs sound for Law 22. Material gap: blocked enqueue select omitted
`idle_timeout` despite comment. Do **not** promote Golden / §25.

**D-033 blocked-enqueue idle select (2026-08-23):** Enqueue budget is now
`idle_timeout.min(remaining_overall)`; on timer fire, distinguish idle vs
overall message. Proof:
`blocked_enqueue_honors_idle_before_overall_deadline` (idle=80ms,
overall=5s, capacity-1 queue, host not receiving → idle, not overall).
Inventory needle added. **Not** Golden / §25.

**Advisor (2026-08-23, Expert idle-enqueue residual):** **PASS — Silver.**
Expert material gap is closed: blocked output enqueue selects control plus
`idle_timeout.min(remaining_overall)` and classifies idle vs overall on
timer fire. Complementary overall-deadline proof
(`full_output_queue_terminates_at_overall_deadline`) still holds. HTTP
stays Connector; product crates still do not depend on testkit. Named
proof and s23 inventory needle exist. Do **not** promote Golden / §25.

Honesty leftovers (do not reopen D-033; do not block this slice):
D-033 acceptance still names only the original three checkboxes (idle
proof is inventory-gated, not listed on the defect); output plus-one
still fail-closes via deadline rather than a distinct limit error;
D-019 cancel-during-blocked-enqueue remains an unchecked inherited
checkbox.

**D-035 admission byte accounting restored (2026-08-23):** Confirmed live
v2 admit did **not** enforce `TransactionLimits.max_input_bytes` /
`max_messages` (roomy `CanonicalInput::try_new` only). Fixed with
`estimate_canonical_input_bytes` (text + names + tool ids + tool names +
serialized args; encode fail-closed) + admit checks via
`RuntimeShared.transaction_limits`. Proofs:
`estimate_counts_names_ids_and_tool_arguments`,
`max_messages_plus_one_rejected_at_admit`,
`max_input_bytes_plus_one_rejected_at_admit`,
`large_tool_arguments_counted_toward_max_input_bytes`. Optional residual:
exhaustive per-variant matrix; continuation still bounds encoded provider
bytes. **Not** Golden / §25.

**Next pick:** independent P0–P2 review **or** remaining exact-limit matrix
polish. Do not promote Golden / §25.

**Advisor (2026-08-23, D-035 admission enforcement restore):** **PASS — Silver.**
Status Fixed is honest for the named bypass: live v2 admit now applies
`RuntimeShared.transaction_limits` (from bootstrap config, not roomy
`InputLimits` construction). `estimate_canonical_input_bytes` covers every
canonical field (text, optional names, tool-call ids, tool names, serialized
args) and encode failure rejects rather than counting zero. Named proofs
pass: `estimate_counts_names_ids_and_tool_arguments`,
`max_messages_plus_one_rejected_at_admit`,
`max_input_bytes_plus_one_rejected_at_admit`,
`large_tool_arguments_counted_toward_max_input_bytes`. Estimate lives in
contracts; enforcement in Loop admission; product crates still do not depend
on testkit. Continuation remaining-budget stays on encoded provider bytes
(`max_remaining_provider_input_bytes`), which matches implementation §12
rather than a silent spec deletion. Exhaustive per-variant plus-one matrix
and exact-limit (`== max` admits) stay optional residuals. Do **not**
promote Golden / §25.

Coverage caveats at gate time (closed same day where noted):
`max_content_parts` plus-one and exact `max_input_bytes` equality were
Advisor leftovers — closed below. Serialize fail-closed remains an `Err`
arm without a dedicated fixture (`serde_json::Value` encode almost never
fails). Independent P0–P2 review still open for Golden / §25.

**D-035 matrix polish (2026-08-23):** Advisor coverage leftovers closed:
`max_content_parts_plus_one_rejected_at_admit`,
`max_input_bytes_exact_admits_plus_one_rejects` (exact admits; exact+1
rejects). Inventory needles added. Serialize-fail fixture still optional.
**Not** Golden / §25.

**Independent residual scan (2026-08-23 — honest, not a §25 claim):**

Live Golden / §25 blockers (named; not chronological noise):
- Independent P0–P2 / security process sign-off (D-025 residual; §23).
- Exhaustive public-limit exact/plus-one matrix completeness (§23 wording).
- Inherited unchecked acceptance boxes on otherwise-Fixed defects that still
  describe real gaps when re-read against code: D-019 cancel-during-blocked
  enqueue; D-032 dedicated start-on-A / submit-from-OS-thread e2e
  (submit-from-OS-thread already proven in lifecycle tests — e2e across
  reactors still desirable); D-036 barrier concurrent-producer order proof
  residual if not covered by ordered publisher tests.
- RuntimeOwner Drop abandon-after-grace honesty residual (prior M6 notes).
- Refreshable MCP undeclared (WP12).

Closed this session (do not re-open as ambient/D-034/D-033/D-035 bypasses):
M5.4.4 ambient constructors; MCP concurrency/duration plus-one; D-033
absolute deadline + idle-on-blocked-enqueue; D-035 admit byte accounting +
content-parts/exact equality.

**Next pick:** independent P0–P2 deep review of a named live blocker
(D-019 cancel-during-blocked-enqueue **or** RuntimeOwner Drop honesty **or**
D-036 residual) — not more optional matrix polish. Do not promote Golden /
§25.

**Advisor (2026-08-23, D-035 leftovers + residual scan):** **PASS — Silver.**
Closing the named D-035 Advisor caveats plus an honest residual inventory
meets the **slice** bar. It does **not** meet Golden / §25. Product shape
holds (estimate in contracts, admit in Loop; no testkit product dep; no
ambient session). Named leftovers are proven:
`max_content_parts_plus_one_rejected_at_admit`,
`max_input_bytes_exact_admits_plus_one_rejects` (inventory-gated; lib tests
pass). D-035 Status Fixed stays honest for the named bypass. Exhaustive
per-variant codegen and a serialize-fail fixture remain optional. Do **not**
treat this scan as the §23 independent P0–P2 review (D-025 residual) — it is
a named-inventory triage, correctly labeled “not a §25 claim”.

Honesty on the listed next picks (do not rewrite closed work):
- **RuntimeOwner Drop abandon-after-grace** is the strongest **behavioral**
  live blocker: `Drop` parks the OS-thread join behind a grace then abandons
  (`owner.rs`); §18.4 MUST still own/join and MUST NOT detach. Prefer this
  as the next named pick.
- **D-019 cancel-during-blocked-enqueue** is a **proof** residual unless
  re-review shows otherwise: blocked enqueue already `select`s
  `wait_control`; missing is a dedicated full-queue cancel test, not a
  second HTTP deadline rewrite.
- **D-036** inherited checkboxes still name deleted v1 files; v2
  `EventPublisher` serializes allocation+enqueue, and
  `s22_6_concurrent_producers_contiguous_sequence` already asserts
  contiguous 1..N delivery. Confirm coverage before re-opening the
  sequencer.

**Next pick:** RuntimeOwner Drop honesty (§18.4) — or, if staying in
Connector proofs, D-019 cancel-during-blocked-enqueue. Do not promote
Golden / §25. Do not spend the next slice on optional D-035 variant
matrix polish.

**RuntimeOwner Drop §18.4 join (2026-08-23):** Removed abandon-after-grace
detach. `Drop` still begins shutdown / releases test hold gates, then
**always** `join`s the executor OS thread (MAY block indefinitely on
non-cooperative work; MUST NOT detach). Proof:
`runtime_owner_drop_joins_executor_thread_reaches_stopped`. Hosts needing
bounded exit MUST use ProcessIsolated + explicit shutdown before drop
(spec §18.4). **Not** Golden / §25.

**Next pick:** D-019 cancel-during-blocked-enqueue proof **or** confirm
D-036 coverage / independent P0–P2 process. Do not promote Golden / §25.

**Advisor (2026-08-23, RuntimeOwner Drop §18.4):** **PASS — Silver.**
Named abandon-after-grace detach is closed. `RuntimeOwner` is `#[must_use]`;
supported path remains `begin_shutdown` + `wait_stopped` until `Stopped`.
Drop on a live owner still initiates shutdown (and `StopSupervisor`), then
**always** `join`s the executor OS-thread handle. It does not timeout-abandon
the join, drop a live `JoinHandle`, detach the thread, or invent `Stopped`
(supervisor still writes the state). Empty cooperative proof
`runtime_owner_drop_joins_executor_thread_reaches_stopped` plus s23 inventory
needle exist. Product crates still do not depend on testkit. Law 23 / §18.4
MUST NOT detach holds for this owner path.

Do **not** reopen as the old grace-then-abandon: `rt.shutdown_timeout(2s)`
runs only after `run_supervisor` returns (Stopped), as Tokio worker teardown
— not Drop dropping the OS join. Optional residual: no Drop-while-JoinOnly
hang proof (empty runtime reaches Stopped before any former grace would
fire). Join-panic on Drop is swallowed (`let _ = thread.join()`); §19
stable diagnostic codes stay a Golden concern on the contract-violation
path. Hosts that need bounded process-exit still MUST use ProcessIsolated
and complete explicit shutdown before drop.

Do **not** promote Golden / §25 (independent P0–P2, exhaustive limit matrix,
inherited D-019/D-036 boxes, M5.4 delete-vaults, WP12 refreshable MCP).

**Next pick:** D-019 cancel-during-blocked-enqueue is a **proof** residual
(blocked enqueue already `select`s `wait_control`; missing is a dedicated
full-queue cancel test). D-036 Status Fixed is likely honest in v2
(`OrderedEventPublisher` serialize allocate+enqueue;
`s22_6_concurrent_producers_contiguous_sequence` asserts contiguous 1..N
delivery) — **confirm** inherited v1 checkboxes before re-opening the
sequencer. Prefer D-019 if adding a test; prefer D-036 confirm if closing
paper. Not more Drop polish.

**D-019 cancel-during-blocked-enqueue proof (2026-08-23):** Added
`cancel_interrupts_blocked_output_enqueue` — capacity-1 queue, host not
receiving, long idle/overall, cancel while second chunk enqueue is blocked →
`ConnectionEndKind::Cancelled`. Named D-019 cancel acceptance checkbox
closed; remaining D-019 residual is optional multi-phase timing harness.
**D-036 confirm (same day):** Inherited v1 file paths updated to v2
`OrderedEventPublisher` + `s22_6_concurrent_producers_contiguous_sequence`
(contiguous 1..N allocation and delivery). SessionEstablished-at-1 /
allocate-only-on-enqueue residuals stay optional harnesses — not a sequencer
reopen. **Not** Golden / §25.

**Next pick:** independent P0–P2 process / §23 security sign-off **or**
optional harness residuals (D-019 timing matrix, D-036 SessionEstablished-at-1).
Do not promote Golden / §25.

**Advisor (2026-08-23, D-019 cancel proof + D-036 v2 confirm):** **PASS — Silver.**
Named D-019 cancel checkbox is honest: blocked HTTP `out_tx.send` is selected
against `wait_control` plus `idle.min(remaining_overall)`, and
`cancel_interrupts_blocked_output_enqueue` (capacity-1 queue, host not
receiving, long idle/overall) completes as `ConnectionEndKind::Cancelled`.
D-036 Status Fixed is honest on the **live** v2 path: a single
`run_event_publisher` task serializes allocate+enqueue; concurrent producers
send commands only. Named proofs pass:
`s22_6_concurrent_producers_contiguous_sequence` (contiguous 1..N and
delivery order), `s22_6_session_established_is_sequence_one`,
`s22_2_failed_enqueue_consumes_no_sequence`,
`s22_6_establish_external_capacity_fail_does_not_steal_seq1`. Product crates
still do not depend on testkit. HTTP stays Connector; event sequencing stays
Component 3. Do **not** promote Golden / §25.

Honesty leftovers closed on paper only (do not reopen the defects):
D-036 Affected no longer names uncompiled `events.rs` /
`OrderedEventPublisher` as the live sequencer. Unit-path seq-1 and
enqueue-fail-before-allocate residuals are covered; remaining optional
harness is create-path concurrent fan-out vs first CanonicalUnit.

Coverage caveats (optional; do not block this slice):
- D-019 cancel proof uses a 50 ms settle, not a fill-queue barrier; cancel
  before the second enqueue still yields `Cancelled` via the outer select.
- D-019 multi-phase elapsed-sum timing matrix remains optional.
- Exhaustive public-limit matrix, M5.4 delete-vaults, WP12 refreshable MCP,
  and D-025 independent review remain Golden / §25 blockers.

**Next pick:** independent P0–P2 process / §23 security sign-off. Do not
spend the next slice on optional D-019/D-036 harness polish. Do not
promote Golden / §25.

**Independent P0–P2 residual review (2026-08-23):**

Scope: live compiled product paths in `monoloop-contracts` /
`monoloop-connector` / `monoloop-loop` (+ WP12 limitations). Not a
substitute for an external security audit (D-025 process residual).

| Class | Finding |
|---|---|
| **P0 (live)** | **None found** in compiled product paths reviewed this session. |
| **P1 behavioral** | Named session closers held: §18.4 Drop joins; D-019 cancel-on-blocked-enqueue proven; D-036 live publisher path + contiguous 1..N / seq-1 / no-steal proofs pass. |
| **P2 / process** | D-025 independent security audit sign-off remains **organizational**. WP12 **Refreshable MCP** is deliberately undeclared (limitation doc), not a silent production promise. Exhaustive public-limit exact/plus-one matrix still incomplete vs §23 wording. |
| **Paper / naming** | Uncompiled deferred modules (`active_registry`, `events`, `exchange`) remain on disk — not live. `RuntimeToolSpill` was a join-vault-shaped **alias**; production set is `OrphanToolPermitSet` only (M5.4). |

**M5.4 delete-vaults naming close (same review):** Internal call sites now use
`OrphanToolPermitSet`. `RuntimeToolSpill` retained as `#[deprecated]` alias
only (not a join vault). Join vault retirement for production path is
**closed**; alias deprecation is API honesty, not a reopen.

**Does not claim:** Golden / §25, full concurrent/race/load suites, Grok
multi-session Golden conformance, or D-025 process sign-off complete.

**Next pick:** organizational D-025 / §23 security process **or** a declared
WP12 profile decision (Refreshable MCP) — not more alias polish. Do not
promote Golden / §25.

**Advisor (2026-08-23, independent P0–P2 triage + M5.4 naming close):**
**PASS — Silver** for this **process slice**.

Honesty holds: the write-up is a named-inventory triage of live compiled
paths (`monoloop-contracts` / `monoloop-connector` / `monoloop-loop` +
WP12), **not** Golden / §25, **not** D-025 external-audit complete, and
**not** the §23 bullet “independent review finds no unresolved P0/P1/P2”
(P2 items remain by design of this slice). Product shape holds (no testkit
product dep; no ambient session; no join vault on the production set).
Spot-check of the M5.4 close: `OrphanToolPermitSet` stores permits/leases
only; `DispatchGuard` parks capacity, not `JoinHandle`s; `JoinOnlySpillInject`
is TaskSupervisor-owned (name retained for API stability). Deprecated
`RuntimeToolSpill` alias + crate re-export is API honesty, not a vault
reopen. Join-vault retirement for the production path stays **closed**.

Do **not** treat “P0 none found (this session / this scope)” as a
kernel-wide security sign-off: Interpreter, profile crates, and live Grok
multi-session were out of this triage’s stated scope. Leftover
vault-shaped **identifiers** (`tool_spill` fields / `tool_spill_pending` /
`reap_finished` no-op / crate-root `RuntimeToolSpill`) are paper, not a
join-vault reopen — do not spend the next slice on alias polish.
Deferred on-disk modules also include `spawn_gate` (and v1
`transaction/exchange.rs`); `lifecycle/exchange.rs` **is** live v2.

Do **not** promote Golden / §25 / D-025 complete.

**Next pick:** organizational D-025 / §23 security process **or** a
declared WP12 Refreshable MCP profile decision. Not more naming polish.

**WP12 Refreshable MCP decision + D-025 checklist (2026-08-23):**
Deliberate posture recorded as **DECISIONS D-042**: initial shipped profiles
MUST NOT declare `Refreshable`; ExternalAgent MCP stays `CreationOnly` (CLI
`None`) until a superseding decision with vendor evidence. Qualification
gate: `six_profile_bindings_register_and_validate` asserts no profile uses
`Refreshable`. WP12 limitations/acceptance/capability report cite D-042.
D-025 process residual gains `doc/SECURITY_REVIEW_CHECKLIST.md` (unsigned —
does **not** close independent audit). **Not** Golden / §25.

**Next pick:** obtain independent D-025 / §23 security sign-off on the
checklist **or** continue optional exact-limit / load harness work. Do not
implement Refreshable without superseding D-042. Do not promote Golden /
§25.

**Advisor (2026-08-23, DECISIONS D-042 + SECURITY_REVIEW_CHECKLIST):**
**PASS — Silver** for this **process slice**.

The slice does what the prior next-pick allowed: a **declared** WP12
Refreshable posture, plus a D-025 organizational artifact. It does **not**
meet Golden / §25 and does **not** close D-025 / §23 independent review.

Holds:
- Product shape: no Refreshable implementation, no testkit product dep,
  no ambient session, no dual session ID, no persistence.
- Law 3/4: deferral is an explicit `DECISIONS.md` contract, not a silent
  gap. CreationOnly reuse still fail-closed at admission (D-014).
- Qualification not marked done: enum variant retained; initial profiles
  MUST NOT declare `Refreshable`; gate is
  `six_profile_bindings_register_and_validate` (+ s23 needle on that
  assertion). WP12 limitations / acceptance / capability report cite the
  decision.
- Checklist is unsigned by design. Item 7 binds MCP posture to
  DECISIONS D-042. Filling it in-session would be a shaped sign-off.

Residuals (paper / process — do not reopen this slice):
- **ID collision:** DEFECTS **D-042** is Fixed M4 ACP owner fusion;
  DECISIONS **D-042** is Refreshable deferral. Cite `DECISIONS D-042` vs
  `DEFECTS D-042`. Do not renumber unless citations become actively
  ambiguous; do not treat spec “M0–M5 landed (… D-042 …)” as the MCP
  decision.
- v2 spec header / Loop README still say Refreshable is **undeclared**;
  WP12 + DECISIONS now say **deferred**. Align on the next doc touch.
  “Undeclared by current profiles” remains factually true (no profile
  sets the variant) but understates the MUST NOT.
- WP-00 worksheet still says CreationOnly “provisional” / “until WP-11
  proves Refreshable” — historical evidence; D-042 supersedes for
  shipped profiles.

Do **not** implement Refreshable, fill the checklist as an agent, or
promote Golden / §25 / D-025 complete. Next pick remains independent
human/contracted sign-off on `doc/SECURITY_REVIEW_CHECKLIST.md`, or
optional exact-limit / load work.

**Doc hygiene (2026-08-23, Advisor paper residuals):** Aligned live status
prose to **deferred (DECISIONS D-042)** in `TRANSACTION_RUNTIME_V2_SPEC.md`
header and Loop README (with DEFECTS vs DECISIONS D-042 citation
disambiguation). WP-00 Grok MCP row notes D-042 supersedes the historical
“provisional / until WP-11 proves Refreshable” worksheet wording. Chronological
DEFECTS bullets that still say “undeclared” remain historical. **Not** Golden /
§25 / D-025 signed-off.

**Next pick:** independent human/contracted sign-off on
`doc/SECURITY_REVIEW_CHECKLIST.md`, **or** optional exact-limit / load work.
Do not implement Refreshable without superseding DECISIONS D-042. Do not
promote Golden / §25.

**Advisor (2026-08-23, D-042 wording hygiene follow-up):** **PASS — Silver**
(doc-hygiene; quality tier unchanged). Named paper residuals closed on live
status docs. Remaining WP-00 agy/Codex “provisional” prose and chronological
DEFECTS “undeclared” bullets are paper leftovers / historical. Do **not**
promote Golden / §25 / D-025 complete.

**Exact-limit polish + WP-00 twins (2026-08-23):** WP-00 agy/Codex MCP prose
aligned to **DECISIONS D-042**. Added
`max_messages_exact_admits_plus_one_rejects` (exact admits; exact+1 rejects)
+ s23 inventory needle. Complements prior `max_input_bytes_exact_*` /
plus-one proofs. **Not** Golden / §25 / D-025 signed-off.

**Next pick:** independent human/contracted sign-off on
`doc/SECURITY_REVIEW_CHECKLIST.md`, **or** load/race harness work. Do not
implement Refreshable without superseding DECISIONS D-042.

**Advisor (2026-08-23, exact max_messages + WP-00 twins):** **PASS — Silver**
for this **optional polish slice**. Quality tier **unchanged**. Does **not**
meet Golden / §25 and does **not** close D-025 / §23 independent review.

The named follow-up is honest and in-bar:
- Admit uses `messages.len() > limits.max_messages` (`admission.rs`);
  `max_messages=1` exact submit succeeds; two messages →
  `AdmissionErrorKind::InvalidInput` + silent reject. Named proof
  `max_messages_exact_admits_plus_one_rejects` (lib test pass) + s23 needle.
- WP-00 agy/Codex MCP rows match **DECISIONS D-042** (CreationOnly;
  Refreshable deferred). Product shape holds: no testkit product dep, no
  ambient session, no Refreshable implementation.

Does **not** satisfy §23 “every public limit has an exact-limit and plus-one
test” (matrix still incomplete: e.g. `max_content_parts` exact-admit still
absent; remaining `TransactionLimits` fields unproven at equality).
Worksheet header “evidence-backed provisional declarations” is WP-00
evidence status, not a Refreshable promise. Checklist unsigned.

**Next pick:** independent human/contracted sign-off on
`doc/SECURITY_REVIEW_CHECKLIST.md`. Optional load/race harness remains
allowed. Do **not** spend the next slice on remaining optional matrix
cells. Do not implement Refreshable without superseding DECISIONS D-042.
Do not promote Golden / §25 / D-025 signed-off.

**Load/race harness — D-010 barrier submit vs shutdown (2026-08-23):**
`submit_versus_shutdown_barrier_race_two_outcomes` — N submitters + one
`begin_shutdown` thread release together; each submit is either
`RuntimeShuttingDown` (silent reject) or fully admitted (one completion).
After `Stopped`: ledger 0, capacity 0. Closes named D-010 acceptance
checkboxes (v2 lifecycle path). **Not** Golden / §25 / D-025 signed-off.

**Expert + Advisor residuals closed (same day):** Quiescing CAS now runs
**under the ledger lock** in both `RuntimeOwner::begin_shutdown` and
`begin_shutdown_inner` (§18.2 / D-010 — same lock as admit install). Late
`Start` while Quiescing terminalizes via `accept_terminal(RuntimeShutdown)`
instead of dropping Queued admits. Barrier test no longer asserts
mid-Quiescing `ledger_len == admitted` (Echo drain race); durable proof is
`completions_published == admitted` after `Stopped`. **PASS — Silver** for
the load/race slice. Gaps that remain optional (not reopen D-010):
all-reject-green single interleaving, Hang vs Echo matrix, full concurrent
load suites / §25 / D-025.

**Next pick:** independent human/contracted sign-off on
`doc/SECURITY_REVIEW_CHECKLIST.md`, **or** further load/race coverage
(Hang matrix / multi-run). Do not spend the next slice on optional
exact-limit matrix cells. Do not implement Refreshable without superseding
DECISIONS D-042.

**Advisor (2026-08-23, D-010 barrier race acceptance):** **PASS — Silver**
for this **load/race slice**. Quality tier **unchanged**. Does **not**
meet Golden / §25 and does **not** close D-025 / §23 independent review.

Named D-010 acceptance is honest on the v2 two-outcome contract:
`submit_versus_shutdown_barrier_race_two_outcomes` (OS-thread submitters +
one `begin_shutdown` after a shared barrier) allows only
`Ok(admit)` or `AdmissionErrorKind::RuntimeShuttingDown`. Rejects are
silent (`assert_rejected_silent`). After `Stopped`: `ledger_len == 0` and
`global_reservations == 0`; `completions_published == admitted`. s23
inventory needle present. Product crates still do not depend on testkit.
Empty-tool / isolation / component shape unchanged. Lib test pass (10/10
repeats this session).

D-010 Status **Fixed** stays honest for the **named bypass** (ghost work
after `Stopped`). Sequential D-040 `submit_versus_begin_shutdown_two_outcomes`
is before/after, not a concurrent race; this slice is the named barrier
proof those checkboxes cited.

Honesty leftovers at that gate (closed same day where noted):
- Echo barrier allowed all-reject-green — addressed by Hang both-outcomes
  proof below.
- §18.2 lock coupling + late-Start terminalize — closed in the Expert
  residual slice (Quiescing under ledger lock; Start while stopping →
  `accept_terminal(RuntimeShutdown)`).
- D-010 **Affected** paper still may name deleted v1 paths — cite live
  `lifecycle/admission.rs` + `owner.rs` + `supervisor.rs`.

**Advisor (2026-08-23, D-010 Expert residual close):** **PASS — Silver.**
Lock coupling + late-Start terminalize + test hygiene hold. Do **not**
promote Golden / §25 / D-025.

**Hang + both outcomes (2026-08-23):**
`submit_versus_shutdown_hang_barrier_both_outcomes` — pre-admit Hang
(live through Quiescing), barrier-race more submits, then a deterministic
post-Quiescing submit that MUST `RuntimeShuttingDown`. Asserts
`admitted >= 1`, `rejected >= 1`, and `completions_published == admitted`
after `Stopped`. Pins both legal outcomes without relying on Echo timing.
**Not** Golden / §25 / D-025 signed-off.

**Next pick:** independent human/contracted sign-off on
`doc/SECURITY_REVIEW_CHECKLIST.md`. Further load/race only for broader
stress, not D-010 reopen. Do not implement Refreshable without
superseding DECISIONS D-042. Do not promote Golden / §25 / D-025.

**Advisor (2026-08-23, D-010 residuals: lock + late Start + test hygiene):**
**PASS — Silver** for this **load/race follow-up**. Quality tier
**unchanged**. Does **not** meet Golden / §25 and does **not** close
D-025 / §23 independent review.

Named Advisor leftovers from the barrier-race slice are closed on the
live v2 path (Laws 8/22/23/25, spec §18.2):

- **Lock coupling:** `RuntimeOwner::begin_shutdown` and
  `begin_shutdown_inner` CAS `Accepting → Quiescing` while holding
  `shared.ledger` — the same non-async critical section `admit` uses
  to re-check state and install. Concurrent admit either inserts
  before the flip (visible to the shutdown snapshot) or sees
  non-`Accepting` and rejects `RuntimeShuttingDown`. Mutex is not
  held across `.await`. Named two-outcome proofs still pass
  (`submit_versus_shutdown_barrier_race_two_outcomes`,
  `submit_versus_begin_shutdown_two_outcomes`; this session).
- **Late Start:** once `stopping`, a Start that is not `Accepting`
  is `accept_terminal(RuntimeShutdown)` rather than dropped.
  `finalize_after_terminal` still lifts `completion_tx` from
  `delivery` if Start never ran. Parked-Start proof
  `parked_starts_reach_stopped_on_shutdown` still holds.
- **Test hygiene:** mid-Quiescing `ledger_len == admitted` is gone;
  durable proof is `completions_published == admitted` after
  `Stopped`, plus ledger 0 / capacity 0. D-010 **Affected** now
  names the v2 files.

D-010 Status **Fixed** stays honest for the named bypass (ghost
install after drain snapshot). Product crates still do not depend
on testkit. Empty-tool / isolation / component shape unchanged.

Honesty leftovers (do **not** reopen D-010; do **not** treat as
Golden load/race):

- Late Start still **drops** when state is not `Accepting` **and**
  `stopping` is still false (owner already flipped Quiescing; supervisor
  has not entered `begin_shutdown_inner`). That window is recovered
  by the lock-coupled snapshot, not by the Start arm. Tightening to
  `else { accept_terminal }` is optional, not a bypass reopen.
- Barrier test still does **not** require both outcomes in one
  interleaving; harness is still default **Echo**, not Hang;
  completion kinds still allow `Cancelled` (slightly loose).
- Exhaustive concurrent/race/load suites, Grok multi-session
  Golden, unsigned `SECURITY_REVIEW_CHECKLIST.md` (D-025),
  exhaustive public-limit matrix, M5.4 delete-vaults, and WP12
  Refreshable (DECISIONS D-042) remain out of this slice.

**Next pick:** independent human/contracted sign-off on
`doc/SECURITY_REVIEW_CHECKLIST.md`. Further load/race only if it
pins Hang + both outcomes in one interleaving. Do not spend the
next slice on the `else if stopping` tighten or optional
exact-limit cells. Do not implement Refreshable without
superseding DECISIONS D-042. Do not promote Golden / §25 /
D-025 signed-off.

**Advisor (2026-08-23, Hang + both outcomes load/race follow-up):**
**PASS — Silver** for this **named load/race follow-up**. Quality
tier **unchanged**. Does **not** meet Golden / §25 and does **not**
close D-025 / §23 independent review.

Named leftover from the barrier-race slice is closed honestly:

- `submit_versus_shutdown_hang_barrier_both_outcomes` uses
  `FakeEndpoint::Hang` (not Echo). Pre-admit stays live through
  Quiescing; six OS-thread submits race `begin_shutdown`; a
  post-Quiescing submit MUST `RuntimeShuttingDown`.
- One run asserts both legal sides: `admitted >= 1` (pre-Hang)
  and `rejected >= 1` (post-Quiescing). Racers still allow only
  `Ok(admit)` or `RuntimeShuttingDown`. Rejects are silent.
- After `Stopped`: `completions_published == admitted`,
  `ledger_len == 0`, `global_reservations == 0`. Hang completions
  MUST NOT be `Completed`. s23 needle present. Lib test pass
  this session.
- Product crates still do not depend on testkit. Empty-tool /
  isolation / component shape unchanged. D-010 Status **Fixed**
  stays honest for the named bypass; this slice does **not**
  reopen it.

Honesty leftovers (do **not** reopen D-010; do **not** treat as
Golden load/race):

- Mixed outcomes are **bookended** (pre-admit + post-Quiescing),
  not forced among the concurrent racers. `race_admitted` /
  `race_rejected` may still be 0 on one side of the barrier
  window. That is deterministic on purpose, not a two-outcome
  contract hole.
- Echo barrier
  (`submit_versus_shutdown_barrier_race_two_outcomes`) still
  does not require both sides in one run.
- Hang completion kinds still allow `Cancelled` | `Terminated`
  besides `RuntimeShutdown` (forbids `Completed` only).
- 30 ms sleep before the barrier is hygiene, not a proof that
  ConnectorOwner is registered.
- Exhaustive concurrent/race/load suites, Grok multi-session
  Golden, unsigned `SECURITY_REVIEW_CHECKLIST.md` (D-025),
  exhaustive public-limit matrix, M5.4 delete-vaults, and WP12
  Refreshable (DECISIONS D-042) remain out of this slice.

**Next pick:** independent human/contracted sign-off on
`doc/SECURITY_REVIEW_CHECKLIST.md`. Further load/race only for
broader stress, not D-010 reopen and not mixed-racer forcing.
Do not spend the next slice on the `else if stopping` tighten,
the 30 ms sleep, or optional exact-limit cells. Do not implement
Refreshable without superseding DECISIONS D-042. Do not promote
Golden / §25 / D-025 signed-off.

**D-025 checklist readiness (2026-08-23):** Enriched
`doc/SECURITY_REVIEW_CHECKLIST.md` with an **evidence map** (pointers to
named proofs / decisions for items 1–8) and an explicit “still open for
Golden” list. Sign-off table remains `_TBD_` — **not** filled by the
implementing agent. This prepares independent review; it does **not**
close D-025 / §23. **Not** Golden / §25.

**Next pick:** independent human/contracted reviewer fills the Sign-off
table on `doc/SECURITY_REVIEW_CHECKLIST.md` and records result in
DEFECTS. Agents must not self-sign. Optional: broader stress only.

**Advisor (2026-08-23, unsigned SECURITY_REVIEW_CHECKLIST evidence map):**
**PASS — Silver** for this **process-readiness slice**. Quality tier
**unchanged**. Does **not** meet Golden / §25 and does **not** close
D-025 / §23 independent review.

The slice is in-bar for what an agent **may** do on the organizational
gate: make the checklist executable for a human/contracted reviewer
without filling Sign-off. Product shape holds (no testkit product dep,
no ambient session, no Refreshable implementation, no self-sign). Named
pointers exist (`duplicate_session_race_admits_exactly_one`,
`runtime_owner_drop_joins_executor_thread_reaches_stopped`,
`six_profile_bindings_register_and_validate`, MCP plus-one,
`empty_loop` / EmptyToolRegistry, DECISIONS D-002 / D-042, Hang + Echo
shutdown barriers). Sign-off table remains `_TBD_`. Item 7 still binds
MCP posture to **DECISIONS D-042**. The “still open for Golden” list
matches live residuals (unsigned table, exhaustive public-limit matrix,
full load/race, live Grok multi-session, Refreshable deferred).

Honesty leftovers (do **not** treat as more checklist work; do **not**
reopen D-025 as a code gate):

- The **named next pick was sign-off**, which agents MUST NOT perform.
  Evidence-map enrichment is allowed **once** as readiness, not as a
  substitute for §23 “independent review finds no unresolved P0/P1/P2”.
  Further checklist polish is not progress toward Golden.
- Several map cells are **category** pointers (Interpreter
  fragmentation/EOF suites; TaskSupervisor abort-then-join proofs;
  StreamingHttp / Grok secret-absent Debug tests), not single named
  tests. That is acceptable for a reviewer packet; it is not a review.
- `six_profile_bindings_register_and_validate` lives in
  `monoloop-testkit` — qualification evidence, not a product→testkit
  dependency.
- Shutdown barrier proofs (items 5–6 “recent load/race”) are D-010
  lifecycle two-outcome contracts, not the exhaustive public-limit
  matrix §23 still requires.

Do **not** fill the Sign-off table in-session (shaped qualification).
Do not implement Refreshable without superseding DECISIONS D-042.
Do not promote Golden / §25 / D-025 signed-off.

**Next pick:** independent human/contracted reviewer fills Sign-off on
`doc/SECURITY_REVIEW_CHECKLIST.md` and records the result in DEFECTS.
Optional broader stress only. Agents must not self-sign.

**Broader stress — concurrent capacity exhaustion (2026-08-23):** Added
Hang-pinned barrier races distinct from sequential
`capacity_plus_one_rejects`:

- `concurrent_global_capacity_exhaustion_admits_exactly_max` — N+1
  concurrent submits at exact `max_active`; exactly N admit, one
  `CapacityExceeded`; every admission completes once on shutdown.
- `concurrent_per_channel_capacity_exhaustion_admits_exactly_channel_max`
  — same shape with `max_active_per_channel` tighter than global.

Inventory needles added in `s23_forbidden_patterns`. Does **not** close
exhaustive §23 public-limit matrix, full load/race Golden, D-025, or
§25. Sign-off table remains `_TBD_`.

**Next pick:** independent human/contracted reviewer fills Sign-off on
`doc/SECURITY_REVIEW_CHECKLIST.md` and records the result in DEFECTS.
Agents must not self-sign. Further optional stress only if named; do not
implement Refreshable without superseding DECISIONS D-042. Do not
promote Golden / §25 / D-025 signed-off.

**Expert + Advisor (2026-08-23, concurrent capacity exhaustion stress):**
**PASS — Silver.** Hang + Barrier N+1 races are sound and distinct from
sequential `capacity_plus_one_*`. Hang pins occupancy so Echo cannot free
slots mid-barrier (false green). Per-channel topology (`global=8`,
`channel_max=2`, three racers) forces channel CAS overflow with provisional
global rollback. Inventory + checklist pointers only; Sign-off still
`_TBD_`. Expert optional sharpening applied:
`global_reservations`/`channel_reservations` exact asserts and Hang end-kind
allowlist on the per-channel test. Does **not** close exhaustive §23
matrix, full load/race Golden, D-025, or §25.

**Next pick:** independent human/contracted reviewer fills Sign-off on
`doc/SECURITY_REVIEW_CHECKLIST.md` and records the result in DEFECTS.
Agents must not self-sign. Further optional stress only if named. Do not
implement Refreshable without superseding DECISIONS D-042. Do not promote
Golden / §25 / D-025 signed-off.

**Golden pursuit — Fake multi-channel load + D-035 cell (2026-08-23):**
Agent-doable Silver→Golden *progress* only (does **not** claim Golden /
§25). Delivered:

- `multi_channel_multi_session_concurrent_load` — Hang barrier across 3
  Channels; shared session-string SessionKey isolation; headroom so
  `SessionAlreadyActive` is not masked by `CapacityExceeded`; fill-to-cap
  overflow; one shutdown / one completion per admission.
- `max_content_parts_exact_admits_plus_one_rejects` — missing exact-admit
  twin for the existing plus-one cell.
- s23 inventory needles + checklist evidence-map pointers. Sign-off still
  `_TBD_`.

Honesty: `tests/hardening.rs` remains **unregistered** (`autotests =
false`; v1 `DefaultTransactionRuntime` / deprecated submit API — does not
compile against current façades). Do not treat on-disk hardening.rs as
live WP-12 evidence until rewritten for `StartedRuntime`. Refreshable
still DECISIONS D-042. Live Grok multi-session still open. Exhaustive
public-limit matrix still incomplete.

**Expert + Advisor (2026-08-23, Golden pursuit — Fake multi-channel load +
D-035 cell):** **PASS — Silver** (progress toward Golden only). Headroom
before session-reject matches capacity-first admit order. Hang pins
occupancy; exact `max_content_parts` cell sound. Does **not** close
D-025 / §23 / §25 / live Grok / exhaustive matrix / Refreshable.

**Next pick:** independent human/contracted D-025 Sign-off on
`doc/SECURITY_REVIEW_CHECKLIST.md` **or** next named agent Golden residual
(further exact-limit cells / Fake load / deterministic Grok mock
multi-session). Agents must not self-sign. Do not implement Refreshable
without superseding DECISIONS D-042. Do not register broken
`tests/hardening.rs`. Do not promote Golden / §25 / D-025 signed-off.

**Golden pursuit — max_tools cell + Grok mock concurrent sessions
(2026-08-23):** Further agent Golden *progress* (tier still Silver):

- `max_tools_per_transaction_exact_admits_plus_one_rejects` on
  `StartedRuntime` (exact=2 admits; plus-one → `InvalidConfiguration`).
  Replaces dead v1 hardening evidence that asserted `InvalidInput`.
- `concurrent_session_new_and_explicit_load` — mock ACP barrier of 4×
  `session/new` + 2× explicit `session/load`; unique ids; load returns
  exact id (no most-recent heuristic). **Not** live Grok qualification.
- Multi-channel Hang `transaction_deadline` widened to 30s (Expert nit).
- s23 / checklist pointers updated. Sign-off still `_TBD_`.

Note: `ChannelLimits.max_distinct_sessions` plus-one is **not** yet proven
on v2 ledger admission (old `ActiveRegistry` path / unregistered
hardening only). Do not invent a passing cell without wiring.

**Expert + Advisor (2026-08-23, max_tools + Grok mock concurrent):**
**PASS — Silver.** `InvalidConfiguration` matches live v2 admission
vocabulary (v1 `InvalidInput` was wrong). Grok mock barrier is sound for
explicit-load / no most-recent; not live. `max_distinct_sessions` honesty
correct — do not invent a cell. Pre-existing residual noted by Expert:
DirectLlm encode still projects `tools: &[]` (out of this slice). Does
**not** close D-025 / §25 / live Grok / exhaustive matrix.

**Next pick:** independent human/contracted D-025 Sign-off on
`doc/SECURITY_REVIEW_CHECKLIST.md` **or** wire+prove
`ChannelLimits.max_distinct_sessions` on v2 ledger admission. Agents must
not self-sign. Do not implement Refreshable without superseding DECISIONS
D-042. Do not register broken `tests/hardening.rs`. Do not promote Golden
/ §25 / D-025 signed-off.

**Golden pursuit — wire+prove max_distinct_sessions on v2 (2026-08-23):**
Closed the named honesty gap: `ChannelLimits.max_distinct_sessions` is
enforced on v2 `LifecycleLedger` / admission (not only old
`ActiveRegistry` / unregistered hardening).

- `ledger.insert_queued(..., max_distinct)` /
  `ledger.bind_session(..., max_distinct)` →
  `LedgerInsertError::DistinctSessionsExceeded` → admit
  `CapacityExceeded`.
- `ChannelIndex::max_distinct_sessions` from live binding; coordinator
  claim path passes channel limit into `bind_session`.
- Proof: `max_distinct_sessions_exact_admits_plus_one_rejects`
  (Hang-pinned exact=2; plus-one CapacityExceeded; session-less admit
  does not consume a distinct slot at admit). s23 + checklist needles.

Does **not** close D-025 / §25 / live Grok / exhaustive matrix /
Refreshable. Sign-off still `_TBD_`.

**Expert + Advisor (2026-08-23, max_distinct_sessions v2 wire):**
**PASS — Silver.** Admit order sound (duplicate before distinct under
lock; active-capacity headroom in proof). Session-less not counting at
admit is correct; ExternalAgent `bind_session` is wired but **not**
proven by the Hang DirectLlm cell (honesty leftover). No reservation
leak on DistinctSessionsExceeded. Does **not** close D-025 / §25.

**Next pick:** independent human/contracted D-025 Sign-off on
`doc/SECURITY_REVIEW_CHECKLIST.md` **or** next named agent residual
(ExternalAgent claim-time distinct plus-one / further exact-limit cells /
Fake load). Agents must not self-sign. Do not implement Refreshable
without superseding DECISIONS D-042. Do not register broken
`tests/hardening.rs`. Do not promote Golden / §25 / D-025 signed-off.

**Golden pursuit — ExternalAgent claim-time distinct plus-one
(2026-08-23):** Closed the Expert honesty leftover from the v2 wire
slice.

- `PromptProceedError::{DistinctSessionsExceeded, Failed}` on the
  prompt-ready gate; DistinctSessions → exchange `LimitExceeded`
  (was conflated as `InvariantFailed`).
- Proof: `external_agent_claim_time_distinct_sessions_plus_one_limit_exceeded`
  — Hang ExternalAgent creates claim 2 SessionKeys; third `session_id:
  None` admits then fails closed at `bind_session` with
  `LimitExceeded`; held creates remain. s23 + checklist needles.

Does **not** close D-025 / §25 / live Grok / exhaustive matrix /
Refreshable. Sign-off still `_TBD_`.

**Expert + Advisor (2026-08-23, ExternalAgent claim-time distinct):**
**PASS — Silver.** `LimitExceeded` is the correct post-admit terminal
(admit-time remains `CapacityExceeded`). Bind fail before
EstablishExternal / activate; no SessionKey leak. Proof Hang-pinned with
active headroom. Does **not** close D-025 / §25 / live Grok.

**Next pick:** independent human/contracted D-025 Sign-off on
`doc/SECURITY_REVIEW_CHECKLIST.md` **or** next named agent residual
(further exact-limit cells / Fake load). Agents must not self-sign. Do
not implement Refreshable without superseding DECISIONS D-042. Do not
register broken `tests/hardening.rs`. Do not promote Golden / §25 /
D-025 signed-off.

## D-046: Event byte capacity is never released after receive

**Priority:** P1

**Status:** Fixed (2026-08-23)

**Affected:**
- `crates/monoloop-contracts/src/delivery.rs`
- `crates/monoloop-loop/src/transaction/lifecycle/delivery.rs`

**Problem:** `TransactionEventSender::send` / `try_send` increments the shared
`queued_bytes` counter, but `TransactionEventReceiver::recv` and `try_recv` do
not decrement it. The only decrement API, `TransactionEventSender::note_received`,
has no production callers and is not available to a receiver that owns no sender.

The configured queue-byte capacity therefore behaves as a cumulative lifetime
quota. A host that continuously drains its event receiver will eventually see
all later events rejected once the sum of previously delivered event sizes
reaches `max_event_bytes`, even when the queue is empty. This violates v2 §6.4,
§12, §22.6, and §25 bounded queued-resource semantics.

**Acceptance review evidence:**
- Repository-wide search finds `note_received` only at its definition.
- Existing delivery tests cover item capacity but do not prove byte capacity is
  recovered after receive or receiver drop.

**Remediation (Fixed):**
- Queued mailbox items carry an RAII `QueuedBytePermit`; Drop releases bytes.
- `recv` / `try_recv` drop the permit; receiver/channel drop releases remaining.
- Removed host-callable `note_received`.

**Acceptance criteria:**
- [x] Repeated send→receive cycles whose cumulative size exceeds the configured
  byte limit continue succeeding when concurrent queued bytes stay within limit
  (`event_byte_capacity_recovered_after_receive`).
- [x] Exact concurrent byte capacity succeeds and plus-one fails closed
  (`event_byte_capacity_exact_and_plus_one`).
- [x] Dropping a receiver releases every outstanding byte reservation
  (`event_byte_capacity_released_on_receiver_drop`).
- [x] Concurrent sender tests prove no underflow, overflow, or leaked capacity
  (`event_byte_capacity_concurrent_send_recv_no_leak`).

## D-047: Event publisher silently discards accepted ordinary events

**Priority:** P1

**Status:** Fixed (2026-08-23)

**Affected:**
- `crates/monoloop-loop/src/transaction/lifecycle/event_publisher.rs`
- `crates/monoloop-loop/src/transaction/lifecycle/coordinator.rs`
- `crates/monoloop-loop/src/transaction/lifecycle/supervisor.rs`

**Problem:** The event publisher uses `event_tx.try_send`. It ignores
`ItemCapacityExceeded`, `ByteCapacityExceeded`, and `EventTooLarge` for ordinary
`Publish` and `EstablishExternal` commands. The coordinator has already observed
successful insertion into the internal publisher-command mailbox, so it cannot
distinguish external publication from silent loss. A transaction can consequently
select `Completed` with missing canonical events.

This is an explicit divergence from v2 §6.4 and §12, which require ordinary
events to wait for capacity under the remaining transaction deadline and require
publication failures to become supervisor-visible. A failed
`EstablishExternal` may also be followed by an ordinary event, violating the
requirement that a new external session publish `SessionEstablished` first.

The current `s22_2_failed_enqueue_consumes_no_sequence` test manually resubmits a
dropped event. Production has no retry or failure command, so that test proves
contiguous numbering but not lossless delivery.

**Required remediation:**
- Use deadline-aware capacity acquisition for ordinary event publication.
- Report queue closure, deadline, and limit failures to the supervisor through a
  bounded `PublisherFailed`/equivalent command.
- Ensure `EstablishExternal` either commits sequence 1 or terminates before any
  ordinary event can publish.
- Give terminal sealing priority so a full publisher-command queue cannot permit
  ordinary events after finalization begins.

**Remediation (Fixed):**
- `TransactionEventSender::send` waits for byte capacity via `Notify` (D-047).
- Event publisher waits under cancel + deadline instead of `try_send` ignore.
- Sticky publication failure; Seal reports it; finalizer upgrades Completed →
  `EventDeliveryFailed` when delivery is Limit/Deadline/QueueClosed.
- Proofs: `s22_2_failed_enqueue_consumes_no_sequence` (wait+drain lossless),
  `d047_full_queue_seal_reports_deadline_not_published`.

**Reopen + close (independent review `b82c763`, 2026-08-23):** Seal priority
criterion was still unchecked. Dedicated `SealCommand` mpsc (cap 1) + biased
select / preempt during ordinary host-capacity wait; publisher no longer retains
a Sender to its own ordinary channel. Proof:
`d047_seal_priority_when_ordinary_cmd_queue_full`.

**Second REJECT + close (same day):** Finalizer/`enqueue_seal` deadline skew —
Finalizer waited `cleanup_deadline` while publisher used the transaction
deadline. Now both sides share `SealCommand.deadline =
now + terminal_event_delivery_deadline`. Proof:
`d047_seal_uses_terminal_deadline_not_transaction_deadline`.

**Third REJECT + close (same day):** Biased Seal preempt dropped pre-accepted
ordinary Publish (lossless fence violation). Seal became an ordering fence
(in-flight under Seal deadline; backlog drain before `Ended`). Exact configured
terminal deadline (no 50ms floor).

**Fourth REJECT + close (same day):** Fence used `try_recv` until Empty without
closing admission — a parked `send` could complete after Empty and be dropped.
Now: `OrdinaryCmdAdmit::close` (Finalizer before Seal + publisher on Seal);
async `recv` to Disconnected under Seal deadline. Proofs:
`d047_seal_fence_drains_queued_ordinary_before_ended`, lossless
`d047_seal_priority_when_ordinary_cmd_queue_full`,
`d047_terminal_deadline_uses_configured_value_exactly`.
Parked-send proof corrected in fifth pass
(`d047_seal_fence_parked_send_delivered_before_ended`).

**Acceptance criteria:**
- [x] A temporarily full but draining host queue loses no ordinary events before
  the transaction deadline (`s22_2_failed_enqueue_consumes_no_sequence`).
- [x] A permanently full queue produces the documented terminal failure rather
  than `Completed` (`d047_full_queue_seal_reports_deadline_not_published` +
  finalizer kind upgrade).
- [x] `SessionEstablished` publication failure becomes sticky and blocks further
  ordinary publishes (same sticky_fail path).
- [x] Seal racing a full ordinary command queue permits no event after terminal
  attempt; pre-fence ordinary is drained before Ended (lossless)
  (`d047_seal_priority_when_ordinary_cmd_queue_full`,
  `d047_seal_fence_drains_queued_ordinary_before_ended`).
- [x] Parked ordinary `send` that holds a pre-fence Sender clone while the
  ordinary queue is full at Seal time completes `Ok` and is delivered before
  Ended (`d047_seal_fence_parked_send_delivered_before_ended` with
  `send_after_pre_fence_hold` sync).
- [x] Seal terminal enqueue and Finalizer reply wait share one authoritative
  `terminal_event_delivery_deadline` Instant exactly (not cleanup / tx deadline,
  no silent floor)
  (`d047_seal_uses_terminal_deadline_not_transaction_deadline`,
  `d047_terminal_deadline_uses_configured_value_exactly`).
- [x] Tests assert delivered sequences after wait/drain (contiguous + count +
  Ended last).

## D-048: Process-isolated tool handles are discarded before wait/reap

**Priority:** P1

**Status:** Fixed (2026-08-23)

**Affected:**
- `crates/monoloop-loop/src/transaction/dispatcher.rs`
- `crates/monoloop-loop/src/transaction/tool_handler.rs`
- `crates/monoloop-loop/src/transaction/process_tool.rs`
- `crates/monoloop-loop/src/transaction/lifecycle/supervisor.rs`

**Problem:** On an interrupted `ProcessIsolated` dispatch, `DispatchGuard::drop`
kills the child but parks only a `ToolPermit` and `OwnedProcessLease`. It does not
park the `ToolKillHandle` or its `Child`. Once drive/guard handles drop, the
runtime can lose its last process handle without observing `wait`/`try_wait`.

`OrphanToolPermitSet::shutdown_progress` then clears those permits and leases
unconditionally. The supervisor's stopped predicate checks an empty ledger and
empty Tokio task set but does not require an authoritative process-owner registry
to be empty. `owned_processes` can therefore become zero because an accounting
lease was dropped rather than because the child was reaped. This violates v2
§3, §7.3, §14.3, §18.2–18.3, §21, §22.4–22.5, and §25.

**Remediation (Fixed):**
- `OwnedProcessRegistry` parks `ToolKillHandle` (+ optional `ToolPermit`) on
  `DispatchGuard` drop for live ProcessIsolated children.
- Quiesce calls `process_registry.shutdown_progress()` (kill + poll); entries
  are not cleared without `has_join() == false`.
- `Stopped` requires `process_registry.is_empty()` in addition to empty ledger
  and tasks.
- Snapshot `owned_processes` prefers registry `live_count`.

**Reopen + close (independent review `b82c763`):** `ProcessIsolatedToolHandler`
no longer blocks `start` on `stdin.write_all`. Kill handle / Child ownership
exist before stdin.

**Second REJECT + close (same day):** first stdin remediation used ambient
`spawn_blocking` (failed §23) and called `note_process_reaped` after kill
without observed exit. Now: `tokio::process::Child` + async stdin write on the
owned drive; post-kill poll until `try_wait` observes exit; reap accounting
only then. Proofs:
`process_isolated_program_owns_before_stdin_and_is_killable`,
`process_isolated_stdin_timeout_reaps_only_after_observed_exit`.

**Golden residual close (2026-08-23):** Sacrificial harness
`tests/d048_process_isolated_sacrificial.rs` aborts `ToolWorker` after spawn
(park path), then quiesces until `kill -0` fails **before**
`registry.is_empty()`. Inventory registered in
`s23_adversarial_lifecycle_subprocess_harness_inventory`.

**Acceptance criteria:**
- [x] Abort the supervising `ToolWorker` immediately after process spawn;
  sacrificial end-to-end PID proof
  (`d048_process_isolated_sacrificial_abort_park_then_pid_not_waitable`).
- [x] Kill/wait timeout can retain the handle in the registry (`park` path);
  Stopped blocked while `live_count > 0`.
- [x] Later reap empties the registry (`registry_retains_until_reap_then_empties`).
- [x] `Stopped` asserts process registry empty, not merely counter zero.
- [x] Process ownership precedes stdin delivery; `start` returns a killable
  handle before any blocking stdin write
  (`process_isolated_program_owns_before_stdin_and_is_killable`).
- [x] Stdin delivery is owned (async on drive); no ambient `spawn_blocking`;
  reap only after observed exit
  (`process_isolated_stdin_timeout_reaps_only_after_observed_exit` + §23).
- [x] Sacrificial PID-not-waitable proof before claiming registry empty
  (`d048_process_isolated_sacrificial_abort_park_then_pid_not_waitable`).

## D-049: `wait_stopped` deadline excludes executor-thread join

**Priority:** P1

**Status:** Fixed (2026-08-23)

**Affected:**
- `crates/monoloop-loop/src/transaction/lifecycle/owner.rs`
- `crates/monoloop-loop/src/transaction/lifecycle/supervisor.rs`

**Problem:** `RuntimeOwner::wait_stopped` applies its deadline only inside
`wait_until_stopped`. After observing `STATE_STOPPED`, it performs an unbounded
`std::thread::JoinHandle::join`. The supervisor stores `STATE_STOPPED` before its
future returns and before the executor thread executes `Runtime::shutdown_timeout`.

Consequently, a short `wait_stopped` call can block past its deadline, and public
runtime state can report `Stopped` while the supervisor future, executor, and OS
thread are still alive. This contradicts v2 §3 and §18.2: the wait deadline must
bound the entire API operation, and only `Stopped` may prove executor/thread join.

**Remediation (Fixed):**
- Supervisor sets `drain_complete` (stays Quiescing); does **not** publish
  public `STATE_STOPPED`.
- Executor thread signals `thread_exited` after `shutdown_timeout`.
- `wait_stopped` budgets drain wait + join wait; on join timeout retains
  `JoinHandle` + exited receiver; publishes `Stopped` only after join.
- Test gate `hold_executor_teardown` delays teardown for deterministic proof.

**Acceptance criteria:**
- [x] `wait_stopped(short)` TimedOut during delayed teardown
  (`wait_stopped_times_out_during_executor_teardown_then_completes`).
- [x] State remains `Quiescing` while the executor thread is alive.
- [x] A subsequent wait joins the retained handle and returns `Stopped`.
- [x] No public handle observes `Stopped` before executor/thread join.

## D-050: Abortable tool ownership still relies on self-asserted booleans

**Priority:** P1

**Status:** Fixed (2026-08-23) — Silver evidence; do not promote Golden / §25 / D-025

**Affected:**
- `crates/monoloop-loop/src/transaction/host_tools.rs`
- `crates/monoloop-loop/src/transaction/tool_handler.rs`
- `crates/monoloop-loop/src/transaction/dispatcher.rs`

**Problem:** `ProcessIsolated` registration has a concrete constructor, but
`AbortableAtYield` accepts any public `dyn ToolHandler` whose
`supports_abort()` returns true. A custom handler can self-assert abortability,
return a fabricated cancel-only handle, and run its actual work elsewhere without
transferring an owned join to the runtime. Post-start validation similarly checks
only the presence of a `ToolKillHandle`.

This violates v2 §14: handler capability booleans are insufficient and
registration must require a structural execution factory for the declared class.
It also reopens the detached-work and premature-capacity-release failure mode the
v2 rewrite was intended to remove.

**Required remediation:**
- Replace the single boolean-driven `ToolHandler` registration seam with typed
  factories/handles per execution class.
- Require `AbortableAtYield` to yield an unspawned future that the runtime places
  directly under `TaskSupervisor`, or an equivalent runtime-owned task handle.
- Define cooperative external work explicitly as host-owned rather than claiming
  runtime join/stop guarantees for it.

**Remediation (Fixed):**
- `RegisteredTool::try_new` rejects `AbortableAtYield` (mirror ProcessIsolated).
- `RegisteredTool::try_new_abortable` accepts only sealed
  `AbortableAtYieldHandler` (`AsyncToolHandler` / `IsolatedKillableToolHandler`),
  which always yield CancelOnly + unspawned `drive` polled on the supervised
  dispatch task (M5.4).
- `ToolHandler::runtime_owns_abortable_drive` structural marker; HostToolRegistry
  re-validates Abortable entries.
- Dispatcher pre-start checks `runtime_owns_abortable_drive`; post-start requires
  CancelOnly kill + `drive.is_some()` before polling the body.
- Forged/mismatched class tests: `host_tools` unit tests, hardening,
  `s22_4_cannot_self_assert_abortable_via_boolean`.

**Acceptance criteria:**
- [x] A custom handler cannot self-assert `AbortableAtYield` through a boolean.
- [x] Every accepted abortable execution has a TaskSupervisor-owned join before
  its body starts (inline `drive` on supervised ToolWorker / dispatch task).
- [x] Abort retains capacity until that join is observed (§22.4 permit proofs).
- [x] Tests attempt forged/mismatched handles for every execution class.

## D-051: Connector ownership begins only after open completes

**Priority:** P2

**Status:** Fixed (2026-08-23) — Silver evidence; do not promote Golden / §25 / D-025

**Affected:**
- `crates/monoloop-connector/src/open.rs`
- `crates/monoloop-connector/src/traits.rs`
- `crates/monoloop-loop/src/transaction/lifecycle/exchange.rs`
- all Connector profile `begin_open` implementations

**Problem:** `PendingRawConnection` contains control and an `OpenCompletion`, but
no owner work/handle. `ConnectionOwnerWork` is available only after
`OpenCompletion` returns `OpenedRawConnection`. Open-time Connector I/O is
therefore driven as part of the transaction coordinator rather than by a
`ConnectorOwner` registered before I/O begins.

This does not necessarily detach current futures, because the coordinator is
supervised, but it fails the mandatory v2 §15 ownership seam: open-owner identity
must exist before I/O starts and control/join must cover pending open as well as
post-open transport work.

**Remediation (Fixed):**
- `PendingRawConnection::open_owned` / `take_owner_work`: required owner future
  drives open I/O then post-open transport; signals `opened` via oneshot.
- `OpenedRawConnection` no longer carries `owner_work` (removed `None` migration).
- Lifecycle exchange spawns `TaskClass::ConnectorOwner` **before** polling
  `pending.opened`; cancel/fail paths join children.
- All product profiles (Fake, HTTP, Proxy failed-pending, Grok, Claude, Z.ai,
  Cursor, Codex, Agy) use `open_owned` / `failed`.
- Proofs: `d051_pending_exposes_owner_before_open_poll`,
  `d051_cancel_during_delayed_open_joins_owner`, plus migrated connector suites.

**Reopen + close (independent review `b82c763`):** Busy/Rejected spawn path
no longer polls the unregistered owner future inline (would start open I/O
under the coordinator). Future is dropped after terminate; returns
`SpawnFailed`. Named poll-detector:
`d051_busy_rejected_drops_unregistered_owner_without_poll`.

**Acceptance criteria:**
- [x] `PendingRawConnection` transfers owner work/identity before open I/O can
  start.
- [x] `ConnectorOwner` registration precedes first poll of the entire open and
  transport owner operation (including: never poll Busy/Rejected unregistered
  owner work — `d051_busy_rejected_drops_unregistered_owner_without_poll`).
- [x] Cancel/terminate during a non-yielding or delayed open retains an observable
  owner join and never fabricates teardown.
- [x] No profile retains the temporary `owner_work: None` migration option.

## D-052: Mandatory format and clippy verification gates fail at delivered HEAD

**Priority:** P1

**Status:** Fixed (2026-08-23) — Silver evidence; do not promote Golden / §25 / D-025

**Affected:**
- workspace formatting
- `crates/monoloop-loop/src/transaction/lifecycle/tests.rs`
- `doc/TRANSACTION_RUNTIME_V2_SPEC.md` status header
- `Makefile` (`gates` / `doc` / `dist`)

**Problem:** At commit `fb0371b`, the specification header claims the §23 core
commands are green, but two mandatory commands fail.

**Acceptance review evidence:**
- `cargo fmt --all -- --check` fails with diffs across Connector tests,
  contracts, Loop sources, lifecycle tests, and MCP tests.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` fails on
  `clippy::needless-borrows-for-generic-args` in lifecycle tests.
- `cargo test --workspace --all-targets --all-features` passes when the review
  environment is allowed to bind loopback sockets.
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` passes.

**Remediation (Fixed):**
- `cargo fmt --all` + clippy fixes (`unused_mut`;
  `needless_borrows_for_generic_args` in lifecycle tests).
- Makefile: `doc` uses `RUSTDOCFLAGS="-D warnings"`; new `gates` target runs
  exactly the four §23 commands; `dist` invokes `gates` before release build.
- Spec status header updated to observed-green-after-D-052, with D-053 honesty
  that `monoloop-loop` `--all-targets` remains a registered subset.

**Acceptance criteria:**
- [x] All four §23 commands pass on the same clean checkout.
- [x] CI / release path runs exactly those commands and blocks release on
  failure (`make gates` / `make dist`).
- [x] The specification status header reports observed state rather than a stale
  green claim.

**Expert + Advisor (2026-08-23, D-052 Fixed):** **PASS — Silver** for this
slice only. Re-ran the four §23 commands on this tree: `cargo fmt --all --
--check`, `cargo clippy --workspace --all-targets --all-features -- -D
warnings`, `cargo test --workspace --all-targets --all-features`,
`RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` — all exit 0.
`make gates` is those four; `make dist` invokes `gates` before release build.
Spec header is observed-green-after-D-052 and explicitly **Not** Golden / §25;
D-053 honesty (`monoloop-loop` `--all-targets` is a registered subset) is
accurate (`hardening.rs` / `admission_lifecycle.rs` / `claim_gate.rs` /
`direct_llm_e2e.rs` / `exchange_e2e.rs` / `runtime_startup.rs` and
`monoloop-loop` `examples/fake_echo.rs` were not executed by the workspace
command). Architecture import gates remain green. Residual: CI
`.github/workflows/ci.yml` still runs `cargo doc --workspace --no-deps`
without `RUSTDOCFLAGS="-D warnings"` (Makefile `doc`/`gates` do). That is a
CI/spec flag mismatch, not a component-law hole and does not reopen this P1.
**Closed same day with D-053:** CI rustdoc now sets `RUSTDOCFLAGS=-D warnings`.
Do **not** promote Golden / §25 / D-025.

## D-053: `--all-targets` excludes six integration suites and package examples

**Priority:** P2

**Status:** Fixed (2026-08-23) — Silver evidence; do not promote Golden / §25 / D-025

**Affected:**
- `crates/monoloop-loop/Cargo.toml`
- `crates/monoloop-loop/tests/admission_lifecycle.rs` (deleted)
- `crates/monoloop-loop/tests/claim_gate.rs` (ported to `StartedRuntime`)
- `crates/monoloop-loop/tests/direct_llm_e2e.rs` (deleted)
- `crates/monoloop-loop/tests/exchange_e2e.rs` (deleted)
- `crates/monoloop-loop/tests/hardening.rs` (deleted)
- `crates/monoloop-loop/tests/runtime_startup.rs` (deleted)
- `crates/monoloop-loop/examples/fake_echo.rs` (rewritten on `StartedRuntime`)
- `doc/D053_COVERAGE_REPLACEMENT.md`

**Problem:** `monoloop-loop` sets `autotests = false` and `autoexamples = false`,
then registers only selected test targets. The six integration files above still
use the deleted/deprecated v1 runtime surface and are neither compiled nor run by
the mandatory workspace `--all-targets` command. Package examples are likewise
excluded unless explicitly registered.

**Acceptance review evidence:**
- `cargo test -p monoloop-loop --test hardening` reports no such test target.
- `cargo test -p monoloop-loop --test direct_llm_e2e` reports no such test target.
- The Cargo manifest comments explicitly say the suites await rewrite for
  `StartedRuntime`, despite the specification header claiming M7 façade cutover.

**Remediation (Fixed):**
- Deleted five v1 suites that imported removed `DefaultTransactionRuntime`,
  with explicit replacement map in `doc/D053_COVERAGE_REPLACEMENT.md`.
- Ported `claim_gate` (omit provider sessionId → `InvariantFailed`) onto
  `StartedRuntime` / push delivery.
- Rewrote `examples/fake_echo.rs` onto `StartedRuntime`.
- Enabled `autotests = true` and `autoexamples = true` so every on-disk suite
  and example is compiled by `--all-targets`.
- Also closed D-052 CI residual: `.github/workflows/ci.yml` rustdoc now sets
  `RUSTDOCFLAGS=-D warnings` to match `make gates`.
- Advisor follow-up: stale `monoloop-loop` README “unregistered v1 files”
  line removed; `fake_echo_exchange_emits_canonical_text_unit` hardened to
  drain events concurrent with completion (workspace `--all-targets` + 30×
  stress of the named test: 0 failures on this tree).

**Honesty follow-up (independent review `b82c763`):** DirectLlm replacement
map **narrowed** — Fake echo + empty_loop are smoke only; HTTP/OpenAI SSE
composition was an open Golden residual.

**Golden residual progress (Phase A+B partial, 2026-08-24):**
`tests/direct_llm_openai_e2e.rs` — HTTP/OpenAI text-only + concurrency;
CallerControlled tool path encodes admitted tools and ends
`ContinuationRequired` without a second provider open; InlineToolContinuation
opens a second exchange via `encode_tool_continuation` + `run_encoded_exchange`
(one continuation round; text second response → Completed); exchange-scoped
`tool_action_id` + preserved `provider_tool_call_id` across sequential admits.
Proofs: `caller_controlled_tool_exchange_ends_continuation_required_without_second_open`,
`inline_tool_continuation_second_exchange_emits_text`,
`reused_provider_call_id_across_exchanges_distinct_action_ids`.
**Multi-round inline landed (2026-08-24):** bounded `1..=max_continuations` loop
with cumulative transcript (`append_tool_round`),
`max_continuation_context_bytes` fail-closed, Loop re-dispatch between rounds;
proof `inline_multi_round_tool_continuation_completes_after_second_tool`.
**FakeConnector parity landed (2026-08-24):** `tests/direct_llm_fake_e2e.rs` —
Fake `ScriptedSequence` + OpenAI dialect stamp covers text, CallerControlled,
one-/multi-round inline, concurrency, call-ID reuse without HTTP.
**LimitExceeded / max_continuations bound e2e landed (2026-08-24):** Fake
`fake_inline_max_continuations_zero_ends_limit_exceeded`,
`fake_inline_max_continuations_one_exhausted_ends_limit_exceeded`,
`fake_inline_continuation_context_bytes_limit_exceeded`; HTTP twins
`http_inline_max_continuations_zero_ends_limit_exceeded`,
`http_inline_max_continuations_one_exhausted_ends_limit_exceeded`
(fake 9/9, HTTP 8/8 on this tree).
**Expert + Advisor (2026-08-24, max_continuations / LimitExceeded bound e2e):**
**PASS — Silver+Golden-progress** for this slice only. Fail-closed composition
proven; D-053 stays Fixed; no Golden/§25/D-025 overclaim. Honesty residual at
stamp time: `max_provider_exchanges` / `max_total_provider_{input,output}_bytes`
were config-only — **closed in the follow-up provider-budget slice**.
**Provider-budget bounds landed (2026-08-24):** coordinator reads
`max_provider_exchanges` + cumulative `max_total_provider_{input,output}_bytes`;
exchange reports encoded/received bytes and fails closed mid-pump on output
overflow. Proofs: Fake+HTTP `max_provider_exchanges=1`, total input before
open, total output during pump; HTTP context-byte twin. Suites on this tree:
fake 12/12, HTTP 12/12.
**Advisor (2026-08-24, provider-budget bound slice):** **PASS — Silver**
for this slice only (Golden-progress on the DirectLlm replacement row, not
a Golden promotion). D-053 stays **Fixed**; the named honesty residual that
`max_provider_exchanges` / `max_total_provider_{input,output}_bytes` were
config-only is **closed**. Enforcement is Component 3 (encode-then-compare
input before Connector open; raw-chunk count mid-pump; continuation remaining
budget), not Connector semantic inspection. Re-ran at stamp: fake e2e 12/12,
HTTP/OpenAI e2e 12/12. Spec header / Loop README / D-053 map still **Not**
Golden / §25 / D-025 — do not promote.
**Provider-budget exact/cumulative polish landed (2026-08-24):** remaining
input/output `== 0` fails closed before open; Fake+HTTP
`max_provider_exchanges=2` exact; cumulative remaining-output exact + plus-one
Fake+HTTP; Fake continuation remaining-input `== 0` (probe-encoded). Suites on
this tree: fake **16/16**, HTTP **15/15**.
**Still open for full Golden:** independent §25 / D-025 sign-off; remaining
§23 extras / D-054 compatibility phase (outside this DirectLlm row).
RuntimeOwner Drop already has a Silver PASS — do not re-pick as open.
**Next pick:** independent Golden / D-025 review (agents must not self-sign)
**or** remaining named §23 / D-054 residuals outside DirectLlm.
**Not** Golden / §25 / D-025. Agents must not self-sign.

**Advisor (2026-08-24, provider-budget exact/cumulative polish):** **PASS —
Silver+Golden-progress** for this slice only (DirectLlm replacement row, not
a Golden promotion). D-053 autodiscovery stays **Fixed**. Map honesty residual
(HTTP plus-one overclaim) **closed** by adding
`http_inline_cumulative_output_budget_plus_one_fails_second_pump` + Fake
`fake_inline_cumulative_input_budget_exhausted_blocks_second_open`. Spec
header / Loop README remain **Not** Golden / §25 / D-025. Re-ran: fake e2e
16/16, HTTP/OpenAI e2e 15/15. Do **not** promote Golden / §25 / D-025.
Agents must not self-sign.
**HTTP e2e determinism harden (2026-08-24):** Advisor FAIL (honesty-closer
review) on intermittent `Cancelled` (labels=`ended_event` only) under the
default parallel harness. Remediation: shared suite Tokio runtime (no
per-test runtime create/destroy), graceful axum shutdown (no
`JoinHandle::abort`), healthz must succeed, and `finish_http_test` settles
connection closeouts under `suite_lock`.

**Advisor (2026-08-24, HTTP e2e determinism harden):** **PASS — Silver**
for this harness slice only (not Golden). Named Cancelled flake not
reproduced after harden; advisor re-ran **20/20** default-harness HTTP +
**10/10** combined Fake+HTTP. Live on-disk counts this tree: Fake
`#[test]` **16**, HTTP `#[test]` **15**; `cargo test` **16/16** + **15/15**.
Historical agent stress (**40/40** exit-code file checks) is retained as
supporting evidence, not a Golden DoD. D-053 stays **Fixed**. Spec header /
Loop README remain **Not** Golden / §25 / D-025. Do **not** promote Golden /
§25 / D-025. Agents must not self-sign.
**Next pick:** independent Golden / D-025 review (agents prepare evidence
only — see `doc/D025_EVIDENCE_PACK.md`) **or** remaining named §23 extras /
D-054 compatibility outside DirectLlm. Do not re-pick RuntimeOwner Drop.

**Advisor (2026-08-24, D-053 honesty closer bar check):** **FAIL — not
Silver-closed as stamped** *(pre-harden; superseded by HTTP determinism
harden PASS above)*. D-053 autodiscovery stays **Fixed**; do not reopen it.
At FAIL time HTTP was flaky (`Cancelled` / 14/15); that named next pick was
**HTTP harden**, now landed. Spec header / Loop README remain **Not** Golden
/ §25 / D-025. Architecture gates 10/10; product crates still do not depend
on testkit. Do **not** promote Golden / §25 / D-025. Agents must not
self-sign.

**Advisor (2026-08-24, HTTP e2e determinism harden bar check):** **PASS —
Silver** for this **harness slice** only (Golden-progress on the DirectLlm
row, **not** a Golden promotion). D-053 autodiscovery stays **Fixed**. Spec
header / Loop README remain **Not** Golden / §25 / D-025. Architecture
gates 10/10; product crates still do not depend on testkit. HTTP stays
Connector; the harden is test-only (`direct_llm_openai_e2e.rs`).

The prior FAIL's named flake is closed on this tree. Shared suite runtime +
`suite_lock` + healthz + graceful axum shutdown (no `JoinHandle::abort` on
the happy path) + `finish_http_test` closeout are live. On-disk counts:
Fake **16** `#[test]`, HTTP **15** `#[test]`. This review: Fake 16/16;
HTTP default-harness **20/20** full-suite passes; combined Fake+HTTP
**10/10**. Named proofs
`http_inline_max_continuations_zero_ends_limit_exceeded` and
`inline_tool_continuation_second_exchange_emits_text` ran inside those
suites (no `Cancelled` / 14/15 this review).

Honesty (do not reopen the flake; do not treat as Golden):
- Linear DEFECTS order had the harden **40/40** claim *above* the FAIL that
  requested it. Treat **40/40 / 60/60** as the implementer's stress note,
  not this review's evidence. Independently verified here: **20/20** HTTP +
  **10/10** combined.
- Independent-verdict polish stamp "**15/15 and 14/14**" is **stale**. Live
  counts are Fake **16/16**, HTTP **15/15**.
- `finish_http_test`'s 5 ms settle is harness closeout, not a product
  Cancelled-root-cause proof. `drain_until_completed` still prefers the
  completion channel (labels=`ended_event` can still be an observation
  artifact if a real `Cancelled` returns).
- `suite_lock` serializes this binary; default `--test-threads` no longer
  races HTTP tests against each other. Product isolation remains
  `concurrent_http_openai_admits_are_isolated`.
- D-053 map remaining-Golden line is narrowed in the same-day map edit:
  §23 extras + D-054 compatibility + D-025, not sign-off-only.

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.
**Next pick:** remaining named **§23 extras / D-054** outside DirectLlm
(exhaustive public-limit exact/plus-one inventory, race/load, live Grok,
or the declared compatibility-alias breaking cut). Independent Golden /
D-025 last. Do not re-pick HTTP 15/15 or RuntimeOwner Drop.

**Advisor (2026-08-24, FakeConnector DirectLlm parity bar check):**
**PASS — Silver+Golden-progress** for this slice only. Three-component
boundaries hold: FakeConnector remains dialect-labelled bytes + per-open
script (no SSE/tool/prompt parse); `output_dialect` is a Connector stamp;
encoders and inline continuation stay in Component 3; Interpreter still
consumes complete units from labelled bytes. Product crates do not depend
on testkit (architecture gates green). D-053 stays **Fixed** — this fills
the named DirectLlm replacement residual, it does not reopen the autodiscovery
defect. Proofs at stamp time: fake e2e 6/6, HTTP/OpenAI e2e 6/6,
`scripted_sequence_advances_per_open_and_fails_closed_when_exhausted`.
Do **not** promote Golden / §25 / D-025. Honesty leftover at stamp time:
`LimitExceeded`/`max_continuations` bound e2e was still absent — **closed in
the follow-up bound-e2e slice**. Workspace clippy `-D warnings` currently
fails on pre-existing `needless_lifetimes` in
`provider_tool_call_id_from_action` (not introduced here — do not claim
§23 re-green from this landing).

**Expert + Advisor (2026-08-24, InlineToolContinuation + call-ID reuse):**
**PASS — Silver+Golden-progress** for this slice only. Fresh ExchangeId /
ConnectionId / InterpretationId on continuation; `encode_tool_continuation`
without double-append; CallerControlled stays one-open; provider call ids
preserved with exchange-scoped action ids. Proofs green (e2e 15× serial).
Do **not** promote Golden / §25 / D-025.

**Acceptance criteria:**
- [x] Port every retained integration suite to the v2 public API and register it,
  or delete it with an explicit coverage replacement map (**honest** map: DirectLlm
  vertical e2e not equivalently replaced).
- [x] Register/enable package examples so `--all-targets` compiles them.
- [x] `cargo test --workspace --all-targets --all-features` demonstrably includes
  the full intended suite rather than a curated subset hidden by autodiscovery
  settings.

**Expert + Advisor (2026-08-23, D-053 Fixed after README + fake_echo harden):**
**PASS — Silver** for this slice only. Re-checked on this tree:

- `monoloop-loop` `autotests`/`autoexamples` enabled; cargo metadata lists
  every on-disk integration suite (`claim_gate`, `empty_loop`, `linked_tools`,
  `mcp_gateway`, `rmcp_loopback_spike`, `s22_*`, `s23_forbidden_patterns`) and
  `examples/fake_echo`; the six v1 files are gone.
- `cargo test --workspace --all-targets --all-features --no-run` exit 0 (full
  intended on-disk suite compiles, including both `fake_echo` examples).
- Architecture gates green; no product crate depends on `monoloop-testkit`
  (including `monoloop-loop` production and dev).
- `fake_echo_exchange_emits_canonical_text_unit` concurrent drain +
  `claim_gate` omit-sessionId → `InvariantFailed` both pass.
- README and spec header: D-053 Fixed; **Not** Golden / §25; D-054 named.

Residuals (do **not** reopen D-053): package `fake_echo` examples still
`try_recv` after completion and do not assert a unit (smoke only). WP-12
evidence rows retargeted under D-054. Obsolete uncompiled modules deleted
under D-054. Do **not** promote Golden / §25 / D-025.

## D-054: M7 deletion and final §23/§25 acceptance are incomplete

**Priority:** P2

**Status:** Fixed (2026-08-23) — Silver honesty/deletion slice after WP-12 /
D-003 retarget. **Deprecated-alias Golden residual closed (2026-08-24)** via
DECISIONS **D-060** after normative status retarget. Do **not** promote
Golden / §25 / D-025.

**Affected:**
- `crates/monoloop-loop/src/transaction/active_registry.rs` (deleted)
- `crates/monoloop-loop/src/transaction/events.rs` (deleted)
- `crates/monoloop-loop/src/transaction/exchange.rs` (deleted)
- `crates/monoloop-loop/src/transaction/spawn_gate.rs` (deleted)
- callback-based compatibility types/aliases in contracts and Loop exports
  (removed under D-060; host `adapt_*` helpers retained)
- `doc/SECURITY_REVIEW_CHECKLIST.md`
- `doc/TRANSACTION_RUNTIME_V2_SPEC.md`

**Problem:** The v2 migration plan §24 M7 requires obsolete lifecycle/event
files and compatibility aliases to be removed after cutover. The four obsolete
files above remain tracked but uncompiled, and deprecated callback/runtime/tool
aliases remain exported. This is not a live behavior defect by itself, but it
means the claimed M7 completion and final definition of done are inaccurate.

The project's security checklist also explicitly records an incomplete exhaustive
public-limit matrix, incomplete broader load/race coverage, live Grok qualification
still open, and an unsigned independent-review gate. At acceptance time D-046–D-053
were also open, so v2 §23 and §25 could not pass.

**Remediation (Fixed — Silver honesty / deletion slice):**
- Deleted the four obsolete uncompiled modules; `transaction/mod.rs` no longer
  comments them as deferred-on-disk.
- Narrowed M7 / status language: façade cutover done; deletion of obsolete
  files done; **compatibility phase** retains `adapt_*` host adapters and
  deprecated aliases until a deliberate breaking cut.
- D-046–D-053 Fixed (Silver); §23 core `make gates` remain the release path.
- WP-12 Pass rows and Proven bullets that named deleted v1 suites /
  `CallbackService` retargeted to lifecycle / `adapt_*` / D-053 map.
- `DECISIONS.md` D-003 updated: obsolete modules **deleted** (D-054); aliases
  remain compatibility-phase, not deferred-on-disk source.

**Still open for Golden / §25 / D-025 (do not claim closed here):**
- Exhaustive public-limit / race-load / live Grok qualifications (§23 extras).
- Unsigned independent security / acceptance review (D-025).
- A subsequent independent acceptance review finding no unresolved P0/P1/P2.
- Optional later: move `adapt_*` out of `monoloop-loop` (not a Golden blocker).

**Acceptance criteria:**
- [x] Delete obsolete uncompiled lifecycle/event modules after coverage migration.
- [x] Remove callback-based core APIs and compatibility aliases at the declared
  breaking boundary, **or** narrow M7/status language to an explicitly incomplete
  compatibility phase. (Silver chose narrow language; **D-060** executed the
  deprecated-only breaking cut.)
- [x] Close D-046–D-053 and rerun mandatory §23 core gates.
- [x] Retarget remaining WP-12 Pass rows that still name deleted v1 suites
  (`hardening`, `exchange_e2e`) or the removed `CallbackService` as live
  evidence.
- [ ] Complete the remaining exact-limit, race/load, and profile qualifications
  claimed by §23/§25. (**Golden residual — not this Silver slice.**)
- [ ] A subsequent independent acceptance review finds no unresolved P0/P1/P2
  lifecycle defect. (**Process residual — agents must not self-sign.**)

**Expert (2026-08-23, D-054 Silver bar check):** **FAIL — not Fixed** for the
honesty slice as recorded. M7 deletion and the chosen compatibility-phase
narrowing **are** sound; the WP-12 retarget claim is not.

Checked against `doc/TRANSACTION_RUNTIME_V2_SPEC.md` §24 M7:

| M7 item | Verdict |
|---|---|
| 1. Public façade → v2 (`StartedRuntime`) | Holds (D-038). Not re-opened. |
| 2. Port examples / intended suites | Holds (D-053 + `D053_COVERAGE_REPLACEMENT.md`). |
| 3. Remove callback core APIs / aliases at breaking boundary | **Incomplete, as declared.** `TransactionRequest` / `TransactionRuntime` have **no** live `impl`. `RuntimeToolSpill` is a `#[deprecated]` alias of `OrphanToolPermitSet`. `adapt_event_sink` / `adapt_completion_callback` run only from `tests/s22_7_host_adapters.rs` (host executor), matching M1 “outside the core.” Spec header + Loop README + M7.3 text all say compatibility phase. Not claimed as the breaking cut. |
| 4. Delete obsolete uncompiled files after consolidation | **Sound.** `active_registry.rs`, `events.rs`, `exchange.rs`, `spawn_gate.rs` were commented-out `mod` lines and imported deleted v1 symbols (`FinalizationGuard`, `executor_spawn`). Live v2 exchange is `lifecycle/exchange.rs`. `s23_forbidden_patterns` (4 tests) pass after deletion. Do not restore. |

Does **not** claim §25 / Golden / alias breaking cut / independent review:
spec header, Loop README “D-054 (partial)”, D-054 Golden boxes, and
`doc/SECURITY_REVIEW_CHECKLIST.md` sign-off table remain unsigned. Correct.

Does **not** pass the named WP-12 remediations. These Pass rows still cite
deleted or removed evidence:

- `WP12_REQUIREMENTS_ACCEPTANCE.md` R-000 cancel/timeout races → `hardening …`
- R-000 no-leaks → `CallbackService drain` (`CallbackService` has **zero**
  production `.rs` hits)
- R-004 concurrent sequence/session → `hardening + exchange_e2e`
- `WP12_CURRENT_LIMITATIONS.md` still lists “Runtime-owned `CallbackService`”
  under Proven

That is the same class of inaccurate-completion defect D-054 was opened for.
D-003 in `DECISIONS.md` still says remaining on-disk modules are “deferred
(not deleted) until their stage”; after this deletion that sentence is stale.

This review did **not** re-run full `make gates` on the mixed D-046–D-054
working tree. D-052 recorded the four §23 commands green; this check compiled
and passed `s23_forbidden_patterns` only.

Do **not** promote Golden / §25 / D-025. Next pick: finish WP-12 leftover
rows (and the D-003 deferred-modules sentence), then re-present D-054 as
Silver Fixed. Agents must not self-sign independent acceptance.

**Advisor (2026-08-23, D-054 named honesty/deletion slice):** **FAIL — not
Fixed Silver.** Expert’s deletion + compatibility-phase findings hold;
the honesty leftover still blocks the stamp.

Independently re-checked on this tree:

- Three-component shape intact. Architecture gates: 10/10
  (`product_crates_do_not_depend_on_testkit`, façade re-exports Connector +
  Interpreter + Loop, Loop production ↛ profiles/testkit).
- No product crate lists `monoloop-testkit` in production, dev, or build
  deps. `s23_forbidden_patterns` 4/4 pass after the four-file deletion.
- Deleted files are gone (`active_registry`, `events`, `exchange`,
  `spawn_gate`); live exchange is `lifecycle/exchange.rs`. Do not restore.
- M7.3 is an explicit compatibility phase, not a breaking cut:
  `TransactionRuntime` has **no** live `impl`; `RuntimeToolSpill` is a
  `#[deprecated]` alias; `adapt_*` stay host-side. Spec header, Loop README
  “D-054 (partial)”, D-054 Golden boxes, and
  `doc/SECURITY_REVIEW_CHECKLIST.md` Sign-off remain unsigned. **Not** a
  §25 / D-025 self-sign.

Honesty leftover (same class as the original D-054 inaccurate-completion
defect) was still present at Advisor FAIL time:

- `doc/WP12_REQUIREMENTS_ACCEPTANCE.md` R-000 cancel/timeout races still
  cited `hardening`; no-leaks still cited `CallbackService drain`;
  R-004 concurrent sequence/session still cited `hardening + exchange_e2e`.
- `doc/WP12_CURRENT_LIMITATIONS.md` Proven still listed “Runtime-owned
  `CallbackService`”.
- `DECISIONS.md` D-003 still said remaining on-disk modules are “deferred
  (not deleted) until their stage.”

**Follow-up (same day, after Advisor FAIL):** those WP-12 Pass/Proven rows and
D-003 were retargeted to lifecycle / `adapt_*` / D-053 map language. Grep of
`doc/WP12_*.md` + `DECISIONS.md` no longer finds live Pass evidence naming
`hardening`, `exchange_e2e`, or `CallbackService` (coverage map may still
name deleted files as **Deleted file** column — correct). Architecture
gates 10/10. Status → **Fixed Silver** for the honesty/deletion slice.
Do **not** promote Golden / §25 / D-025. Next pick: independent acceptance /
D-025 sign-off or named Golden residuals. Agents must not self-sign.

**Expert (2026-08-23, after WP-12 / D-003 retarget):** **PASS — Silver** for
the honesty/deletion slice only. The prior leftover FAIL tail is superseded.

Re-checked against `doc/TRANSACTION_RUNTIME_V2_SPEC.md` §24 M7 and the named
acceptance boxes:

| Check | Verdict |
|---|---|
| Four obsolete modules gone | `active_registry.rs`, v1 `events.rs`, v1 `exchange.rs`, `spawn_gate.rs` absent. Live exchange is `lifecycle/exchange.rs`. `s23_forbidden_patterns` 4/4 pass. Do not restore. |
| Compatibility phase explicit | M7.3 incomplete as declared. `TransactionRuntime` has no live `impl`. `RuntimeToolSpill` is a `#[deprecated]` alias. `adapt_*` host-only (`s22_7_host_adapters.rs`). Spec header + Loop README “D-054 (partial)” + M7.3 text all say compatibility phase, not the breaking cut. |
| WP-12 Pass/Proven retarget | `doc/WP12_*.md` has **zero** hits for `hardening`, `exchange_e2e`, or `CallbackService`. R-000 cancel/timeout → `lifecycle/tests.rs` `s22_2_*` (those tests exist). No-leaks → `Stopped` + host `adapt_*` drain. R-004 concurrent sequence → `s22_6_concurrent_producers_contiguous_sequence` (exists). Proven host adapters cite `adapt_event_sink` / `adapt_completion_callback`, not runtime-owned `CallbackService`. Coverage map may still name deleted files in the **Deleted file** column — correct. |
| D-003 | Obsolete modules **deleted** (D-054); aliases are compatibility-phase, **not** deferred-on-disk source. |
| Empty-registry path | Unchanged: `empty_loop::empty_registry_unavailable_zero_effects` → `ToolUnavailable` + `OutboundToolOutcome::ToolUnavailable`; waiting never dispatches. |
| Golden / §25 / D-025 | **Not claimed.** Sign-off table unsigned. Exhaustive public-limit / race-load / live Grok / alias breaking cut remain Golden residuals. |

This review re-ran `s23_forbidden_patterns` only (4/4). It did **not** re-run
full `make gates`. D-052 already recorded the four §23 commands green.

Do **not** promote Golden / §25 / D-025. Next pick: independent acceptance /
D-025 sign-off or named Golden residuals. Agents must not self-sign.

**Advisor (2026-08-23, D-054 named honesty/deletion slice after WP-12 / D-003
retarget):** **PASS — Silver** for this slice only. The prior leftover FAIL
is superseded. Independently re-checked on this tree (not a self-sign of
D-025 / §25 / independent acceptance):

- Three-component shape intact. Architecture gates **10/10**
  (`product_crates_do_not_depend_on_testkit`, façade re-exports Connector +
  Interpreter + Loop, Loop production ↛ profiles/testkit).
- No product crate lists `monoloop-testkit` in production, dev, or build
  deps. `s23_forbidden_patterns` **4/4** pass. Deleted files remain gone
  (`active_registry`, v1 `events`, v1 `exchange`, `spawn_gate`); live
  exchange is `lifecycle/exchange.rs`. Do not restore.
- M7.3 is an explicit compatibility phase, not a breaking cut:
  `TransactionRuntime` has **no** live `impl`; `RuntimeToolSpill` is a
  `#[deprecated]` alias of `OrphanToolPermitSet`; `adapt_*` stay host-side.
  Spec header, Loop README “D-054 (partial)”, D-054 Golden boxes, and
  `doc/SECURITY_REVIEW_CHECKLIST.md` Sign-off remain unsigned. **Not** a
  §25 / D-025 self-sign.
- Named honesty leftover closed: `doc/WP12_*.md` has **zero** live Pass
  evidence naming `hardening`, `exchange_e2e`, or `CallbackService`
  (coverage map **Deleted file** column may still name them — correct).
  D-003: obsolete modules **deleted**; aliases are compatibility-phase, not
  deferred-on-disk source. Empty-registry path unchanged
  (`empty_loop::empty_registry_unavailable_zero_effects`).
- Paper nit closed same day: WP-12 R-004 push-events evidence cites
  `TransactionSubmitRequest.delivery` / `transaction_delivery`.

This review re-ran architecture gates + `s23_forbidden_patterns` only. It
did **not** re-run full `make gates`. D-052 already recorded the four §23
commands green.

**Next pick:** independent acceptance / D-025 sign-off **or** named Golden
work (exhaustive public-limit / race-load / live Grok; breaking alias
cut). Agents must not self-sign. Do **not** promote Golden / §25 / D-025.

## Independent Transaction Runtime v2 acceptance verdict — 2026-08-23 (re-review)

**Reviewed commit:** `b82c763` (+ uncommitted remediations)

**Result:** **REJECT — not §25 / Golden / D-025** (second independent pass)

### Second-pass findings (supersede prior premature Silver PASS)

A prior agent stamp of **Expert + Advisor PASS — Silver** on this section is
**superseded and withdrawn**. That stamp was premature: remediations were
uncommitted, `s23_no_undocumented_ambient_tokio_spawn_in_production_src`
failed on ambient `spawn_blocking` stdin, and the ambient writer / false-reap
path was a lifecycle ownership defect (not a harmless residual).

1. **P1 D-048** stdin via ambient `spawn_blocking` (not TaskSupervisor-owned;
   timeout detaches writer) + `note_process_reaped` after `kill` without
   observed `wait`/`try_wait`. Also failed mandatory §23 gate.
2. **P1 D-047** Finalizer waited `cleanup_deadline` for Seal reply while
   publisher `enqueue_seal` used the long transaction deadline — completion
   could record `DeadlineExceeded` then a later `EndedEvent` could still
   publish. `terminal_event_delivery_deadline` was unused.
3. **P1 release gate** `s23_no_undocumented_ambient_tokio_spawn_in_production_src`
   failed on `process_tool.rs` `spawn_blocking`.
4. **P2** Premature Silver PASS in this record (withdrawn above).

Confirmed still holding from first reopen close: dedicated Seal channel;
Busy/Rejected owner drop; D-053 DirectLlm honesty.

### Remediation (same day, after second REJECT)

| Finding | Fix | Proof |
|---|---|---|
| P1 D-048 detached writer + false reap | `tokio::process` Child + async stdin on owned drive; post-kill poll until observed `try_wait`; `note_process_reaped` only after observed exit | `process_isolated_program_owns_before_stdin_and_is_killable`, `process_isolated_stdin_timeout_reaps_only_after_observed_exit`; `s23_no_undocumented_ambient_tokio_spawn_in_production_src` |
| P1 D-047 Seal vs Finalizer deadline skew | `SealCommand.deadline` = `now + terminal_event_delivery_deadline`; publisher `enqueue_seal` and Finalizer reply wait share that Instant | `d047_seal_uses_terminal_deadline_not_transaction_deadline`, `d047_seal_priority_when_ordinary_cmd_queue_full` |
| P1 §23 gate | No production `spawn_blocking` in process_tool | `s23_forbidden_patterns` green |
| P2 premature PASS | This REJECT supersedes the withdrawn stamp | (this record) |

Sacrificial PID residual under D-048 remains. DirectLlm vertical e2e remains a
Golden residual. **Not** Golden / §25 / D-025. Agents must not self-sign.

**Expert + Advisor (2026-08-23, second-pass remediations Silver bar):**
**WITHDRAWN** — superseded by third independent REJECT (Seal fence dropped
pre-accepted ordinary events; `chat_projection_from_transaction_events_only`
failed; priority proof did not assert losslessness; silent 50ms terminal
deadline floor).

### Third-pass findings (independent REJECT)

1. **P1 D-047** Seal biased-preempt discarded queued / in-flight ordinary
   Publish — `EndedEvent` without preceding CanonicalUnit (suite flake
   `chat_projection_from_transaction_events_only`).
2. **P1 test** `d047_seal_priority_when_ordinary_cmd_queue_full` asserted only
   Seal Published, permitting silent ordinary loss.
3. **P2** `terminal_event_delivery_deadline.max(50ms)` silently raised exact
   configured budgets.
4. **P2** Sacrificial abort-after-spawn PID proof still unchecked (residual).

### Remediation (same day, after third REJECT)

| Finding | Fix | Proof |
|---|---|---|
| P1 Seal drops pre-fence ordinary | Seal is an ordering fence: finish in-flight ordinary under Seal deadline (commit or sticky-fail); `try_recv` drain ordinary backlog before `Ended` | `d047_seal_fence_drains_queued_ordinary_before_ended`, strengthened `d047_seal_priority_when_ordinary_cmd_queue_full`; `chat_projection_from_transaction_events_only` 30× green |
| P1 weak priority proof | Assert diagnostic before Ended, contiguous sequences, `last_sequence` matches | same |
| P2 silent 50ms floor | Use configured `terminal_event_delivery_deadline` exactly | `d047_terminal_deadline_uses_configured_value_exactly` |
| P2 sacrificial PID | Still residual (unchecked) | — |

**Expert + Advisor (2026-08-23, third-pass remediations Silver bar):**
**WITHDRAWN** — superseded by fourth independent REJECT (Seal fence not
linearizable: `try_recv` until Empty allowed a parked `send` to complete after
drain and be dropped; `coordinator_publishes_sequenced_unit_and_completed`
flaked; parked-send proof absent; premature PASS before human review).

### Fourth-pass findings (independent REJECT)

1. **P1** Seal drain used `try_recv` until Empty without closing admission — a
   blocking `send` parked on a full ordinary queue could complete after Empty
   and be lost when the publisher exited.
2. **P1** Full workspace gate: `coordinator_publishes_sequenced_unit_and_completed`
   missing CanonicalUnit (scheduling-sensitive).
3. **P2** Priority proof used `try_send`/`Full` only — no parked `send()` proof.
4. **P2** Premature Silver PASS stamp (withdrawn above).

### Remediation (same day, after fourth REJECT)

| Finding | Fix | Proof |
|---|---|---|
| P1 non-linearizable fence | `OrdinaryCmdAdmit::close` at Seal (Finalizer + publisher); async `recv` to Disconnected under Seal deadline | fence/priority proofs; production close+drain |
| P1 flake | Same close+drain linearization | `coordinator_publishes_sequenced_unit_and_completed` + lib suite |
| P2 parked-send proof (first attempt) | Insufficient — see fifth REJECT | — |
| P2 premature PASS | Withdrawn | (this record) |

**Expert + Advisor (2026-08-23, fourth-pass remediations Silver bar):**
**WITHDRAWN** — superseded by fifth independent REJECT: the parked-send test
did not force a send blocked across Seal (publisher drained before Seal;
`Ok`/`Err` both allowed). Production close+drain remains sound.

### Fifth-pass finding (independent REJECT — proof only)

**P2** `d047_seal_fence_parked_send_*` started the publisher before filling the
queue and slept 20ms before Seal, so both ordinary commands usually completed
pre-Seal. Fix: fill queue with publisher stopped; `send_after_pre_fence_hold`
signals after Sender clone; assert still Full; queue Seal; then start
publisher; require parked `Ok` and `queued`→`parked`→`Ended`.

**Proof:** `d047_seal_fence_parked_send_delivered_before_ended` (50× green).

**Expert + Advisor (2026-08-23, fifth-pass parked-send proof Silver bar):**
**PASS — Silver** for this proof slice only. Production close+drain was already
sound; this stamp covers only the forced pre-Seal park + `Ok` +
`queued`→`parked`→`Ended` proof. Fourth-pass PASS remains withdrawn.
Sacrificial PID residual under D-048 remains. DirectLlm vertical e2e remains
Golden residual.

**Next pick:** independent human re-review of this Silver remediation slice.
Do **not** promote Golden / §25 / D-025.

## Independent Transaction Runtime v2 acceptance verdict — 2026-08-23

**Reviewed commit:** `fb0371b`

**Result:** **REJECT — not v2 spec-complete; not releasable as Golden / §25**

The review found no immediate P0 memory-safety defect. It found five live P1
lifecycle/ownership defects (D-046–D-050), one P1 release-gate defect (D-052),
and three P2 contract/migration defects (D-051, D-053, D-054).

Positive findings retained from the delivery:
- production owns its executor;
- synchronous admission uses ledger installation, state recheck, and rollback;
- TaskSupervisor registers tasks before releasing their start gate;
- arbitrary event/completion callbacks execute outside the core executor;
- deliberately non-yielding supervised work keeps shutdown `Quiescing`;
- registered workspace tests pass with required loopback access; and
- rustdoc succeeds with warnings denied.

These strengths do not override the open defects above. Recommended remediation
order is D-046, D-047, D-048, D-049, D-050, D-051, then D-052–D-054 followed by
a fresh independent acceptance review. Do not mark D-025, §23, §25, or Golden
complete while any of these records remain open.

**Remediation progress (2026-08-23):** D-046–D-054 Fixed for their named Silver
slices (see each defect). D-054 honesty/deletion + WP-12/D-003 retarget:
Expert **PASS — Silver**; Advisor **PASS — Silver** (this record).
**Re-review `b82c763` named P1/P2:** fourth-pass Silver PASS **withdrawn**
after fifth independent REJECT; fifth-pass parked-send proof: Expert + Advisor
**PASS — Silver**. D-048 sacrificial PID proofs closed (see D-048 record).
DirectLlm Phase A+B partial landed: HTTP/OpenAI text+concurrency;
CallerControlled `ContinuationRequired`; InlineToolContinuation one-round
second exchange (`encode_tool_continuation`); call-ID reuse across sequential
admits. **Multi-round inline landed:** bounded N>1 tool→model loops with
cumulative context + `LimitExceeded` on bound / context overflow
(`inline_multi_round_tool_continuation_completes_after_second_tool`).
**FakeConnector parity landed (2026-08-24):** `tests/direct_llm_fake_e2e.rs`
mirrors HTTP/OpenAI scenarios via FakeConnector `ScriptedSequence` +
configurable `output_dialect` (text, CallerControlled, one-/multi-round
inline, concurrency, call-ID reuse). Connector unit proof
`scripted_sequence_advances_per_open_and_fails_closed_when_exhausted`.
**LimitExceeded / max_continuations bound e2e landed (2026-08-24):** Fake
9/9 and HTTP 8/8 on this tree include zero/one continuation ceiling and
Fake context-byte ceiling → `LimitExceeded` without over-opening.
**Expert + Advisor PASS — Silver+Golden-progress** (bound-e2e slice).
**Provider-budget bounds landed (2026-08-24):**
`max_provider_exchanges` + `max_total_provider_{input,output}_bytes` enforced
as `LimitExceeded` (Fake+HTTP 12/12 each on this tree). DirectLlm replacement
row independent continuation/provider bounds are now live, not config-only.
**Advisor (2026-08-24, provider-budget bound slice):** **PASS — Silver**
for this slice only. D-053 config-only residual **closed**.
**Provider-budget exact/cumulative polish landed** (fake 16/16, HTTP 15/15):
remaining==0 before open; `max_provider_exchanges=2` exact; cumulative output
exact + plus-one Fake+HTTP; Fake continuation remaining-input `== 0`.
HTTP harness determinism hardened (shared RT + graceful SSE shutdown);
Advisor **PASS — Silver** on harden (20/20 + 10/10 combined in that review).
Unsigned evidence pack for human Sign-off: `doc/D025_EVIDENCE_PACK.md`.
**Advisor (2026-08-24, unsigned D-025 evidence pack):** **PASS — Silver,
process readiness only.** Sign-off not self-filled; on-disk counts Fake 16 /
HTTP 15 honest; does **not** close D-025 / Golden / §25.
**Not** Golden / §25 / D-025.
**§23 public-limit matrix honesty (2026-08-24):** Added
`doc/S23_PUBLIC_LIMIT_MATRIX.md` naming every `TransactionLimits` field as
Covered / Partial / Open; wired into
`s23_exact_limit_plus_one_inventory_present` (matrix must list all fields;
DirectLlm continuation/provider-budget needles registered).
**Advisor (2026-08-24, matrix honesty):** **PASS — Silver** with honesty fix:
`max_event_queue*` demoted to **Open (unwired)** — DeliveryLimits proofs are
not this field. **Wiring follow-up:** `max_actor_commands` → control `mpsc`
capacity; `DispatcherLimits` from `TransactionLimits` (concurrent/queued/payload/
output) instead of hardcoded `8/16`/`usize::MAX`; proof
`max_tool_output_bytes_plus_one_fails_closed`. Matrix updated.
**Advisor (2026-08-24, Covered honesty re-check):** **FAIL — not honesty-closed
as stamped.** `max_tool_output_bytes` / concurrent / queued were still
**Covered** while proofs set `DispatcherLimits` or only showed wiring.
**Honesty fix:** those three → **Partial**; added
`fake_transaction_limits_max_tool_output_bytes_plus_one_fails_closed` which
sets **`TransactionLimits.max_tool_output_bytes`** through `StartedRuntime` /
`limits_from_transaction` → field **Covered**. Concurrent/queued remain
**Partial** (wired, no field cell).
**Advisor (2026-08-24, Covered honesty fix):** **PASS — Silver.** Legend vs
statuses hold; Fake **17/17**; no Golden overclaim. D-053 count leftover
(16→17) closed on next docs touch.
**Partial→Covered follow-up:** `fake_transaction_limits_max_tool_payload_bytes_plus_one_rejects`
sets **`TransactionLimits.max_tool_payload_bytes`** → payload **Covered**.
Fake suite **18/18**. Concurrent/queued/actor-commands remain Partial.
**Advisor (2026-08-24, payload Covered):** **PASS — Silver.** Matches matrix
legend; not §23-exhaustive.
Still Open: event-queue fields, actor-command bytes, tool schema, diagnostics,
callback deadline. Concurrent/queued/actor-commands remain Partial pending
field cells.
**Partial→Covered (concurrent):** `transaction_limits_max_concurrent_tools_plus_one_rejects`
sets **`TransactionLimits.max_concurrent_tools_per_transaction`** via
`limits_from_transaction` → concurrent **Covered**.
**Advisor (2026-08-24, concurrent Covered):** **PASS — Silver.** Legend holds;
next pick was queued (same class).
**Partial→Covered (queued):** `transaction_limits_max_queued_tools_plus_one_rejects`
sets **`TransactionLimits.max_queued_tools_per_transaction`** → queued
**Covered**. Actor-commands remain Partial. Fake DirectLlm **18/18**.
**Next pick:** Partial→Covered (`max_actor_commands`) **or** Open product
bounds **or** race/load / live Grok / D-054 **or** independent D-025 Sign-off.
Do not re-pick HTTP 15/15, RuntimeOwner Drop, or concurrent/queued/payload
Covered cells.
**Still open for full Golden:** independent §25 / D-025 sign-off (agents
must not self-sign); remaining §23 extras / D-054 compatibility phase.
Fake+HTTP DirectLlm replacement is **not** a Golden / §25 / D-025 claim.
RuntimeOwner Drop already Silver-passed — do not re-list as open next work.
Do **not** promote Golden / §25 / D-025.
**Advisor (2026-08-24, Covered honesty fix bar check):** **PASS — Silver**
for this honesty slice. Golden / §25 / D-025 are **not** overclaimed (matrix
header, spec, Loop README, security checklist, D-025 pack keep exhaustive
§23 / Sign-off open). Previous shaped-Covered residual **closed**:
`max_concurrent_tools_per_transaction` / `max_queued_tools_per_transaction`
are **Partial** (wired via `limits_from_transaction`; no test assigns those
`TransactionLimits` fields; `capacity_limit_plus_one_rejects` is shared
global capacity). `max_tool_output_bytes` **Covered** cites
`fake_transaction_limits_max_tool_output_bytes_plus_one_fails_closed`, which
sets `TransactionLimits.max_tool_output_bytes` through `StartedRuntime` /
`limits_from_transaction` and fails closed (`output_contract_violated`).
DispatcherLimits-only `max_tool_output_bytes_plus_one_fails_closed` is
correctly adjacent. Field inventory 26/26. Legend vs statuses hold:
Covered = this field set + fail-closed; Partial = wired/adjacent; Open =
unwired (event-queue, actor-command bytes, tool schema, diagnostics,
callback deadline). Fake suite **17/17** on this tree (new cell included).
Inventory gate keeps Covered needles on disk; it still does **not** codegen
status vs proof.

Standing caveat (not a fail of this slice): legend Covered is fail-closed
at the bound (exact *and/or* plus-one), weaker than §23 “exact **and**
plus-one”; acceptable only while the exhaustive bullet stays open.
`max_tool_output_bytes` is plus-one fail-closed, not exact-admits.

Leftover (do not reopen this honesty cut): `doc/D053_COVERAGE_REPLACEMENT.md`
still says Fake **16/16**; on-disk / D-025 pack are **17**. Bump the map on
the next docs touch.

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.
**Next pick:** dedicated `TransactionLimits` exact/plus-one on a wired
Partial cell (`max_concurrent_tools_per_transaction` /
`max_queued_tools_per_transaction` / `max_tool_payload_bytes` /
`max_actor_commands`) through `StartedRuntime` — **or** wire+prove /
spec-retire an Open product bound (event-queue vs `DeliveryLimits`, tool
schema, diagnostics, actor-command bytes) — **or** race/load / live Grok /
D-054 alias cut. Independent D-025 last. Do not re-pick HTTP 15/15,
RuntimeOwner Drop, DispatcherLimits re-wire, or this Covered/Partial relabel.
**Advisor (2026-08-24, §23 public-limit matrix honesty bar check):** **FAIL —
not honesty-closed as stamped.** Golden / §25 / D-025 are **not** overclaimed
(matrix header, D-025 pack, security checklist, D-053 map, spec header all
keep the exhaustive §23 bullet open). All nine **Open** rows stay Open and
have no dedicated exact/plus-one proofs. Field inventory matches
`TransactionLimits` (26/26). Inventory gate names fields + keeps needles on
disk; it does **not** codegen every public limit.

Honesty residual (shaped Covered / next pick):
- `max_event_queue` / `max_event_queue_bytes` **Covered** cites
  `s22_6_event_{item,byte}_plus_one_*`, which exercise host
  `DeliveryLimits`, not these `TransactionLimits` fields. The fields are
  validate-only (no Loop use site). Status should be **Partial** or **Open
  (unwired)** — same pattern already used for
  `max_concurrent_tools_per_transaction`.
- Several **Open** examples in the next pick (`max_actor_commands` /
  `max_actor_command_bytes`, `max_tool_schema_bytes`,
  `max_queued_tools_per_transaction`, `max_diagnostic_*`,
  `callback_deadline`, `max_tool_{payload,output}_bytes`) are config /
  validate-only or hardcoded on the production dispatcher
  (`with_runtime_resources(..., 8, 16)` + `usize::MAX` payload/output).
  Plus-one tests on those cells without wiring (or a spec deletion) would
  be shaped-done.
- Legend **Covered** = exact *and/or* plus-one is weaker than §23 “exact
  **and** plus-one”; acceptable only while the exhaustive bullet stays open.

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.
**Next pick:** relabel event-queue rows and mark unwired Open cells as
unwired (finish this honesty slice) **then** wire+prove a real Open bound
(tool payload/output or actor command, if still a product limit) **or**
race/load / live Grok / D-054 alias cut. Independent D-025 last. Do not
re-pick HTTP 15/15 or RuntimeOwner Drop.
**Advisor (2026-08-24, matrix honesty fix + TransactionLimits wiring bar check):**
**FAIL — not honesty-closed as stamped.** Golden / §25 / D-025 are **not**
overclaimed (matrix header, spec, Loop README, security checklist, D-025 pack
keep exhaustive §23 / Sign-off open). Previous event-queue residual **closed**:
`max_event_queue*` are **Open (unwired)**; `s22_6_event_*` remain DeliveryLimits
needles, not these fields. Field inventory still 26/26. Wiring is real Silver
progress: control `mpsc` uses `max_actor_commands`; production dispatchers use
`limits_from_transaction` (no hardcoded `8/16` + `usize::MAX` on the
coordinator path). `max_actor_commands` **Partial** and remaining Open rows
(event-queue, actor-command **bytes**, tool schema, diagnostics, callback
deadline) are honest.

Honesty residual (shaped **Covered** vs own legend):
- Legend **Covered** = proof sets **this** `TransactionLimits` field and fails
  closed. **Partial** = wired or adjacent proof, not a field-exact+plus-one cell.
- `max_tool_output_bytes` **Covered** cites
  `max_tool_output_bytes_plus_one_fails_closed`, which constructs
  `DispatcherLimits { max_tool_output_bytes: 4 }` — not
  `TransactionLimits.max_tool_output_bytes`. Same adjacent-proof class as
  payload (correctly **Partial**). Status should be **Partial**.
- `max_concurrent_tools_per_transaction` / `max_queued_tools_per_transaction`
  **Covered** on wiring only. No test sets those `TransactionLimits` fields.
  `capacity_limit_plus_one_rejects` is shared global capacity, not these
  fields. Status should be **Partial**.
- Covered = exact *and/or* plus-one remains weaker than §23 “exact **and**
  plus-one”; acceptable only while the exhaustive bullet stays open.

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.
**Next pick:** relabel the three shaped Covered rows to **Partial** (finish
this honesty slice) **then** either a dedicated `TransactionLimits` exact+plus-one
cell on a wired field (output / concurrent / queued / actor-commands) **or**
wire+prove a still-Open product bound (event-queue vs DeliveryLimits, tool
schema, diagnostics, actor-command bytes) **or** race/load / live Grok /
D-054 alias cut. Independent D-025 last. Do not re-pick HTTP 15/15,
RuntimeOwner Drop, or re-wire DispatcherLimits.
**Advisor (2026-08-24, provider-budget exact/cumulative polish):** **PASS —
Silver+Golden-progress** for this slice only. Counts re-verified 15/15 and
14/14. D-053 map HTTP plus-one clause narrowed. Do **not** promote Golden /
§25 / D-025.

**Advisor (2026-08-24, D-053 honesty closer bar check):** **FAIL — not
Silver-closed as stamped.** Fake 16/16 holds; HTTP 15/15 does not
(flakes to 14/15). Map plus-one tests exist; remaining-Golden-as-sign-off-
only is shaped-done. See D-053 record. Do **not** promote Golden / §25 /
D-025. **Next pick:** deterministic HTTP DirectLlm e2e, then §23 extras /
D-054. Agents must not self-sign.

**Advisor (2026-08-24, HTTP e2e determinism harden bar check):** **PASS —
Silver** for this harness slice only. Named HTTP flake closed on this tree
(Fake **16/16**, HTTP **15/15**; this review 20/20 HTTP full-suite + 10/10
combined). Polish stamp "15/15 and 14/14" is stale. Spec / Loop README
**Not** Golden / §25 / D-025. **Next pick:** remaining named §23 extras /
D-054 outside DirectLlm. Independent Golden / D-025 last. Agents must not
self-sign.

**Expert + Advisor (2026-08-23, D-046 Fixed):** **PASS — Silver** for this
slice only.

**D-047 remediation note (2026-08-23):** Ordinary publish waits under
deadline (not cancel — cancel is set before Seal). Seal uses deadline-only
wait. Sticky fail → Seal reports Deadline/Limit/QueueClosed; finalizer
upgrades Completed → EventDeliveryFailed. Residual: dedicated Seal priority
channel when command mpsc is Full. **Superseded same day:** dedicated
`SealCommand` channel + `d047_seal_priority_when_ordinary_cmd_queue_full`
(see re-review Advisor stamp).

**Expert + Advisor (2026-08-23, D-047 Fixed):** **PASS — Silver** for this
slice only. Wait-for-capacity ordinary publish + sticky fail → Seal delivery
+ finalizer `EventDeliveryFailed` upgrade meet the checked acceptance boxes.
Residual Seal-priority on full publisher-command mpsc was **closed** in the
`b82c763` re-review slice (dedicated channel + named test). Do **not**
promote Golden / §25 / D-025.

**Remediation progress:** D-046 Fixed; D-047 Fixed; D-048 Fixed
(`OwnedProcessRegistry` + Stopped gate). Sacrificial abort-after-spawn PID
proof remains a noted residual under D-048.

**Expert + Advisor (2026-08-23, D-048 Fixed):** **PASS — Silver** for this
slice only. Registry parks live `ToolKillHandle` until reap; Stopped requires
`process_registry.is_empty()`; snapshot prefers `live_count`. Residual:
sacrificial abort-after-spawn PID-not-waitable proof. Do **not** promote
Golden / §25 / D-025.

**Remediation progress:** D-046–D-054 Fixed (Silver slices).

**Expert + Advisor (2026-08-23, D-049 Fixed):** **PASS — Silver** for this
slice only. Supervisor signals `drain_complete` without public `Stopped`;
executor thread signals `thread_exited` after `shutdown_timeout`;
`wait_stopped` budgets drain + join and retains the handle on timeout;
`hold_executor_teardown` proves TimedOut→Quiescing then Stopped after join.
Do **not** promote Golden / §25 / D-025.

**Expert + Advisor (2026-08-23, D-050 Fixed):** **PASS — Silver** for this
slice only. `RegisteredTool::try_new` rejects `AbortableAtYield`;
`try_new_abortable` requires sealed `AbortableAtYieldHandler` (crate
`AsyncToolHandler` / `IsolatedKillableToolHandler`); dispatcher requires
CancelOnly + unspawned `drive` before polling. Boolean `supports_abort` /
`runtime_owns_abortable_drive` cannot register the class. Product crates
still do not depend on testkit. Evidence: `host_tools` unit tests,
`s22_4_cannot_self_assert_abortable_via_boolean`,
`s22_4_abortable_permit_held_until_join`. Residual: §14.2 “Tokio join handle
abort” wording vs M5.4 inline drive remains a spec-alignment item, not a
boolean hole. Do **not** promote Golden / §25 / D-025.

**Expert + Advisor (2026-08-23, D-051 Fixed):** **PASS — Silver** for this
slice only. `PendingRawConnection::open_owned` / `failed` transfer required
owner work; `OpenedRawConnection` no longer carries `owner_work`. Live
`lifecycle/exchange.rs` takes owner and registers `TaskClass::ConnectorOwner`
before the first poll of `pending.opened`. All product `begin_open` paths
(Fake, HTTP, Proxy failed-pending, Grok, Claude, Z.ai, Cursor, Codex, Agy)
use `open_owned` / `failed`. Product crates still do not depend on testkit
(architecture gates green). Evidence: `d051_pending_exposes_owner_before_open_poll`,
`d051_cancel_during_delayed_open_joins_owner`, migrated connector suites.
Residuals: v2 §15 `ConnectionOwnerHandle` sketch vs `ConnectionOwnerWork` +
supervisor (representation MAY differ); `CONNECTOR.md` Pending sketch still
omits the transfer; Grok profile `connect()` still uses an internal connect
task joined/aborted by `PendingGrokServer` after the outer owner starts.
Do **not** promote Golden / §25 / D-025.

**Expert + Advisor (2026-08-23, D-052 Fixed):** **PASS — Silver** for this
slice only. Four §23 commands green on this tree; `make gates` / `make dist`
are release-blocking. CI rustdoc now matches with `RUSTDOCFLAGS=-D warnings`
(closed with D-053). No Golden / §25 / D-025 claim.

**Expert + Advisor (2026-08-23, D-053 Fixed after README + fake_echo harden):**
**PASS — Silver** for this slice only. Intended on-disk suite compiles under
workspace `--all-targets`; no testkit bleed; README/spec stay Fixed Silver.
Do **not** promote Golden / §25 / D-025.

**Advisor (2026-08-23, first pass):** D-054 did **not** meet Silver Fixed
while WP-12 leftover Pass rows still cited deleted suites / `CallbackService`.

**Follow-up (same day):** WP-12 + D-003 retargeted; Expert **PASS — Silver**.
**Advisor (same day, after retarget):** **PASS — Silver** for the named
honesty/deletion slice (see D-054 record). **Next pick:** independent
acceptance / D-025 sign-off or named Golden work. Agents must not
self-sign. Do **not** promote Golden / §25 / D-025.

**Advisor (2026-08-24, max_tool_payload_bytes Covered promotion):** **PASS —
Silver** for this honesty slice. Legend vs status hold: the cited
`fake_transaction_limits_max_tool_payload_bytes_plus_one_rejects` sets
**`TransactionLimits.max_tool_payload_bytes`** through `StartedRuntime` /
`limits_from_transaction` (coordinator production path; binding
`min(spec.max_input_bytes, transaction cap)`), then fail-closes before
handler start (`validate_tool_input` → `DispatchOutcome::Rejected` /
`oversized_input`). Not a DispatcherLimits-only or wiring-only cell.
Fake DirectLlm **18/18** on this tree. Concurrent/queued remain **Partial**;
event-queue / actor-command bytes / schema / diagnostics / callback stay
**Open**. Do **not** promote Golden / §25 / D-025.

Standing caveats (not a fail of this relabel): legend Covered is fail-closed
at the bound (exact *and/or* plus-one), weaker than §23 “exact **and**
plus-one”. The payload cell is oversize fail-closed (cap `5` vs
`{"q":"hello-world"}`), not exact-admits. The assertion matches
`Completed`/`DomainFailed` message substring because `DispatchRejected`
is remapped to `tool_execution_failed` + `oversized_input:…`; the
`RuntimeFailed` arm is unused. Same class as `max_tool_output_bytes`.

**Next pick:** dedicated `TransactionLimits` field cell on a remaining
wired **Partial** (`max_concurrent_tools_per_transaction` /
`max_queued_tools_per_transaction` / `max_actor_commands`) through
`StartedRuntime` — **or** wire+prove / spec-retire an **Open** product
bound (event-queue vs `DeliveryLimits`, `max_actor_command_bytes`, tool
schema, diagnostics, `callback_deadline`) — **or** race/load / live Grok /
D-054 alias cut. Independent D-025 last. Do not re-pick this payload
Covered relabel, `max_tool_output_bytes` Covered, HTTP 15/15,
RuntimeOwner Drop, or DispatcherLimits re-wire.

**Advisor (2026-08-24, max_queued_tools_per_transaction Covered promotion):**
**FAIL — not honesty-closed as Covered.** Golden / §25 / D-025 are **not**
overclaimed. Concurrent / payload / output Covered cells are **not** reopened.

Legend **Covered** = proof sets **this** `TransactionLimits` field and fails
closed **at the bound**. The cited
`transaction_limits_max_queued_tools_plus_one_rejects` does set
`TransactionLimits.max_queued_tools_per_transaction = 1` and maps through
`limits_from_transaction` (same dispatcher layer as concurrent — that part
matches the concurrent PASS). It does **not** prove the bound:

- `try_enqueue` occupies the queue slot only until `try_acquire` succeeds,
  then dequeues. Echo is `ImmediateToolHandler`; txn concurrent is 32, so
  acquire is immediate when the scheduler serializes.
- The cell is a 16-task barrier stampede asserting `queue_full >= 1`. That
  is a load/race interleaving, not exact-admit of 1 queued start and
  plus-one reject of the 2nd. Per-tool `max_concurrent: 2` (from `make_spec`)
  may make overlap common (30/30 green on this tree) — still not a bound cell.
- No exact-admit assertion. `>= 1` under parallel enqueue is the shaped-
  qualification pattern (Law/style: deterministic fixtures over scheduler
  races). Adjacent concurrent cell is the contrast: hold one running, second
  deterministically `tool_capacity_exceeded`.

Status should stay **Partial** (wired + reject code can fire) until a
deterministic plus-one exists. Do **not** skip to `max_actor_commands` while
this row is Covered on a stampede.

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.

**Next pick:** relabel `max_queued_tools_per_transaction` → **Partial** **or**
replace the stampede with a deterministic field cell (hold 1 running so a
second dispatch stays queued, then plus-one `tool_queue_full`; optionally
exact-admit) — then re-ask Covered. After that: Partial→Covered
`max_actor_commands` **or** wire+prove / spec-retire an Open product bound
(event-queue vs `DeliveryLimits`, `max_actor_command_bytes`, tool schema,
diagnostics, `callback_deadline`) **or** race/load / live Grok / D-054 alias
cut. Independent D-025 last. Do not re-pick concurrent/payload/output Covered,
HTTP 15/15, RuntimeOwner Drop, or DispatcherLimits re-wire.

**Agent (2026-08-24, max_queued_tools_per_transaction deterministic rewrite):**
Replaced the 16-way barrier stampede with a hold-plus-one field cell matching
the concurrent Covered pattern and the Advisor FAIL next pick:

- Sets `TransactionLimits.max_queued_tools_per_transaction = 1` (and
  `max_concurrent_tools_per_transaction = 1`) through
  `TransactionToolDispatcher::limits_from_transaction`.
- Hold tool occupies the sole concurrent slot (`active_tools() == 1`).
- Second echo enqueue occupies the sole queue slot (`queued_tools() == 1`)
  while spinning ≤50ms for acquire (exact-admit of bound=1).
- Third echo → deterministic `tool_queue_full` (plus-one fail-closed).
- Second then → `tool_capacity_exceeded` after the spin window.
- Observation: `TransactionToolDispatcher::{active_tools,queued_tools}`
  forward to existing `TransactionToolCapacity` counters (bounded-resource
  observation only; no ambient identity).

Needle unchanged: `transaction_limits_max_queued_tools_plus_one_rejects`.
Matrix row stays **Covered** with updated proof note. linked_tools 17/17;
queued cell 10/10 consecutive; s23 inventory 4/4.

Do **not** promote Golden / §25 / D-025. Awaiting Expert/Advisor Covered PASS
on this deterministic cell before Partial→Covered `max_actor_commands`.

**Advisor (2026-08-24, max_queued_tools_per_transaction Covered after rewrite):**
**PASS — Covered** for this cell only. Golden / §25 / D-025 are **not**
claimed. Concurrent / payload / output Covered cells are **not** reopened.

Prior FAIL residual **closed**: the 16-way barrier stampede (`queue_full >= 1`)
is gone. Cited
`transaction_limits_max_queued_tools_plus_one_rejects` now:

- Sets **`TransactionLimits.max_queued_tools_per_transaction = 1`** (and
  concurrent=1 so the hold occupies the only running slot) through
  `limits_from_transaction` (production dispatcher mapping).
- Hold until `active_tools() == 1`; second dispatch until
  `queued_tools() == 1` (exact occupancy of bound=1).
- Sequential third → `tool_queue_full` (plus-one fail-closed). Waiter then
  `tool_capacity_exceeded` after the bounded spin. Missed occupancy fails
  the test (40ms wait vs 50ms spin), not green.

Same dispatcher-layer class as concurrent Covered PASS. Observation APIs
`active_tools` / `queued_tools` wrap existing `TransactionToolCapacity`
counters — not ambient identity, not testkit bleed. Inventory needle
unchanged. Re-verified this tree: linked_tools **17/17**, cell **10/10**,
s23 inventory **4/4**.

Standing caveats (not a fail of this relabel): legend Covered is fail-closed
at the bound (exact *and/or* plus-one), weaker than §23 “exact **and**
plus-one”; this cell has occupancy + plus-one. `max(1)` floor on mapping
is out of cell scope.

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.

**Next pick:** Partial→Covered `max_actor_commands` — set
`TransactionLimits.max_actor_commands` on `StartedRuntime` / owner control
`mpsc` (`owner.rs` already wires capacity); exact-admit then plus-one
`ControlCapacityExceeded` (not a generic Full/AlreadyTerminal lie). **Or**
wire+prove / spec-retire an Open product bound (event-queue vs
`DeliveryLimits`, `max_actor_command_bytes`, tool schema, diagnostics,
`callback_deadline`) **or** race/load / live Grok / D-054 alias cut.
Independent D-025 last. Do not re-pick queued/concurrent/payload/output
Covered, HTTP 15/15, RuntimeOwner Drop, or DispatcherLimits re-wire.

**Agent (2026-08-24, max_actor_commands Partial→Covered candidate):**
Sets **`TransactionLimits.max_actor_commands = 1`** on `StartedRuntime`
(`owner.rs` already sizes control `mpsc` from that field). Test-only
`ControlHoldGate` / `RuntimeConfig.hold_control` pauses preferential + select
control drain (same class as `StartHoldGate` / `hold_start`; production
default `None`). Hang admit stays non-terminal:

- First `terminate(Cancel)` → `Accepted` (exact-admit of bound=1).
- Second `terminate(Cancel)` same live tx → `ControlCapacityExceeded`
  (plus-one fail-closed; not Full→AlreadyTerminal).
- Needle: `transaction_limits_max_actor_commands_plus_one_rejects`.
- Matrix → **Covered**; s23 inventory needle added.
- Re-verify: cell **10/10**; s23 **4/4**; adjacent D-039/D-040 control/start
  tests still green.

Do **not** promote Golden / §25 / D-025. Awaiting Expert/Advisor Covered PASS.

**Advisor (2026-08-24, max_actor_commands Partial→Covered):** **PASS —
Covered** for this cell only. Golden / §25 / D-025 are **not** claimed.
Queued / concurrent / payload / output Covered cells are **not** reopened.

Legend **Covered** = proof sets **this** `TransactionLimits` field and fails
closed at the bound. Cited
`transaction_limits_max_actor_commands_plus_one_rejects` does that:

- Sets **`TransactionLimits.max_actor_commands = 1`** on `StartedRuntime`
  (`RuntimeConfig.transaction_limits`).
- Owner sizes the supervisor control `mpsc` from that field (`owner.rs`;
  not `max_active+8`). Production `hold_control` is `None`; test-only
  `ControlHoldGate` pauses preferential `try_recv` **and** `select` recv
  (`control_drain_enabled`) so occupancy is deterministic — same class as
  `hold_start` / D-040.
- First `terminate(Cancel)` on a live Hang tx → `Accepted` (exact-admit of
  bound=1). Second same live tx → `ControlCapacityExceeded` (plus-one
  fail-closed; D-039 ledger check still runs first — not Full→AlreadyTerminal).
- Needle in `s23_exact_limit_plus_one_inventory_present`. Matrix row
  **Covered**. No ambient identity; Loop-owned; no product→testkit bleed.

Re-verified this tree: needle **ok**; s23 inventory **4/4**;
`terminate_after_cancel_is_ledger_honest` (D-039) **ok**;
`start_queue_full_rolls_back_all_permits` (D-040) **ok**. Agent “cell 10/10”
not reproduced as a named filter here; the field cell itself is green.

Standing caveats (not a fail of this relabel): legend Covered is weaker than
§23 exhaustive exact-**and**-plus-one of every public limit. Spec §6 still
names per-actor `command_rx` for child results; production maps this field
to the supervisor control queue (D-015 accepted wiring; no `ActorCommand`
channel in-tree). `.max(1)` is defensive — `TransactionLimits::validate`
already rejects zero. `max_actor_command_bytes` stays **Open** (item
capacity only; control messages are closed enums).

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.

**Next pick:** wire+prove or spec-retire an **Open** product bound —
prefer `max_event_queue` / `max_event_queue_bytes` vs caller
`DeliveryLimits` (unwired TransactionLimits fields; existing s22.6 proofs
are the wrong type) — **or** `max_actor_command_bytes` / `max_tool_schema_bytes`
/ diagnostics / `callback_deadline` — **or** Partial deadline cells
(`transaction_deadline` / `cleanup_deadline` /
`terminal_event_delivery_deadline`) — **or** race/load / live Grok / D-054
alias cut. Independent D-025 last. Do not re-pick
queued/concurrent/payload/output/`max_actor_commands` Covered, HTTP 15/15,
RuntimeOwner Drop, or DispatcherLimits re-wire.

**Agent (2026-08-24, max_event_queue* Open→Covered candidate):**
Wired `TransactionLimits.max_event_queue` / `max_event_queue_bytes` as
**admission ceilings** over caller `DeliveryLimits` (D-055). Enqueue
fail-closed stays on DeliveryLimits (`s22_6_event_*` unchanged).

- `delivery.event_tx` exposes `max_event_items()` (installed at
  `transaction_delivery`) alongside existing `max_event_bytes()`.
- Admission rejects `InvalidConfiguration` when items/bytes exceed the
  runtime ceiling (before reservations/ledger).
- Needles:
  `transaction_limits_max_event_queue_exact_admits_plus_one_rejects`,
  `transaction_limits_max_event_queue_bytes_exact_admits_plus_one_rejects`.
- Matrix → **Covered**; s23 inventory updated; DECISIONS D-055;
  IMPLEMENTATION comment.
- Re-verify: both cells green (10× filter); s23 **4/4**; contracts delivery
  **8/8**; adjacent `capacity_plus_one` / `max_messages` green.

Do **not** promote Golden / §25 / D-025. Awaiting Expert/Advisor Covered PASS.
Do not re-pick max_actor_commands / queued / concurrent / payload / output.

**Advisor (2026-08-24, max_event_queue* Open→Covered via D-055):** **PASS —
Covered both** for these two cells only. Golden / §25 / D-025 are **not**
claimed. `max_actor_commands` / queued / concurrent / payload / output
Covered cells are **not** reopened.

Legend **Covered** = proof sets **this** `TransactionLimits` field and fails
closed at the bound. Cited needles do that:

- Sets **`TransactionLimits.max_event_queue = 1`** /
  **`max_event_queue_bytes = 1024`** on `StartedRuntime`
  (`RuntimeConfig.transaction_limits`).
- `StartedRuntime` copies limits onto `RuntimeShared`; `handle.submit` →
  `admit` **before** reservations. Rejects `InvalidConfiguration` when
  `delivery.event_tx.max_event_items() > max_event_queue` or
  `max_event_bytes() > max_event_queue_bytes` (D-055). Accessors are the
  capacities installed at `transaction_delivery`, not ambient defaults.
- Exact admits (`items=1` / `bytes=1024`); plus-one rejects (`items=2` /
  `bytes=1025`) with the field name in the error. Equality uses `>` so the
  ceiling is inclusive.
- Needles in `s23_exact_limit_plus_one_inventory_present`. Matrix rows
  **Covered**. Adjacent `s22_6_event_{item,byte}_plus_one_fails_closed`
  remain DeliveryLimits enqueue proofs (not this field). Loop-owned; explicit
  `SessionId`; no product→testkit bleed.

Re-verified this tree: both cells **10/10**; s23 inventory **4/4**; adjacent
`s22_6_event_*` **ok**; contracts `delivery` **8/8**.

Standing caveats (not a fail of this relabel): D-055 is an **admission
ceiling** over caller-built mailboxes — it does not size the queue; enqueue
fail-closed stays on `DeliveryLimits`. Legend Covered is weaker than §23
exhaustive exact-**and**-plus-one of every public limit (these two cells
happen to have both). Field inventory still 26/26.

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.

**Next pick:** wire+prove or spec-retire an **Open** product bound —
`max_actor_command_bytes` / `max_tool_schema_bytes` / `max_diagnostic_count` /
`max_diagnostic_bytes` / `callback_deadline` — **or** Partial deadline cells
(`transaction_deadline` / `cleanup_deadline` /
`terminal_event_delivery_deadline`) — **or** race/load / live Grok / D-054
alias cut. Independent D-025 last. Do not re-pick
queued/concurrent/payload/output/`max_actor_commands`/`max_event_queue*`
Covered.

**Advisor (2026-08-24, max_event_queue* Open→Covered via D-055):** **PASS —
Covered** for both cells only. Golden / §25 / D-025 are **not** claimed.
Queued / concurrent / payload / output / `max_actor_commands` Covered cells
are **not** reopened.

Legend **Covered** = proof sets **this** `TransactionLimits` field and fails
closed at the bound. Cited needles do that:

- Sets **`TransactionLimits.max_event_queue` / `max_event_queue_bytes`** on
  `StartedRuntime`; admission rejects `InvalidConfiguration` when caller
  `DeliveryLimits` exceed the ceiling (before reservations) — D-055.
- Exact-admit + plus-one for items (1/2) and bytes (1024/1025).
- Enqueue fail-closed remains on DeliveryLimits (`s22_6_event_*` adjacent).
- Same class as `max_messages` admit ceilings. No ambient identity;
  Loop-owned; no product→testkit bleed.

Re-verified this tree: both cells **10×**; s23 **4/4**; contracts delivery
**8/8**. Expert PASS aligns.

Standing caveats (not a fail): Covered legend is weaker than exhaustive §23
exact-**and**-plus-one of every public limit. Design §13 “enforces event
queue” under push delivery means runtime ceiling over caller ports, not
runtime-allocated mailbox.

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.

**Next pick:** wire+prove or spec-retire an **Open** product bound —
`max_actor_command_bytes` / `max_tool_schema_bytes` / diagnostics /
`callback_deadline` — **or** Partial deadline cells — **or** race/load /
live Grok / D-054 alias cut. Independent D-025 last. Do not re-pick
queued/concurrent/payload/output/`max_actor_commands`/`max_event_queue*`
Covered, HTTP 15/15, RuntimeOwner Drop, or DispatcherLimits re-wire.

**Agent (2026-08-24, max_tool_schema_bytes Open→Covered candidate):**
Wired `TransactionLimits.max_tool_schema_bytes` at `StartedRuntime::start`
(D-056). Replaced hardcoded `64 * 1024` in `HostToolRegistry::build` with
`TransactionLimits::default().max_tool_schema_bytes`; start re-checks against
the runtime field so tighter ceilings cannot be bypassed.

- Needle: `transaction_limits_max_tool_schema_bytes_exact_admits_plus_one_rejects`
  (exact schema size admits; size−1 → `InvalidConfig`).
- Matrix → **Covered**; s23 inventory; DECISIONS D-056.
- Re-verify: cell **10×**; s23 **4/4**; linked_tools green.

Do **not** promote Golden / §25 / D-025. Awaiting Expert/Advisor Covered PASS.
Do not re-pick event-queue / max_actor_commands / queued / concurrent /
payload / output Covered.

**Expert (2026-08-24, max_tool_schema_bytes Open→Covered via D-056):** **PASS —
Covered** for this cell only. `max_event_queue*` / `max_actor_commands` Covered
cells are **not** reopened.

Legend **Covered** = proof sets **this** `TransactionLimits` field and fails
closed at the bound. Cited needle does that:

- Sets **`TransactionLimits.max_tool_schema_bytes`** on `StartedRuntime`.
- `StartedRuntime::start` measures `serde_json::to_vec(input_schema)` **before**
  executor spawn; `>` ceiling → `InvalidConfig("tool schema exceeds
  max_tool_schema_bytes")`. Inclusive exact size. Same `ToolSpec` for both
  sides.
- Needle: exact `schema_bytes` admits; `schema_bytes-1` rejects at start.
- Build-under-default then tighter start is **closed** (D-056). Registry is
  immutable; no post-start insert; no ambient identity; no lock-across-await.
- Empty registry: loop is a no-op; does not start `ToolRuntime`.

Re-verified this tree: cell **ok**; s23 inventory **ok**.

Standing caveats (not a fail of this relabel): construction hygiene in
`HostToolRegistry::build` remains pinned to `TransactionLimits::default()`
(cannot raise above 64 KiB via the runtime field). `.max(1)` is defensive —
`validate` already rejects zero. Build `unwrap_or(0)` on serialize failure is
dead for `serde_json::Value`. Output-contract schemas are not this field.

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.

**Next pick:** wire+prove or spec-retire an **Open** product bound —
`max_actor_command_bytes` / diagnostics / `callback_deadline` — **or** Partial
deadline cells — **or** race/load / live Grok / D-054 alias cut. Independent
D-025 last. Do not re-pick queued / concurrent / payload / output /
`max_actor_commands` / `max_event_queue*` / `max_tool_schema_bytes` Covered.

**Advisor (2026-08-24, max_tool_schema_bytes Open→Covered via D-056):** **PASS —
Covered** for this cell only. Golden / §25 / D-025 are **not** claimed.
Queued / concurrent / payload / output / `max_actor_commands` /
`max_event_queue*` Covered cells are **not** reopened.

Legend **Covered** = proof sets **this** `TransactionLimits` field and fails
closed at the bound. Cited needle does that:

- Sets **`TransactionLimits.max_tool_schema_bytes`** on `StartedRuntime`.
- `StartedRuntime::start` re-validates `serde_json::to_vec(input_schema)`
  against the bootstrap field **before** executor spawn; `>` ceiling →
  `InvalidConfig("tool schema exceeds max_tool_schema_bytes")` (D-056).
- Exact schema size admits; `schema_bytes - 1` (one byte over) rejects.
  Same `ToolSpec` both sides. Inclusive exact.
- Loop-owned (`monoloop-loop` lifecycle); no ambient session/run identity;
  no product→testkit bleed (`monoloop-loop` Cargo.toml has no testkit dep).
- Empty registry: start loop is a no-op; does not start `ToolRuntime`.

Re-verified this tree: cell **10/10**; s23 inventory **4/4**; linked_tools
**17/17**. Expert PASS aligns.

Standing caveats (not a fail): construction hygiene in
`HostToolRegistry::build` stays pinned to `TransactionLimits::default()`
(cannot raise above 64 KiB via the runtime field). Covered legend is weaker
than exhaustive §23 exact-**and**-plus-one of every public limit.

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.

**Next pick:** wire+prove or spec-retire an **Open** product bound —
`max_actor_command_bytes` (likely spec-retire; closed-enum control msgs) /
`max_diagnostic_*` / `callback_deadline` — **or** Partial deadline cells —
**or** race/load / live Grok / D-054 alias cut. Independent D-025 last.
Do not re-pick event-queue / `max_actor_commands` / queued / concurrent /
payload / output / `max_tool_schema_bytes` Covered.

**Agent (2026-08-24, max_actor_command_bytes Open→Retired candidate):**
Spec-retired via **D-057**. Supervisor `ControlCommand` is a closed enum
(Cancel / ForceTerminate / BeginShutdown / StopSupervisor); product control
bound remains item capacity **`max_actor_commands`** (already Covered). Field
retained for validate nonzero / ABI; no byte accounting use site invented.

- Matrix legend adds **Retired**; row → Retired citing D-057.
- `limits.rs` / IMPLEMENTATION comments updated.
- Golden residual note: Retired is decision-closed, not Open.

Do **not** promote Golden / §25 / D-025. Awaiting Expert/Advisor Retired PASS.
Do not re-pick max_tool_schema_bytes / event-queue / max_actor_commands /
queued / concurrent / payload / output Covered.

**Advisor (2026-08-24, max_actor_command_bytes Open→Retired):** **PASS —
Retired (D-057).** Not Covered.

Re-verified this tree:

- `ControlCommand` is a closed four-variant enum (`Cancel` / `ForceTerminate` /
  `BeginShutdown` / `StopSupervisor`); payloads are `TransactionId` or unit —
  no variable command bytes to budget.
- Production control `mpsc` is sized from `TransactionLimits.max_actor_commands`
  (Covered needle `transaction_limits_max_actor_commands_plus_one_rejects`).
  `max_actor_command_bytes` has **no** enqueue / accounting use site (validate
  nonzero + ABI only, as D-057 states).
- `DECISIONS.md` D-057; matrix legend includes **Retired**; row cites D-057;
  Golden residual treats Retired as decision-closed, not Open. Field comments
  in `limits.rs` and IMPLEMENTATION §12 match.
- s23 inventory **4/4**. Loop-owned; no ambient session/run identity; no
  product→testkit bleed (`monoloop-loop` has no testkit dep).

Standing caveats (not a fail of Retired): IMPLEMENTATION §6 still names
per-actor `command_rx` / `ActorCommand` for child results — historical target
architecture; production remaps via D-015/D-057 to the supervisor control
queue (no in-tree `ActorCommand`). Reintroducing payload-bearing control
messages requires a superseding decision plus a real byte bound. Covered
legend remains weaker than exhaustive §23 of every public limit.

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.

**Next pick:** wire+prove or spec-retire an **Open** product bound —
prefer `max_diagnostic_count` / `max_diagnostic_bytes` (no production
`TransactionEventPayload::Diagnostic` emission path yet — `SafeDiagnostic`
construction uses `TransactionLimits::default()`, not the runtime field;
wire a real emission path **or** defer with a DECISIONS entry) —
**or** `callback_deadline` (validate-only; no production wait site) —
**or** Partial deadline cells (`transaction_deadline` / `cleanup_deadline` /
`terminal_event_delivery_deadline`) — **or** race/load / live Grok / D-054
alias cut. Independent D-025 last.
Do not re-pick schema / event-queue / `max_actor_commands` / queued /
concurrent / payload / output Covered, and do not re-pick this Retired cell.

**Agent (2026-08-24, diagnostics defer D-058 + transaction_deadline Covered candidate):**

1. **`max_diagnostic_count` / `max_diagnostic_bytes`:** Deferred via **D-058**.
   No production `TransactionDiagnostic` emission (`CanonicalUnit` path only;
   completion diagnostics always empty; ledger count unused). Matrix rows stay
   **Open** with D-058 note — not Covered, not invented emission.

2. **`transaction_deadline` Partial→Covered candidate:**
   `transaction_limits_transaction_deadline_hang_ends_deadline_exceeded` sets
   `TransactionLimits.transaction_deadline = 80ms` on `StartedRuntime`; Hang
   exchange → `TransactionEndKind::DeadlineExceeded`. Needle + matrix Covered.
   Re-verify: cell **10×**; s23 **4/4**.

Do **not** promote Golden / §25 / D-025. Awaiting Expert/Advisor on both
(D-058 deferral honesty + deadline Covered). Do not re-pick Retired
max_actor_command_bytes / schema / event-queue / max_actor_commands /
queued / concurrent / payload / output Covered.

**Advisor (2026-08-24, §23 matrix honesty dual gate):** **PASS** on both
(D-058 deferral honesty + `transaction_deadline` Covered). Not Golden / §25 /
D-025.

1. **D-058 — PASS deferral.** `max_diagnostic_count` / `max_diagnostic_bytes`
   stay **Open**. `DECISIONS.md` D-058; matrix notes cite it. Production does
   not emit `TransactionEventPayload::Diagnostic` (coordinator publishes
   `CanonicalUnit` only; `build_completion` / `end_event` pass empty
   diagnostics; ledger `diagnostic_count` is admission `0` with no increment).
   `SafeDiagnostic::try_new_default` uses `TransactionLimits::default()`, not
   the runtime field. No shaped Covered; no invented emission path.

2. **`transaction_deadline` Partial→Covered — PASS Covered.** Needle
   `transaction_limits_transaction_deadline_hang_ends_deadline_exceeded` sets
   `TransactionLimits.transaction_deadline = 80ms` on `StartedRuntime`.
   Production maps that field to `RuntimeShared.default_deadline` → exchange
   `remaining()`; Fake Hang stays open until local cancel/terminate; elapsed
   remaining → `ExchangeFailure::DeadlineExceeded` →
   `TransactionEndKind::DeadlineExceeded`. `cleanup_deadline` in the cell is
   500ms (distinct). Inventory needle present. Re-verified this tree: cell
   **10/10** (~0.09s each, consistent with 80ms not the 2s wait cap); s23
   inventory **4/4**. Loop-owned (`monoloop-loop` lifecycle); no ambient
   session/run identity; no product→testkit bleed (`monoloop-loop` has no
   testkit dep).

Standing caveats (not a fail): Covered legend remains weaker than exhaustive
§23 exact-**and**-plus-one of every public limit. Duration cell is fail-closed
at the bound (not a count exact/plus-one). Open/Partial residuals remain:
`max_diagnostic_*` (D-058), `callback_deadline`, `cleanup_deadline`,
`terminal_event_delivery_deadline`.

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.

**Next pick:** Partial→Covered on `cleanup_deadline` /
`terminal_event_delivery_deadline` **or** wire+prove / spec-retire
`callback_deadline` **or** race/load / live Grok / D-054 alias cut.
Independent D-025 last. Do not re-pick Retired `max_actor_command_bytes` or
schema / event-queue / `max_actor_commands` / queued / concurrent / payload /
output Covered, and do not invent diagnostic emission to green D-058.

**Agent (2026-08-24, terminal_event_delivery_deadline Partial→Covered candidate):**
Sets **`TransactionLimits.terminal_event_delivery_deadline = 1ms`** on
`StartedRuntime` (production Finalizer Seal budget). Fake echo + host mailbox
capacity 1 (undrained) so Seal waits; completion reports
`TerminalEventDelivery::DeadlineExceeded` and
`TransactionEndKind::EventDeliveryFailed` (sticky remap). Long
`transaction_deadline` (5s) so the cell is Seal budget, not tx deadline.

Needle: `transaction_limits_terminal_event_delivery_deadline_seal_fails_closed`.
Matrix → **Covered**; adjacent D-047 unit proofs retained. Re-verify: cell
**10×**; Hang `transaction_deadline` still green; D-047 adjacent green; s23
**4/4**.

Do **not** promote Golden / §25 / D-025. Awaiting Expert/Advisor Covered PASS.
Do not re-pick transaction_deadline Covered / D-058 deferral / Retired
actor-command-bytes / schema / event-queue / max_actor_commands / queued /
concurrent / payload / output Covered.

**Expert (2026-08-24, terminal_event_delivery_deadline Partial→Covered):** **PASS —
Covered** for this cell only.

1. **Wire.** `StartedRuntime` copies
   `TransactionLimits.terminal_event_delivery_deadline` onto
   `RuntimeShared`. Finalizer Seal sets
   `seal_budget = shared.terminal_event_delivery_deadline` and
   `SealCommand.deadline = now + seal_budget`. Not `cleanup_deadline`, not
   `default_deadline` / `transaction_deadline`, no 50ms floor. Reply wait is
   `seal_budget + 100ms` slack only; Ended enqueue uses the Instant.

2. **Proof.** Needle
   `transaction_limits_terminal_event_delivery_deadline_seal_fails_closed`
   sets the field to **1ms** on `StartedRuntime`; `transaction_deadline=5s`,
   `cleanup_deadline=500ms`; Fake echo; `DeliveryLimits` items=1 undrained.
   Completion: `TerminalEventDelivery::DeadlineExceeded` + sticky
   `EventDeliveryFailed`. Re-verified **10/10** (~0.01s, not 500ms/5s);
   Hang `transaction_deadline` still green; D-047 adjacent green; s23 **4/4**.

3. **Races / distinctness.** Coordinator awaits ordinary `Publish` onto the
   publisher cmd queue **before** `TerminalProposal`; Finalizer Seals after
   coordinator exit — not Seal-before-first-queued-unit. Fake echo of `hi`
   via `TestTextEncoder` (`hi. `) is a complete Text unit (sibling
   `fake_echo_exchange_emits_canonical_text_unit`). Cap-1 undrained mailbox
   then blocks `Ended` under the Seal Instant. D-047 needles construct
   `run_event_publisher` with a **local** Instant and do **not** set this
   `TransactionLimits` field — they stay adjacent Partial Instant proofs.

Standing caveats (not a fail): Covered legend is fail-closed at the duration
bound, not count exact/plus-one. 1ms Instant can theoretically expire from
scheduling; 10/10 plus coordinator-before-Seal + echo unit make occupancy
the production path. Open/Partial residuals unchanged:
`max_diagnostic_*` (D-058), `callback_deadline`, `cleanup_deadline`.

Do not re-pick `transaction_deadline` Covered or D-058.

**Advisor (2026-08-24, terminal_event_delivery_deadline Partial→Covered):** **PASS —
Covered** for this cell only. Not Golden / §25 / D-025.

1. **Wire.** `StartedRuntime` copies
   `TransactionLimits.terminal_event_delivery_deadline` onto `RuntimeShared`
   (`owner.rs`). Finalizer Seal (`supervisor.rs`) uses
   `seal_budget = shared.terminal_event_delivery_deadline` and
   `SealCommand.deadline = now + seal_budget`. Publisher
   `enqueue_under_deadline` / fence drain share that Instant. Not
   `cleanup_deadline`, not `transaction_deadline`, no 50ms floor. Reply wait
   slack (`+100ms`) does not extend Ended enqueue.

2. **Proof.** Needle
   `transaction_limits_terminal_event_delivery_deadline_seal_fails_closed`
   sets the field to **1ms** on `StartedRuntime`; `transaction_deadline=5s`,
   `cleanup_deadline=500ms`; Fake echo; host mailbox items=1 undrained.
   Completion: `TerminalEventDelivery::DeadlineExceeded` + sticky
   `TransactionEndKind::EventDeliveryFailed`. Re-verified this tree: cell
   **10/10** (~0.01s, not 500ms/5s); Hang
   `transaction_limits_transaction_deadline_hang_ends_deadline_exceeded`
   **3/3** (~0.09s); s23 inventory **4/4**. Inventory needle present. Matrix
   row **Covered**. Loop-owned (`monoloop-loop` lifecycle); no ambient
   session/run identity; no product→testkit bleed.

3. **Adjacent D-047.** Named matrix adjacent
   `d047_seal_uses_terminal_deadline_not_transaction_deadline` green;
   `d047_terminal_deadline_uses_configured_value_exactly` and
   `d047_full_queue_seal_reports_deadline_not_published` green. Those D-047
   needles construct `run_event_publisher` with a local Instant and do **not**
   set this `TransactionLimits` field. They stay adjacent Instant proofs.

Standing caveats (not a fail): Covered legend is fail-closed at the duration
bound, not count exact/plus-one. `d047_seal_priority_when_ordinary_cmd_queue_full`
is racy in isolation (1/10 here: ordinary queue drained before the Full
assert) — it does not set this field and is **not** the Covered cell. Open /
Partial residuals: `max_diagnostic_*` (D-058), `callback_deadline`,
`cleanup_deadline`.

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.

**Next pick:** Partial→Covered on `cleanup_deadline` **or** wire+prove /
spec-retire `callback_deadline` **or** race/load / live Grok / D-054 alias
cut. Independent D-025 last. Do not re-pick `transaction_deadline` Covered /
D-058 deferral / Retired actor-command-bytes / schema / event-queue /
`max_actor_commands` / queued / concurrent / payload / output Covered /
this `terminal_event_delivery_deadline` Covered. Do not invent diagnostic
emission to green D-058.

**Agent (2026-08-24, callback_deadline D-059 deferral + cleanup_deadline honesty):**

1. **`callback_deadline`:** Deferred via **D-059**. Validate-only; M7 push
   completion has no core host-callback wait. Matrix stays **Open** with
   D-059 note — not Covered, not invented wait.

2. **`cleanup_deadline`:** Remains **Partial**. Wired to exchange
   `children.wait(cleanup_deadline)` (exact) and quiesce hard-grace
   (`cleanup_deadline.max(2s)` floor on that path). No distinct fail-closed
   completion code suitable for a Covered duration cell without inventing
   observability. Matrix note updated.

Do **not** promote Golden / §25 / D-025. Awaiting Expert/Advisor on D-059
deferral honesty (+ cleanup Partial note). Do not re-pick
terminal_event_delivery_deadline / transaction_deadline Covered or D-058.

**Expert (2026-08-24, D-059 callback_deadline + cleanup_deadline Partial):** **PASS**
on both. Not a self-sign of remaining Open/Partial rows.

1. **A `callback_deadline` D-059 — PASS deferral.** Field is validate-nonzero
   only (`TransactionLimits::validate`). Production completion is M7 push
   oneshot (`TransactionCompletionSender`); core MUST NOT invoke
   `CompletionCallback` (v2 §6.1–6.3). Host `adapt_completion_callback` is
   unbounded `recv` on the caller task and does not read this field.
   Inventing a core wait/timeout solely to green Covered would contradict
   non-blocking completion send and put host-callback execution back on the
   kernel. Open + D-059 (same honesty class as D-058) is correct; not
   Covered; not Retired unless a later decision deletes the product bound.

2. **B `cleanup_deadline` stay Partial — PASS honesty.** Exchange
   `ChildJoins::wait` uses the field exactly (`tokio::time::timeout`, result
   discarded). Quiesce hard-grace uses `cleanup_deadline.max(2s)` (D-045:
   never abort Finalizer; EventPublisher only after grace). Completion always
   publishes `CleanupStatus::Pending { counts }` — timeout does not rewrite
   terminal cause (v2 §13.1) and has no distinct fail-closed completion
   code. **Do not remove the 2s floor to claim Covered via hard-grace:**
   that path is shutdown residual abort, not a field-exact fail-closed
   transaction cell; shrinking grace races Seal/completion (D-045). Stay
   Partial until a real cleanup-timeout observation exists (e.g. distinct
   `CleanupStatus` / join-timeout proof that still does not rewrite cause).

Do **not** reopen `terminal_event_delivery_deadline` Covered.

**Next pick:** remaining Open/Partial (`max_diagnostic_*` D-058,
`callback_deadline` D-059, `cleanup_deadline` Partial) **or** race/load /
live Grok / D-054 alias cut. Independent D-025 last. Do not invent a
callback wait or strip the quiesce floor to green Covered.

**Advisor (2026-08-24, §23 matrix honesty dual gate):** **PASS** on both
(D-059 deferral + `cleanup_deadline` Partial). Not Golden / §25 / D-025.

1. **D-059 `callback_deadline` — PASS deferral.** Field stays **Open**, not
   Covered. `DECISIONS.md` D-059; matrix cites it. Production use is
   validate-nonzero only (`TransactionLimits::validate`). Core completion is
   M7 push oneshot (`TransactionCompletionSender`); v2 §6.1–6.3 **MUST NOT**
   invoke `CompletionCallback`. `callback_deadline` has **no** production
   wait/join in product crates (contracts field + validate only). Host
   `adapt_completion_callback` is unbounded `recv` on the caller task and does
   not read this field. Same honesty class as D-058: Open + DECISIONS, not
   invented wait, not Retired (product bound retained until a superseding
   decision wires a real core or documented host-adapter wait).

2. **`cleanup_deadline` stay Partial — PASS honesty.** Exchange
   `ChildJoins::wait` applies the field exactly (`tokio::time::timeout`;
   result discarded). Supervisor quiesce hard-grace uses
   `cleanup_deadline.max(Duration::from_secs(2))` (D-045: do not abort
   Finalizer; EventPublisher only after grace). Completion always publishes
   `CleanupStatus::Pending { live counts }` — join timeout does not rewrite
   terminal cause (v2 §13.1) and has no distinct fail-closed completion code.
   **Do not require Covered** without an observable cleanup-timeout signal.
   **Do not require floor removal** to fake a duration cell: shrinking grace
   races Seal/completion. Covered later only if floor removal **and** a
   fail-closed observation are explicitly agreed (still must not rewrite
   cause).

Re-verified this tree: s23 **4/4**;
`transaction_limits_terminal_event_delivery_deadline_seal_fails_closed`
green (~0.06s). Loop-owned (`monoloop-loop` lifecycle); no ambient
session/run identity; no product→testkit bleed (`monoloop-loop` has no
testkit dep).

Standing caveats (not a fail): Covered legend remains weaker than exhaustive
§23 exact-and-plus-one of every public limit. Open/Partial residuals remain
Golden blockers: `max_diagnostic_*` (D-058), `callback_deadline` (D-059),
`cleanup_deadline` (Partial). `TRANSACTION_RUNTIME_IMPLEMENTATION.md` §7
still describes v1 callback reservation/deadline steps; v2 + D-059 supersede
that path — do not treat the old numbered list as a Covered obligation.

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.

**Next pick:** race/load / live Grok / D-054 alias cut. Independent D-025
last. `cleanup_deadline` Covered **only** if floor removal + observable
fail-closed signal are agreed. Do not invent diagnostic emission (D-058) or
a callback wait (D-059). Do not re-pick `terminal_event_delivery_deadline`
Covered, `transaction_deadline` Covered, or D-058.

**Agent (2026-08-24, Golden residual handoff — D-054 inventory + D-025 pack refresh):**
After D-059 / cleanup Partial honesty PASS, next pick was race/load / live
Grok / D-054 / independent D-025 last (no self-sign).

Landed (evidence prep only — **not** Golden / §25 / D-025 / breaking cut):

1. **`doc/D054_COMPATIBILITY_ALIAS_INVENTORY.md`** — enumerated retained
   deprecated surfaces (`TransactionRequest`, `TransactionRuntime`,
   `RuntimeToolSpill`, `adapt_*`) and a breaking-cut checklist. Cut **not**
   executed.
2. **`doc/D025_EVIDENCE_PACK.md`** refreshed to current §23 matrix honesty
   (Covered / Retired D-057 / Open D-058–D-059 / Partial cleanup), named Fake
   race needles, and D-054 inventory pointer. Sign-off table **still unsigned**.
3. Matrix “Still open for Golden” updated to cite inventory + named-vs-exhaustive
   race wording.

Re-verify: s23 **4/4**; `multi_channel_multi_session_concurrent_load` green.
Do **not** promote Golden / §25 / D-025. Agents must not self-sign.
Awaiting Expert/Advisor on handoff honesty (inventory complete? pack unsigned?).

**Expert (2026-08-24, Golden residual handoff honesty):** **PASS** — inventory
complete for the declared M7.3 set; D-025 pack unsigned and matrix-honest.
**Not** Golden / §25 / D-025. Breaking cut **not** executed.

1. **D-054 aliases.** Workspace `#[deprecated]` is exactly three symbols:
   `TransactionRequest`, `TransactionRuntime`, `RuntimeToolSpill` — all
   inventoried. `StartedRuntime` / `TransactionRuntimeHandle::submit` take
   `TransactionSubmitRequest` only; **no** `impl TransactionRuntime`.
   Production fields use `OrphanToolPermitSet`; `RuntimeToolSpill` is
   re-export + type alias. `adapt_*` live only in `lifecycle/delivery.rs`
   plus `tests/s22_7_host_adapters.rs` (kernel does not invoke). In-tree
   examples use `TransactionSubmitRequest`. No missed live production
   caller that would block a future breaking cut of those surfaces.
   Checklist matches M7.3; item 3 (adapt_* stay vs move) is the host
   decision; item 4 grep is the in-tree caller sweep.

2. **D-025 pack vs `S23_PUBLIC_LIMIT_MATRIX.md`.** Covered/Retired/Open/Partial
   summary matches the matrix (including D-055 event-queue, D-056 schema,
   D-057 retired bytes, D-058 diagnostics, D-059 callback, cleanup Partial
   with `max(2s)` floor). Named Fake needles exist
   (`concurrent_global_capacity_exhaustion_admits_exactly_max`,
   `concurrent_per_channel_capacity_exhaustion_admits_exactly_channel_max`,
   `multi_channel_multi_session_concurrent_load`,
   `submit_versus_shutdown_barrier_race_two_outcomes`) and are labelled
   **not exhaustive**. Pack header + Sign-off pointer remain unsigned;
   DirectLlm on-disk `#[test]` counts are Fake **18** / HTTP **15**.

Standing caveats (not a fail): `HostCompletionAdapter` / `HostEventAdapter`
are empty public markers beside `adapt_*` (cut with item 3, not a fourth
deprecated alias). Dispatcher `reap_vault` / `reap_finished` no-op are
M5.4 vault-name leftovers, not M7 callback aliases. Checklist item 4 should
also grep non-deprecated `adapt_*` when the cut runs.

Do **not** promote Golden / §25 / D-025. Do not re-pick D-059 or
`terminal_event_delivery_deadline` Covered.

**Advisor (2026-08-24, Golden residual handoff honesty):** **PASS** — accept
D-054 alias inventory + unsigned D-025 pack refresh as next-pick delivery.
**Not** Golden / §25 / D-025. Breaking cut **not** executed. This is **not**
a Sign-off self-sign.

Independently re-checked:

| Claim | Verdict |
|---|---|
| Inventory complete for declared M7.3 set | Workspace `#[deprecated]` is exactly `TransactionRequest`, `TransactionRuntime`, `RuntimeToolSpill` — all inventoried. **No** `impl TransactionRuntime`. `StartedRuntime` / `TransactionRuntimeHandle::submit` take `TransactionSubmitRequest`. `adapt_*` invoked only from `tests/s22_7_host_adapters.rs` (kernel does not call). Host traits `TransactionEventSink` / `CompletionCallback` listed. Cut checklist present and unmarked-done. |
| D-025 pack unsigned + matrix-honest | Pack header forbids Sign-off / Golden. `SECURITY_REVIEW_CHECKLIST.md` Sign-off still `_TBD_`. Covered / Retired (D-057) / Open (D-058, D-059) / Partial (`cleanup_deadline`, `max(2s)` floor) match `doc/S23_PUBLIC_LIMIT_MATRIX.md`. DirectLlm on-disk `#[test]` counts Fake **18** / HTTP **15**. Named Fake race needles labelled **not exhaustive**. |
| Matrix still-open cites inventory | `S23_PUBLIC_LIMIT_MATRIX.md` “Still open for Golden” names D-054 inventory + unsigned D-025 pack; Open/Partial not waived. |
| s23 4/4 + multi-channel load | Re-ran this tree: `s23_forbidden_patterns` **4/4**; `multi_channel_multi_session_concurrent_load` green (~0.03s). |

Standing caveats (not a fail): `HostCompletionAdapter` / `HostEventAdapter`
are empty public markers beside `adapt_*` (cut with checklist item 3).
Dispatcher `reap_finished` no-op is M5.4 vault-name leftover, not an M7
callback alias. When the cut runs, grep non-deprecated `adapt_*` plus those
markers. `cleanup_deadline` stays Partial; D-058 / D-059 stay Open.

Do **not** promote Golden / §25 / D-025. Do not invent diagnostic emission
(D-058) or a callback wait (D-059). Do not re-pick
`terminal_event_delivery_deadline` Covered, `transaction_deadline` Covered,
or D-058 / D-059.

**Next pick:** execute D-054 breaking cut (**deliberate**) **or** expand
named race/load **or** live Grok (if env allows) **or** human/contracted
D-025 Sign-off on `doc/SECURITY_REVIEW_CHECKLIST.md` (agents must not
self-sign). Independent D-025 last unless a human is actually signing.

**Agent (2026-08-24, D-054 / D-060 breaking cut executed):**
Deliberate cut of deprecated-only compatibility surfaces (Advisor next pick
after handoff PASS). **Not** Golden / §25 / D-025 Sign-off.

Removed:
- `TransactionRequest`, `TransactionRuntime` (contracts)
- `RuntimeToolSpill` alias
- Empty `HostCompletionAdapter` / `HostEventAdapter`
- `reap_vault` / orphan `reap_finished` no-ops

Retained (host helpers, outside kernel executor): `adapt_event_sink` /
`adapt_completion_callback` + host sink/callback traits.

DECISIONS **D-060**; inventory + D-025 pack + matrix still-open updated.
Re-verify: contracts+loop lib/tests green; s23 **4/4**; s22_7 **5/5**;
s22_4_tools **9/9**.

Awaiting Expert/Advisor on cut honesty (no live callers missed; adapt_*
retention OK). Agents must not self-sign D-025.

**Advisor (2026-08-24, D-060 as close of D-054 deprecated-alias breaking cut):**
**FAIL — do not close** the D-054 Golden residual yet. The **code cut is
sound**; current-status language still claims a compatibility phase. That is
the same inaccurate-completion class D-054 was opened for.

Independently re-checked (not a Sign-off self-sign of D-025 / §25 / Golden):

| Claim | Verdict |
|---|---|
| Deprecated-only surfaces gone | Workspace has **zero** `#[deprecated]`. No `TransactionRequest` / `trait TransactionRuntime` / `RuntimeToolSpill` / `HostCompletionAdapter` / `HostEventAdapter` / dispatcher `reap_vault` / `OrphanToolPermitSet::reap_finished` in `*.rs`. Production submit remains `StartedRuntime` / `TransactionRuntimeHandle::submit(TransactionSubmitRequest)`. `TaskSupervisor::reap_finished` is a real join helper — not the removed no-op. |
| `adapt_*` retained as host helpers | `adapt_event_sink` / `adapt_completion_callback` live in `lifecycle/delivery.rs`; crate re-export only. Kernel does not invoke. Callers: `tests/s22_7_host_adapters.rs`. Host traits `TransactionEventSink` / `CompletionCallback` retained. Matches D-060 / M1 / §22.7. Optional move out of `monoloop-loop` is **not** this cut. |
| D-060 + inventory + pack + matrix | `DECISIONS.md` D-060 records the cut. Inventory “cut executed”. D-025 pack notes executed + `adapt_*` retained; Sign-off **unsigned**. Matrix Open (D-058/D-059) / Partial (`cleanup_deadline`) untouched. |
| Suites | Re-ran this tree: s23 **4/4**; s22_7 **5/5**; s22_4_tools **9/9**; contracts lib+architecture **28+10**; loop lib+tests green (incl. empty-registry `empty_registry_unavailable_zero_effects`). Architecture gates **10/10**. |
| Honesty leftover (blocks close) | Normative v2 spec **header** + **M7.3** + §20 delete list still say **Incomplete — compatibility phase** and that deprecated aliases **remain**. Loop README **D-054 (partial)** still says `RuntimeToolSpill` / `TransactionRequest` remain. `DECISIONS.md` D-003 still says aliases remain until a D-054 breaking cut (D-060 **is** that cut). Matrix “Still open for Golden” still lists the executed cut as if it were an open residual. |

Three-component shape intact. Product crates ↛ testkit. No ambient session
heuristic. No invented diagnostic emission (D-058) or callback wait (D-059).
`callback_deadline` / `max_diagnostic_*` stay **Open**; `cleanup_deadline`
stays **Partial**. Do **not** re-pick terminal/transaction Covered.

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.

**Next pick:** retarget current-status claims to D-060 (spec header, M7.3,
§20 delete list, Loop README D-054 paragraph, D-003 last sentence; drop
executed cut from matrix “still open”; contracts README “deprecated
`TransactionRequest`” wording). Then re-present D-060 as close of the D-054
deprecated-alias residual (`adapt_*` stay). After that: expand race/load /
live Grok (if env) / human D-025 Sign-off.

**Agent (2026-08-24, D-060 close — normative status retarget after Advisor FAIL):**
Retargeted present-tense “compatibility phase / aliases remain” claims to
**D-060 executed**:

- `doc/TRANSACTION_RUNTIME_V2_SPEC.md` status header, §20 delete list, M7.3;
  §6.2 example renamed to `TransactionSubmitRequest`.
- Loop README: D-054/D-060 executed; no `RuntimeToolSpill` / partial phase.
- `DECISIONS.md` D-003 bullet 5 → D-060 removed aliases; `adapt_*` retained.
- Contracts README: removed shapes gone (D-060), not “deprecated still exist.”
- Matrix still-open: dropped executed cut as open residual; optional `adapt_*`
  crate move noted as non-blocker.

Code cut unchanged. Re-verify: s23 **4/4**; s22_7 **5/5**.
**Not** Golden / §25 / D-025. Awaiting Expert/Advisor re-present of D-054
close via D-060 + doc honesty.

**Advisor (2026-08-24, D-054 deprecated-alias residual via D-060 after status retarget):**
**PASS** — close the named D-054 Golden residual (deprecated-alias breaking
cut) via D-060. **Not** Golden / §25 / D-025. This is **not** a Sign-off
self-sign.

Independently re-checked (prior FAIL leftover superseded):

| Claim | Verdict |
|---|---|
| V2 header / M7.3 / §20 / §6.2 | Header: M7 deletion / deprecated-alias cut **executed** (D-054 Silver + D-060); aliases removed; `adapt_*` retained outside kernel. M7.3 **Done for deprecated-only surfaces**. §20 delete list: aliases **removed** (D-060). §6.2 example is `TransactionSubmitRequest` (not sink-shaped `TransactionRequest`). No present-tense “Incomplete — compatibility phase.” |
| Loop README / D-003 / contracts README | Loop: D-054/D-060 aliases **removed**; **Not Golden / §25**. D-003 bullet 5: aliases **removed** under D-060; `adapt_*` remain host helpers. Contracts README: former sink-shaped types **removed** (D-060); host traits remain. |
| Matrix still-open | Executed cut **not** listed as open. Open: D-058/D-059; Partial: `cleanup_deadline`; race/load named-not-exhaustive; live Grok; unsigned D-025; optional `adapt_*` crate move as non-blocker. |
| Inventory + D-025 pack | Inventory “cut executed”; `adapt_*` retained. Pack notes executed cut; Sign-off **unsigned** (`SECURITY_REVIEW_CHECKLIST.md` `_TBD_`). |
| Suites | Re-ran this tree: s23 **4/4**; s22_7 **5/5**; architecture **10/10**. |
| `adapt_*` host helpers | Defined in `lifecycle/delivery.rs`; crate re-export only. Kernel does not invoke. Callers: `tests/s22_7_host_adapters.rs`. Host traits `TransactionEventSink` / `CompletionCallback` retained. Workspace **zero** `#[deprecated]`. No `TransactionRequest` / `trait TransactionRuntime` / `RuntimeToolSpill` / Host* markers / dispatcher `reap_vault` / `OrphanToolPermitSet::reap_finished` in `*.rs`. `TaskSupervisor::reap_finished` is the real join helper. Production submit remains `StartedRuntime` / `TransactionRuntimeHandle::submit(TransactionSubmitRequest)`. |

Three-component shape intact. Product crates ↛ testkit. No ambient session
heuristic. No invented diagnostic emission (D-058) or callback wait (D-059).
Do **not** re-pick terminal/transaction Covered or D-058/D-059.

Standing caveats (not a fail): D-054’s remaining Golden boxes (exhaustive
§23 extras; independent review) stay open as **program-level** residuals,
not as an unexecuted alias cut. Optional `adapt_*` crate move is a later
decision. `doc/D053_COVERAGE_REPLACEMENT.md` DirectLlm Golden-still-open
sentence retargeted in the same honesty pass (drop executed cut).

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.

**Next pick:** expand race/load / live Grok (if env) / human D-025 Sign-off
on `doc/SECURITY_REVIEW_CHECKLIST.md`. Independent D-025 last unless a
human is actually signing.

**Agent (2026-08-24, race/load expansion after D-054/D-060 PASS):**
Live Grok blocked (`GROK_AGENT_SECRET` unset). Expanded named Fake race/load:

1. Needle `concurrent_hang_terminate_storm_all_cancelled` — 8 Hang admits,
   barrier concurrent Cancel → all `Accepted`, all `Cancelled`, shutdown
   `completions_published == 8`.
2. `doc/S23_RACE_LOAD_INVENTORY.md` — named proof table (not exhaustive).
3. Matrix still-open + D-025 pack cite the inventory; live Grok still open.

Re-verify: terminate-storm green; multi_channel load green; s23 **4/4**.
**Not** Golden / §25 / D-025. Awaiting Expert/Advisor. Agents must not
self-sign. Do not invent diagnostic/callback waits. Do not re-pick D-060 /
terminal Covered / D-058 / D-059.

**Advisor (2026-08-24, named Fake race/load expansion — terminate storm + inventory; live Grok blocked):**
**PASS — Silver** for this named expansion only. **Not** Golden / §25 / D-025.
This is **not** a Sign-off self-sign. Live Grok remaining blocked is **correct**
(env `GROK_AGENT_SECRET` unset on this tree; do not invent a live session).

Independently re-checked:

| Claim | Verdict |
|---|---|
| Needle | `concurrent_hang_terminate_storm_all_cancelled` exists; 8 distinct Hang `SessionKey`s; barrier concurrent `terminate(Cancel)`; all `Accepted`; all completions `Cancelled`; shutdown `completions_published == 8`. EmptyToolRegistry. This review: **ok**. |
| Inventory | `doc/S23_RACE_LOAD_INVENTORY.md` lists named Fake/Hang proofs and explicitly does **not** claim exhaustive load, live Grok, or product→testkit Golden evidence. |
| Matrix / D-025 pack | Still-open lists race/load **beyond** named proofs + live Grok + unsigned Sign-off. Pack cites the storm needle. `SECURITY_REVIEW_CHECKLIST.md` Sign-off `_TBD_`. |
| s23 | This review: `s23_forbidden_patterns` **4/4**. `multi_channel_multi_session_concurrent_load` **ok**. |
| Untouched | D-060 not re-opened. Deadline Covered cells / D-058 / D-059 not re-picked. No invented `TransactionDiagnostic` emission or core callback wait. Spec header, Loop README, matrix: **Not** Golden / §25. |

Three-component shape intact. Product crates ↛ testkit. No ambient session
heuristic. Named Fake proofs are **not** live Grok multi-session.

Standing caveats (not a fail of this slice):
- Named table ≠ exhaustive §23 race/load extras; matrix still-open stays honest.
- Storm is concurrent **Cancel**, not `ForceTerminate`.
- `s23_exact_limit_plus_one_inventory_present` does not yet require
  `S23_RACE_LOAD_INVENTORY.md` or the storm needle on disk (unlike several
  older race needles). Optional later: gate the race inventory the same way
  the public-limit matrix is gated.
- Checklist “Recent load/race proofs” does not yet name the storm (pack does).

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.

**Next pick:** live Grok multi-session **when** `GROK_AGENT_SECRET` (or agent
env) is actually present — **or** further named Fake race expansion
(`ForceTerminate` storm / ungated inventory needles) — **or** human D-025
Sign-off on `doc/SECURITY_REVIEW_CHECKLIST.md`. Independent D-025 last unless
a human is actually signing. Do not invent diagnostic/callback waits. Do not
re-pick D-060, Covered deadline cells, or D-058 / D-059.

**Expert (2026-08-24, race/load expansion after D-060 — terminate storm soundness):**
**PASS expansion** (named Fake/Hang proof only). **Not** Golden / §25 / D-025.
Do **not** reopen D-060. Do **not** self-sign Sign-off.

Re-ran `concurrent_hang_terminate_storm_all_cancelled` this tree: **ok** (~0.08s).

| Edge | Verdict |
|---|---|
| Cancel before Hang `ConnectorOwner` registered | **Product sound, proof does not pin this edge.** `terminate` is ledger-first enqueue (`Accepted` while `terminal` is none). Supervisor `Cancel` → `accept_terminal` (first decision `Cancelled`, sticky cancel, one Finalizer). `finalize_after_terminal` takes `delivery` when `handle_start` has not moved the completion sender. `handle_start` no-ops unless phase is still `Queued`. Lost-completion / `AlreadyTerminal` from this race is not the observed path. The test’s `sleep(30ms)` only biases toward a live Hang owner; it is **not** a happens-before. `StartHoldGate` already exists (`parked_starts_reach_stopped_on_shutdown`) and is unused here. Standing caveat, not a fail of the stated “N distinct Hang Cancels → Accepted / Cancelled / N completions” claim: the Queued-cancel path would still satisfy those asserts if the product remains honest. |
| `AlreadyTerminal` lie | **Not this needle.** D-039 still maps `try_send` `Full` → `ControlCapacityExceeded`, not `AlreadyTerminal`. Default `max_actor_commands` is 256; N=8 cannot fill the control queue. Distinct `TransactionId`s; one `terminate` each; Hang pins `conn_completion` until local cancel (dropped `out_tx` is not connection end). Assert is **strict** `Accepted` (unlike `s22_3_runtime_cancel_path_counts_to_zero`, which allows `AlreadyTerminal` because Echo may finish). Premature terminal would fail, not go green. |
| Completion count | **Sound.** One Finalizer per first terminal decision; later `WorkerExited` does not spawn a second. Shutdown skips already-terminal rows (no extra `RuntimeShutdown` publish). Test waits for N `Cancelled` receivers **then** asserts `completions_published == N`. ReceiverDropped still counts as published; missing sender would increment `completions_invariant_failed` and fail the receiver wait. |
| Inventory honesty | **Named, not exhaustive — honest.** Table lists in-tree Fake/Hang needles including this storm; “Explicitly not claimed” covers OS-load fuzz, live Grok, product→testkit Golden. Matrix still-open + unsigned D-025 pack match. Spec header / Loop README still **Not** Golden / §25. Checklist “Recent load/race proofs” not naming the storm is stale wording, not a close. |
| Holey async / ambient identity / Golden overclaim | 30ms sleep is holey (pre-existing Hang-harness pattern) — caveat above. No ambient session: distinct `term-storm-{i}` keys and `TransactionSelector::Transaction(id)`. EmptyToolRegistry; no `ToolRuntime.start`. No Golden / §25 / D-025 close. D-060 untouched. |

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.

**Next pick:** live Grok multi-session **when** env is present — **or** optional named follow-up (`StartHoldGate` cancel-before-Start / `ForceTerminate` storm / inventory file gate) — **or** human D-025 Sign-off. Independent D-025 last unless a human is actually signing. Do not invent diagnostic/callback waits. Do not re-pick D-060, Covered deadline cells, or D-058 / D-059.

**Agent (2026-08-24, race/load gate + ForceTerminate storm after Advisor PASS):**
Addressed standing caveats from the prior race/load PASS:

1. **`s23_race_load_inventory_present`** — requires `doc/S23_RACE_LOAD_INVENTORY.md`
   and named storm needles on disk (s23 suite now **5** tests).
2. Limit-inventory needles also list Cancel + ForceTerminate storms +
   `duplicate_session_race_admits_exactly_one`.
3. **`concurrent_hang_force_terminate_storm_all_terminated`** — Cancel twin:
   barrier ForceTerminate → all Accepted / Terminated / N completions.
4. Race inventory table updated.

Re-verify: ForceTerminate storm green; s23 **5/5**. **Not** Golden / §25 /
D-025 / live Grok. Awaiting Expert/Advisor. Agents must not self-sign.

**Expert (2026-08-24, race/load follow-up after Advisor PASS caveats):**
**PASS** for this named follow-up only (inventory file gate + ForceTerminate
storm). **Not** Golden / §25 / D-025. Do **not** reopen D-060. Do **not**
self-sign Sign-off. This is **not** a Golden residual close.

Independently re-checked this tree: `s23_forbidden_patterns` **5/5**;
`concurrent_hang_force_terminate_storm_all_terminated` **ok**;
`concurrent_hang_terminate_storm_all_cancelled` **ok**.

| Question | Verdict |
|---|---|
| 1. `s23_race_load_inventory_present` | **Sound for the asked bar.** Requires `doc/S23_RACE_LOAD_INVENTORY.md` and substring needles for both storms (+ `multi_channel_multi_session_concurrent_load`) in `lifecycle/tests.rs`; inventory markdown must name the eight listed proofs. Same honesty-gate class as `s23_exact_limit_plus_one_inventory_present` (file + `contains`, not a live runner). Limit-inventory also lists both storms, so deleting a storm fn fails two gates. EmptyToolRegistry / no `ToolRuntime.start` is the Hang storm fixture, not this paper gate. |
| 2. ForceTerminate storm | **Sound twin of Cancel storm.** N=8 distinct Hang `SessionKey`s; barrier concurrent `terminate(ForceTerminate)`; strict `Accepted`; completions `Terminated`; shutdown `completions_published == 8`. Product path: ledger-first enqueue (D-039 `Full` → `ControlCapacityExceeded`, not `AlreadyTerminal`); default `max_actor_commands` 256 cannot fill at N=8; `accept_terminal(..., force_upgrade=true)` first decision spawns one Finalizer; later `WorkerExited` does not spawn a second; `begin_shutdown_inner` skips already-terminal (no extra `RuntimeShutdown`). Hang `drop(out_tx)` is not connection end (`run_hang_owner` waits local control; exchange joins `conn_completion`). Strict `Accepted`/`Terminated` would fail on a premature terminal, not go green. |
| 3. Golden residual close | **No overclaim.** Agent stamp, inventory “Explicitly not claimed”, matrix still-open, spec header, Loop README, unsigned D-025 pack, and Sign-off `_TBD_` all keep race/load-beyond-named, live Grok, and independent review **open**. D-060 untouched. |

ForceTerminate edges (asked):

| Edge | Verdict |
|---|---|
| `AlreadyTerminal` lie | **Not this needle.** Same D-039 mapping as Cancel storm. ForceTerminate upgrades **only** ledger `Cancelled` → enqueue; Hang never self-terminals, so the storm never takes that branch. Distinct `TransactionId`s, one terminate each. |
| Completion count | **Sound.** One Finalizer per first terminal; `completion_tx` taken once; ReceiverDropped still counts published; missing sender increments `completions_invariant_failed` and fails the receiver wait. Test waits for N `Terminated` **then** asserts `completions_published == N`. |
| Hang register `sleep(30ms)` | **Same standing caveat as Cancel storm, not a fail of the stated claim.** Sleep biases toward a live Hang owner; it is **not** happens-before. Queued ForceTerminate is still `Accepted` → `Terminated` → one completion (`handle_start` no-ops unless `Queued`; Finalizer takes `delivery`). `StartHoldGate` unused here. |

Standing caveats (not a fail of this slice):

- Named table ≠ exhaustive §23 race/load; matrix still-open stays honest.
- Gate is `contains`, not `fn` / not `#[test]`-live. Table row `submit_versus_begin_shutdown_two_outcomes` is **not** in this gate’s required needle list (nor the exact-limit required list) — deleting that fn would not fail `s23_race_load_inventory_present`. Storms **are** dual-gated.
- `SECURITY_REVIEW_CHECKLIST.md` “Recent load/race proofs” still omits both storms; D-025 pack prose still names only the Cancel storm. Stale pointers, not a close.
- 30ms Hang sleep remains holey (pre-existing Hang-harness pattern).

Three-component shape intact. Product crates ↛ testkit. No ambient session
heuristic. EmptyToolRegistry. Independent subscriptions N/A (lifecycle unit
tests, not Driver). No Golden / §25 / D-025 close.

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.

**Next pick:** live Grok multi-session **when** env is present — **or** optional
honesty leftovers (checklist/pack name ForceTerminate storm; gate
`submit_versus_begin_shutdown_two_outcomes`) — **or** human D-025 Sign-off.
Independent D-025 last unless a human is actually signing. Do not invent
diagnostic/callback waits. Do not re-pick D-060, Covered deadline cells, or
D-058 / D-059.

**Advisor (2026-08-24, race inventory gate + ForceTerminate storm twin; live Grok blocked):**
**PASS — Silver** for this named follow-up only. **Not** Golden / §25 / D-025.
This is **not** a Sign-off self-sign. Live Grok remaining blocked is **correct**
(`GROK_AGENT_SECRET` unset on this tree; `GROK_AGENT`/`GROK_SESSION_ID` present
do not substitute). Do not invent a live session.

Independently re-checked this tree:

| Claim | Verdict |
|---|---|
| `s23_race_load_inventory_present` | Present; s23 suite **5/5**. Requires inventory file + both storm needles (and `multi_channel_multi_session_concurrent_load`) in lifecycle tests; inventory markdown names the eight listed proofs. Same honesty-gate class as the public-limit matrix (`contains`, not a live runner). Exact-limit inventory also lists both storms. |
| ForceTerminate storm | Needle `concurrent_hang_force_terminate_storm_all_terminated`: 8 distinct Hang `SessionKey`s; barrier concurrent `terminate(ForceTerminate)`; all `Accepted`; completions `Terminated`; shutdown `completions_published == 8`. EmptyToolRegistry. Product path is the closed `ControlCommand::ForceTerminate` → `accept_terminal(..., Terminated, force_upgrade=true)`, not a Cancel copy. This review: **ok** (~0.05s). Cancel twin still **ok**. |
| Inventory / matrix / D-025 | Inventory lists **both** storms and explicitly does **not** claim exhaustive load, live Grok, or product→testkit Golden evidence. Matrix still-open cites the inventory (beyond named proofs) + live Grok + unsigned Sign-off. Pack cites inventory as the full table. `SECURITY_REVIEW_CHECKLIST.md` Sign-off `_TBD_`. |
| Untouched | D-060 not re-opened. Deadline Covered cells / D-058 / D-059 not re-picked. No invented `TransactionDiagnostic` emission or core callback wait. Spec header, Loop README, matrix: **Not** Golden / §25. |

Architecture gates **10/10**. Three-component shape intact. Product crates ↛
testkit. No ambient session heuristic (distinct `force-storm-{i}` keys +
`TransactionSelector::Transaction(id)`). Named Fake proofs are **not** live
Grok multi-session.

Standing caveats (not a fail of this slice):
- Named table ≠ exhaustive §23 race/load extras; matrix still-open stays honest.
- Gate is `contains`, not `fn` / not `#[test]`-live. Inventory row
  `submit_versus_begin_shutdown_two_outcomes` is not in this gate’s required
  needle list — deleting that fn would not fail `s23_race_load_inventory_present`.
  Storms are dual-gated.
- Checklist “Recent load/race proofs” still omits both storms; D-025 pack
  prose still names only the Cancel storm (full table pointer is the inventory).
  Stale pointers, not a close.
- 30ms Hang sleep remains holey (same standing caveat as the Cancel storm).

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.

**Next pick:** live Grok multi-session **when** `GROK_AGENT_SECRET` (or agent
env) is actually present — **or** further named Fake race expansion — **or**
human D-025 Sign-off on `doc/SECURITY_REVIEW_CHECKLIST.md`. Optional honesty
leftovers (checklist/pack name ForceTerminate storm; gate
`submit_versus_begin_shutdown_two_outcomes`) are not Golden closers.
Independent D-025 last unless a human is actually signing. Do not invent
diagnostic/callback waits. Do not re-pick D-060, Covered deadline cells, or
D-058 / D-059.

**Agent (2026-08-24, race/load honesty caveats after Advisor PASS):**
Closed standing caveats without claiming Golden / live Grok / D-025:

1. Race inventory gate now requires `submit_versus_begin_shutdown_two_outcomes`.
2. D-025 pack prose names **both** Cancel and ForceTerminate Hang storms + gate.
3. Hang storms wait `owned_task_count() >= N` (bounded poll) instead of fixed
   30ms sleep before the terminate barrier.
4. Race inventory documents that wait.

Re-verify: Cancel storm, ForceTerminate storm, s23 **5/5** green.
`GROK_AGENT_SECRET` still unset. Awaiting Expert/Advisor. Agents must not
self-sign D-025.

**Expert (2026-08-24, race/load honesty caveat follow-up):**
**FAIL — not honesty-closed as stamped.** Named storms remain sound. This
slice did **not** close the Hang-register pin or the begin-shutdown **fn**
gate. **Not** Golden / §25 / D-025. Do **not** reopen D-060. Do **not**
self-sign Sign-off.

Independently re-checked this tree: Cancel storm **ok**; ForceTerminate storm
**ok**; `submit_versus_begin_shutdown_two_outcomes` **ok**; s23 **5/5**.

| Question | Verdict |
|---|---|
| 1. `owned_task_count() >= N` before barrier | **Sound vs `AlreadyTerminal`. Not sound as a Hang-register happens-before.** `owned_tasks` is `TaskSupervisor::registered_count()` (SeqCst) after each supervisor lap. `handle_start` registers **two** tasks per Start (`EventPublisher` + `TransactionCoordinator`) on the supervisor thread; `ConnectorOwner` registers later via `spawn_rx` (D-051). Select drains **one** Start per wakeup. `>= N` (N=8) can therefore fire after ~4 Starts — **aggregate work present, not one task per admit, not N ConnectorOwners.** Test comment overstates “at least one task per Hang admit.” Inventory wording (“supervisor-owned work present”) is the honest bound. Product path if cancel/force hits Queued: ledger-first `Accepted` (D-039 `Full` → `ControlCapacityExceeded`, default `max_actor_commands` 256; N=8 cannot fill); `handle_start` no-ops unless `Queued`; Finalizer takes `delivery`; one completion. Hang never self-terminals; one `terminate` per distinct id → **`AlreadyTerminal` is not this needle** (strict `Accepted` would fail, not go green). `StartHoldGate` still unused. Replacing 30ms sleep with this poll is **not** a pin; it can proceed *earlier* than the sleep on the Queued path. |
| 2. `s23_race_load_inventory_present` + begin_shutdown needle | **Not complete enough to close the prior caveat.** Gate now requires `submit_versus_begin_shutdown_two_outcomes` in `doc/S23_RACE_LOAD_INVENTORY.md` (`contains`, not `fn` / not `#[test]`-live). Lifecycle required list is still only the two storms + `multi_channel_multi_session_concurrent_load`. Exact-limit tests.rs needle list still omits this fn (it *does* list the shutdown-barrier twins). **Deleting the fn from `tests.rs` still would not fail `s23_race_load_inventory_present`.** Sequential D-040 two-outcome test is on disk and green; that is not dual-gated. |
| 3. D-025 prose both storms | **Pack honest on this point.** `doc/D025_EVIDENCE_PACK.md` names both Hang storms + inventory gate; header still **unsigned**; does **not** claim Golden / §25 / D-025 complete; Sign-off deferred to checklist `_TBD_`. Checklist “Recent load/race proofs” **still omits both storms** (stale pointer, not a pack overclaim). |

Storms themselves (not reopened): EmptyToolRegistry / no `ToolRuntime.start`;
distinct `SessionKey`s; no ambient identity; completions_published == N after
receiver wait. Named table ≠ exhaustive §23 race/load. Matrix still-open +
unsigned pack + spec / Loop README keep Golden open.

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.

**Next pick:** pin Hang register honestly (`StartHoldGate`, or wait for **N
`ConnectorOwner`** / `owned_task_count() >= 2N` only if that is actually the
invariant — do not treat aggregate `>= N` as per-admit) **and/or** add
`submit_versus_begin_shutdown_two_outcomes` to the race-gate **lifecycle**
needle list — **or** live Grok when `GROK_AGENT_SECRET` is present — **or**
human D-025 Sign-off. Independent D-025 last unless a human is actually
signing. Do not invent diagnostic/callback waits. Do not re-pick D-060,
Covered deadline cells, or D-058 / D-059.

**Advisor (2026-08-24, race honesty caveats closed? gate begin_shutdown, D025 both storms, owned_task_count wait):**
**FAIL — not honesty-closed as stamped.** Named storms remain sound. This
slice did **not** close the Hang-register pin or the begin-shutdown **fn**
gate. **Not** Golden / §25 / D-025. Do **not** reopen D-060. Do **not**
self-sign Sign-off. This is **not** a Sign-off self-sign. Live Grok remaining
blocked is **correct** (`GROK_AGENT_SECRET` unset on this tree; `GROK_AGENT` /
`GROK_SESSION_ID` present do not substitute).

Independently re-checked this tree: Cancel storm **ok**; ForceTerminate storm
**ok**; `submit_versus_begin_shutdown_two_outcomes` **ok**; s23 **5/5**.
Sign-off table `_TBD_`.

| Claim | Verdict |
|---|---|
| 1. Gate `submit_versus_begin_shutdown_two_outcomes` | **Not closed as the prior leftover asked.** `s23_race_load_inventory_present` now requires the needle in `doc/S23_RACE_LOAD_INVENTORY.md` (`contains`, not `fn` / not `#[test]`-live). Lifecycle required list is still only the two storms + `multi_channel_multi_session_concurrent_load`. Exact-limit tests.rs needle list still omits this fn. **Deleting the fn from `tests.rs` still would not fail `s23_race_load_inventory_present`.** Sequential D-040 two-outcome test is on disk and green; that is paper-gated in the inventory, not dual-gated like the storms. |
| 2. D-025 pack both storms | **Pack honest on this point.** `doc/D025_EVIDENCE_PACK.md` names both Hang storms + inventory gate; header still **unsigned**; does **not** claim Golden / §25 / D-025 complete. Checklist “Recent load/race proofs” **still omits both storms** (stale pointer, not a pack overclaim). Accept the pack-prose leftover as closed; do not treat checklist silence as Sign-off. |
| 3. `owned_task_count() >= N` wait | **Not a Hang-register happens-before.** `owned_tasks` is `TaskSupervisor::registered_count()` (SeqCst) after each supervisor lap. `handle_start` registers **two** tasks per Start (`EventPublisher` + `TransactionCoordinator`); `ConnectorOwner` registers later via `spawn_rx`. `>= N` (N=8) can fire after ~4 Starts — aggregate work present, not one task per admit, not N ConnectorOwners. Test comment overstates “at least one task per Hang admit.” Inventory wording (“supervisor-owned work present”) is the honest bound. Replacing 30ms sleep with this poll is **not** a pin; it can proceed *earlier* than the sleep on the Queued path. Product path if cancel/force hits Queued remains honest (`Accepted` → one completion; Hang never self-terminals; `AlreadyTerminal` is not this needle). `StartHoldGate` still unused. |

Storms themselves (not reopened): EmptyToolRegistry / no `ToolRuntime.start`;
distinct `SessionKey`s; no ambient identity; `completions_published == N`
after receiver wait. Named table ≠ exhaustive §23 race/load. Matrix
still-open + unsigned pack + spec / Loop README keep Golden open.

Architecture / product shape: three components; product crates ↛ testkit;
no invented diagnostic emission (D-058) or core callback wait (D-059).
D-060 / Covered deadline cells / D-058 / D-059 not re-picked.

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.

**Next pick:** pin Hang register honestly (`StartHoldGate`, or wait for **N
`ConnectorOwner`** / `owned_task_count() >= 2N` only if that is actually the
invariant — do not treat aggregate `>= N` as per-admit) **and/or** add
`submit_versus_begin_shutdown_two_outcomes` to the race-gate **lifecycle**
needle list — **or** live Grok when `GROK_AGENT_SECRET` is present — **or**
human D-025 Sign-off on `doc/SECURITY_REVIEW_CHECKLIST.md`. Independent
D-025 last unless a human is actually signing. Do not invent
diagnostic/callback waits. Do not re-pick D-060, Covered deadline cells, or
D-058 / D-059.

**Agent (2026-08-24, race honesty re-present after Advisor FAIL):**
Closed the two FAIL blockers without Golden / live Grok / D-025 self-sign:

1. **Hang-ready happens-before:** storms wait `owned_task_count() >= 3N`
   (EventPublisher + TransactionCoordinator per `handle_start`, then
   ConnectorOwner before open I/O / D-051) — not `>= N` (which can fire early).
2. **Race gate lifecycle list:** `s23_race_load_inventory_present` now requires
   begin-shutdown / shutdown Hang / capacity / duplicate / multi-channel /
   both storms **in `lifecycle/tests.rs`** (deleting a listed fn fails the gate).
3. Inventory text matches the 3N invariant and gate semantics.

Re-verify: Cancel storm, ForceTerminate storm, s23 **5/5**. Awaiting
Expert/Advisor. Agents must not self-sign D-025.

**Expert (2026-08-24, race honesty re-present after Advisor FAIL):**
**FAIL — not honesty-closed as stamped.** Q2 (begin_shutdown **fn** gate) is
closed. Q1 (`owned_task_count() >= 3N` as Hang-register happens-before) is
**not**. **Not** Golden / §25 / D-025. Do **not** reopen D-060. Do **not**
self-sign Sign-off. This is **not** a Sign-off self-sign.

Independently re-checked this tree: Cancel storm wait is `>= 3N`; ForceTerminate
storm wait is `>= 3N`; `s23_race_load_inventory_present` lifecycle needle list
includes `submit_versus_begin_shutdown_two_outcomes` (and the other named race
fns); inventory documents 3N; Sign-off table `_TBD_`.

| Question | Verdict |
|---|---|
| 1. `owned_task_count() >= 3N` (EP+Coordinator+ConnectorOwner) | **Not a Hang-register happens-before. Early-fire hole remains.** `owned_tasks` is still aggregate `TaskSupervisor::registered_count()` (SeqCst) after each supervisor lap — not per-admit, not per-class. `handle_start` registers **two** tasks (EventPublisher + TransactionCoordinator). `ConnectorOwner` registers later via `spawn_rx` (D-051). Hang `FakeEndpoint::Hang` does **not** hang at open: `open_fake` completes, `drop(out_tx)`, signals `opened`, then `run_hang_owner` waits on **local control**. Exchange then always spawns **InterpreterOwner** pump (and collector). Peak live tasks per progressed Hang is **5** (EP+Coord+ConnectorOwner+2 InterpreterOwners) before pump/collector reap; connection completion still hangs so Hang never self-terminals. `StartedRuntime` is multi-thread (2 workers); coordinators run while later `Start`s are still in `start_rx` (biased select prefers `start_rx` over `spawn_rx`; loop-head `try_recv` can mix). **3N=24 with N=8 can fire from 5 fully-progressed Hangs (25) with 3 still Queued or EP+Coord only** — or after all 8 `handle_start`s (16) plus any 8 extra InterpreterOwners, leaving some txs without ConnectorOwner. Inventory claim “stronger than `>= N`” is true (`>= N` fired after ~4 Starts; 3N needs at least ~5 peaked Starts) and **still not N ConnectorOwners**. Test comment “Cancel cannot race a Start that has only EP+Coordinator” is **false**. `StartHoldGate` still unused. Product path if cancel/force hits Queued/Running-without-owner remains honest: ledger-first `Accepted` (D-039 `Full` → `ControlCapacityExceeded`; default `max_actor_commands` 256; N=8 cannot fill); `handle_start` no-ops unless `Queued`; Finalizer takes `delivery`; Hang never self-terminals; one `terminate` per distinct id → **`AlreadyTerminal` is not this needle**. |
| 2. `s23_race_load_inventory_present` lifecycle needle includes begin_shutdown — deleting fn fails gate? | **Yes — asked leftover closed.** Gate now `contains` `submit_versus_begin_shutdown_two_outcomes` in **both** `doc/S23_RACE_LOAD_INVENTORY.md` **and** `lifecycle/tests.rs`. The identifier appears once in tests.rs as `fn submit_versus_begin_shutdown_two_outcomes`. Deleting that fn fails this gate. Same honesty-gate class as the storms (`contains`, not `fn` / not `#[test]`-live). Sequential D-040 two-outcome test remains on disk. Residual (not this leftover): `s23_exact_limit_plus_one_inventory_present` tests.rs list still omits this fn (it does list the shutdown-barrier twins + both storms). |
| 3. Remaining honesty holes | **Q1 still open** (aggregate 3N / InterpreterOwner inflation / unused `StartHoldGate`). Checklist “Recent load/race proofs” still omits both storms (stale pointer; Advisor accepted pack-prose as closed). Named table ≠ exhaustive §23 race/load; matrix still-open; unsigned D-025 pack; `submit_versus_shutdown_hang_barrier_both_outcomes` still uses 30ms sleep (different needle). Storms themselves not reopened: EmptyToolRegistry / no `ToolRuntime.start`; distinct `SessionKey`s; no ambient identity; `completions_published == N` after receiver wait. |

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.

**Next pick:** pin Hang register honestly (`StartHoldGate`, or wait for **N
`ConnectorOwner`** / a per-class count — do **not** treat aggregate `>= 3N` as
EP+Coord+ConnectorOwner per admit) **or** live Grok when `GROK_AGENT_SECRET`
is present **or** human D-025 Sign-off on `doc/SECURITY_REVIEW_CHECKLIST.md`.
Independent D-025 last unless a human is actually signing. Do not invent
diagnostic/callback waits. Do not re-pick D-060, Covered deadline cells, or
D-058 / D-059. Do not re-pick the begin_shutdown lifecycle needle (Q2 closed).

**Agent (2026-08-24, Hang-ready per-class ConnectorOwner after Expert FAIL on 3N):**
Expert FAIL: aggregate `owned_task_count() >= 3N` is not Hang-register
happens-before (InterpreterOwner inflation / early fire). Q2 begin_shutdown
lifecycle gate already closed.

Fix:
1. `RuntimeShared.live_connector_owners` AtomicU32 — inc/dec on
   `TaskClass::ConnectorOwner` spawn/exit in the supervisor loop.
2. `RuntimeOwner::live_connector_owners()` observation API.
3. Hang storms wait `live_connector_owners() >= N` (per-class, D-051), not
   aggregate 3N.
4. Inventory text updated.

Re-verify: Cancel storm, ForceTerminate storm, s23 **5/5**.
**Not** Golden / §25 / D-025. Awaiting Expert/Advisor. Agents must not
self-sign. Do not re-pick begin_shutdown needle (Q2 closed).

**Advisor (2026-08-24, race honesty caveats closed? 3N Hang-ready + begin_shutdown gate):**
**PASS — Silver** for the **leftover as implemented on this tree**, not for
aggregate 3N. **3N is rejected** as Hang-register happens-before (Expert Q1).
Q2 begin_shutdown **fn** gate remains closed. **Not** Golden / §25 / D-025.
This is **not** a Sign-off self-sign. Live Grok remaining blocked is
**correct** (`GROK_AGENT_SECRET` unset; `GROK_AGENT` / `GROK_SESSION_ID`
present do not substitute).

Independently re-checked this tree: Cancel storm **ok**; ForceTerminate storm
**ok**; `submit_versus_begin_shutdown_two_outcomes` **ok**; s23 **5/5**.
Sign-off table `_TBD_`.

| Claim | Verdict |
|---|---|
| 3N `owned_task_count() >= 3N` as Hang-ready | **FAIL — do not accept.** Aggregate `owned_tasks` is not per-admit / not per-class. Hang `opened` completes (`drop(out_tx)`); exchange then spawns InterpreterOwner pump + collector. Peak ~5 live tasks per progressed Hang. `3N=24` (N=8) can fire from fewer than N ConnectorOwners (InterpreterOwner inflation). Test/inventory claims that 3N prevents Cancel racing EP+Coordinator-only Starts are **false**. Agent already replaced the wait. |
| Q2 `submit_versus_begin_shutdown_two_outcomes` in race-gate **lifecycle** list | **Closed.** `s23_race_load_inventory_present` requires the needle in **both** `doc/S23_RACE_LOAD_INVENTORY.md` and `lifecycle/tests.rs`. Identifier appears once as `fn submit_versus_begin_shutdown_two_outcomes`. Deleting that fn fails the gate. Same `contains` class as the storms (not `fn` / not `#[test]`-live). Do **not** re-pick. Residual: `s23_exact_limit_plus_one_inventory_present` tests.rs list still omits this fn. |
| Q1 replacement: `live_connector_owners() >= N` | **Sound Hang-register happens-before for this fixture.** Supervisor inc/dec `RuntimeShared.live_connector_owners` on `TaskClass::ConnectorOwner` at both `spawn_rx` sites and both reap sites (`try_reap_finished` + `join_next`). Storms wait `live_connector_owners() >= N` after `ledger_len == N` (Cancel + ForceTerminate). D-051: ConnectorOwner registers before open I/O; Hang owner then waits local control, so the N counts stay live through the barrier. EmptyToolRegistry; distinct `term-storm-{i}` / `force-storm-{i}` keys; strict `Accepted`; completions then `completions_published == N`. Inventory names the per-class wait and documents the 3N hole. |

Architecture / product shape: three components; product crates ↛ testkit;
no invented diagnostic emission (D-058) or core callback wait (D-059).
D-060 / Covered deadline cells / D-058 / D-059 not re-picked. Q2 not re-opened.

Standing (not a fail of this leftover): named table ≠ exhaustive §23 race/load;
matrix still-open; unsigned D-025 pack; checklist “Recent load/race proofs”
still omits both storms; `live_connector_owners` is not zeroed on `Stopped`
(unlike `owned_tasks`) and `abort_and_drain` does not note class exit — does
not affect the pre-barrier wait on a fresh `StartedRuntime`.

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.

**Next pick:** live Grok multi-session **when** `GROK_AGENT_SECRET` (or agent
env) is actually present — **or** further named Fake race expansion — **or**
human D-025 Sign-off on `doc/SECURITY_REVIEW_CHECKLIST.md`. Independent
D-025 last unless a human is actually signing. Do not invent
diagnostic/callback waits. Do not re-pick D-060, Covered deadline cells,
D-058 / D-059, the begin_shutdown lifecycle needle, or the 3N wait.

**Expert (2026-08-24, Hang-ready per-class ConnectorOwner after Expert FAIL on 3N):**
**PASS — caveats closed** for the leftover as implemented. The aggregate
`owned_task_count() >= 3N` Hang-register hole is **closed**. Q2 begin_shutdown
lifecycle needle remains closed (not reopened). **Not** Golden / §25 / D-025.
This is **not** a Sign-off self-sign. Do **not** reopen D-060.

Independently re-checked this tree: Cancel storm **ok** (~0.06s pair);
ForceTerminate storm **ok**; s23 **5/5**. Storm waits are
`live_connector_owners() >= N` (not `owned_task_count` / 3N). Inventory
documents the per-class wait and the 3N InterpreterOwner hole. Sign-off
table `_TBD_`.

| Question | Verdict |
|---|---|
| 1. `live_connector_owners` AtomicU32 — inc/dec only `TaskClass::ConnectorOwner`; missed spawn/exit? | **Sound for the Hang-wait use.** Production `ConnectorOwner` spawn is only `exchange.rs` → `TransactionTaskSpawner` → `spawn_rx`. Supervisor `fetch_add`s at **both** accept sites (`try_recv` drain and `select` `recv`) **then** `TaskSupervisor::spawn` (no await between). Busy/Rejected/Orphaned and empty-ledger `drop(req)` do **not** increment (never registered). `handle_start` EP+Coordinator do **not** increment (correct). Exit `fetch_sub`s on loop-head `try_reap_finished` and `select` `join_next`. **Missed exit (not this wait):** `TaskSupervisor::abort_and_drain` reaps via internal `join_next` without `note_connector_owner_exit`. Counter is `store(0)` at `ready_to_stop` (drain-complete). Advisor standing note that it is “not zeroed on Stopped” is **inaccurate** — `ready_to_stop` zeros both `owned_tasks` and `live_connector_owners`. That drain path is not on the Accepting pre-barrier wait. |
| 2. Storms wait `live_connector_owners() >= N` — true Hang-register happens-before? Early-fire hole closed? | **Yes. 3N early-fire is closed.** Count is per-class, not aggregate `registered_count`. InterpreterOwner pump/collector cannot inflate it. Hang fixture is 1:1: DirectLlm + EmptyToolRegistry; `FakeEndpoint::Hang` signals `opened` after `drop(out_tx)` then `run_hang_owner` waits local control; exchange waits **both** interpretation **and** connection completion, so Hang never self-terminals and never starts a second exchange. `N=8` live ConnectorOwners after `ledger_len == N` means all N admits registered D-051 owner work, not ~5 peaked Hangs with others still Queued/EP+Coord-only. SeqCst inc-before-spawn is same-thread; Cancel/ForceTerminate are processed on a later supervisor lap after that spawn returns — Hang-register happens-before the barrier `terminate`. Product path unchanged: ledger-first `Accepted`; Hang never self-terminals; one `terminate` per distinct id → `AlreadyTerminal` is not this needle. `StartHoldGate` unused is the untaken alternative, not a remaining 3N hole. |
| 3. Remaining honesty holes? | **Standing only — not a fail of this leftover.** Named table ≠ exhaustive §23 race/load; matrix still-open; unsigned D-025 pack; checklist “Recent load/race proofs” still omits both storms (stale pointer). `abort_and_drain` still does not note class exit (counter observation during quiesce drain can inflate until `ready_to_stop` zero). Scalar count is not a per-tx set; 1:1 holds for this Hang fixture only. `submit_versus_shutdown_hang_barrier_both_outcomes` 30ms sleep is a **different** needle. Storms themselves not reopened: EmptyToolRegistry / no `ToolRuntime.start`; distinct `SessionKey`s; no ambient identity; `completions_published == N` after receiver wait. |

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.

**Next pick:** live Grok multi-session **when** `GROK_AGENT_SECRET` is present
— **or** further named Fake race expansion — **or** human D-025 Sign-off on
`doc/SECURITY_REVIEW_CHECKLIST.md`. Independent D-025 last unless a human is
actually signing. Do not invent diagnostic/callback waits. Do not re-pick
D-060, Covered deadline cells, D-058 / D-059, the begin_shutdown lifecycle
needle, or the 3N wait.

**Advisor (2026-08-24, Hang-ready via per-class `live_connector_owners()>=N`):**
**PASS — Silver.** Accept Hang-ready as **fixed** by per-class
`RuntimeOwner::live_connector_owners() >= N`. **Reject** aggregate
`owned_task_count() >= 3N` as Hang-register happens-before (Expert Q1 hole
remains closed). Q2 begin_shutdown **fn** gate remains dual-gated (not
reopened). **Not** Golden / §25 / D-025. This is **not** a Sign-off self-sign.

Independently re-checked this tree: Cancel storm **ok**; ForceTerminate storm
**ok**; s23 **5/5**. Storm waits are `live_connector_owners() >= N` after
`ledger_len == N`. Inventory names the per-class wait and the InterpreterOwner
3N hole. `submit_versus_begin_shutdown_two_outcomes` remains in both
`doc/S23_RACE_LOAD_INVENTORY.md` and `s23_race_load_inventory_present`
lifecycle needles. Sign-off table `_TBD_`. `GROK_AGENT_SECRET` unset;
`GROK_AGENT` / `GROK_SESSION_ID` present do **not** substitute.

| Claim | Verdict |
|---|---|
| Accept Hang-ready via `live_connector_owners() >= N` (not 3N) | **PASS.** Supervisor `fetch_add`s only `TaskClass::ConnectorOwner` at both `spawn_rx` accept sites (loop-head `try_recv` and `select` `recv`) then `TaskSupervisor::spawn`. `fetch_sub` on `try_reap_finished` and `join_next`. InterpreterOwner pump/collector cannot inflate the counter. D-051 register-before-I/O: Hang `run_hang_owner` waits local control after open `drop(out_tx)`; Hang never self-terminals. N live ConnectorOwners ⇒ N registered Hang owners, not EP+Coord-only Starts. EmptyToolRegistry; distinct `term-storm-{i}` / `force-storm-{i}`; strict `Accepted`; completions then `completions_published == N`. |
| 3N aggregate wait | **Do not accept; do not restore.** Closed hole. Do **not** re-pick. |
| begin_shutdown dual-gated | **Still closed.** Deleting `fn submit_versus_begin_shutdown_two_outcomes` fails `s23_race_load_inventory_present`. Do **not** re-pick. |

Architecture / product shape: three components; product crates ↛ testkit;
no invented diagnostic emission (D-058) or core callback wait (D-059).
D-060 / Covered deadline cells / D-058 / D-059 not re-picked.

Standing (not a fail of this leftover): named table ≠ exhaustive §23 race/load;
matrix still-open; unsigned D-025 pack; checklist “Recent load/race proofs”
still omits both storms (stale pointer). `abort_and_drain` still does not
`note_connector_owner_exit` (quiesce observation can inflate until
`ready_to_stop` zeros both `owned_tasks` and `live_connector_owners` — prior
Advisor “not zeroed on Stopped” is **inaccurate** for this tree; that path is
not the Accepting pre-barrier wait). Scalar count is 1:1 only for this Hang
fixture. `submit_versus_shutdown_hang_barrier_both_outcomes` 30ms sleep is a
**different** needle.

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.

**Next pick:** live Grok multi-session **when** `GROK_AGENT_SECRET` is actually
present — **or** further named Fake race expansion — **or** human D-025
Sign-off on `doc/SECURITY_REVIEW_CHECKLIST.md`. Independent D-025 last unless
a human is actually signing. Do not invent diagnostic/callback waits. Do not
re-pick D-060, Covered deadline cells, D-058 / D-059, the begin_shutdown
lifecycle needle, or the 3N wait.

**Named Fake race expansion (2026-08-24):** With `GROK_AGENT_SECRET` unset
(live Grok blocked; `GROK_AGENT` / `GROK_SESSION_ID` do not substitute) and
D-025 reserved for human Sign-off, landed further named Fake race:

- Needle: `concurrent_hang_cancel_versus_force_terminate_one_terminal`
- One Hang admit; wait `live_connector_owners() >= 1`; barrier Cancel vs
  ForceTerminate on the same `TransactionId`.
- Dispositions only `{Accepted, AlreadyTerminal}` with ≥1 Accepted.
- Exactly one completion in `{Cancelled, Terminated}` (honest: Force may
  upgrade Cancel before Seal, or Cancel may Seal first — not Terminated-only).
- Shutdown `completions_published == 1`. EmptyToolRegistry.
- Inventory + `s23_race_load_inventory_present` dual-gated; checklist “Recent
  load/race proofs” now includes both Hang storms + this needle; D-025 pack
  named-race list updated (still unsigned).

Verified this tree: `concurrent_hang_*` **3/3 ok**; s23 **5/5**. Do **not**
promote Golden / §25 / D-025. Do not re-pick Hang-ready / 3N / begin_shutdown /
D-060 / D-058 / D-059 / Covered deadline cells.

**Expert (2026-08-24, named Fake race `concurrent_hang_cancel_versus_force_terminate_one_terminal`):**
**PASS** for the leftover as implemented. Barrier Cancel×Force on one Hang id
pins concurrent terminal selection honestly; disposition and end-kind asserts
match the product (§22.2 / §13.1 upgrade-before-commit, including Cancelled when
Seal wins). **Not** Golden / §25 / D-025. This is **not** a Sign-off self-sign.
Do **not** reopen Hang-ready / 3N / begin_shutdown / D-060 / D-058 / D-059.

Independently re-checked this tree: `concurrent_hang_*` **3/3 ok**; Cancel×Force
needle **10/10 ok**; s23 **5/5** (`s23_race_load_inventory_present` dual-gates
inventory + lifecycle fn). `GROK_AGENT_SECRET` unset. Wait is
`live_connector_owners() >= 1` (per-class; not aggregate / 3N). Sign-off table
`_TBD_`.

| Question | Verdict |
|---|---|
| 1. Barrier race pins concurrent Cancel×Force — miss / double-complete / invented waits? | **Sound.** `Barrier(2)` then immediate terminate on the same `TransactionId`. Hang never self-terminals. Pre-barrier `live_connector_owners() >= 1` after `ledger_len == 1` — D-051 Hang-register happens-before. Ledger-first terminate (D-039); one Finalizer (`first_decision`); `finalize_after_terminal` `yield_now` then re-reads kind before Seal. Default `max_actor_commands=256` → both enqueues fit. Double-complete blocked; `completions_published == 1`. EmptyToolRegistry. |
| 2. Disposition / end-kind asserts honest (incl. double-Accepted)? | **Yes.** `{Accepted, AlreadyTerminal}` with ≥1 Accepted covers double-Accepted (both see `terminal=None`) and second sticky. End kind `{Cancelled, Terminated}` is required honesty — Terminated-only would be a false claim. |
| 3. Holey async / flake / early-fire / product-bleed? | **No fail of this leftover.** Per-class ConnectorOwner ready gate; no product→testkit bleed. Stress 10/10. |
| 4. Remaining honesty residuals? | **Standing only.** Named inventory ≠ exhaustive §23 race/load; matrix still-open; unsigned D-025; `abort_and_drain` class-exit observation residual. |

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.

**Next pick:** live Grok multi-session **when** `GROK_AGENT_SECRET` is actually
present — **or** further named Fake race expansion — **or** human D-025
Sign-off on `doc/SECURITY_REVIEW_CHECKLIST.md`. Independent D-025 last unless
a human is actually signing. Do not invent diagnostic/callback waits. Do not
re-pick Hang-ready / 3N / begin_shutdown / D-060 / D-058 / D-059 / Covered
deadline cells / this Cancel×Force needle.

**Advisor (2026-08-24, named Fake race — Cancel×Force one Hang terminal):**
**PASS — Silver** for this **named Fake race expansion**. Quality tier
**unchanged**. Does **not** meet Golden / §25 and does **not** close D-025 /
§23 independent review. This is **not** a Sign-off self-sign.

Independently re-checked this tree: `concurrent_hang_*` **3/3 ok**; s23 **5/5**
including `s23_race_load_inventory_present`. `GROK_AGENT_SECRET` unset
(`GROK_AGENT` / `GROK_SESSION_ID` present do **not** substitute). Sign-off
table `_TBD_`.

| Claim | Verdict |
|---|---|
| Needle `concurrent_hang_cancel_versus_force_terminate_one_terminal` | **PASS.** One Hang; `live_connector_owners() >= 1`; barrier Cancel vs ForceTerminate on same id. Dispositions `{Accepted, AlreadyTerminal}` ≥1 Accepted; one completion `{Cancelled, Terminated}`; `completions_published == 1`. Honest, not Terminated-only. |
| Ownership / zero effects / identities | **PASS.** Loop lifecycle test; EmptyToolRegistry; explicit TransactionId + session string; no ambient identity; no product→testkit. |
| Checklist storm omission | **Closed.** Checklist lists both Hang storms **and** this needle. |
| Inventory dual-gate | **Closed.** Needle in inventory **and** lifecycle via `s23_race_load_inventory_present`. |
| Spec drift / invented waits / scope | **In bar.** Reuses Hang-ready wait; no D-058/D-059 invention; D025 pack updated, still unsigned. |

Do **not** re-pick Hang-ready / 3N / begin_shutdown / D-060 / D-058 / D-059 /
Covered deadline cells.

Standing (not a fail of this leftover): named table ≠ exhaustive §23 race/load;
matrix still-open; unsigned D-025 pack; live Grok still open when secret
present; `abort_and_drain` class-exit observation residual unchanged.

Do **not** promote Golden / §25 / D-025. Agents must not self-sign.

**Next pick:** live Grok multi-session **when** `GROK_AGENT_SECRET` is actually
present — **or** further named Fake race (e.g. Session-selector storm) — **or**
human D-025 Sign-off on `doc/SECURITY_REVIEW_CHECKLIST.md`. Independent D-025
last unless a human is actually signing. Do not invent diagnostic/callback
waits.
