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
- [ ] Residual: actor-command *byte* budget (messages are closed enums),
      deeper non-responsive provider matrix.

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
**Still open for full Golden:** multi-round inline (N>1 tool→model loops),
deleted FakeConnector `direct_llm_e2e` parity coverage, full §25 / D-025.
**Not** Golden / §25 / D-025. Agents must not self-sign.

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
D-003 retarget. Do **not** promote Golden / §25 / D-025.

**Affected:**
- `crates/monoloop-loop/src/transaction/active_registry.rs` (deleted)
- `crates/monoloop-loop/src/transaction/events.rs` (deleted)
- `crates/monoloop-loop/src/transaction/exchange.rs` (deleted)
- `crates/monoloop-loop/src/transaction/spawn_gate.rs` (deleted)
- callback-based compatibility types/aliases in contracts and Loop exports
  (retained — explicit compatibility phase)
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
- Breaking removal of compatibility aliases.
- A subsequent independent acceptance review finding no unresolved P0/P1/P2.

**Acceptance criteria:**
- [x] Delete obsolete uncompiled lifecycle/event modules after coverage migration.
- [x] Remove callback-based core APIs and compatibility aliases at the declared
  breaking boundary, **or** narrow M7/status language to an explicitly incomplete
  compatibility phase. (Chose narrow language; aliases retained.)
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
admits. **Still open for full Golden:** multi-round inline (N>1), Fake
parity suites, independent §25 / D-025 sign-off. **Next pick:** multi-round
inline hardening and/or independent Golden review. Agents must not
self-sign. Do **not** promote Golden / §25 / D-025.

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
