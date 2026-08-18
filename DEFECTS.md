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
**Status:** Fixed (2026-08-18) — Accepting re-check under registry lock
**Affected:**
- `crates/monoloop-loop/src/transaction/runtime.rs:201-217`
- `crates/monoloop-loop/src/transaction/runtime.rs:350-370`

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

- [ ] A barrier-controlled `submit` versus `shutdown` race has only two legal
      outcomes: rejected admission with no callback, or admitted and included in
      shutdown finalization.
- [ ] No registry entry can appear after shutdown's active snapshot.
- [ ] Runtime `Stopped` implies zero active actors, routes, callbacks, and held
      capacity.

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
- [ ] Residual: explicit per-capability/global concurrency + request duration
      bounds beyond body size and rmcp session cancel.

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

- [ ] An unbounded error body cannot increase retained memory beyond the
      configured diagnostic/drain limit.
- [ ] Cancellation interrupts a blocked output enqueue.
- [ ] Connect, headers, body, idle, and overall timeout tests use independent
      deterministic barriers.
- [ ] Total elapsed work cannot exceed the configured overall deadline plus
      cleanup grace.

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
| D-018 | Fixed (via D-034) | token hex canonicalize; residual: per-route concurrency/duration exact-limit tests |
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
| D-028 | Fixed (residual closed) | Shared `StickyCancel` always joined (no detach after cleanup); missing kill joins to completion; kill capability checked before start |
| D-029 | Fixed (residual closed) | Shutdown aborts actor+delivery (not only reaper); finalize only after join; callback abort never unbounded; disposition counts callback outcomes |
| D-030 | Fixed | ExchangeId-scoped ToolActionId; empty allowlist → rejection Completed; CallerControlled after observe |
| D-031 | Fixed (residual closed) | OpenAI continuation encodes transcript only (no duplicate `results` append) |
| D-032 | Fixed (residual closed) | `try_spawn` confirms start on multi-thread (mpsc rendezvous); rejects cancelled-never-started; current-thread sync admit keeps immediate check only |
| D-033 | Fixed | absolute request deadline; enqueue selects deadline; output queue from output budget |
| D-034 | Fixed (known residual) | Canonical hex + global/per-cap permits before body; body+dispatch share duration budget; process-global service map remains |
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

- [ ] Header delay plus body delay cannot exceed one request timeout.
- [ ] A full output queue terminates at overall deadline without host receive.
- [ ] Exact output-byte capacity plus one fails closed.

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
- [ ] Concurrency and duration exact-limit/plus-one tests still desirable.
      (process-global service map remains a known structural residual)

## D-035: Runtime canonical-input byte accounting omits bounded fields

**Priority:** P2
**Status:** Fixed
**Affected:**
- `crates/monoloop-loop/src/transaction/admission.rs:98-140`
- `crates/monoloop-loop/src/transaction/admission.rs:421-446`

**Problem:** `estimate_input_bytes` omits message names, assistant tool argument
JSON, and Tool-message correlation IDs. A request can therefore exceed the
runtime's `max_input_bytes` while passing admission, especially through large
historical assistant tool arguments.

**Required remediation:**

- Define one canonical deterministic byte-size function covering every field.
- Use it in admission and continuation accounting.
- Avoid serialization-error fallbacks that count malformed values as zero.

**Acceptance criteria:**

- [ ] Every canonical message variant and optional field has exact-limit and
      plus-one coverage.
- [ ] Large historical tool arguments and IDs cannot bypass `max_input_bytes`.

## D-036: Concurrent event producers can deliver sequence numbers out of order

**Priority:** P1
**Status:** Fixed
**Affected:**
- `crates/monoloop-loop/src/transaction/finalization.rs:10-33`
- `crates/monoloop-loop/src/transaction/actor.rs:456-529`
- `crates/monoloop-loop/src/transaction/actor.rs:949-969`

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

- [ ] Barrier-controlled concurrent producers always deliver `1..N` in order.
- [ ] New external sessions always deliver `SessionEstablished` at sequence 1.
- [ ] No sequence is allocated for an event that cannot be enqueued.

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
