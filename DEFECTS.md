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
**Status:** Fixed (2026-08-18) — live unit_tx fan-out concurrent with exchange
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
**Status:** Fixed (2026-08-18) — ExchangeGuard + cleanup_deadline; cancel
during open and hang-response wait both release capacity
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
**Status:** Open
**Affected:**
- `crates/monoloop-loop/src/transaction/actor.rs:140-229`
- `crates/monoloop-connector-grok/src/channel_binding.rs:108-168`
- `crates/monoloop-connector-cursor/src/channel_binding.rs:92-141`
- equivalent Codex and Antigravity profile adapters

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

- [ ] Existing SessionId causes explicit provider load and never provider create.
- [ ] Missing SessionId causes provider create and the receipt/event/callback
      use the authoritative returned ID.
- [ ] Provider ID mismatch fails before prompt transmission.
- [ ] Create followed by reuse works deterministically for each external
      profile.
- [ ] Unknown or failed loads do not silently create replacement sessions.

## D-014: CreationOnly MCP capabilities are installed through the unsupported refresh path

**Priority:** P1
**Status:** Partial (2026-08-18) — empty-tools skip MCP; admission reject CreationOnly reuse+tools
**Affected:**
- `crates/monoloop-loop/src/transaction/actor.rs:140-152`
- `crates/monoloop-loop/src/transaction/actor.rs:231-293`
- `crates/monoloop-connector-grok/src/channel_binding.rs:170-180`
- `crates/monoloop-connector-cursor/src/channel_binding.rs:143-152`
- equivalent Codex and Antigravity profile adapters

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

- [ ] New CreationOnly session receives MCP configuration in its create request.
- [ ] The unsupported refresh method is never called for CreationOnly install.
- [ ] Empty-tool external transactions run without unnecessary MCP activation.
- [ ] Tool-enabled existing-session reuse is rejected at admission with no
      callback.
- [ ] A real HTTP MCP initialize/list/call path works through the profile-created
      descriptor.

## D-015: Most configured transaction and Channel limits are inert

**Priority:** P1
**Status:** Fixed (partial→stronger, 2026-08-18) — admission/actor/dispatcher
+ distinct sessions + encoded exchange + diagnostic bound helper; residual:
actor-command *byte* budget (control is enum-only), full non-responsive matrix
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
**Status:** Fixed (2026-08-18) — Ready only on tool_calls finish
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
**Status:** Fixed (2026-08-18) — shared per-token Streamable HTTP service;
real initialize → initialized → tools/list → tools/call over HTTP; body bound;
scoped revoke/shutdown cancel
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
**Status:** Fixed (2026-08-18) — HTTP error body + cancel on send
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
**Status:** Fixed (2026-08-18) — global shutdown deadline + concurrent guard
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
**Status:** Fixed (2026-08-18) — invoke/poll isolation + runtime-owned
`CallbackService` with bounded concurrency; actor does not await host callback
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
**Status:** Fixed (2026-08-18) — Rejected → DomainFailed CanonicalToolResult
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
**Status:** Open
**Affected:**
- `crates/monoloop-loop/src/transaction/admission.rs:72-88`
- `crates/monoloop-loop/src/transaction/admission.rs:248-261`
- `crates/monoloop-contracts/src/config.rs:153-162`
- `crates/monoloop-contracts/src/config.rs:386-427`
- `crates/monoloop-loop/src/transaction/openai_encoder.rs:52-133`
- `crates/monoloop-loop/src/transaction/acp_encoder.rs`

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

- [ ] Two Channels can accept different option/extension sets.
- [ ] Unknown key/version fails admission.
- [ ] Every accepted extension appears in the encoded provider request.
- [ ] No accepted configuration is silently ignored.

## D-024: Declared tool cancellation policy is not enforced by handler registration or cleanup

**Priority:** P2
**Status:** Open
**Affected:**
- `crates/monoloop-loop/src/transaction/host_tools.rs:26-71`
- `crates/monoloop-loop/src/transaction/dispatcher.rs:208-261`
- `crates/monoloop-loop/src/transaction/tool_handler.rs:12-95`

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

- [ ] An unstoppable in-process handler cannot be registered.
- [ ] Cooperative grace expiry escalates according to declared policy.
- [ ] Abortable and isolated-killable tests prove zero work after terminal.
- [ ] Termination failure selects `ToolExchangeFailed` and records a safe
      diagnostic.

## D-025: WP-12 does not meet its own acceptance and formatting gates

**Priority:** P2
**Status:** Open
**Affected:**
- `doc/WP12_REQUIREMENTS_ACCEPTANCE.md:9-94`
- `doc/WP12_CURRENT_LIMITATIONS.md:25-38`
- delivered Rust files reported by `cargo fmt --all -- --check`

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

- [ ] Every required R-000 through R-004 item is Pass with a direct,
      non-conditional test.
- [ ] Open items are either implemented or explicitly shown to be out of scope
      by the accepted requirements—not merely deferred.
- [ ] All six profile paths have deterministic create/reuse/termination
      qualification appropriate to their declared capabilities.
- [ ] Formatting, tests, strict Clippy, and documentation gates all pass.
- [ ] Independent re-review finds no unresolved P0, P1, or P2 defect.


### Remediation progress (2026-08-18, continued)

| ID | Status | Notes |
|---|---|---|
| D-009 | Fixed | start_gate; install under Accepting+registry lock |
| D-010 | Fixed | shared Arc state; re-check under lock |
| D-011 | Fixed | live canonical unit fan-out during exchange |
| D-012 | Fixed | cleanup_deadline; cancel during open + Hang response-wait |
| D-013 | Fixed | attach create+load; create_mode; provider id after open; known maps shared |
| D-014 | Fixed | MCP install before attach; initial_mcp on create; no CreationOnly refresh |
| D-015 | Fixed (partial→stronger) | + distinct sessions; encoded exchange; bound_diagnostics; actor command cap |
| D-016 | Fixed | Ready only on `tool_calls` finish |
| D-017 | Fixed | single ExchangeId |
| D-018 | Fixed | shared per-token service; real HTTP initialize/list/call; body 1MiB; scoped revoke |
| D-019 | Fixed | HTTP bounds/cancel |
| D-020 | Fixed | absolute shutdown deadline |
| D-021 | Fixed | CallbackService + panic isolation; capacity free while callback runs |
| D-022 | Fixed | Rejected → CanonicalToolResult |
| D-023 | Fixed | empty extension allowlist denies all extensions |
| D-024 | Fixed | RegisteredTool::try_new validates policy vs handler supports_* |
| D-025 | Partial | D-009–D-024 largely fixed; full R-000 re-sign-off still open |
