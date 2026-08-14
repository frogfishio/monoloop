# Component 03 — The Loop

**Status:** Foundational component specification

**Product:** [Monoloop](MONOLOOP.md)

**Component kind:** Active asynchronous event-driven state machine

**Consumes:** [Component 02 — Interpreter](INTERPRETER.md) canonical events
through an independent lossless subscription

**Parallel test observer:** [Console Renderer](CONSOLE_RENDERER.md)

**Produces:** Typed loop/tool lifecycle events and canonical outbound tool
results

**Initial tool catalogue:** Empty

**Parent index:** [README.md](README.md)

The words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and
**MAY** are normative requirements.

---

## 1. Purpose

The Loop subscribes to canonical in-memory events and reacts to complete tool
requests.

Its initial responsibility is deliberately narrow:

> Consume complete canonical tool requests, dispatch them through an abstract
> empty-capable tool runtime, track their execution asynchronously by identity,
> and emit truthful canonical outcomes.

The first implementation contains no actual tools. A complete request for an
unregistered tool produces a deterministic `ToolUnavailable` outcome. It does
not invoke a shell, simulate the result, ask another model, disappear, or wait
forever.

## 2. Position in the system

```text
                          +--> Console Renderer
                          |       passive/debug
Canonical event source ---+
                          |
                          +--> The Loop
                                  |
                                  v
                           abstract Tool Registry
                                  |
                                  v
                           abstract Tool Runtime
                                  |
                                  v
                       Loop events + OutboundToolResult
```

The Console Renderer and The Loop receive separate subscriptions. They do not
compete for events on one receiver.

## 3. Architectural decision

The Loop is an explicit state machine. Events are inputs to that machine; they
are not the machine itself.

The Loop never infers actionable work from prose, Markdown, arrival timing, UI
state, or raw provider events. In the initial implementation, only a canonical
`ToolRequestReady` event can cause tool resolution and dispatch.

Each Loop instance owns its in-memory state and all child tool executions. Many
Loop instances run concurrently. There is no process-global current loop,
connection, session, tool, or action.

In Monoloop composition, exactly one Loop instance belongs to exactly one
`MonoloopRunId`. A Loop can observe several explicitly admitted
Interpretations/connections only when all belong to that same run. It cannot be
shared between runs or survive its owning run.

## 4. Initial scope

Component 03 implements:

- loop instance lifecycle;
- lossless canonical event consumption;
- event identity validation and deduplication;
- tool-action state tracking;
- complete-request recognition;
- abstract tool resolution;
- bounded asynchronous tool dispatch;
- child execution observation/cancellation;
- canonical tool-result production;
- loop events, health, and terminal reporting; and
- empty-tool behavior.

It does not implement the complete Channel exchange. In the initial test
composition, the Driver owns exchange continuation and a separate outbound
encoder owns dialect encoding. A later production host may provide equivalent
composition. Prompt construction and higher-product completion remain outside
Monoloop entirely.

## 5. Explicit non-responsibilities

The Loop MUST NOT:

- consume raw Connector bytes or dialect-native events;
- parse or reassemble text, Markdown, JSON fragments, or tool fragments;
- act on `ToolActionWaiting` as an executable request;
- contain a filesystem, shell, browser, MCP, Kanban, memory, or other concrete
  tool implementation;
- invent or simulate unavailable tools;
- call a model to produce a fake tool result;
- encode tool results into a provider dialect;
- write directly to Connector input;
- choose a Connector, provider, model, route, or prompt;
- decide that an interpretation boundary means a turn is complete;
- persist loop, event, request, execution, or result state;
- render console or product UI output;
- infer project/session/activity identity;
- obtain identity from task-local/global current state;
- retry a semantic tool operation without an explicit later policy; or
- mutate Interpreter canonical units.

## 6. Loop runtime and instance

```rust
pub trait LoopRuntime: Send + Sync {
    fn start(
        &self,
        request: StartLoop,
    ) -> Result<LoopHandle, LoopError>;
}

pub struct StartLoop {
    pub monoloop_run_id: MonoloopRunId,
    pub loop_id: LoopId,
    pub scope: LoopScope,
    pub subscription: CanonicalEventSubscription,
    pub tool_registry: Arc<dyn ToolRegistry>,
    pub tool_runtime: Arc<dyn ToolRuntime>,
    pub output: LoopEventSink,
    pub limits: LoopLimits,
}

pub struct LoopHandle {
    pub loop_id: LoopId,
    pub control: LoopControl,
    pub health: LoopHealth,
    pub completion: LoopCompletion,
}
```

The exact Rust spelling may vary. Required semantics:

- `start` returns without waiting for the loop lifetime;
- the instance is owned by exactly one `monoloop_run_id`;
- one Loop instance has one serialized state owner;
- the instance receives a separately owned lossless subscription;
- all queues, tables, and concurrent executions are bounded;
- cancellation and completion are explicit; and
- every task spawned by the instance is owned and joined by it.

## 7. Loop scope

```text
LoopScope
    monoloop_run_id
    loop_id
    accepted_interpretation_ids[]
    accepted_connection_ids[]
    accepted_external_session_ids[]
    host_scope_ref?
```

A scope may contain one or several explicitly admitted Interpretations, but
every admitted Interpretation and connection MUST belong to the same
`monoloop_run_id`. This supports a single run involving multiple connections
without creating an ambient current connection or a cross-run Loop.

When the Connector supplies an external session identity, the Loop admits and
validates it explicitly. For Grok Build this is Grok's `sessionId`; The Loop
propagates it for correlation but assigns it no authority beyond the configured
scope.

The Loop rejects events outside its scope. It never expands scope because an
event happens to arrive on its subscription.

`host_scope_ref` is an opaque correlation reference supplied by later
composition. It is not interpreted as project/session/task authority by The
Loop.

`monoloop_run_id` is immutable. No event, tool request, registry result, or
runtime callback can expand or replace it.

## 8. Required event-distribution contract

The Interpreter has one bounded canonical output. Console and Loop require
independent subscriptions. Composition therefore supplies a canonical event
distribution boundary with these semantics:

```text
one accepted canonical event
    -> Console subscription       best effort or lossless by configuration
    -> Loop subscription          lossless, gap-detecting
    -> future subscribers         independently bounded
```

Component 03 does not implement or own the process-wide distributor. It requires
a `CanonicalEventSubscription` with:

```text
subscriber_id
subscription_scope
delivery_sequence
canonical event
source terminal/gap notification
```

The Loop subscription MUST NOT silently drop, coalesce, or reorder actionable
events. A detected gap causes fail-closed behavior described in §27.

Cloning an `mpsc::Receiver` or allowing Console and Loop to race for the same
event is prohibited.

## 9. Input vocabulary

The Loop consumes the closed canonical vocabulary but initially reacts only to:

```text
ToolActionWaiting
ToolRequestReady
ToolActionIncomplete
InterpretationEnd
subscription status/gap
```

Behavior:

| Event | Initial behavior |
|---|---|
| Complete sentence/structure | Observe and advance delivery sequence; no action |
| Tool waiting | Track safe lifecycle state; do not resolve/dispatch |
| Tool request ready | Validate, deduplicate, resolve, and possibly dispatch |
| Tool incomplete/malformed | Mark terminal non-dispatched state; emit loop event |
| Interpretation end | Record source terminal; do not infer turn completion |
| Subscription gap/loss | Fail closed for new dispatch |

Future event reactions require an explicit revision to this specification.

## 10. Event validation

Before applying an event, The Loop validates:

- subscription scope;
- interpretation and connection membership;
- external session membership when present;
- canonical schema version;
- delivery sequence continuity;
- unit identity;
- unit generation monotonicity;
- event kind/state compatibility;
- causal-parent references where required; and
- tool request completeness.

Invalid events do not mutate tool state or cause dispatch.

## 11. Event deduplication

Canonical generations are keyed by:

```text
interpretation_id
unit_id / tool_action_id
unit_generation
canonical semantic digest
```

Rules:

- same identity/generation/same digest is an idempotent duplicate;
- same identity/generation/different digest is an invariant failure;
- older generations are stale and cannot reverse state;
- a generation gap is explicit and blocks that action pending reconciliation;
- duplicated `ToolRequestReady` never causes a second dispatch; and
- deduplication storage is bounded by Loop scope and retention limits.

## 12. Tool-action state machine

Per ToolActionId:

```text
unseen
    -> observed_waiting
    -> request_ready
    -> resolving_tool
        -> unavailable
        -> dispatch_rejected
        -> dispatching
            -> running
                -> resolved
                -> failed
                -> cancelled
                -> execution_lost

observed_waiting | request_ready
    -> input_incomplete
    -> input_malformed
    -> source_terminated
```

Terminal states cannot return to running. A later generation that contradicts a
terminal request identity is an invariant failure, not a new action.

## 13. Dispatch trigger

Only `ToolRequestReady` may trigger resolution.

It must contain:

```text
tool_action_id
complete tool name
complete syntactically valid request payload
request generation
flow/lane/correlation identity
```

Partial JSON, raw fragments, a waiting placeholder, a text sentence describing
a tool, or a tool-looking Markdown block cannot trigger dispatch.

The Loop validates the request once more against canonical contract invariants
before calling the registry.

## 14. Abstract tool registry

```rust
pub trait ToolRegistry: Send + Sync {
    async fn resolve(
        &self,
        request: ResolveToolRequest,
    ) -> Result<ToolResolution, ToolRegistryError>;
}

pub enum ToolResolution {
    Available(ToolDescriptorRef),
    Unavailable(ToolUnavailableReason),
}
```

The registry contract is intentionally minimal. Tool schemas, permissions,
approval, capability attenuation, versions, and selection policy remain TBD for
later tool-component specifications.

The Loop does not enumerate concrete tools or interpret tool-specific payloads.

## 15. Empty tool registry

The required first implementation is `EmptyToolRegistry`:

```text
resolve(any complete request)
    -> Unavailable(no_registered_tool)
```

Required behavior:

- deterministic;
- non-blocking except normal async scheduling;
- no filesystem/network/process access;
- no fallback lookup;
- no string aliases that accidentally resolve;
- no tool execution started; and
- one canonical unavailable result emitted.

This is a successful qualification of the Loop path, not a Loop failure.

## 16. Abstract tool runtime

```rust
pub trait ToolRuntime: Send + Sync {
    fn start(
        &self,
        request: StartToolExecution,
    ) -> Result<ToolExecutionHandle, ToolRuntimeError>;
}

pub struct ToolExecutionHandle {
    pub execution_id: ToolExecutionId,
    pub observations: ToolExecutionObservationStream,
    pub control: ToolExecutionControl,
    pub completion: ToolExecutionCompletion,
}
```

The initial runtime may be a `NoToolRuntime` that asserts `start` is never
called when paired with `EmptyToolRegistry`.

The Loop knows only normalized lifecycle observations and terminal results. It
does not import tool implementation types.

## 17. Stable execution identity

For an available request, The Loop derives or allocates one stable
`ToolExecutionId` bound to:

```text
loop_id
interpretation_id
tool_action_id
request_generation
request_digest
```

The same accepted request cannot acquire a different execution identity within
one Loop incarnation.

This provides in-memory at-most-once dispatch per Loop incarnation. It does not
claim crash-safe exactly-once tool effects. Durable idempotency and recovery are
deferred until persistence/effect contracts exist.

## 18. Dispatch protocol

The order is fixed:

```text
1. accept ToolRequestReady generation
2. validate scope, identity, digest, completeness, and limits
3. record request_ready in Loop memory
4. emit ToolDispatchRequested
5. resolve through ToolRegistry
6a. unavailable: record terminal unavailable and emit result
6b. available: allocate stable ToolExecutionId
7. reserve bounded concurrency capacity
8. record dispatching
9. call ToolRuntime.start
10. record running only after start succeeds
11. consume observations and terminal completion
12. record one terminal state
13. emit lifecycle event and canonical OutboundToolResult
```

The Loop cannot mark an action running before the runtime accepts ownership.

## 19. Concurrent execution

Many ready tools may run concurrently subject to `LoopLimits`:

```text
T1 running
T2 resolving
T3 queued_for_capacity
T4 unavailable
T5 resolved
```

Rules:

- each action has independent identity/state/control/completion;
- completion order does not change tool ownership or causal relationships;
- concurrency limits are explicit and bounded;
- queue order is deterministic within the configured scheduling policy;
- one slow or failed tool does not block unrelated running tools;
- per-Loop and process composition may impose separate limits; and
- capacity exhaustion is visible, never silent.

The initial scheduling policy is FIFO by accepted `ToolRequestReady` delivery
sequence within one Loop. Priority/dependency scheduling is deferred.

## 20. Loop output vocabulary

The closed initial output is:

```rust
pub enum LoopOutputEvent {
    ToolDispatchRequested(ToolDispatchRequested),
    ToolUnavailable(ToolUnavailable),
    ToolExecutionQueued(ToolExecutionQueued),
    ToolExecutionStarted(ToolExecutionStarted),
    ToolExecutionObservation(ToolExecutionObservation),
    ToolExecutionResolved(ToolExecutionResolved),
    ToolExecutionFailed(ToolExecutionFailed),
    ToolExecutionCancelled(ToolExecutionCancelled),
    ToolExecutionLost(ToolExecutionLost),
    OutboundToolResult(OutboundToolResult),
    Diagnostic(LoopDiagnostic),
    LoopEnded(LoopEnd),
}
```

All outputs carry Loop, source interpretation, tool action, request generation,
execution identity where applicable, plus the explicitly supplied external
session identity when present.

Output publication is asynchronous, in-memory, bounded, and immediate after
each accepted state transition.

## 21. Canonical outbound tool result

```text
OutboundToolResult
    outbound_result_id
    monoloop_run_id
    loop_id
    source_interpretation_id
    source_connection_id
    external_session_id?
    tool_action_id
    request_generation
    tool_execution_id?
    outcome
    complete result payload or safe error
    causal references
```

Outcomes include:

```text
success
tool_unavailable
dispatch_rejected
execution_failed
cancelled
execution_lost
```

The result is provider-neutral. The Loop does not serialize it as OpenAI,
Anthropic, ACP, Cursor, Grok Build, JSONL, or any other dialect.

The test kit's separate outbound Encoder consumes this product and its Driver
writes the encoded bytes through the Connector input. A later host may provide
the same seams. Neither responsibility enters The Loop.

## 22. Output publication rule

One serialized Loop owner performs:

```text
validate transition
    -> commit new in-memory action state
    -> enqueue typed Loop output
    -> continue event/tool processing
```

The Loop does not batch completed tool results until Interpretation end.

If its required output sink is unavailable or full, The Loop applies
backpressure and stops accepting further actionable input. It never drops a
tool result or continues dispatching while outcomes are invisible.

Nonessential progress observations may later have a separately configured
best-effort projection, but terminal and outbound results are always lossless.

## 23. Result completeness

The Loop emits `OutboundToolResult` only when the selected outcome is terminal
and its result/error representation is complete under the abstract runtime
contract.

Partial tool stdout, progress messages, and intermediate observations may be
emitted as lifecycle observations but cannot become an outbound terminal result.

If the tool runtime terminates without a valid result, the outcome is
`execution_lost` or `execution_failed`, never success.

## 24. No direct feedback into Interpreter

The Loop never mutates or calls back into the Interpreter to “complete” an
Interpreter-owned tool unit.

Instead:

- Interpreter events remain immutable observations of the inbound dialect;
- Loop events describe host-side tool resolution;
- both retain the same ToolActionId/correlation; and
- a later projection may show their combined lifecycle.

This prevents cyclic ownership and lets every component terminate independently.

## 25. Source termination

An `InterpretationEnd` is an input fact, not automatic Loop completion.

On source end, The Loop:

- rejects new events from that source;
- marks unresolved inbound actions according to their known state;
- allows already dispatched tool executions to drain or cancels them according
  to explicit Loop shutdown policy;
- emits terminal results for every owned execution where possible; and
- remains alive if its scope contains other active Interpretations.

The Loop does not declare a user turn complete.

## 26. Loop cancellation and shutdown

```rust
pub trait LoopControl: Send + Sync {
    fn cancel(&self, reason: LoopCancellationReason) -> ControlDisposition;
}
```

Cancellation:

- stops accepting new actionable events;
- cancels queued requests before dispatch;
- propagates cancellation to owned running ToolExecutionHandles;
- waits only within configured bounded grace periods;
- marks unresolved executions cancelled or lost truthfully;
- emits required terminal/outbound results;
- releases the subscription and output handles; and
- resolves Loop completion exactly once.

The Loop does not directly cancel Connector connections. The test Driver or a
future host may signal both from one higher-level cancellation decision.

## 27. Subscription gap and loss

The Loop subscription is control-significant and fail-closed.

If a delivery sequence gap, overflow, distributor reset, or unexplained source
loss is detected:

1. stop dispatching new tools;
2. mark the Loop degraded;
3. preserve already known action state;
4. cancel or drain owned running executions according to explicit policy;
5. emit `LoopDiagnostic(subscription_gap)`;
6. terminate or await a future explicit reconciliation mechanism; and
7. never infer the missing event was non-actionable.

There is no best-effort mode for actionable Loop input.

## 28. Loop lifecycle

```text
configured
    -> starting
        -> running
            -> quiescing
            -> cancelling
            -> degraded
            -> source_drained
        -> start_failed

quiescing
    -> drained
    -> cancellation_escalated

cancelling, degraded, source_drained, drained,
cancellation_escalated, start_failed
    -> terminal
```

This is the Loop lifecycle only. It is not the model invocation, connection,
interpretation, user turn, task, or activity lifecycle.

The owning Monoloop run initiates Loop quiescence or cancellation before that
run may terminate. Loop terminal completion is therefore a required child
completion of the run; it is never a detached continuation.

## 29. Loop completion

```text
LoopEnd
    monoloop_run_id
    loop_id
    kind
    delivery_events_received
    duplicate_events
    stale_events
    tool_actions_by_terminal_state
    tool_executions_started
    tool_executions_terminal
    outbound_results_emitted
    pending_actions_at_end
    safe_diagnostics[]
```

Kinds:

```text
drained
cancelled
subscription_lost
output_failed
invariant_failed
configuration_failed
```

Exactly one LoopEnd is emitted. None of these values means `turn_complete`.

After `LoopEnd`, every action table, deduplication entry, queue, execution
handle, observation buffer, and outbound staging record owned by the instance is
destroyed. Nothing is retained for a later Monoloop run.

## 30. In-memory durability boundary

All Component 03 state is in memory:

- event deduplication;
- action state machines;
- pending request queue;
- concurrency reservations;
- execution handles;
- observations; and
- outbound result staging.

Component 03 performs no database or file write.

A process crash may lose Loop state and in-flight tool knowledge. The initial
component makes no crash recovery or exactly-once-effect claim. Those guarantees
require later durable command/effect receipts and must not be faked here.

## 31. Async and multitasking requirements

The complete design is asynchronous from the first implementation:

- many Loop instances operate concurrently;
- each instance has exactly one owning Monoloop run;
- one Loop may manage several admitted connections/Interpretations;
- each tool execution has an independently owned async handle;
- no blocking I/O or blocking wait runs on an async worker;
- bounded queues apply backpressure;
- cancellation wakes subscription, registry, runtime, output, and child waits;
- completion is joined exactly once without polling a completed JoinHandle;
- shared registries contain handles/configuration, not ambient action state; and
- one Loop's failure/cancellation cannot affect siblings.
- no Loop instance or child execution may outlive its owning run's bounded
  terminal teardown.

Every spawned task has an owner, bounded inputs, cancellation, a terminal result,
and cleanup responsibility. Detached fire-and-forget execution is prohibited.

The Rust implementation is safe on a multi-threaded async runtime. It performs
no blocking waits and holds no synchronization guard across an await. Separate
Loop instances and independent tool executions may progress on different
runtime workers while each Loop retains one serialized state owner.

## 32. Limits

```text
LoopLimits
    maximum admitted interpretations/connections
    maximum tracked canonical generations
    maximum simultaneous tool actions
    maximum queued ready requests
    maximum concurrent tool executions
    maximum observations per execution
    maximum observation/result bytes
    maximum output queue items/bytes
    registry resolution deadline
    tool start deadline
    tool execution deadline/default policy placeholder
    cancellation grace period
    shutdown deadline
```

Tool-specific limits remain TBD. The generic Loop enforces its own aggregate
bounds independently.

## 33. Error vocabulary

Closed initial errors:

```text
event_out_of_scope
event_schema_unsupported
delivery_sequence_gap
unit_identity_conflict
unit_generation_conflict
tool_request_incomplete
tool_request_digest_conflict
tool_unavailable
tool_registry_failed
tool_start_failed
tool_execution_failed
tool_execution_lost
concurrency_limit_exceeded
queue_limit_exceeded
output_backpressure_exceeded
cancelled
shutdown_deadline_exceeded
invariant_violation
```

Errors contain safe bounded correlation and classification. Tool payloads,
secrets, provider bodies, and raw canonical content are not copied into default
diagnostics.

## 34. Security and trust

Canonical means structurally complete, not authorized or safe to execute.

The initial empty registry ensures no effect can occur. When tools are later
introduced, an explicit policy/admission component must authorize them before
the abstract runtime starts execution.

The Loop MUST:

- treat tool names and payloads as untrusted data;
- never dynamically load code from a tool name;
- avoid string-to-shell interpretation;
- preserve source/causal identity;
- keep tool results scoped to the owning action;
- prevent cross-Loop cancellation/result injection;
- bound all diagnostic/result material; and
- expose no credential or environment lookup.

## 35. Observability

Content-free metrics include:

```text
loop_instances{state}
loop_input_events{kind,result}
loop_duplicate_events{kind}
loop_subscription_gaps
loop_tool_actions{state}
loop_tool_registry_resolution{result}
loop_tool_executions{state}
loop_tool_execution_latency{terminal_kind}
loop_ready_queue_depth
loop_running_tool_count
loop_output_queue_depth
loop_outbound_results{outcome}
loop_terminal{kind}
```

Labels are bounded and omit raw tool names/payloads, connection/session/project
IDs, secrets, paths, and unbounded errors.

## 36. Required tests

### 36.1 Event reaction

- Text, paragraph, structure, usage, and ordinary diagnostics cause no dispatch.
- `ToolActionWaiting` causes no registry lookup or dispatch.
- `ToolRequestReady` causes exactly one registry resolution.
- Incomplete/malformed tool input never executes.
- Interpretation end is not turn completion.

### 36.2 Empty registry

- Every complete request resolves to `no_registered_tool`.
- One `ToolUnavailable` and one `OutboundToolResult` are emitted.
- `ToolRuntime.start` is never called.
- No fallback shell/network/filesystem/model behavior occurs.
- Many unavailable requests resolve concurrently within bounds.

### 36.3 Identity and deduplication

- Same event identity/digest is idempotent.
- Same identity/generation/different digest fails.
- Duplicate request-ready event never double-dispatches.
- Older generation cannot reverse action state.
- Generation gap blocks that action.
- Equal ToolActionIds in different Interpretations/Loops remain isolated.

### 36.4 Tool runtime fixture

Using deterministic fake tools only:

- accepted start transitions dispatching to running correctly;
- start failure never becomes running;
- partial observations never become terminal result;
- success/failure/cancel/lost each emit one truthful outbound result;
- result identity matches the request and execution;
- runtime completion after another tool cannot cross-correlate;
- completion cannot be awaited/polled in a way that panics after completion.

### 36.5 Concurrency

- Multiple tools execute concurrently to the configured limit.
- Excess requests queue deterministically.
- One slow tool does not block other executions or event intake within bounds.
- Out-of-order completion preserves action identity.
- Multiple connections and Loop instances do not leak state.
- Cancelling one Loop leaves siblings unchanged.

### 36.6 Subscription and output

- Console and Loop receive the same event through separate subscriptions.
- Console failure/drop policy cannot remove the Loop event.
- Loop input gap causes fail-closed behavior.
- Actionable event is never silently dropped/coalesced.
- Required output backpressure stops further dispatch.
- Terminal/outbound results are never best-effort.

### 36.7 Cancellation and shutdown

- Cancellation stops new dispatch.
- Queued requests become cancelled without ToolRuntime start.
- Running child handles receive cancellation.
- Nonresponsive child escalates to lost after bounded deadline.
- Loop produces exactly one terminal result.
- Every owned task/handle/buffer is released.
- Loop cancellation does not directly cancel Connector/Interpreter.

### 36.8 Bounds and load

- Every queue/table/concurrency/byte limit is enforced.
- Unbounded request flood cannot cause unbounded memory/task growth.
- Thousands of fake connections/actions remain identity-isolated.
- Slow output consumer applies bounded fail-closed backpressure.
- Deduplication retention remains bounded.

### 36.9 Architecture

- The Loop does not import Connector implementations, dialect decoders,
  Console Renderer, product UI, host agent, Kanban, DAL, Residiuum, prompt,
  router, or concrete tool modules.
- No raw byte or dialect-native input API exists.
- No persistence path is reachable.
- No Connector write/control method is reachable.
- No concrete tools are registered in the initial component.
- No unbounded channel/global current-state registry exists.
- No prose/text event can reach tool dispatch.
- A Loop cannot accept events from or emit results for another Monoloop run.
- No Loop-owned state survives run termination.

## 37. Acceptance criteria

Component 03 is accepted only when:

1. it consumes a distinct lossless canonical subscription;
2. only a complete `ToolRequestReady` can trigger resolution;
3. waiting, malformed, incomplete, duplicate, stale, and out-of-scope events
   cannot execute;
4. action state is explicit, identity-scoped, and generation-checked;
5. the empty registry deterministically returns one unavailable outcome;
6. the initial runtime starts zero actual tools;
7. fake-tool qualification proves bounded concurrent dispatch and correlation;
8. every terminal tool state produces one complete provider-neutral outbound
   result;
9. Loop output is immediate, async, in-memory, bounded, and lossless for
   terminal/control-significant events;
10. source completion is not treated as user-turn completion;
11. Loop failure/cancellation cannot affect sibling Loops or directly control
    Connector/Interpreter;
12. no persistence or crash-safe exactly-once claim is made;
13. no concrete tool, dialect encoding, model routing, prompting, UI, or product
    responsibility enters the component;
14. subscription gaps and output failure are fail-closed;
15. all async work has explicit ownership, cancellation, completion, and cleanup;
16. architecture gates enforce every dependency boundary; and
17. all identity, deduplication, empty-registry, fake-runtime, concurrency,
    cancellation, load, and failure suites pass without partial or “shaped”
    qualification; and
18. run-ownership tests prove that cross-run events are rejected and all Loop
    state is destroyed before the owning Monoloop run completes.

## 38. Updated ground-zero qualification

Components 01–04 form this executable slice:

```text
Connector(s)
    -> Interpreter(s)
    -> canonical event distributor/subscriptions
        -> Console Renderer
        -> The Loop
            -> EmptyToolRegistry
            -> NoToolRuntime
            -> ToolUnavailable / OutboundToolResult
```

The slice proves:

- real-time interpretation and passive rendering;
- two independent subscribers see the same canonical event;
- actionable tool requests are complete before reaction;
- unavailable tools resolve mechanically;
- concurrent streams remain isolated;
- cancellation and terminal accounting are bounded; and
- no product UI, host agent, persistence, concrete tools, or prompt engine is
  required.

This Component 01–03 slice alone does not send the outbound result back to the
external system. The test Driver's outbound Encoder seam, or an equivalent
future host composition, supplies that behavior.

## 39. Deferred work

Explicitly deferred:

- actual tool catalogue and implementations;
- tool schemas and payload typing;
- authorization, approval, capabilities, and sandboxing;
- durable command/effect receipts and crash recovery;
- retry and scheduling policy beyond bounded FIFO;
- outbound dialect encoding and Connector input, which belong to the test
  Driver or future host composition;
- Channel continuation and invocation lifecycle, which belong to the test
  Driver or future host coordinator;
- context/prompt compilation;
- product/task/turn state;
- event distributor implementation;
- product UI projections and controls; and
- human interaction tools.

These are separate components or later revisions. They must not be smuggled into
The Loop for convenience.

## 40. Governing rule

> The Loop reacts only to complete canonical tool requests. It dispatches them
> through abstract bounded async capabilities, tracks every action by identity,
> and emits truthful canonical outcomes. It contains no tools and invents no
> effects.
