# Monoloop — Product architecture

**Status:** Foundational product specification

**Product:** Monoloop

**Product kind:** Stateless asynchronous canonical request/response processor

**Component specifications:**

- [Component 01 — Connector](CONNECTOR.md)
- [Component 02 — Interpreter](INTERPRETER.md)
- [Component 03 — Console Renderer](CONSOLE_RENDERER.md) — test only
- [Component 04 — The Loop](THE_LOOP.md)
- [Component 05 — Console Input](CONSOLE_INPUT.md) — test only

**Parent index:** [README.md](README.md)

**Production cognitive integration:**
[Cognitive Runtime ↔ Monoloop](../CONTEXT_COMPILER/COGNITIVE_RUNTIME_MONOLOOP.md)

The words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and
**MAY** are normative requirements.

---

## 1. Product definition

Monoloop does one thing:

> Correctly delegate one canonical request to one explicitly selected channel,
> receive the channel response, process any configured tool exchanges, and
> convert the entire interaction into a provider-neutral canonical event-driven
> result.

Monoloop is the one response processor shared by every supported channel.

It is not a chat application, an agent, a prompt engine, a memory system, a task
system, a model router, or a persistence service.

## 2. Product outcome

For each accepted request Monoloop returns immediately with a run handle:

```rust
pub trait Monoloop: Send + Sync {
    fn process(
        &self,
        request: MonoloopRequest,
    ) -> Result<MonoloopRun, MonoloopError>;
}

pub struct MonoloopRun {
    pub run_id: MonoloopRunId,
    pub events: CanonicalRuntimeEventSubscription,
    pub control: MonoloopRunControl,
    pub health: MonoloopRunHealth,
    pub completion: MonoloopRunCompletion,
}
```

The run emits canonical events in real time and resolves exactly once with a
terminal processing result.

## 3. Statelessness

Monoloop retains no product state between runs.

It has no:

- conversation or provider history;
- session memory;
- user/project memory;
- task, plan, review, or working state;
- durable current request;
- database or file store;
- model-routing history;
- global current channel/session/run; or
- background consolidation.

One active run necessarily owns bounded transient state:

- Connector and channel handles;
- Interpreter framing and assembly buffers;
- event distribution queues;
- The Loop's tool-action state;
- in-flight tool handles;
- outbound write state;
- cancellation and terminal coordination; and
- safe counters/diagnostics.

Every item is scoped to `MonoloopRunId` and destroyed when the run terminates.
This transient state does not violate stateless product semantics.

## 4. Concurrency model

Many runs may execute concurrently:

```text
Run A -> Channel A -> isolated transient state A
Run B -> Channel B -> isolated transient state B
Run C -> Channel C -> isolated transient state C
```

Runs do not share mutable request, interpretation, tool, event, cancellation, or
completion state.

Shared immutable configuration, Connector factories, dialect implementations,
tool descriptors, and bounded transport pools are permitted. Shared resources
must not create ambient current identity or cross-run correlation.

## 5. Canonical request

```text
MonoloopRequest
    run_id
    request_id
    selected_channel
    canonical_input
    tool_configuration
    limits
    deadline?
    caller_correlation?
    continuation_policy
```

`canonical_input` is a complete provider-neutral request product. Initially it
may contain one text prompt and minimal model-interaction options. It is never a
provider-native JSON object or raw wire body.

The caller explicitly selects `selected_channel`. Monoloop does not rank,
recommend, or choose channels.

`caller_correlation` is bounded opaque data returned unchanged in run events and
the terminal result. It grants no authority and cannot alter run behavior.

Initial continuation policies:

```text
inline_tool_continuation
caller_controlled
```

The Fabled Cognitive Runtime requires `caller_controlled` so every later model
decision receives a freshly compiled frame.

## 6. Channel definition

A Channel is a configured composition, not a new intelligence layer:

```text
ChannelBinding
    channel_id
    connector factory/configuration
    required dialect range
    canonical outbound encoder
    Interpreter factory/profile
    declared interaction capabilities
```

Examples:

```text
OpenAI channel
Grok Build channel
Cursor channel
local model channel
future channel
```

The selected Channel determines how canonical outbound products become dialect
bytes and which Interpreter decodes the returned dialect bytes. Its internal
parts remain separate components.

## 7. Outbound encoder seam

A mechanical outbound encoder is required:

```rust
pub trait OutboundDialectEncoder: Send + Sync {
    fn encode_request(
        &self,
        dialect: &DialectDescriptor,
        request: &CanonicalRequest,
    ) -> Result<EncodedOutboundMessage, EncodingError>;

    fn encode_tool_result(
        &self,
        dialect: &DialectDescriptor,
        result: &OutboundToolResult,
    ) -> Result<EncodedOutboundMessage, EncodingError>;
}
```

This is a required supporting seam whose detailed component specification is
deferred. Until it exists, tests may use a deterministic test dialect encoder.

The encoder:

- is selected by the Channel's negotiated input dialect;
- performs deterministic canonical-to-dialect encoding;
- returns complete bounded bytes plus safe metadata;
- contains no routing, prompting, tool execution, persistence, or session state;
  and
- is not implemented inside Console Input, Connector, Interpreter, or The Loop.

## 8. End-to-end composition

```text
caller
  -> MonoloopRequest + explicit ChannelBinding
  -> outbound dialect encoder
  -> Connector raw input
  -> external channel
  -> Connector raw output + dialect
  -> Interpreter
  -> canonical event distributor
       +-> caller event subscription
       +-> Console Renderer subscription (test only)
       +-> The Loop lossless subscription
              -> abstract ToolRegistry/ToolRuntime
              -> OutboundToolResult
              -> continuation policy
                   +-> inline: outbound dialect encoder -> Connector raw input
                   +-> caller-controlled: terminal continuation evidence
  -> canonical terminal events
  -> MonoloopRunEnd
  -> destroy all run state
```

No component may bypass this flow with a provider-specific side channel.

## 9. Run coordinator

Monoloop contains a minimal per-run coordinator. It owns composition and
lifecycle, not cognition.

It:

- validates the request and selected Channel;
- creates one run-scoped cancellation domain;
- begins Connector opening;
- waits for negotiated dialect binding;
- selects the exact encoder and Interpreter;
- creates run-scoped canonical event distribution;
- starts The Loop with its lossless subscription;
- encodes and writes the initial request;
- forwards Interpreter and Loop outputs into the run event stream;
- encodes complete outbound tool results when applicable;
- applies the Channel exchange-completion contract;
- coordinates cancellation and bounded shutdown; and
- emits exactly one run terminal result.

It does not inspect prompt content or decide what the model should know.

## 10. Run state machine

```text
created
    -> validating
        -> opening_channel
            -> starting_interpreter
                -> starting_loop
                    -> sending_request
                        -> receiving
                            -> tool_exchange
                                -> inline_continuation -> receiving
                                -> caller_controlled_finalizing
                            -> finalizing
                                -> completed

caller_controlled_finalizing
    -> continuation_required

any nonterminal state
    -> cancelling
        -> cancelled

any nonterminal state
    -> failed
```

The state machine is per run and disappears at terminal state.

`tool_exchange` may occur zero or more times under inline continuation. Under
caller-controlled continuation, the first completed tool-exchange set terminates
the run with continuation evidence instead of initiating another model decision.
With the initial empty tool registry, a tool request produces a canonical
unavailable result and follows the selected continuation policy.

## 11. Channel delegation

Monoloop delegates only to the Channel selected in the request.

It MUST NOT:

- silently substitute another Channel;
- retry against another provider/system;
- consult model ranking or cost policy;
- interpret a display label as a Channel identity;
- reuse an unrelated live connection; or
- fall back from a failed configured dialect to an unrequested dialect.

Unavailable, unsupported, failed, and cancelled Channel states remain distinct.

## 12. Initial request protocol

The coordinator performs:

```text
1. validate complete canonical request
2. validate explicit ChannelBinding
3. begin Connector open and retain immediate control
4. obtain negotiated DialectBinding
5. choose exact encoder and Interpreter implementation
6. start Interpreter and event distribution
7. start The Loop with a distinct lossless subscription
8. encode the canonical request
9. send complete encoded bytes to Connector input
10. finish/retain input according to Channel exchange contract
11. consume canonical events until terminal coordination
```

No provider request bytes are created before the actual negotiated dialect is
known unless the Channel declares a fixed dialect and qualification proves it.

## 13. Canonical runtime events

The run-level event vocabulary composes, without rewriting:

```text
Interpreter CanonicalUnitEvent
Interpreter InterpretationEnd
LoopOutputEvent
Monoloop lifecycle/diagnostic events
```

Every event carries `MonoloopRunId` in its run envelope plus its component-local
identities.

The run stream contains fully assembled canonical events. It never publishes
Connector byte chunks, provider tokens, partial text, or partial tool payloads.

## 14. In-memory event distribution

Each run owns a bounded event distributor. It provides independent
subscriptions for:

- The Loop: lossless and gap-detecting;
- the caller: declared lossless or best-effort policy;
- Console Renderer: test-only policy; and
- future run-scoped observers.

Requirements:

- one accepted event may be delivered to every admitted subscriber;
- subscribers never race for one queue entry;
- one subscriber's cursor/state is not another's;
- delivery sequence and event identity are explicit;
- actionable Loop events are never silently dropped;
- slow optional observers cannot create unbounded memory;
- gap/loss is explicit; and
- the distributor is destroyed with the run.

Monoloop has no process-global event bus.

## 15. Tool exchanges

The Loop receives complete canonical tool requests. It resolves and dispatches
through the configured abstract tool capability.

When The Loop emits `OutboundToolResult`, the coordinator:

1. validates run/action/result correlation;
2. publishes safe lifecycle observations; and
3. applies the request's immutable continuation policy.

For `inline_tool_continuation`, the coordinator selects the already bound input
dialect encoder, encodes the complete result, writes it to the same owned
Channel exchange when supported, and resumes receiving canonical output.

For `caller_controlled`, the coordinator does not write the tool result back for
another model decision. It drains owned work, emits the complete result, and
terminates with `continuation_required`. A caller may then compile a new request
and start a new run.

The coordinator does not reinterpret the result or create tool-specific logic.

If a Channel cannot accept a tool result, that capability mismatch is explicit.

## 16. Empty-tool behavior

The first Monoloop composition contains:

```text
EmptyToolRegistry
NoToolRuntime
```

A channel tool request therefore yields:

```text
ToolUnavailable(no_registered_tool)
OutboundToolResult(tool_unavailable)
```

Under inline continuation, the Channel contract determines whether the
unavailable result can be encoded and returned. Under caller-controlled
continuation, it is returned to the caller as terminal continuation evidence.
Monoloop never pretends the tool succeeded.

This behavior is required for the initial product qualification.

## 17. Response completion

Monoloop terminates a successful run only when all are true:

- the selected dialect has produced its qualified terminal response boundary;
- the Interpreter has emitted its compatible complete terminal report;
- no owned tool action is unresolved or running;
- no required outbound result/write remains pending;
- the Connector has reached the Channel's valid terminal/close condition; and
- all mandatory canonical events have been accepted by required subscribers.

Remote EOF alone is not necessarily success. A text sentence saying “done” is
not success. Tool request readiness is not response completion.

The exact Channel completion rule is versioned in `ChannelBinding`.

`continuation_required` is a truthful non-failure terminal result with a
different completion predicate: the inbound response reached a qualified tool
boundary, all owned tool actions and canonical outputs are terminal, no write is
pending, and bounded Connector/Interpreter/Loop teardown completed. It is not
reported as `completed`.

## 18. Run terminal result

```text
MonoloopRunEnd
    run_id
    request_id
    channel_id
    dialect_binding
    kind
    canonical_event_count
    interpreter_terminal_kind
    loop_terminal_summary
    connector_terminal_kind
    tool_actions_by_outcome
    bytes_sent
    bytes_received
    safe_diagnostics[]
```

Kinds:

```text
completed
continuation_required
cancelled
channel_open_failed
encoding_failed
connector_failed
interpretation_failed
tool_exchange_failed
event_distribution_failed
deadline_exceeded
invariant_failed
```

Exactly one terminal result is produced. It is a run-processing result, not a
task, plan, review, or product completion claim.

## 19. Cancellation

`MonoloopRunControl.cancel(reason)` is the one caller-facing cancellation entry
point.

The coordinator propagates cancellation concurrently to:

- Connector opening/connection control;
- Interpreter input/completion;
- The Loop and owned tool executions;
- pending outbound writes;
- run event distribution; and
- optional run-scoped test observers according to subscription ownership.

Cancellation is idempotent, preempts blocked work, uses bounded grace/escalation,
and produces one terminal result.

The coordinator does not depend on a UI button state to know cancellation was
requested or completed.

## 20. Failure isolation

- One run failure cannot cancel sibling runs.
- One Channel connection cannot receive another run's bytes.
- One Interpreter cannot consume another Connector output.
- One Loop cannot execute another run's tool action.
- One outbound result cannot be written to another connection.
- One optional Console Renderer failure cannot fail the run.
- A required event/output failure fails only its owning run.

All routing uses explicit identities and owned handles, never completion order
or global current state.

## 21. Async requirements

Monoloop is asynchronous from the first implementation:

- `process` returns a live handle immediately;
- connection, interpretation, loop, tools, event distribution, and output writes
  progress independently within one owned cancellation domain;
- all queues and concurrency are bounded;
- no blocking I/O runs on an async worker;
- no busy polling or UI polling drives progress;
- every spawned task has an owner and terminal result;
- completion handles are awaited/joined exactly once safely;
- shutdown wakes every blocked operation; and
- thousands of fake concurrent runs can be qualified without cross-run state.

Async does not permit detached fire-and-forget work.

## 22. Resource bounds

Each request supplies or resolves a qualified `MonoloopLimits`:

```text
connect/open deadline
overall run deadline
Connector input/output bounds
Interpreter assembly/correlation bounds
event distributor subscriber/queue/byte bounds
Loop action/execution/output bounds
outbound encoding/message bounds
cancellation grace and shutdown deadline
safe diagnostic limits
```

There is also a process composition limit on concurrent runs and aggregate
resources. Exceeding a limit is explicit and bounded; Monoloop never grows an
unbounded queue to remain apparently responsive.

## 23. No persistence

Monoloop does not open a database, file, history log, cache, session store, or
checkpoint repository.

Canonical events may be consumed by a caller that chooses to persist a higher-
level product. That consumer is outside Monoloop.

Console JSONL output is diagnostic output, not product persistence.

No run can be resumed after process loss. The caller may start a new request
with a new run identity.

## 24. No prompt engine

Monoloop receives a complete canonical request. It does not determine what
context belongs in that request.

The future Cognitive Context Engine may compile a request and pass it to
Monoloop. This dependency direction is one-way:

```text
Context Engine -> Monoloop
Monoloop -X-> Context Engine internals
```

Monoloop never reads memory, project files, task history, or conversation state
to improve a prompt.

## 25. No model router

The caller selects a Channel. Monoloop validates and uses it.

Routing may later occur before `Monoloop.process`, but no router state, ranking,
cost policy, preference model, or provider fallback enters this product.

This permits identical Monoloop behavior under deterministic test routing and
future intelligent routing.

## 26. No presentation dependency

Console Input and Console Renderer are test adapters.

Monoloop Core MUST compile and pass its complete non-console suite without:

- terminal detection;
- stdin/stdout/stderr;
- ANSI support;
- command-line parsing;
- product UI;
- Tauri; or
- any graphical renderer.

Deleting the console adapter package must not change Monoloop semantics.

## 27. Security and trust

Monoloop treats all channel output and canonical tool requests as untrusted.

Requirements:

- Channel configuration and dialect binding are explicit;
- credentials remain in Connector/Channel configuration boundaries;
- raw payloads are not logged or persisted;
- tool requests do not execute without an available abstract tool and later
  authorization contract;
- tool results remain correlated to their owning run/action;
- limits apply before attacker-controlled allocation growth;
- canonical events distinguish structural validity from authority; and
- cross-run injection/cancellation is rejected.

The initial empty tool registry provides zero external effects.

## 28. Product errors

Closed top-level families:

```text
request_invalid
channel_not_selected
channel_unsupported
channel_open_failed
dialect_unsupported
encoding_failed
connector_failed
interpretation_failed
tool_exchange_failed
event_distribution_failed
deadline_exceeded
cancelled
resource_limit_exceeded
identity_conflict
invariant_failed
```

Component errors retain their typed source. Monoloop does not flatten everything
into a provider error or string.

Errors contain bounded safe diagnostics and never raw credentials, prompts,
provider bodies, tool payloads, or unrestricted endpoint values.

## 29. Observability

Content-free product metrics include:

```text
monoloop_runs{state,channel_kind}
monoloop_run_duration{terminal_kind,channel_kind}
monoloop_events{kind}
monoloop_event_queue_depth{subscriber_kind}
monoloop_tool_actions{outcome}
monoloop_bytes{direction,channel_kind}
monoloop_cancellation_latency
monoloop_component_failure{component,error_kind}
monoloop_terminal{kind}
```

Labels are bounded. Run/request/session/project IDs, raw text, prompts, tool
payloads, paths, credentials, and provider error bodies are excluded.

## 30. Required tests

### 30.1 Statelessness

- A completed run leaves no request/session/conversation state.
- A new run cannot observe prior canonical events or tool actions.
- Equal request text does not cause implicit reuse.
- No database/file/cache API is reachable from Monoloop Core.
- Process restart requires a new run rather than hidden recovery.

### 30.2 Channel selection

- Exactly the selected Channel is used.
- Missing/unknown/unsupported Channel fails before input is sent.
- No fallback Channel is attempted.
- Concurrent runs using different Channel dialects remain isolated.
- Fixed and negotiated dialect selection obey their contracts.

### 30.3 End-to-end canonical processing

- Arbitrarily fragmented response bytes yield complete sentence events.
- Events appear before response completion.
- Tool request fragments do not escape or dispatch early.
- Complete canonical tool requests reach The Loop exactly once.
- Caller-controlled tool completion cannot trigger another model decision in
  the same run.
- Terminal run result waits for Interpreter, Loop, writes, and Channel contract.
- EOF/text “done” cannot counterfeit successful completion.

### 30.4 Empty-tool exchange

- Tool request produces deterministic unavailable lifecycle/result.
- No actual ToolRuntime execution occurs.
- Under inline continuation, when Channel accepts tool results, unavailable
  result is encoded/correlated correctly.
- Under caller-controlled continuation, unavailable result becomes terminal
  continuation evidence and no second request is sent.
- When Channel cannot accept tool results, capability mismatch is explicit.
- Monoloop never invents successful tool output.

### 30.5 Event distribution

- Caller, Console, and Loop receive independent subscribed copies.
- Console best-effort loss cannot remove Loop events.
- Loop gap causes fail-closed run behavior.
- Slow optional subscriber stays within bounds.
- Required event loss/output failure fails only its run.
- Distributor is destroyed and releases queues at run terminal.

### 30.6 Cancellation and races

- Cancel during Connector open terminates promptly.
- Cancel during blocked read/write/interpretation/tool execution propagates.
- Cancel concurrent with semantic completion yields one terminal outcome.
- Tool result concurrent with cancellation cannot be written cross-run or twice.
- Every child/task/handle is joined safely exactly once.
- Cancellation of one run leaves siblings unchanged.

### 30.7 Concurrency and load

- Many simultaneous runs across multiple Channel kinds remain isolated.
- One slow Connector/Interpreter/tool/subscriber does not globally block others.
- Aggregate run limits reject excess work explicitly.
- Memory remains bounded under long responses and tool floods.
- Completion order never changes correlation.
- No process-global current identity appears under stress.

### 30.8 Architecture

- Monoloop Core does not import host agent, product UI, Kanban, DAL, Residiuum,
  context compiler, memory, router, or concrete tool modules.
- Console adapters are absent from the Core dependency graph.
- Connector contains no encoder/interpreter logic.
- Interpreter contains no Loop/tool execution logic.
- The Loop contains no concrete tool/encoder/Connector calls.
- No component persists run data.
- No unbounded channel or detached task exists.
- No provider-native DTO crosses the canonical boundary.

## 31. Acceptance criteria

Monoloop is accepted only when:

1. one canonical request and explicit Channel produce one isolated run;
2. real-time output consists solely of fully assembled canonical units and
   truthful lifecycle states;
3. one implementation processes at least an HTTP streaming Channel, a
   process/agent Channel, and the deterministic test Channel;
4. arbitrary transport fragmentation does not affect canonical output;
5. tool requests dispatch only after complete assembly;
6. the empty tool configuration causes zero external effects and one truthful
   unavailable result;
7. outbound request/result encoding, when required by the continuation policy,
   is isolated by dialect and deterministic;
8. successful completion requires the complete cross-component terminal
   contract;
9. cancellation terminates every owned path within bounds and exactly once;
10. many concurrent runs remain identity- and resource-isolated;
11. every queue, buffer, table, execution, and deadline is bounded;
12. terminal cleanup retains no run/session/conversation state;
13. Monoloop has no persistence, memory, router, prompt compiler, task system,
    agent, host runtime, product UI, or concrete-tool dependency;
14. console input/output can be removed without changing Core;
15. architecture gates enforce all component dependency directions;
16. all component and product suites pass without partial or “shaped”
    qualification; and
17. caller-controlled continuation returns complete tool evidence without
    creating an uncompiled model continuation.

## 32. Product package boundary

The preferred logical packaging is:

```text
monoloop-contracts
    canonical request/events, identities, errors and port contracts

monoloop-connector
    abstract Connector contract and shared transport primitives

monoloop-interpreter
    canonical Interpreter and dialect plugin contracts

monoloop-loop
    tool-reactive Loop and abstract tool ports

monoloop-core
    run coordinator, Channel composition and public facade

monoloop-console
    test-only Console Input and Console Renderer

monoloop-conformance
    deterministic Channel/Connector/dialect/tool fixtures and qualification
```

Physical crate consolidation is permitted initially, but dependency rules and
public seams apply from the first implementation.

## 33. Prohibited shortcuts

- Store conversation history “temporarily.”
- Put prompt augmentation into Console Input or coordinator.
- Put request encoding into Connector.
- Let Console and Loop compete for one receiver.
- Feed Interpreter fragments directly to UI or tool execution.
- Hard-code OpenAI, Grok Build, Cursor, or tool behavior into Core.
- Treat remote EOF, final text, or Interpreter completion as sufficient run
  success.
- Add a database for diagnostics or resumability.
- Use global current session/channel/tool state.
- Retry through another Channel implicitly.
- Claim statelessness while retaining hidden provider sessions between runs.
- Make console adapters required dependencies of the product.

## 34. Governing rule

> Monoloop accepts one canonical request and one explicitly selected Channel. It
> processes the complete interaction into canonical real-time events, returns
> one truthful terminal result, and forgets everything.
