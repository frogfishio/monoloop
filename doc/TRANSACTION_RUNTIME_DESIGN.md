# Transaction Runtime Design

**Status:** Accepted architecture  
**Implements:** `REQUIREMENTS.md` R-000 through R-004  
**Development contract:** `TRANSACTION_RUNTIME_IMPLEMENTATION.md`

This design turns the accepted requirements into one production architecture.
It intentionally avoids provider-specific orchestration, dynamic tool loading,
persistence, prompt construction, and presentation concerns.

## 1. Design decision

Monoloop remains three product components:

```text
Connector -> Interpreter -> Loop
```

The accepted transaction API requires Component 3, the Loop, to become the
composition owner for one complete transaction. The current Loop's canonical
event and tool state machine remains useful, but the production Loop must also:

- admit and correlate transactions;
- compose the selected Connector and Interpreter;
- expose canonical events;
- execute request-scoped tools;
- coordinate direct-LLM continuations;
- terminate all transaction-owned work; and
- publish exactly one completion callback.

This is a deliberate evolution of the current empty-tool Loop. It avoids a
fourth product component and gives `loopRequest(...)` one real production owner.

The component boundaries remain strict:

- Connector owns transport, authentication, external-session routing, raw
  bytes, cancellation, and one transport terminal result.
- Interpreter owns incremental dialect decoding and complete canonical units.
- Loop owns transaction admission, composition, event sequencing, tool
  execution, continuation, and one transaction terminal result.
- Outbound encoders and MCP are adapters used by the Loop. They do not become
  independent decision-making components.

## 2. Public transaction API

The API is push-based. Submission performs bounded synchronous admission and
returns without waiting for the transaction to finish.

Conceptually:

```rust
pub struct TransactionRequest {
    pub channel_id: ChannelId,
    pub session_id: Option<SessionId>,
    pub input: CanonicalInput,
    pub config: InvocationConfig,
    pub tools: Vec<ToolId>,
    pub events: TransactionEventSink,
    pub completion: CompletionCallback,
}

pub struct AdmissionReceipt {
    pub transaction_id: TransactionId,
    pub session_id: Option<SessionId>,
}

pub enum TransactionSelector {
    Transaction(TransactionId),
    Session(SessionId),
}

pub trait TransactionRuntime: Send + Sync {
    fn submit(
        &self,
        request: TransactionRequest,
    ) -> Result<AdmissionReceipt, AdmissionError>;

    fn terminate(
        &self,
        selector: TransactionSelector,
        mode: TerminationMode,
    ) -> TerminationDisposition;
}
```

`AdmissionReceipt.transaction_id` is always present and permits termination
while a new external session is still being created.
`AdmissionReceipt.session_id` is present immediately when supplied by the caller
or generated for a direct LLM. When an external system must create its own
session asynchronously, the authoritative ID is delivered in the first
`SessionEstablished` event and every subsequent ordinary event.

The completion callback is a single-use asynchronous callback as specified in
`TRANSACTION_RUNTIME_IMPLEMENTATION.md`. It is not a future that the caller
must poll to drive transaction progress.

## 3. Core contracts

The provider-neutral contracts belong in `monoloop-contracts`.

### 3.1 Identity

```text
SessionId
    caller-supplied or ephemeral direct-LLM correlation identity

ExternalSessionId
    authoritative session identity returned by an external agent

TransactionId
    public control identity for one admitted transaction

MonoloopRunId
    internal component identity derived one-to-one from TransactionId
```

For an external-agent transaction, the effective public `SessionId` is the
external system's authoritative session ID. For a direct LLM it is only an
ephemeral correlation key.

`TransactionId` is returned by admission and permits termination before a new
external session has produced its ID. `MonoloopRunId` remains internal. Both
prevent a late task from an older transaction from being accepted after the
same external session starts a later transaction.

### 3.2 Canonical input

The first production schema should be deliberately small:

```text
CanonicalInput
    ordered messages[]

CanonicalMessage
    role
    content_parts[]

ContentPart
    text
```

The enum can gain new versioned content parts later without changing prompt
ownership. Monoloop validates bounds and order but never creates, rewrites, or
improves messages.

### 3.3 Transaction events

```rust
pub struct TransactionEvent {
    pub transaction_id: TransactionId,
    pub session_id: SessionId,
    pub sequence: u64,
    pub payload: TransactionEventPayload,
}

pub enum TransactionEventPayload {
    SessionEstablished,
    CanonicalUnit(CanonicalUnitEvent),
    ToolLifecycle(ToolLifecycleEvent),
    Diagnostic(TransactionDiagnostic),
    Ended(TransactionEnd),
}
```

The transaction actor allocates `sequence`. Neither Connector tasks,
Interpreter tasks, MCP handlers, nor tool tasks may publish directly to caller
sinks. They send internal commands to the actor, which validates run identity,
assigns sequence, and publishes in order.

### 3.4 Completion

`TransactionEnd` contains the transaction ID, effective session ID when one was
established, terminal kind, event count, safe diagnostics, and bounded usage
facts. It does not contain presentation output.

Terminal kinds are closed and truthful:

```text
Completed
ContinuationRequired
Cancelled
Terminated
DeadlineExceeded
ChannelOpenFailed
EncodingFailed
ConnectorFailed
InterpretationFailed
ToolExchangeFailed
EventDeliveryFailed
LimitExceeded
InvariantFailed
```

An admission error is returned synchronously and does not create a transaction
or invoke the completion callback.

`TransactionEnd.session_id` is optional only when creation of a new external
session fails or is terminated before the external system returns its
authoritative identity. `TransactionId` remains available in that result.

## 4. Runtime ownership

`DefaultTransactionRuntime` owns only bounded, ephemeral process state:

```text
DefaultTransactionRuntime
    immutable ChannelRegistry
    immutable HostToolRegistry
    bounded ActiveTransactionRegistry
    shared McpGateway
    callback executor
    global limits
```

### 4.1 Active transaction registry

The registry is keyed by effective `SessionId`.

Admission:

1. validate request bounds;
2. resolve the selected Channel;
3. determine or provision the session identity;
4. resolve all tool IDs against the host registry;
5. reserve the session ID;
6. attach the event sink and completion callback;
7. spawn one transaction actor; and
8. return the admission receipt.

The reservation and duplicate check occur in one short synchronous critical
section with no I/O and no `.await`.

A second transaction for an active session ID is rejected immediately. It is
never queued.

For an external session created asynchronously, admission first reserves the
internal run ID. When the Connector returns the authoritative session ID, the
actor atomically claims that ID before publishing `SessionEstablished`. A
collision fails that transaction before its prompt is sent.

### 4.2 One actor per transaction

Each admitted transaction has one actor that exclusively owns its mutable state:

```text
TransactionActor
    state
    effective session identity
    run identity
    selected ChannelBinding
    ResolvedToolSet
    Connector handles
    Interpreter handles
    Loop tool state
    event sequence
    terminal selector
    child-task set
    cancellation token
```

All asynchronous producers report through one bounded command channel. The
actor is the only code allowed to:

- advance transaction state;
- publish caller events;
- start a tool call;
- send a continuation;
- select terminal state; or
- release the active-session reservation.

This actor model removes lock ordering from transaction logic and makes terminal
races deterministic.

No actor lock is held across `.await`. All child tasks are tracked and joined or
aborted during bounded teardown.

## 5. State machine

```text
admitted
  -> opening_channel
  -> establishing_session
  -> activating_tools
  -> sending
  -> receiving
       -> executing_tools
       -> sending_continuation
       -> receiving
  -> finalizing
  -> terminal

any nonterminal state
  -> cancelling
  -> terminal

any nonterminal state
  -> terminating
  -> terminal

any nonterminal state
  -> failed
  -> terminal
```

For external agents, provider-owned inner turns stay inside `receiving`.
MCP calls may enter `executing_tools`, but their results return through MCP and
the external agent controls its next inner turn.

For direct LLMs, a canonical tool request enters `executing_tools`. The Loop
encodes the tool result as a continuation, sends it to the same Channel, and
returns to `receiving`. All model/tool/model cycles remain one transaction.

## 6. Channel architecture

A `ChannelBinding` is configuration plus implementations:

```text
ChannelBinding
    channel_id
    channel_kind
    Connector factory/config
    outbound dialect encoder
    Interpreter factory/profile
    session adapter
    tool execution mode
    capabilities
    static defaults
    limits
```

Channel kinds:

```text
ExternalAgent
DirectLlm
```

Tool execution modes:

```text
McpGateway
ModelToolCalls
None
```

This mode is important. An external agent may emit observational tool events
while also invoking the actual tool through MCP. Those observed events must not
trigger a second local execution. Only the configured authoritative path may
execute tools.

### 6.1 Provider configuration

Direct-LLM providers are data, not Rust branches:

```text
provider profile
    endpoint
    credential reference
    request dialect
    response dialect
    model/default options
    declared capabilities
    bounded compatibility flags
```

OpenAI, OpenRouter, Together, Ollama, or vLLM can reuse one HTTP Connector and
one dialect implementation when they conform to that dialect.

A materially different request or streaming protocol gets a different encoder
or Interpreter dialect, not a provider-specific transaction handler.

### 6.2 Configuration merge

Before transport opens, the Channel deterministically builds an immutable
effective configuration:

```text
Channel defaults
  <- session configuration
  <- permitted invocation overrides
```

Unknown options and attempts to change immutable external-session configuration
fail explicitly. The Connector receives transport configuration and encoded
bytes; it does not interpret model options.

## 7. Host tool architecture

### 7.1 Static host registry

The host constructs one immutable registry during startup:

```rust
pub struct ToolSpec {
    pub id: ToolId,
    pub name: ToolName,
    pub description: String,
    pub input_schema: JsonSchema,
    pub output_contract: ToolOutputContract,
    pub limits: ToolLimits,
}

pub trait ToolHandler: Send + Sync {
    fn start(
        &self,
        call: ToolCall,
        context: ToolCallContext,
    ) -> Result<ToolExecutionHandle, ToolStartError>;
}

pub struct RegisteredTool {
    pub spec: ToolSpec,
    pub handler: Arc<dyn ToolHandler>,
}
```

The registry rejects duplicate IDs, duplicate public names, invalid schemas,
unbounded descriptions, and incompatible declarations at host construction.

Requests contain only `ToolId` values.

### 7.2 Resolved tool set

Admission resolves the requested IDs once:

```text
HostToolRegistry + requested ToolId[]
    -> immutable ResolvedToolSet
```

`ResolvedToolSet` contains bounded lookup maps by ID and public name. Both MCP
and model dialects project definitions from this exact object. Neither path may
construct or alter a separate tool schema.

### 7.3 One execution path

Both exposure mechanisms call one `TransactionToolDispatcher`:

```text
MCP tools/call --------------------+
                                    +-> TransactionToolDispatcher
direct-LLM canonical tool request -+      -> validate active run
                                           -> validate allowlist
                                           -> validate payload schema
                                           -> enforce limits
                                           -> start linked ToolHandler
                                           -> emit lifecycle
                                           -> return canonical result
```

The dispatcher requires `SessionId`, `MonoloopRunId`, `ToolActionId`, and
`ToolId`. A late request with the correct session but an old run ID is rejected.

Tool execution is asynchronous. `ToolExecutionHandle` must expose:

- execution ID;
- cancellation control;
- one completion future consumed by the transaction actor; and
- a declared cancellation behavior.

The current `Available -> DispatchRejected` branch is replaced only when this
handle and its conformance tests exist. It must not be relabelled complete while
real execution remains deferred.

## 8. MCP hosting

### 8.1 One gateway, scoped bindings

Monoloop hosts one bounded MCP gateway per runtime, not one server process per
transaction.

The initial transport is MCP Streamable HTTP on a loopback listener. Each
external session attachment receives an unguessable capability URL/token:

```text
http://127.0.0.1:<port>/mcp/<capability>
```

The token is a secret routing capability. It is never logged, emitted as a
diagnostic, or accepted from transaction request configuration.

The gateway owns bounded `McpSessionBinding` entries:

```text
McpSessionBinding
    capability token
    external session attachment
    active run ID?
    active ResolvedToolSet?
    transaction command sender?
    limits
```

The MCP descriptor is supplied through the existing `mcpServers` session
configuration when an external session is created or attached.

### 8.2 Request-scoped availability on a persistent session

External sessions persist across transactions, but requested tools change.
Therefore the MCP endpoint remains attached to the external session while its
active tool set changes atomically:

```text
before prompt send
    bind active run + ResolvedToolSet

during transaction
    tools/list -> active set only
    tools/call -> active set only

before terminal callback
    clear active run + tool set
```

Because Monoloop permits only one in-flow transaction per session, MCP requests
cannot be ambiguous.

When no transaction is active, `tools/list` returns an empty set and
`tools/call` returns a typed inactive-session error.

A request using a stale binding, stale run, unknown tool, or tool outside the
active allowlist fails closed and never reaches a handler.

### 8.3 MCP-to-Loop flow

```text
external agent
  -> MCP tools/call
  -> McpGateway authenticates capability
  -> TransactionActor command
  -> TransactionToolDispatcher
  -> linked ToolHandler
  -> canonical tool events
  -> MCP result
  -> external agent continues internally
```

The MCP response is the tool result transport for an external agent. The Loop
does not also encode that result into the agent's model stream.

If a Connector profile cannot install or refresh the Monoloop MCP descriptor
for an attached session, a tool-enabled transaction is rejected. Monoloop must
not claim tool parity on a profile that cannot provide it.

## 9. Direct-LLM flow

```text
TransactionRequest
  -> resolve provider profile and tools
  -> OpenAI-compatible outbound encoder
       input + effective config + ResolvedToolSet
  -> generic HTTP Connector
  -> streaming response body
  -> OpenAI dialect Interpreter
  -> canonical events
  -> canonical tool request?
       -> TransactionToolDispatcher
       -> linked ToolHandler
       -> canonical result
       -> outbound encoder continuation
       -> same transaction receives again
  -> final model result
  -> TransactionEnd
```

The first protocol implementation is OpenAI Chat Completions v1 over streaming
HTTP/SSE. OpenAI Responses is a separate later dialect and is not accepted by
the Chat Completions profile.

The generic HTTP Connector owns HTTP/TLS/authentication/body streaming and
cancellation. It does not inspect prompts, tools, model options, or SSE event
semantics.

## 10. Event delivery

Every transaction has one required caller event sink attached at admission.
The sink uses a bounded queue and is lossless.

The transaction actor publishes events in order. If the required sink closes,
the transaction fails as `EventDeliveryFailed`. If it stops consuming,
backpressure affects only that transaction; global and per-transaction
deadlines still apply.

Optional presentation/demo subscribers are separate best-effort consumers and
cannot weaken the required caller stream or the Loop's internal tool stream.

Presentation projection is downstream. It never feeds back into transaction
state.

## 11. Terminal protocol

Terminal selection is actor-owned and idempotent.

The finalization order is:

1. select the terminal kind once;
2. reject new actor and MCP tool commands;
3. clear the active MCP tool binding;
4. cancel Connector, Interpreter, Loop, and running tools as required;
5. join or abort all transaction child tasks within cleanup limits;
6. deliver the final `Ended` transaction event and await bounded acceptance;
7. report `EventDeliveryFailed` through completion if final delivery fails;
8. close the event stream;
9. remove the active-session reservation;
10. destroy all transaction-owned state; and
11. schedule the single completion callback on the bounded callback executor.

The reservation is removed before callback invocation, so a callback may submit
the next transaction for the same session.

No producer can publish after terminal selection because producers do not own
the caller sink and their commands are rejected by run ID.

## 12. Cancellation and termination

`terminate(session_id, mode)` sends a high-priority command to the actor.

Cancellation and termination are raced against every blocking phase:

- Connector open;
- external session create/load;
- request send;
- response receive;
- Interpreter publication;
- tool queue admission;
- tool execution;
- continuation send; and
- final cleanup.

The actor never awaits an unbounded provider or tool operation directly.
Operations run in tracked child tasks and report completion through the actor
command channel. Termination can therefore preempt them.

Connector cancel/terminate, tool cancel, and task abort have bounded escalation
deadlines. Expiry changes the terminal diagnostic but cannot prevent local
transaction completion.

## 13. Bounds

The runtime enforces configuration for:

- global active transactions;
- active transactions per Channel;
- one active transaction per session;
- actor command queue;
- event queue;
- callback queue and callback concurrency;
- MCP session bindings and requests;
- tools per transaction;
- tool schema and payload bytes;
- queued and concurrent tool calls;
- per-tool concurrency;
- Connector, Interpreter, and Loop queues;
- diagnostics;
- transaction deadline; and
- cleanup deadline.

Zero and contradictory limits fail configuration validation. Capacity is
enforced in the unit named by the contract: byte limits are not approximated by
unrelated message counts.

## 14. Ephemeral shutdown

Monoloop persists nothing.

Runtime shutdown:

1. rejects new admissions;
2. sends termination to every actor;
3. waits only for the configured global shutdown deadline;
4. aborts remaining local tasks and processes;
5. closes the MCP listener;
6. releases registries, callbacks, and routing entries; and
7. exits without writing recovery state.

Outstanding external sessions remain owned by their external systems.

## 15. Package changes

Keep the package set small:

- `monoloop-contracts`: transaction, channel, canonical input, tool, and error
  contracts.
- `monoloop-connector`: existing transport API and a generic HTTP Connector
  implementation/profile.
- `monoloop-interpreter`: OpenAI dialect decoders in addition to existing
  profiles.
- `monoloop-loop`: transaction runtime, actor, active registry, event
  distribution, host tool registry, dispatcher, and MCP gateway adapter.
- `monoloop-testkit`: deterministic fake Channels, fake MCP client, fault
  injection, callback recorder, and presentation demos only.

Start MCP as a module of `monoloop-loop`. Extracting it into another package is
justified only if dependency isolation or independent protocol qualification
requires it. A package split must not create another product responsibility.

## 16. Implementation sequence

Each slice is complete and honestly labelled within its scope.

### Slice 1: transaction kernel

- contracts and public submission API;
- active-session admission and duplicate rejection;
- transaction actor and state machine;
- canonical event sink and completion callback;
- unified cancellation/termination;
- fake Channel end-to-end tests.

### Slice 2: real local tools

- `ToolSpec`, immutable host registry, and admission resolution;
- `ResolvedToolSet`;
- asynchronous `ToolExecutionHandle`;
- dispatcher, limits, cancellation, and canonical lifecycle;
- direct fake-model tool continuation tests.

### Slice 3: MCP parity

- loopback MCP gateway and capability bindings;
- `tools/list` and `tools/call`;
- external-agent Channel wiring through `mcpServers`;
- stale-call and cross-session isolation tests;
- parity tests proving MCP and local paths use the same registry entry.

### Slice 4: direct OpenAI-compatible LLM

- generic HTTP Connector;
- one explicitly selected OpenAI dialect encoder and Interpreter;
- streaming text;
- model tool calls and inline continuation;
- two provider configurations using the same implementation.

### Slice 5: connector migration and qualification

- adapt all six external profiles to `ChannelBinding`;
- verify session creation/reuse and MCP support honestly per profile;
- concurrent multi-session qualification;
- remove connector-local prompt shortcuts that bypass canonical encoding.

No slice is marked complete with an `Available -> DispatchRejected` production
branch, conditional test assertion, ignored error, or demo-only proof.

## 17. Verification strategy

Deterministic tests are the primary gate.

Required scenarios include:

- duplicate active session admission;
- generated direct-LLM session identity;
- external session creation and reuse;
- many concurrent sessions with no cross-routing;
- cancel/terminate at every state-machine phase;
- simultaneous completion/cancel/timeout races;
- one final event and one callback under every race;
- event sequence continuity and subscriber loss;
- different tool sets on concurrent transactions;
- unknown, duplicate, and disallowed tools;
- schema-invalid and oversized tool payloads;
- tool concurrency and queue limits;
- tool cancellation and cleanup;
- MCP inactive, stale, unauthorized, and cross-session calls;
- identical MCP/local tool schema and implementation identity;
- direct-LLM model/tool/model continuation;
- malformed and oversized provider streams;
- Connector, Interpreter, tool, subscriber, and callback failures;
- runtime shutdown with active work; and
- zero leaked actors, tasks, processes, routes, or callbacks.

Tests use barriers and paused time to force race orderings. Live providers are
qualification evidence only.

The implementation is complete only when the R-000 verification gate passes and
all acceptance criteria implemented by the delivered slice have direct,
non-conditional tests.
