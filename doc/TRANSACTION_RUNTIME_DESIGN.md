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
    pub session_config: Option<SessionConfig>,
    pub invocation_config: InvocationConfig,
    pub tools: Vec<ToolId>,
    pub events: Arc<dyn TransactionEventSink>,
    pub completion: Box<dyn CompletionCallback>,
}

pub struct AdmissionReceipt {
    pub transaction_id: TransactionId,
    pub session_id: Option<SessionId>,
}

pub enum TransactionSelector {
    Transaction(TransactionId),
    Session(SessionKey),
}

pub enum TerminationMode {
    Cancel { reason: CancellationReason },
    ForceTerminate { reason: TerminationReason },
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

    fn shutdown(&self, deadline: Duration) -> Shutdown;
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

Concrete runtime construction is asynchronous through
`DefaultTransactionRuntime::start(RuntimeBootstrap)`. It validates registries,
constructs matched Connector/SessionAdapter instances, binds MCP, and exposes
the runtime only after startup succeeds.

## 3. Core contracts

The provider-neutral contracts belong in `monoloop-contracts`.

### 3.1 Identity

```text
SessionId
    caller-supplied or ephemeral direct-LLM correlation identity

SessionKey
    ChannelId + SessionId; registry and control identity

ExternalSessionId
    authoritative session identity returned by an external agent

TransactionId
    public control identity for one admitted transaction

ExchangeId
    identity for one provider request/response cycle inside a transaction

MonoloopRunId
    internal component identity derived one-to-one from TransactionId
```

For an external-agent transaction, the effective public `SessionId` is the
external system's authoritative session ID. For a direct LLM it is only an
ephemeral correlation key. Provider session strings are not assumed globally
unique: exclusion, lookup, and session-directed termination use
`SessionKey { channel_id, session_id }`.

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
    System(text parts)
    User(text parts)
    Assistant(text parts?, canonical tool calls[])
    Tool(tool_call_id, text parts)

CanonicalAssistantToolCall
    tool_call_id
    tool_name
    arguments
```

This can faithfully carry historical assistant tool calls and their tool-result
messages, including assistant tool-call messages without text. The enum can gain
new versioned content later without changing prompt ownership. Monoloop
validates bounds, references, and order but never creates, rewrites, or improves
messages.

### 3.3 Transaction events

```rust
pub struct TransactionEvent {
    pub transaction_id: TransactionId,
    pub channel_id: ChannelId,
    pub session_id: SessionId,
    pub sequence: u64,
    pub payload: TransactionEventPayload,
}

pub enum TransactionEventPayload {
    SessionEstablished { external_session_id: ExternalSessionId },
    CanonicalUnit(CanonicalUnitEvent),
    ToolLifecycle(ToolLifecycleEvent),
    Diagnostic(TransactionDiagnostic),
    Ended(TransactionEnd),
}
```

The runtime-owned event sequencer in the transaction's `FinalizationGuard`
allocates `sequence`. The live actor is its only ordinary caller; after an actor
is aborted and joined, the shutdown supervisor may use it once for `Ended`.
Connector tasks, Interpreter tasks, MCP handlers, and tool tasks never publish
directly to caller sinks.

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
RuntimeShutdown
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

An asynchronous `DefaultTransactionRuntime::start(RuntimeBootstrap)` validates
registries and Channel combinations, constructs each Channel's matched
Connector/SessionAdapter instance, binds the MCP listener, and returns only
after the runtime is accepting submissions. Partial startup is cleaned up and
returned as a typed `StartupError`.

### 4.1 Active transaction registry

The registry always has a `TransactionId` index. Once a session is known it also
has a `SessionKey { ChannelId, SessionId }` index. The same opaque provider
session string may therefore be active on different Channels without collision,
while a duplicate on the same Channel is rejected.

Admission:

1. validate request bounds;
2. resolve the selected Channel;
3. determine or provision the session identity;
4. resolve all tool IDs against the host registry;
5. reserve the TransactionId and, when already known, the SessionKey;
6. attach the event sink and completion callback;
7. spawn one transaction actor; and
8. return the admission receipt.

The reservation and duplicate check occur in one short synchronous critical
section with no I/O and no `.await`.

A second transaction for an active session key is rejected immediately. It is
never queued.

For an external session created asynchronously, admission first reserves the
internal run ID. When the Connector returns the authoritative session ID, the
actor atomically claims the selected Channel/session pair before publishing
`SessionEstablished`. A collision fails that transaction before its prompt is
sent.

### 4.2 One actor per transaction

Each admitted transaction has one actor that exclusively owns its mutable state:

```text
TransactionActor
    state
    effective session identity
    run identity
    selected ChannelBinding
    ResolvedToolSet
    per-ExchangeId Connector/Interpreter handles
    Loop tool state
    shared exactly-once FinalizationGuard / EventSequencer handle
    child-task set
    cancellation token
```

All asynchronous producers report through one bounded command channel. During
normal operation the actor is the only code allowed to:

- advance transaction state;
- publish caller events;
- start a tool call;
- send a continuation;
- select terminal state; or
- release the active-session reservation.

The sole exception is forced runtime shutdown after the actor has been aborted
and joined: the shutdown supervisor may claim the transaction's
`FinalizationGuard` to publish/attempt `Ended`, release routing, and invoke the
callback.

This actor model removes lock ordering from transaction logic and makes terminal
races deterministic.

No actor lock is held across `.await`. All child tasks are tracked and joined or
aborted during bounded teardown.

## 5. State machine

```text
admitted
  -> establishing_session
  -> activating_tools
  -> opening_channel
  -> sending
  -> receiving
       -> executing_tools
       -> sending_continuation
       -> receiving
  -> finalizing
  -> terminal

any nonterminal state
  -> cancelling
  -> finalizing
  -> terminal

any nonterminal state
  -> terminating
  -> finalizing
  -> terminal

any nonterminal state
  -> failed
  -> finalizing
  -> terminal
```

For external agents, provider-owned inner turns stay inside `receiving`.
MCP calls may enter `executing_tools`, but their results return through MCP and
the external agent controls its next inner turn.

`DirectLlm` skips `establishing_session`. External agents always establish an
owned attachment before transaction-level Connector open. `activating_tools` is
a validated no-op except for `McpGateway`, where descriptor
installation/refresh, SessionKey claim, and route activation must complete
before opening/sending.

For direct LLMs, a canonical tool request enters `executing_tools`. The Loop
encodes the tool result as a continuation, sends it to the same Channel, and
returns to `receiving`. All model/tool/model cycles remain one transaction.
Each initial request and continuation is nevertheless a distinct exchange with
a fresh `ExchangeId`, `ConnectionId`, and `InterpretationId`. A completed
Interpreter is never reused for another HTTP response.

## 6. Channel architecture

A `ChannelBinding` is configuration plus implementations:

```text
ChannelBinding
    channel_id
    channel_kind
    ConnectorFactory producing one matched Connector/SessionAdapter instance
    outbound dialect encoder
    Interpreter factory/profile
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

External-agent capabilities separately declare MCP configuration as
`None`, `CreationOnly`, or `Refreshable`. `CreationOnly` supports tools only
while creating a new external session and makes that attachment ineligible for
later Monoloop transactions. `Refreshable` is required for request-scoped tool
changes on a reused session.

This mode is important. An external agent may emit observational tool events
while also invoking the actual tool through MCP. Those observed events must not
trigger a second local execution. Only the configured authoritative path may
execute tools.

Session attachment and transport opening cannot be assembled from unrelated
instances. A `ConnectorFactory` produces one `ConnectorInstance` containing the
Connector and, for external-agent Channels, its matching `SessionAdapter`.
Attachments carry an opaque route and owner identity that the Connector
validates on open.

That instance supports bounded concurrent work for distinct SessionKeys; a
profile may use bounded internal semaphores but cannot hold one async mutex
across unrelated provider operations. Transaction-level order is fixed:
external attach/create/load before exchange open, while direct LLMs skip
attachment and open directly.

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
    pub cancellation: ToolCancellationPolicy,
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
                                           -> validate declared output
                                           -> emit lifecycle
                                           -> return canonical result
```

The dispatcher requires `SessionKey`, `TransactionId`, `ExchangeId` where
applicable, `ToolActionId`, and `ToolId`. A late request with the correct
session but an old transaction/exchange identity is rejected.

Tool execution is asynchronous. `ToolExecutionHandle` must expose:

- execution ID;
- cancellation control;
- one completion future consumed by the transaction actor; and
- a declared cancellation behavior.

Supported cancellation behavior is cooperative within a bounded grace period,
directly abortable, or isolated behind a killable execution boundary.
Unstoppable in-process execution is rejected at registry construction because
it cannot satisfy bounded transaction teardown.

Tool completion distinguishes:

- canonical success;
- declared bounded domain failure, which is returned to MCP/the model as a tool
  result; and
- runtime failure, panic, lost completion, or output-contract violation, which
  fails the transaction as `ToolExchangeFailed`.

Successful output and domain errors are validated against `ToolOutputContract`
and byte limits before publication.

The current `Available -> DispatchRejected` branch is replaced only when this
handle and its conformance tests exist. It must not be relabelled complete while
real execution remains deferred.

## 8. MCP hosting

### 8.1 One gateway, transaction-scoped bindings

Monoloop hosts one bounded MCP gateway per runtime, not one server process per
transaction. Routing capabilities are nevertheless unique per transaction.

The initial transport is MCP Streamable HTTP on a loopback listener. Each
`McpGateway` external-agent transaction, including one with an empty tool set,
receives a new unguessable capability URL/token:

```text
http://127.0.0.1:<port>/mcp/<transaction-capability>
```

The token is a secret routing capability. It is never logged, emitted as a
diagnostic, or accepted from transaction request configuration.

The gateway first creates bounded disabled pending entries:

```text
PendingMcpTransactionBinding
    capability token
    TransactionId
    ResolvedToolSet
    transaction command sender
    limits
```

For a new external session, the descriptor is included in session creation;
after the authoritative ID is returned and `SessionKey` is claimed, the route
activates as an immutable `McpTransactionBinding`. For a reused session, its
owning `SessionAdapter` refreshes the `mcpServers` descriptor and the route
activates only after confirmation. A pending route rejects calls as not ready.
Only an active route may receive a prompt-associated tool call.

### 8.2 Request-scoped availability on a persistent session

External sessions persist across transactions, but requested tools change.
Therefore the MCP descriptor and capability rotate for every transaction:

```text
before prompt send
    create pending capability + ResolvedToolSet binding
    install during new-session creation or refresh existing session
    claim SessionKey and activate route

during transaction
    tools/list -> active set only
    tools/call -> active set only

before terminal event
    revoke and remove capability
    bounded attempt to remove descriptor from external session
```

Capability values are never reused. A delayed call from an older transaction
therefore addresses a removed route and cannot enter a newer transaction.
Unknown, revoked, stale, cross-session, or out-of-allowlist calls fail closed
and never reach a handler.

Local revocation happens before attempting external descriptor removal, so an
unresponsive external session cannot keep the capability valid.

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

If a Connector profile can install but not refresh the Monoloop MCP descriptor,
it may qualify as `CreationOnly`; it must reject supplied/reused sessions and
must not advertise reusable-session tool parity. A `None` profile accepts only
empty tool sets. Monoloop does not claim parity beyond the capability a profile
actually proves.

The initial gateway is loopback-only. A Channel qualifies it only when the
external agent shares the Monoloop host's loopback network namespace. Remote or
container-isolated agents require a separately secured transport profile.

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

One tracked `ExchangeDriver` owns each HTTP exchange. It pumps
`RawOutputHandle` bytes directly into one new Interpretation, reconciles both
terminal handles, and fan-outs accepted canonical events to the transaction
actor and—only for `ModelToolCalls`—the inner Loop. Raw byte chunks do not pass
through the actor command queue.

Provider tool-call IDs are scoped by `ExchangeId`; the same provider ID in a
later exchange creates a distinct action. Continuation context, total provider
input/output bytes, exchange count, and continuation count are bounded without
silent truncation.

## 10. Event delivery

Every transaction has one required caller event sink attached at admission.
The sink uses a bounded queue and is lossless.

The transaction actor publishes events in order. If the required sink closes,
the transaction fails as `EventDeliveryFailed`. If it stops consuming,
backpressure affects only that transaction; global and per-transaction
deadlines still apply.

The final `Ended` event uses a separate bounded terminal-delivery budget from
cleanup. It does not reuse an already expired transaction deadline.

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
6. deliver the final `Ended` transaction event using the terminal-delivery
   deadline and await bounded acceptance;
7. report `EventDeliveryFailed` through completion if final delivery fails;
8. close the event stream;
9. remove the active-session reservation;
10. destroy all transaction-owned state; and
11. schedule the single completion callback on the bounded callback executor.

The reservation is removed before callback invocation, so a callback may submit
the next transaction for the same session.

No producer can publish after terminal selection because producers do not own
the caller sink and their commands are rejected by run ID.

The actor and shutdown supervisor share an exactly-once `FinalizationGuard`.
Normal completion is actor-owned. If forced shutdown must abort an actor, the
supervisor claims the still-unclaimed guard, emits/attempts `RuntimeShutdown`,
removes routing, and invokes the callback. No admitted transaction is discarded
without one callback invocation attempt.

## 12. Cancellation and termination

`terminate(TransactionId | SessionKey, mode)` sends a high-priority command to
the actor.

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
- one active transaction per SessionKey;
- actor command queue items and bytes;
- event queue items and bytes;
- callback queue and callback concurrency;
- MCP session bindings and requests;
- tools per transaction;
- tool schema, input payload, and output bytes;
- queued and concurrent tool calls;
- per-tool concurrency;
- Connector, Interpreter, and Loop queues;
- diagnostics;
- continuation-context, provider exchange, and total provider input/output
  bytes;
- transaction deadline; and
- cleanup, terminal-event delivery, callback, and shutdown deadlines.

Zero and contradictory limits fail configuration validation. Capacity is
enforced in the unit named by the contract: byte limits are not approximated by
unrelated message counts.

## 14. Ephemeral shutdown

Monoloop persists nothing.

Runtime shutdown:

1. rejects new admissions;
2. sends `RuntimeShutdown` terminalization to every actor;
3. waits within the configured global shutdown deadline;
4. aborts and joins actors that exceed their actor grace;
5. uses each aborted actor's `FinalizationGuard` to attempt the final event,
   clear routing, and invoke its callback exactly once;
6. revokes all MCP capabilities and closes the listener;
7. bounds remaining callback futures by the lesser of their own deadline and
   remaining shutdown time;
8. verifies all admitted finalization guards were claimed;
9. releases registries and bounded services; and
10. exits without writing recovery state.

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
- verify session creation/reuse and
  `None`/`CreationOnly`/`Refreshable` MCP support honestly per profile;
- concurrent multi-session qualification;
- remove connector-local prompt shortcuts that bypass canonical encoding.

No slice is marked complete with an `Available -> DispatchRejected` production
branch, conditional test assertion, ignored error, or demo-only proof.

## 17. Verification strategy

Deterministic tests are the primary gate.

Required scenarios include:

- duplicate active SessionKey admission;
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
