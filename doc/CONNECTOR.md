# Component 01 — Connector

**Status:** Foundational component specification

**Product:** [Monoloop](MONOLOOP.md)

**System:** Ground-zero cognitive runtime

**Component kind:** Abstract transport and explicit session-routing boundary

**Implementations:** LLM connector, Grok Build connector, Cursor connector, and
future connectors

**Parent index:** [README.md](README.md)

The words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and
**MAY** are normative requirements.

---

## 1. Purpose

A Connector provides an ordered dialect-labelled path between the runtime and
one external system. A transport profile may additionally perform the minimum
protocol-envelope work required to authenticate, negotiate, create/load
sessions, and route messages by explicit session identity.

It exposes:

1. a raw input stream sent to the external system;
2. a raw output stream received from the external system;
3. the dialect binding spoken by that connection;
4. an out-of-band early cancellation/termination control; and
5. one unambiguous terminal transport outcome.

That is the complete protocol responsibility of this component. It does not
interpret assistant content, reasoning, tools, plans, or other model semantics.

```text
                    raw dialect-encoded input
runtime  ----------------------------------------------> external system

runtime  <---------------------------------------------- external system
                    raw dialect-encoded output

runtime  ---------------- cancel / terminate ----------> connection control

runtime  <--------------- terminal outcome ------------- connection
```

The Connector moves bytes and explicit routing envelopes. It does not
understand the conversation, model, agent, tool, task, turn, or user-visible
meaning of semantic payloads.

## 2. Architectural decision

The Connector is a transport adapter, not an LLM abstraction.

An LLM HTTP API, Grok Build server, Cursor ACP process, local model socket, and
future remote service may require different transport implementations. Once
opened, every implementation presents the same host-neutral contract:

```text
RawConnection
    connection identity
    selected dialect binding
    raw input
    raw output
    control
    completion
```

Downstream components select encoders and decoders from the dialect binding.
They—not the Connector—interpret model events or decide execution state. A
Connector profile may decode only bounded framing and routing fields, such as a
JSON-RPC request ID and Grok `sessionId`, when required to isolate logical
sessions sharing one server transport.

## 3. Responsibilities

The Connector MUST:

- establish the configured transport connection;
- perform declared transport/protocol authentication and initialization;
- create or load an explicitly requested external session when the profile
  requires it;
- retain bounded in-memory session correlation and routing state;
- expose ordered input bytes;
- expose ordered output bytes;
- report the selected input/output dialects;
- apply bounded transport buffering and backpressure;
- accept cancellation independently of input/output traffic;
- force termination when requested;
- interrupt blocked transport operations during cancellation/termination;
- release connection-owned transport resources;
- publish exactly one terminal outcome; and
- provide bounded, content-free transport observations.

The Connector MAY perform transport-required work such as:

- opening HTTP, socket, pipe, or in-process channels;
- adding transport authentication supplied through configuration;
- performing a transport/dialect-version handshake;
- applying TLS or local pipe security;
- half-closing an input side where the transport supports it; and
- translating operating-system I/O failures into connector errors.

## 4. Explicit non-responsibilities

The Connector MUST NOT:

- build, augment, summarize, or inspect prompts;
- choose a provider, model, route, profile, or reasoning level;
- encode semantic requests;
- decode or normalize semantic payload content;
- recognize assistant text, thinking, tool calls, usage, or model errors;
- decide that a model invocation or user turn completed;
- maintain its own copy of provider conversation history;
- assign activity, task, turn, decision, or model-invocation identity;
- execute tools or approve effects;
- retry a semantic operation;
- persist input, output, transcripts, or connection events;
- update product or cognitive state;
- emit product UI presentation blocks;
- start Tasker, specialists, subagents, or another model invocation; or
- infer semantic meaning from payload contents.

The fact that an output payload visually resembles Markdown, an error, a tool
call, or a completion marker does not permit the Connector to interpret it.
Parsing a declared JSON-RPC/ACP envelope solely to authenticate, correlate a
request, or route an explicitly identified session is permitted and must not
promote semantic fields into Connector state.

## 5. Connector and connection

`Connector` and `RawConnection` are distinct:

### 5.1 Connector

A reusable factory/configured transport implementation. It may open many
independent connections. It contains no ambient current connection.

### 5.2 RawConnection

One live logical transport attachment created for one caller-owned scope. It
may carry several ordered operations when its declared profile supports that
behavior. It owns the logical connection resources associated with that scope.

Two logical connections created by the same Connector do not share input,
output, cancellation state, terminal state, or mutable per-connection buffers.
A profile may use a shared bounded physical transport and session-routing table,
but correlation, backpressure, cancellation, and terminal outcomes remain
logically isolated.

## 6. Conceptual interface

The exact Rust spelling may vary, but these semantics are required:

```rust
pub trait Connector: Send + Sync {
    fn descriptor(&self) -> &ConnectorDescriptor;

    fn begin_open(
        &self,
        request: OpenConnection,
    ) -> PendingRawConnection;
}

pub struct PendingRawConnection {
    pub connection_id: ConnectionId,
    pub control: ConnectionControl,
    pub opened: OpenCompletion,
}

pub struct OpenedRawConnection {
    pub connection_id: ConnectionId,
    pub external_session_id: Option<ExternalSessionId>,
    pub dialect: DialectBinding,
    pub input: RawInput,
    pub output: RawOutput,
    pub control: ConnectionControl,
    pub completion: ConnectionCompletion,
}
```

`begin_open` returns without waiting for network, process, handshake, or remote
readiness. `OpenCompletion` resolves to `Result<OpenedRawConnection,
ConnectorError>`. The `ConnectionControl` returned by `PendingRawConnection` is
the same connection-scoped control represented by the opened connection and is
therefore available while opening is blocked.

`RawInput`, `RawOutput`, `ConnectionControl`, and `ConnectionCompletion` MAY be
cloneable handles where needed, but their ownership and concurrency semantics
must be explicit.

`external_session_id` is present for a logical connection attached to an
externally identified session. For the Grok Build profile it is exactly the
Grok-returned `sessionId`.

The public interface contains no host runtime, product UI, provider-native, Tauri, MCP,
ACP implementation, database, or UI type.

## 7. Connection identity

Every opened connection has a unique `ConnectionId` supplied by the caller or
allocated through an injected identity source.

The identity exists only for correlation and control. It does not imply:

- a user turn;
- an LLM invocation;
- a model session;
- a task;
- a work package; or
- semantic ownership.

The execution component that opens the connection binds `ConnectionId` to its
own higher-level identity.

`ConnectionId` and external session identity are distinct. The former
correlates one local logical transport attachment; the latter correlates the
externally owned resumable session across requests or reconnections.

There is no process-global “current connection.”

## 8. Raw input

Raw input is an ordered sequence of bytes already encoded for the selected
input dialect.

```rust
pub trait RawInput: Send {
    async fn send(&mut self, bytes: Bytes) -> Result<(), ConnectorError>;
    async fn finish(&mut self) -> Result<(), ConnectorError>;
}
```

Required behavior:

- accepted bytes are delivered in accepted order;
- partial writes are hidden or returned as explicit failure;
- `finish` performs an input half-close when supported;
- send after finish/cancel/terminal fails explicitly;
- the buffer is bounded;
- backpressure is visible to the caller; and
- cancellation/termination can interrupt a blocked send.

The Connector does not inspect, concatenate for semantic purposes, rewrite, or
log the input body.

## 9. Raw output

Raw output is an ordered sequence of bytes as exposed by the transport body,
pipe, socket, or channel.

```rust
pub trait RawOutput: Send {
    async fn receive(&mut self) -> Result<Option<Bytes>, ConnectorError>;
}
```

`Some(bytes)` means ordered transport bytes are available. `None` means the raw
output side closed; the caller obtains the authoritative reason from
`ConnectionCompletion`.

Required behavior:

- byte order is preserved;
- no byte is duplicated by the Connector;
- transport chunk boundaries have no semantic meaning;
- one chunk may contain a partial dialect frame or several frames;
- empty reads do not become semantic keep-alive events;
- the output buffer is bounded; and
- cancellation/termination interrupts a blocked receive.

“Raw” means the transport payload, not necessarily the entire underlying wire.
For example, an HTTP Connector may expose response-body bytes after HTTP/TLS
handling; it does not expose encrypted packets or HTTP headers as model output.
This boundary is declared by the Connector descriptor.

## 10. Dialect binding

Each opened connection returns the dialect actually selected for both
directions:

```text
DialectBinding
    input: DialectDescriptor
    output: DialectDescriptor
    negotiation: fixed | negotiated
```

```text
DialectDescriptor
    family
    version
    framing
    profile?
```

Examples may include:

```text
openai_responses / v1 / sse
anthropic_messages / v1 / sse
acp / v1 / json_rpc
cursor_acp / negotiated / json_rpc
grok_build / v1 / jsonl
```

The descriptor is stable, bounded, versioned data. The Connector reports it but
does not expose an encoder or decoder implementation.

Dialect negotiation must complete before `OpenCompletion` yields an
`OpenedRawConnection`. If the selected dialect is unsupported or ambiguous,
opening fails. The Connector cannot begin streaming under one dialect and
silently switch to another.

## 11. Out-of-band control

Cancellation and termination use an independent control path:

```rust
pub trait ConnectionControl: Send + Sync {
    fn cancel(
        &self,
        reason: CancellationReason,
    ) -> ControlDisposition;

    fn terminate(
        &self,
        reason: TerminationReason,
    ) -> ControlDisposition;
}
```

Control MUST NOT be encoded as input bytes unless a separate downstream dialect
component explicitly chooses to send a protocol-level cancellation message.

The control path MUST:

- remain usable while input/output queues are full;
- not wait behind queued input;
- be callable from a task other than the I/O consumer;
- wake blocked open/read/write work;
- be idempotent;
- avoid unbounded allocation or task creation; and
- lead to one terminal outcome within a configured bounded interval.

## 12. Cancellation and termination semantics

### 12.1 Cancel

`cancel` requests immediate cooperative abortion of the current connection.
The Connector stops accepting new input, interrupts pending I/O, invokes any
transport-level cooperative close available, and begins cleanup.

Cancellation does not mean the external semantic operation accepted or obeyed
the request. It means the local connection entered cancellation.

### 12.2 Terminate

`terminate` requests forced local transport closure. It is used when cooperative
cancellation is inappropriate, unavailable, or has exceeded its deadline.

Termination closes the Connector-owned socket, response body, channel, or pipe.
If an external process is owned by a separate process supervisor, terminating
the Connector does not claim that the process was killed.

### 12.3 Escalation

The caller may implement:

```text
cancel
  -> wait bounded grace period
  -> terminate connection
  -> separately terminate supervised process if required
```

The Connector does not choose the grace period or semantic retry policy.

## 13. Control disposition

Control calls return immediately with one of:

```text
accepted
already_requested
already_terminal
wrong_connection
control_unavailable
```

`accepted` means the control signal was recorded by the connection owner. It is
not the final terminal outcome. Callers observe completion separately.

Repeated cancellation or termination cannot create multiple cleanup paths or
terminal events.

## 14. Connection lifecycle

The lifecycle is transport-only:

```text
configured
    -> opening
        -> open
            -> input_finished
            -> cancelling
            -> terminating
            -> remote_closed
            -> failed
        -> open_failed

cancelling  -> closed
terminating -> closed
remote_closed, failed, open_failed, closed -> terminal
```

This lifecycle must not contain states such as:

```text
thinking
tool_calling
assistant_complete
turn_complete
reviewing
tasker_running
```

Those belong to downstream machines.

## 15. Exactly one terminal outcome

Every successfully opened connection produces exactly one `ConnectionEnd`:

```text
ConnectionEnd
    connection_id
    kind
    initiated_by
    safe_transport_error?
    bytes_accepted
    bytes_received
    opened_at
    ended_at
```

Closed terminal kinds:

```text
remote_eof
cancelled
terminated
transport_failure
local_shutdown
```

The outcome contains transport facts only. It does not say that a model, tool,
agent, activity, or turn succeeded or completed.

## 16. Terminal-race rule

Connection completion is serialized by one connection-state owner.

Rules:

1. A terminal outcome already published cannot be replaced.
2. A cancel accepted before terminal publication wins over a later observed EOF
   or ordinary transport failure and yields `cancelled`.
3. A terminate accepted before terminal publication wins over cancellation,
   later EOF, or ordinary transport failure and yields `terminated`.
4. A remote EOF or transport failure committed before control is accepted
   remains the terminal outcome; the control call returns `already_terminal`.
5. Cleanup errors after terminal selection are diagnostics attached to that
   outcome, not a second terminal outcome.

This makes cancel/EOF and terminate/failure races mechanically testable.

## 17. Completion handle

```rust
pub trait ConnectionCompletion: Send {
    async fn wait(self) -> ConnectionEnd;
}
```

`wait` is safe after the raw output ends and safe when called immediately after
opening. It resolves exactly once and cannot panic when polled after internal
I/O completion.

Dropping an output reader does not silently invent a successful terminal state.
The connection continues or is cancelled according to explicit ownership
policy supplied when opened.

## 18. Open request

`OpenConnection` contains transport requirements only:

```text
connection_id
endpoint/configuration reference
external session identity?
credential reference?
required dialect family/version range
connect deadline
I/O buffer limits
transport security requirements
caller trace context?
```

It does not contain a prompt, task, conversation, agent role, Kanban record,
model-routing decision, or product UI block.

An external session identity is an opaque value returned by the external system
and chosen explicitly by the caller when loading or addressing an existing
session. For Grok Build it is Grok's `sessionId`. The Connector does not
interpret the conversation or select an implicit session. A profile may retain
the explicit session correlation in its bounded in-memory routing table, but
never makes it the ambient current session.

`begin_open` returns a pending handle synchronously, making its control path
available before connection establishment can block. Cancellation or
termination during opening resolves `OpenCompletion` with the corresponding
typed error and releases all partially acquired resources.

A credential reference is resolved through a transport configuration boundary;
credential material must not appear in descriptors, errors, traces, or terminal
outcomes.

## 19. Connector descriptor

```text
ConnectorDescriptor
    connector_kind
    implementation_id
    implementation_version
    transport_kind
    supported_dialects[]
    raw_boundary
    control_capabilities
```

`connector_kind` describes transport integration, not model intelligence.

`raw_boundary` states what bytes are exposed, such as HTTP body, process stdout,
socket payload, or in-process channel payload.

Descriptors are immutable for the lifetime of a Connector. The negotiated
`DialectBinding` is immutable for the lifetime of one RawConnection.

## 20. Implementation families

Initial expected implementations:

### 20.1 LLM HTTP connector

- writes an encoded request body;
- reads response-body bytes, including streaming bodies;
- owns HTTP connection/response-body cancellation;
- reports the selected API dialect; and
- does not understand provider events.

### 20.2 Grok Build connector

- implements the [Grok Build Network Connector
  Profile](GROK_BUILD_CONNECTOR.md);
- attaches over authenticated WebSocket to one configured long-lived Grok Build
  server;
- supports many logical sessions addressed by Grok's returned `sessionId`;
- sends initial session configuration through ACP `session/new`;
- retains bounded in-memory session routing and pending-operation state;
- reports the actual negotiated ACP/JSON-RPC dialect; and
- does not interpret agent messages, thoughts, plans, work packages, or tool
  events.

### 20.3 Cursor connector

- attaches to the configured Cursor transport;
- exposes ordered raw payload bytes;
- reports its dialect binding; and
- does not interpret ACP messages.

### 20.4 Future connectors

Local-model sockets, remote workers, subscription labor, test doubles, and
other systems implement the same contract without adding conditional branches
to downstream execution logic.

## 21. Process ownership boundary

A Connector may communicate through an external process but does not thereby
own the process lifecycle.

```text
Process Supervisor
    spawn, monitor, signal, kill, reap, resource accounting

Connector
    attach to transport, move bytes, close transport
```

If one implementation necessarily creates a process to obtain its transport,
that creation occurs through an injected supervisor handle. The Connector must
still report connection termination separately from process termination.

Claims such as “Cursor was killed” require supervisor evidence, not merely a
closed pipe.

The same rule applies to resumable external sessions:

```text
External Application / Process Supervisor
    returns authoritative session identity
    owns conversation state, resumability, process and durable continuity

Connector
    keeps a bounded in-memory routing table keyed by external session identity
    moves bytes and explicit routing envelopes
```

Closing a Connector connection does not claim that the external session ended.
While live, the Connector may route later requests through its explicitly keyed
in-memory session table. After that table is lost, a later connection may
reattach only when its caller supplies the external session identity again. The
Connector never selects a last or most-recent session implicitly.

## 22. Backpressure and resource bounds

Every Connector declares and enforces:

- maximum queued input bytes;
- maximum queued output bytes;
- maximum individual byte chunk accepted from its caller;
- connect deadline behavior;
- cancellation grace behavior supplied by its caller; and
- cleanup deadline behavior.

It must not accumulate an entire response merely because downstream decoding or
rendering is slow.

When downstream stops reading, the transport applies bounded backpressure or
fails explicitly. It does not create an unbounded buffering task.

Control remains available under backpressure.

## 23. Retry boundary

The Connector never retries a semantic request.

It MAY retry an internal transport primitive only when all of the following are
true:

- no caller input byte has been accepted for that connection;
- no output byte has been exposed;
- the retry cannot duplicate an external operation;
- the behavior is declared in its descriptor; and
- the same connection terminal contract remains truthful.

Otherwise it returns a transport failure. A downstream execution policy decides
whether to open a new connection and retry the higher-level operation.

## 24. Errors

Closed connector error families:

```text
configuration_invalid
dialect_unavailable
credential_unavailable
connection_failed
write_failed
read_failed
remote_closed
deadline_exceeded
cancelled
terminated
local_resource_failed
invariant_violation
```

Errors include connection identity, safe transport classification, retry
knowledge where mechanically certain, and redacted diagnostics. They never
persist raw bodies, headers, credentials, external session identities, prompts,
or provider error content.

A dialect-level error received as bytes remains bytes until decoded downstream.

## 25. Observability

The Connector may expose bounded transport measurements:

```text
connect latency
bytes accepted
bytes received
time to first byte
time to terminal outcome
input/output backpressure duration
cancellation accepted-to-closed latency
termination accepted-to-closed latency
transport failure classification
```

It must not label metrics with prompt text, raw bytes, credentials, user text,
project IDs, or unbounded endpoint values.

Logging raw input/output is prohibited by default. Diagnostic capture, if ever
added, belongs to an explicit secured higher-level facility.

## 26. Security

Required behavior:

- transport credentials remain opaque and redacted;
- external session identities remain opaque and redacted;
- TLS/pipe/socket security requirements fail closed;
- cancellation cannot target another connection identity;
- an external session identity cannot be substituted with or inferred from
  another connection;
- connector instances do not share mutable connection state;
- raw data is not copied into diagnostics;
- local pipes/files have appropriate ownership and permissions; and
- closing one connection cannot close a sibling connection.

The Connector does not decide whether semantic content is safe. That belongs to
upstream encoding/policy and downstream decoding/validation.

## 27. Concurrency

The design supports many simultaneous connections:

- connections are independently addressable;
- control is connection-scoped;
- byte ordering is guaranteed only within one direction of one connection;
- one slow connection cannot monopolize all Connector workers;
- one cancellation cannot affect siblings;
- shared pools must not share semantic state; and
- shutdown enumerates and terminates connections through explicit ownership.

Completion order has no relationship to higher-level activity priority or turn
order.

The initial Rust implementation runs on a multi-threaded async runtime. All
network I/O, queue waits, control paths, session routing, and completion waits
are non-blocking async operations. No synchronization guard is held across
network I/O, and unavoidable blocking work is isolated on a bounded blocking
facility rather than an async worker.

## 28. Required tests

### 28.1 Raw transport

- Input bytes arrive exactly once and in order.
- Output bytes arrive exactly once and in order.
- Arbitrary fragmentation and coalescing preserve the byte sequence.
- Binary and invalid UTF-8 payloads pass unchanged.
- Input finish performs the declared half-close behavior.
- No semantic parsing occurs for JSON/SSE/ACP-looking bytes.

### 28.2 Dialect

- Fixed dialect is reported exactly.
- Negotiated dialect is frozen before `OpenCompletion` succeeds.
- Unsupported/ambiguous dialect fails opening.
- Input and output dialects may differ.
- Dialect cannot change mid-connection.

### 28.3 Cancellation

- Cancel interrupts blocked open, read, and write operations.
- Cancel remains available when input/output buffers are full.
- Repeated cancel is idempotent.
- Cancel produces one `cancelled` terminal outcome.
- Cancel acknowledgment is not confused with terminal completion.
- Cancel/EOF race follows §16.

### 28.4 Termination

- Terminate interrupts blocked open, read, and write operations.
- Terminate escalates an already-cancelling connection.
- Repeated terminate is idempotent.
- Terminate produces one `terminated` terminal outcome.
- Terminate/failure and terminate/EOF races follow §16.
- Closing transport does not claim an external process was killed.
- Closing transport does not claim an externally owned session ended.
- A new connection can reattach to a supervised external session only with an
  explicitly supplied external session identity.

### 28.5 Lifecycle and completion

- Every successfully opened connection yields exactly one terminal outcome.
- Failed open does not yield a usable RawConnection.
- Send after finish or terminal fails explicitly.
- Completion may be awaited before or after output closure.
- Completion cannot panic through double polling/internal join reuse.
- Cleanup errors do not create a second outcome.

### 28.6 Bounds and multitasking

- Input and output queues enforce configured bounds.
- Backpressure does not prevent control.
- Thousands of concurrent fake connections remain isolated.
- Cancelling one connection leaves all siblings unchanged.
- Slow consumer memory remains bounded.
- Shutdown has a bounded terminal outcome for every owned connection.

### 28.7 Architecture

- Connector contracts contain no host runtime, product UI, provider-native, database,
  task, agent, or UI types.
- Connector implementations do not import prompt, Kanban, specialist, context
  compiler, renderer, or DAL modules.
- No persistence call is reachable from Connector code.
- No semantic decoder is reachable from Connector code.
- No unbounded channel exists.

## 29. Acceptance criteria

Component 01 is accepted only when:

1. one abstract contract supports HTTP, process-pipe/socket, and deterministic
   fake implementations;
2. byte fidelity and ordering pass under arbitrary fragmentation;
3. the selected dialect is explicit and immutable per connection;
4. cancellation and termination are out-of-band and preempt blocked I/O;
5. exactly one terminal outcome is guaranteed under every race;
6. transport termination is never represented as semantic turn completion;
7. all buffering and cleanup are bounded;
8. simultaneous connections remain isolated;
9. no durable persistence, semantic payload interpretation, provider
   conversation copy, tool, or UI responsibility exists;
10. a process-backed connector does not counterfeit process termination;
11. a Connector retains only bounded in-memory correlation/routing state for an
    external resumable session and never owns its conversation or durable
    representation;
12. architecture tests enforce the dependency boundary; and
13. all required transport, cancellation, race, and load tests pass without a
    partial or “shaped” qualification.

## 30. Deferred enrichment

Later components may add, outside the Connector:

- dialect encoder and incremental decoder;
- normalized semantic event stream;
- provider/model capabilities;
- invocation lifecycle and retry policy;
- process supervision;
- tool-call validation and execution;
- cognitive state and context compilation;
- persistence and receipts; and
- product UI projections.

Those components consume the Connector contract. They do not expand its
responsibility.

## 31. Governing rule

> A Connector is a dialect-labelled, ordered, bidirectional raw byte transport
> with an independent immediate cancellation/termination path and exactly one
> terminal transport outcome. It moves bytes and reports transport truth. It
> does not understand what the bytes mean.
