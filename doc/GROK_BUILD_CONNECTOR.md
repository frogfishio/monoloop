# Grok Build Network Connector Profile

**Status:** Initial implementation specification

**Language:** Rust

**Async runtime:** Tokio multi-thread runtime

**Implements:** [Component 01 — Connector](CONNECTOR.md)

**Dialect:** ACP / JSON-RPC 2.0 over authenticated WebSocket

**Server topology:** One long-lived Grok Build instance, many logical sessions

**Parent index:** [README.md](README.md)

The words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and
**MAY** are normative requirements.

---

## 1. Purpose

The initial Connector integrates with the Grok Build application already
authenticated on the host computer.

It connects to one long-lived Grok Build agent server over the network. That
server hosts multiple independently addressed Grok sessions. Monoloop never
passes prompts on a command line and never creates one Grok process per
session.

The intended server mode is conceptually:

```text
grok agent serve --bind 127.0.0.1:2419 --secret <injected-secret>
```

The command illustrates the external server configuration only. Prompts,
session configuration, and session control travel exclusively through ACP
JSON-RPC messages over WebSocket.

Exactly one Grok Build server instance belongs to one configured deployment of
this profile. Opening another Monoloop session MUST NOT spawn another Grok
instance. Server startup or discovery is performed once by an external
supervisor or test-fixture setup, never as a side effect of `session/new`.

## 2. Topology

```text
One authenticated Grok Build server instance
    |
    | ACP / JSON-RPC 2.0 over WebSocket
    |
Rust Grok Connector
    +-- Grok sessionId A
    +-- Grok sessionId B
    +-- Grok sessionId C
```

The public contract promises one server with multiple concurrently progressing
logical sessions. It does not require one particular socket topology. An
implementation may multiplex sessions over one qualified WebSocket or use a
bounded set of WebSocket connections to the same server, according to
negotiated capabilities and conformance evidence.

## 3. Identity and correlation

The `sessionId` returned by Grok from `session/new` is Monoloop's session
correlation identity for that Grok session.

Rules:

- Monoloop MUST NOT allocate a second competing session identity;
- every session-scoped request, notification, state transition, diagnostic, and
  cancellation carries the Grok `sessionId`;
- JSON-RPC request IDs correlate individual RPC calls and remain distinct from
  the session ID;
- WebSocket connection identity is transport correlation only;
- a response or update without mechanically valid correlation fails closed; and
- no most-recent/current-session heuristic is permitted.

The Grok session ID is opaque. Monoloop compares and routes it but does not
derive meaning or authority from its contents.

## 4. Conceptual Rust interface

The exact Rust spelling may vary, but the public semantics are:

```rust
pub trait GrokConnector: Send + Sync {
    fn connect(
        &self,
        config: GrokServerConfig,
    ) -> Result<PendingGrokServer, GrokConnectorError>;
}

pub struct GrokServerHandle {
    pub control: GrokServerControl,
    pub health: GrokServerHealth,
    pub completion: GrokServerCompletion,
    pub sessions: GrokSessionFactory,
}

pub trait GrokSessionFactory: Send + Sync {
    fn begin_new(
        &self,
        config: GrokSessionConfig,
    ) -> Result<PendingGrokSession, GrokConnectorError>;

    fn begin_load(
        &self,
        session_id: GrokSessionId,
        config: GrokSessionLoadConfig,
    ) -> Result<PendingGrokSession, GrokConnectorError>;
}

pub struct GrokSessionHandle {
    pub session_id: GrokSessionId,
    pub input: GrokSessionInput,
    pub output: RawOutput,
    pub control: GrokSessionControl,
    pub health: GrokSessionHealth,
    pub completion: GrokSessionCompletion,
}

pub trait GrokSessionInput: Send + Sync {
    fn begin_send(
        &self,
        message: EncodedAcpSessionMessage,
    ) -> Result<PendingGrokExchange, GrokConnectorError>;
}
```

`connect`, `begin_new`, and `begin_load` return pending handles immediately;
network connection, ACP initialization, and session creation/loading complete
asynchronously. Control is available while those operations are pending.

`GrokSessionId` is a validated opaque wrapper around Grok's returned
`sessionId`, not a Monoloop-generated value.

`EncodedAcpSessionMessage` is a complete bounded output of the selected outbound
ACP encoder. It contains the declared method and complete encoded parameters but
does not choose its wire JSON-RPC request ID or another session. The Connector
allocates the wire request ID, binds the authoritative `sessionId` from the
handle, serializes the routing envelope, and owns response correlation. It does
not inspect the prompt or other semantic payload content.

This profile-specific input refines the generic raw-input contract because many
logical sessions share network transport and JSON-RPC request IDs must be unique
within that shared scope. It does not move semantic interpretation into the
Connector.

## 5. Server connection

Configuration is supplied before connecting:

```text
GrokServerConfig
    websocket_endpoint
    authentication_secret_ref
    expected ACP version/range
    TLS/loopback security policy
    connect/handshake deadlines
    WebSocket frame/message limits
    maximum network connections
    maximum sessions
    aggregate queue/byte limits
```

The default local deployment binds to loopback. Non-loopback deployment
requires an explicit authenticated transport-security policy.

Authentication material is resolved through an injected secret boundary. It is
never returned in descriptors, diagnostics, traces, terminal results, or metric
labels.

Two authentication domains remain separate:

- Grok Build itself uses the existing host authentication already established
  on the computer to reach its backend; and
- Monoloop authenticates its WebSocket connection to the local Grok server with
  the configured server secret.

Monoloop never reads, copies, parses, refreshes, or logs Grok's host account
credential files.

## 6. Protocol initialization

After WebSocket authentication, the client performs ACP initialization:

```text
WebSocket connected
    -> JSON-RPC initialize(client capabilities)
    <- negotiated ACP/server capabilities
    -> ready for session/new or session/load
```

The negotiated protocol version and capabilities are immutable for that
connection. Unsupported or ambiguous versions fail before a session is
admitted.

Grok-specific `x.ai/*` extensions are used only when advertised by the
initialization result and explicitly required by this profile. Unknown
extensions never become implicit control paths.

## 7. Creating a session

All initial session configuration is sent through ACP `session/new`:

```text
GrokSessionConfig
    cwd
    mcp_servers[]
    rules?
    system_prompt_override?
    agent_profile?
    permission_mode
    declared extension metadata?
    configuration limits/digest
```

Conceptual exchange:

```text
session/new(config)
    -> Grok validates and creates the session
    <- sessionId
    -> Connector registers bounded in-memory state keyed by sessionId
```

No prompt, rule, configuration document, or session identifier is passed as a
command-line prompt argument.

Configuration is immutable for the local session attachment unless ACP exposes
an explicit qualified reconfiguration method. A changed configuration creates
a new session or uses that explicit method; it is never silently applied to an
existing session.

## 8. Loading and resuming a session

When a known Grok session must be reattached after network disconnection or
Connector restart, the caller explicitly supplies its Grok `sessionId` and the
Connector performs ACP `session/load`.

```text
known sessionId + required load configuration
    -> session/load
    <- accepted attachment or typed failure
```

The Connector never scans for or selects the latest session. It never assumes
that an unknown ID is safe to create. Failure to load remains distinct from
creating a new session.

Grok Build owns any durable session representation. Monoloop retains only its
bounded in-memory correlation and routing table.

## 9. In-memory session state

One Connector instance conceptually owns:

```text
GrokConnectorState
    server endpoint and negotiated capabilities
    live WebSocket connection handles
    JSON-RPC request ID allocator
    bounded pending-RPC correlation table
    bounded session table:
        sessionId -> GrokSessionState

GrokSessionState
    sessionId
    configuration digest and safe routing metadata
    lifecycle state
    assigned connection identity
    serialized prompt admission queue
    pending request identities
    cancellation handles
    last safe activity/health observations
```

The table is memory-only and bounded by count and bytes. Removing a local entry
does not claim that Grok deleted its externally owned session.

## 10. Session lifecycle

```text
configured
    -> creating | loading
        -> ready
            -> prompt_active
                -> ready
            -> reconnecting
                -> loading
            -> closing
        -> create_failed | load_failed

any live state -> cancelled | connection_lost | failed
terminal local state -> detached
```

`detached` describes local routing state only. It is not proof that the Grok
session was deleted or became non-resumable.

## 11. Prompt exchange

The outbound encoder produces a complete `session/prompt` message. The
Connector binds it to the Grok `sessionId`, allocates the JSON-RPC request ID,
and sends it through `GrokSessionInput`. Inbound `session/update` notifications
and the terminal prompt response retain that identity and are exposed as
ordered dialect bytes/events for the Interpreter.

Different sessions MUST progress concurrently. Within one session, prompt
mutations are serialized unless Grok's negotiated capabilities explicitly
declare concurrent prompts safe. This prevents two requests from racing to
mutate one conversation history.

The Connector does not turn `agent_message_chunk`, `agent_thought_chunk`, tool
updates, plans, or other ACP semantic events into canonical units. That remains
the Interpreter's responsibility.

## 12. Async Rust architecture

The implementation uses Tokio's multi-thread runtime with non-blocking
WebSocket I/O. Protocol and domain contracts remain isolated from Tokio-specific
handle types where practical so tests can control time and I/O deterministically.

Required ownership model:

```text
connection owner
    one async reader
    one serialized async writer
    request/response correlation

session owner per sessionId
    serialized session state transitions
    bounded prompt admission
    session-scoped cancellation

connector supervisor
    bounded connection/session registry
    aggregate limits and shutdown
```

Requirements:

- no blocking socket, process, filesystem, or synchronization wait runs on an
  async worker;
- unavoidable blocking work uses a dedicated bounded blocking facility;
- no mutex, read guard, or write guard is held across network I/O;
- one WebSocket has one reader owner and serialized complete-message writes;
- JSON-RPC frames from concurrent tasks cannot interleave;
- pending calls use async notification rather than polling;
- tasks are bounded and owned; detached fire-and-forget work is prohibited;
- one slow session cannot monopolize the reader, writer, or all runtime workers;
- every blocked operation has cancellation and deadline paths; and
- shutdown joins every owned task exactly once within a declared bound.

## 13. Demultiplexing

The network reader classifies complete bounded JSON-RPC messages mechanically:

```text
response id
    -> matching pending RPC waiter

session-scoped notification
    -> session route keyed by Grok sessionId

server request, including permission request
    -> explicitly registered handler

unknown/unscoped message
    -> typed protocol diagnostic or fail-closed error
```

Correlation is based on explicit IDs, never arrival adjacency, last-active
session, or completion order.

## 14. Backpressure and bounds

The profile declares and enforces:

- maximum WebSocket frame and message bytes;
- maximum pending JSON-RPC requests;
- maximum sessions per Connector;
- maximum queued prompts per session;
- maximum queued inbound updates per session;
- maximum aggregate queued bytes;
- maximum concurrent active prompts across sessions;
- connection, handshake, request, prompt, and cancellation deadlines;
- reconnect attempt/time bounds; and
- maximum diagnostics.

When a per-session queue fills, only that session is backpressured or failed
according to policy. Aggregate exhaustion rejects new work explicitly. No path
creates an unbounded forwarding task to preserve apparent responsiveness.

## 15. Cancellation and connection loss

Cancellation is scoped to the addressed request or Grok session and cannot
affect sibling sessions unless the server connection itself must be closed.

On connection loss:

- every pending operation on that connection is awakened promptly;
- acceptance-unknown prompt requests are not automatically replayed;
- affected sessions enter an explicit disconnected/reconnecting state;
- reattachment uses `session/load` with the same Grok `sessionId` when policy
  permits; and
- sibling sessions on unaffected connections continue.

Automatic replay is prohibited whenever it could duplicate model or tool
effects.

## 16. Permissions

Permission requests are protocol messages and require explicit handling.

The initial safe profile does not enable automatic approval implicitly. A test
or host may select a declared permission mode in session configuration. The
choice is immutable for the admitted session unless an explicit ACP method
changes it.

One session's permission response cannot satisfy another session's request.
Permission IDs, session IDs, and request identities are all validated before a
response is sent.

## 17. Failure isolation

- One session failure cannot cancel or corrupt sibling sessions.
- One session's update cannot be delivered to another session.
- One JSON-RPC response cannot resolve the wrong waiter.
- Completion order cannot change correlation.
- A malformed message fails only the smallest safely isolatable scope.
- A server-wide connection failure is reported distinctly from a session
  failure.
- Closing a local session attachment does not claim that Grok deleted the
  session.

## 18. Required tests

### 18.1 Protocol and identity

- Initialize negotiates the expected ACP version and capabilities.
- `session/new` receives configuration and returns the authoritative session ID.
- The Grok session ID is used directly as Monoloop's session correlation ID.
- JSON-RPC request IDs remain distinct from session IDs.
- Unknown, missing, and conflicting correlations fail closed.

### 18.2 Multiple sessions

- One Grok server hosts many concurrently progressing sessions.
- Interleaved responses and notifications route to the correct session.
- Equal content across sessions never merges state.
- A slow session does not block sibling sessions.
- Prompts within one session serialize by default.

### 18.3 Configuration and resume

- Complete configuration is supplied through `session/new`, not prompt
  arguments.
- Configuration digests and bounds are stable.
- A known session can reconnect through `session/load`.
- Missing/invalid session IDs fail without creating a replacement session.
- No latest-session inference exists.

### 18.4 Async and load

- All network I/O is non-blocking.
- Thousands of deterministic concurrent session exchanges stay within bounds.
- Reader and writer tasks remain responsive under per-session backpressure.
- Cancellation wakes blocked admission, write, response, and update waits.
- No lock is held across a simulated slow network await.
- Every task and waiter is released during bounded shutdown.

### 18.5 Races and failures

- Cancel/response, disconnect/response, and reconnect/cancel races select one
  truthful outcome.
- Acceptance-unknown prompts are never replayed automatically.
- A malformed session update cannot cross-inject into another session.
- Connection-wide failure and session-local failure remain distinguishable.

### 18.6 Security

- Secrets never appear in logs, errors, snapshots, or metrics.
- Default bind assumptions reject unprotected non-loopback endpoints.
- Permission responses remain session-scoped.
- Oversized or deeply malformed messages fail before unbounded allocation.

## 19. Initial acceptance criteria

The Grok Build Connector profile is accepted only when:

1. it communicates with one authenticated Grok Build server over WebSocket;
2. it never passes prompts through command-line arguments;
3. one server supports many explicitly correlated Grok sessions;
4. Grok's returned `sessionId` is the sole Monoloop session correlation ID;
5. initial configuration is sent through `session/new`;
6. session correlation and routing state remain bounded and in memory;
7. different sessions progress concurrently while same-session prompts
   serialize by default;
8. all I/O and coordination are async and non-blocking on a multi-threaded Rust
   runtime;
9. reconnect uses explicit `session/load` and never most-recent inference;
10. unknown-acceptance requests are not replayed automatically;
11. cancellation and failure remain isolated by session wherever mechanically
    possible;
12. authentication and permission authority remain explicit and redacted; and
13. deterministic conformance and authenticated integration suites pass.

## 20. Protocol references

- [Grok Build Agent mode: ACP and WebSocket server](https://github.com/xai-org/grok-build/blob/main/crates/codegen/xai-grok-pager/docs/user-guide/15-agent-mode.md)
- [Grok Build overview](https://docs.x.ai/build/overview)
- [Agent Client Protocol](https://agentclientprotocol.com/)

## 21. Governing rule

> One authenticated Grok Build server hosts many logical sessions. Grok's
> `sessionId` is Monoloop's session correlation identity. Configuration enters
> through `session/new`; prompts and updates travel through ACP JSON-RPC over
> WebSocket; routing state is bounded and in memory; and all Rust execution is
> multi-threaded, asynchronous, non-blocking, and explicitly correlated.
