# Monoloop Requirements Register

This register captures product requirements that guide later architecture and
implementation work. A requirement describes required behavior and boundaries;
it does not prescribe a separate product component unless explicitly stated.

## R-000: Engineering quality standard

**Status:** Accepted  
**Minimum passing grade:** A-

Monoloop must be complete, robust, and reliable. Architectural language,
documentation, abstractions, demos, and passing happy-path tests do not
compensate for incomplete production behavior.

An implementation is not complete when it contains a stub, deferred production
path, silent fallback, placeholder result, ignored failure, unbounded resource,
or test that avoids exercising the behavior it claims to verify.

### Required engineering practices

1. All advertised production paths work end to end.
2. Errors are typed, propagated, bounded, and truthful. Failures are never
   converted into success, silently ignored, or hidden behind generic terminal
   outcomes.
3. Cancellation, termination, timeout, completion, and transport-loss races
   resolve deterministically with exactly one terminal result.
4. Concurrency is bounded, isolated, and tested under simultaneous load.
5. Every queue, buffer, registry, payload, callback set, session set, tool set,
   and in-flow transaction set has an enforced bound.
6. No production task, process, connection, waiter, lock, or session is leaked
   after completion or failure.
7. Backpressure behavior is explicit and cannot silently lose required events.
8. Security-sensitive behavior is fail-closed and requires explicit opt-in.
9. Provider, session, transaction, event, and tool correlation cannot
   cross-route under concurrency.
10. Public contracts and configuration limits are enforced exactly as
    documented.
11. Production code contains no `todo!`, `unimplemented!`, placeholder success,
    knowingly deferred branch, or panic reachable from valid external input.
12. Unsafe or lossy conversions require explicit justification and tests.
13. Duplicated provider implementations must share tested common behavior where
    divergence would create inconsistent reliability.
14. Documentation states actual behavior and limitations; it does not claim
    acceptance criteria that tests or implementation do not satisfy.

### Verification gate

A requirement may be marked complete only when:

- its normal, boundary, failure, cancellation, and concurrency paths have
  meaningful tests;
- tests assert outcomes rather than merely executing code;
- tests cannot pass by skipping the behavior under conditional assertions;
- the full workspace test suite passes;
- strict workspace Clippy passes with warnings denied;
- formatting and documentation checks pass;
- resource cleanup and terminal-result invariants are verified;
- relevant malformed, oversized, delayed, disconnected, and duplicate inputs
  are covered; and
- review finds no unresolved P0, P1, or P2 correctness, security, concurrency,
  resource, or lifecycle defects in the delivered scope.

Live-provider demos are useful qualification evidence but are not substitutes
for deterministic tests.

### Completion rule

Partially implemented work must be labelled incomplete. It must not be
described as fixed, complete, supported, production-ready, or acceptance-tested
until the implementation and verification gate above are satisfied.

## R-001: Configurable direct-LLM channels

**Status:** Accepted  
**Initial target:** OpenAI-compatible HTTP APIs

Monoloop must support direct interaction with plain LLM APIs in addition to the
six existing external agent/tool integrations. The first direct-LLM family will
use OpenAI-compatible protocols.

Support must not require a new provider-specific handler for every hosted or
local model service. Provider identity, transport, dialect, and configuration
are separate concerns:

```text
Provider channel
    = transport
    + request/response dialect
    + endpoint and authentication configuration
    + static provider/model defaults
    + declared capabilities and compatibility options

Invocation
    = canonical input
    + dynamic configuration overrides
```

### Required behavior

1. A caller explicitly selects a configured Channel; Monoloop does not choose,
   rank, or fall back between providers.
2. Multiple providers that implement the same protocol reuse the same
   Connector, outbound encoder, and Interpreter profile.
3. A materially different wire request, streaming format, or response semantic
   is represented by a different dialect implementation, not a provider branch
   in shared execution code.
4. Small compatibility differences may be declared as bounded, versioned
   profile configuration or capabilities.
5. Each invocation carries canonical input plus dynamic model configuration.
6. Effective configuration is produced deterministically from:

   ```text
   Channel/provider defaults
       overridden by session configuration where applicable
       overridden by invocation configuration where permitted
   ```

7. Configuration has explicit lifetime and scope:
   - **Channel configuration:** endpoint, credential reference, dialect,
     provider defaults, model defaults, capabilities, and transport limits.
   - **Session configuration:** settings fixed or negotiated when an external
     session is created or loaded, such as specialist profile, mode,
     permissions, rules, or MCP servers.
   - **Invocation configuration:** settings valid for one model call, such as
     model override, temperature/warmth, reasoning effort, token limits, stop
     conditions, and response format.
8. A setting that cannot be changed on an existing session must produce an
   explicit reconfiguration error or require a new session. It must not be
   silently ignored.
9. Common cross-provider settings should have provider-neutral typed fields.
   Provider-specific settings use a bounded, namespaced, versioned extension
   payload interpreted by the selected outbound dialect encoder.
10. Unknown, unsupported, or incompatible options fail explicitly according to
    the Channel's declared option policy; they are not silently discarded.
11. Credentials, secret values, and transport endpoints are never accepted in
    invocation configuration.
12. Configuration size, nesting, keys, and values are bounded before encoding.

### Architectural boundary

The raw Connector remains responsible for transport, authentication,
cancellation, termination, and ordered bytes. It does not interpret model
parameters.

The selected Channel's outbound encoder transforms canonical input and effective
configuration into provider-dialect bytes. The matching Interpreter transforms
the returned dialect stream into canonical events.

```text
Canonical invocation (input + config)
    -> selected Channel
    -> dialect outbound encoder
    -> raw Connector
    -> external provider
    -> dialect Interpreter
    -> canonical events
```

### Initial protocol scope

“OpenAI-compatible” must be qualified by dialect. OpenAI Chat Completions and
OpenAI Responses are distinct dialects and may be implemented independently.
The initial implementation must explicitly choose and advertise which dialect
it supports rather than treating all OpenAI-compatible endpoints as identical.

Example configured providers may include OpenAI, OpenRouter, Together, vLLM,
Ollama, or another service without introducing one Rust handler per provider,
provided each service conforms to the selected dialect and declared profile
capabilities.

### Acceptance criteria

- [ ] At least one direct OpenAI-compatible LLM Channel can stream a text
  response into canonical events.
- [ ] Two differently configured providers can reuse the same protocol
  implementation without provider-specific execution branches.
- [ ] Provider defaults and invocation overrides merge deterministically.
- [ ] Session-only and invocation-level settings cannot be confused.
- [ ] Unsupported configuration produces a typed error.
- [ ] Provider-specific options are namespaced, versioned, and bounded.
- [ ] Secrets and endpoint configuration cannot enter invocation payloads.
- [ ] Cancellation and transport limits remain governed by the Connector
  contract.

## R-002: Transaction lifecycle, correlation, and concurrency

**Status:** Accepted

Monoloop uses **transaction** for one complete externally requested interaction:

```text
transaction start
    = prompt admitted for a selected Channel

transaction end
    = exactly one final terminal result
```

A transaction spans the full turn. Provider-internal model calls, tool calls,
continuations, or other inner turns do not create additional Monoloop
transactions.

### Identity model

Every transaction has a `transaction_id` from admission and belongs to one
effective `session_id` once session establishment succeeds. The transaction ID
is the control identity before a new external session exists; the resulting
`SessionKey` is the session correlation/control identity afterward. External
callers own its relationship to a UI conversation, task, user, or durable
record.

Session strings are scoped by the selected Channel. Routing, duplicate
exclusion, and session-directed termination use the pair
`SessionKey { channel_id, session_id }`; external IDs are not assumed globally
unique across providers.

For an external agent/tool, the external system creates and manages the
meaningful session ID. The Connector must propagate and reuse that authoritative
ID. The external system owns its history, resumability, and inner turns.

Direct LLM APIs do not own sessions. For those Channels, `session_id` is only
Monoloop's bounded in-memory routing key for the in-flight transaction. It does
not create provider-side session state, conversation history, or persistence.
The caller may supply this ID; when omitted, Monoloop creates an ephemeral ID
that lives only for the transaction and returns it on every event and
completion. The caller supplies any prior conversation context required by the
model.

### Transaction input and output

Conceptually:

```text
TransactionInput
    session_id?
    selected_channel
    prompt / canonical_input
    session_config?
    invocation_config
    deadline?

TransactionEvent
    session_id
    sequence
    event

TransactionEnd
    session_id
    terminal_kind
```

Every ordinary event and final result carry the effective `session_id` once
established. A transaction terminated or failed before a new external system
creates its session has `session_id: None` in completion and remains identified
by `transaction_id`. Conceptually, successful completion is
`SessionKey.done`.

### Session reuse and external agents

1. A caller may supply an existing external `session_id` with a transaction.
2. The selected Connector attaches to or addresses exactly that session.
3. Monoloop must not request a new external session when an explicit reusable
   session ID was supplied.
4. If no session ID is supplied, the selected external-agent Channel asks the
   external system to create one and returns the authoritative ID it receives.
5. External session state, history, resumability, and inner-turn execution
   remain owned by the external agent/tool.
6. For these systems Monoloop is a correlated, bounded conduit. It does not
   reinterpret provider-owned inner turns as new transactions.

### Concurrency and multitasking

1. Monoloop must support a bounded number of simultaneous in-flow transactions
   for different session IDs.
2. Transactions for different external sessions must progress concurrently.
3. Direct-LLM transactions with different session IDs must progress
   concurrently.
4. Each Connector resolves transport work for the sessions/connections it owns;
   the Loop maintains the bounded in-memory in-flow registry and routes each
   result to the correct session ID.
5. No in-flow transaction or session-routing state is persisted by Monoloop.
6. Resource limits must bound total transactions, per-Channel transactions,
   queued input/output, and transaction lifetime.
7. Only one transaction may be in flow for a given `SessionKey` at a time. A
   second request for the same Channel/session pair is rejected; it is not
   queued. An identical opaque session string on a different Channel is a
   different key.
8. Backpressure or failure in one transaction must not corrupt, reorder, or
   terminate unrelated transactions.

### User termination

A caller must be able to request cancellation or forced termination by
`transaction_id` at every phase, including before a new external session has an
ID, and by `SessionKey` after session establishment.

Termination must:

- be idempotent;
- stop local production and routing promptly;
- invoke the selected Connector's cancellation/termination behavior;
- release bounded in-memory transaction resources; and
- produce exactly one terminal transaction result.

### Transaction atomicity

Atomicity applies to the transaction lifecycle defined above:

1. A prompt and its configuration are admitted together as one transaction.
2. The transaction remains bound to exactly one `session_id`.
3. Every emitted event belongs to that transaction and carries a
   monotonic transaction-local sequence.
4. Exactly one terminal result is selected and published.
5. No event may be emitted after the terminal result.
6. Terminal selection is race-safe across normal completion, cancellation,
   termination, timeout, transport loss, and limit failure.
7. Failure of one transaction cannot be reported as success or failure of
   another transaction.
8. Outputs from simultaneous transactions cannot be mixed.

### Architectural boundary

The transaction coordinator belongs to the composed Monoloop runtime and does
not turn the raw Connector into a conversation or task manager. The Connector
continues to own transport and external-session routing. The Interpreter
continues to decode one dialect stream. The Loop/runtime owns bounded
transaction admission, correlation, lifecycle races, and terminal publication.

### Acceptance criteria

- [ ] A transaction can be cancelled or terminated during every lifecycle
  phase, including Connector open and an active provider request.
- [ ] Every admitted transaction produces exactly one terminal result.
- [ ] No transaction event is emitted after its terminal result.
- [ ] Multiple external sessions progress concurrently without cross-routing.
- [ ] A second request for an in-flow SessionKey is rejected immediately.
- [ ] Identical session strings on different Channels remain isolated.
- [ ] Multiple stateless LLM transactions progress concurrently.
- [ ] Reusing an explicit external-tool session ID does not create a new
  session.
- [ ] An external tool creates and returns its own meaningful session ID when
  one was not supplied.
- [ ] A direct LLM uses a supplied session ID or an ephemeral Monoloop-generated
  ID when absent.
- [ ] Direct LLM session IDs remain in-memory routing keys only.
- [ ] Transaction state is bounded and remains in memory only.
- [ ] Tests exercise completion/cancel/terminate races and prove one terminal
  result under each race.

## R-003: Request-scoped tool parity and callback completion

**Status:** Accepted

Every transaction request supplies the complete set of tools available during
that transaction:

```text
transactionRequest(
    prompt,
    config,
    session_id?,
    tools[],
    completion_callback
)
```

Tool implementations are linked into the Monoloop host at compile time.
Individual requests do not load executable code. Instead, each request selects
which linked tools are available for that transaction.

Two simultaneous transactions may expose completely different tool sets.
Availability is isolated by `TransactionId` and `SessionKey` and must not leak
between transactions.

### Canonical tool specification

The request's `tools[]` contains stable tool IDs only. Supplying canonical
descriptions, schemas, or implementations is the host registry's
responsibility, not the client's.

Each registered tool supplies a canonical specification including at least:

```text
ToolSpec
    tool_id
    name
    description
    input_schema
    output_contract
```

The host maintains a static registry from `tool_id` to its canonical
specification and linked implementation. Transaction admission resolves the
request's tool ID list against that registry.

- Unknown or unavailable tool IDs reject the transaction with a typed
  admission error.
- Duplicate tool IDs reject the transaction.
- An empty list means that no tools are available.
- The resolved tool set is immutable for the lifetime of the transaction.
- A tool call is authorized only when its tool ID is present in that
  transaction's resolved set.

### Agent and direct-LLM parity

The same canonical tool set is exposed through the mechanism required by the
selected Channel:

```text
External agent/tool Channel
    canonical tools[] -> transaction-scoped MCP exposure

Direct LLM Channel
    canonical tools[] -> model-dialect tool definitions
                       -> local Loop execution
```

MCP exposure and direct-LLM exposure must remain at parity:

1. They originate from the same canonical `ToolSpec`.
2. They expose equivalent names, descriptions, and input schemas.
3. They route to the same linked implementation.
4. They enforce the same request-scoped availability decision.
5. They produce equivalent canonical tool lifecycle and result events.
6. A tool unavailable to one transaction cannot be discovered or invoked
   through another transaction's MCP or local model-tool path.
7. A delayed MCP request from an earlier transaction cannot be routed into a
   later transaction on the same external session.
8. A profile that can install MCP only during session creation is explicitly
   `CreationOnly`: tool-enabled use requires a new session and that attachment
   cannot later be reused by Monoloop. Reusable-session tool parity requires a
   profile that can refresh the MCP descriptor per transaction.

Provider-specific encoding differences belong to the Channel's outbound
encoder or MCP adapter. They must not create separate tool definitions with
divergent semantics.

### Asynchronous submission

The production `TransactionRuntime` in Component 3 must be asynchronous and
non-blocking. Submitting a request performs only bounded validation and
admission; it does not wait for the transaction to finish.

Conceptually:

```text
loopRequest(prompt, config, session_id?, tools[], callback)
    -> admitted | admission_error

later:
    callback(TransactionEnd { session_id, ... })
```

The calling system does not poll a completion handle and does not block or
await the final result.

### Completion callback contract

1. Every admitted transaction registers exactly one completion callback.
2. The callback is invoked exactly once with the transaction's final result.
3. The callback carries the transaction ID and its effective `session_id` when
   session establishment succeeded.
4. Normal completion, user termination, timeout, transport failure, and limit
   failure all complete through the same callback contract.
5. Callback invocation occurs only after terminal selection and after final
   `Ended` delivery is attempted under `terminal_event_delivery_deadline`.
6. No transaction event is emitted after that final attempt or after scheduling
   the completion callback.
7. Callback execution must not block transaction processing, Connector I/O, or
   unrelated callbacks.
8. A slow or failing callback must not delay or alter other transactions.
9. Callback state is held in bounded memory only and released after invocation.
10. A request rejected before admission does not create a transaction; the
    submission call returns a typed admission error directly.

The concrete Rust API may represent the callback as an asynchronous completion
sink rather than a literal function pointer, but its observable behavior must
remain push-based: submit now, receive one completion notification later,
without polling.

### Concurrency requirements

- Tool calls from different transactions may execute concurrently within
  configured global and per-tool limits.
- Calls belonging to one transaction retain transaction-local ordering and
  correlation.
- Backpressure in MCP, local tool execution, or callback delivery must be
  bounded and isolated from unrelated transactions.
- Transaction termination prevents new tool calls from starting for that
  transaction and completes or cancels already-started calls according to the
  linked tool's declared cancellation policy.
- Every linked tool has a bounded termination mechanism: cooperative within a
  deadline, directly abortable, or isolated behind a killable boundary.
- Successful outputs and declared domain failures are bounded and validated
  against the canonical output contract. Runtime/contract failures remain
  distinct from ordinary tool-domain failures.

### Acceptance criteria

- [ ] Every transaction request can specify an independent tool set.
- [ ] Tool implementations are linked/registered by the host, not loaded from
  request data.
- [ ] Clients select tools by stable ID and cannot redefine their schemas.
- [ ] Unknown or duplicate tool IDs fail admission.
- [ ] Empty `tools[]` exposes and executes no tools.
- [ ] External-agent MCP and direct-LLM tool definitions are generated from the
  same canonical specifications.
- [ ] Both paths invoke the same linked implementation and emit equivalent
  canonical results.
- [ ] One transaction cannot discover or invoke another transaction's tools.
- [ ] MCP capabilities rotate per transaction and stale capabilities fail
  closed.
- [ ] Creation-only and refreshable MCP profiles are distinguished honestly;
  unsupported session reuse is rejected.
- [ ] Tool outputs are validated and domain failures remain distinguishable
  from runtime failures.
- [ ] Unstoppable in-process tool handlers are rejected.
- [ ] Request submission returns after bounded admission without waiting for
  completion.
- [ ] Every admitted transaction invokes its completion callback exactly once.
- [ ] Callers do not need to poll or await a completion handle.
- [ ] Slow/failing callbacks and tool calls do not block unrelated
  transactions.
- [ ] Termination prevents further tool dispatch for the terminated
  transaction.

## R-004: Canonical input, live event subscriptions, and ephemerality

**Status:** Accepted

### Canonical input boundary

The first canonical input schema is an ordered list of typed messages: system
and user text messages; assistant messages containing text and/or canonical
tool-call declarations; and tool-result messages referencing a preceding
assistant tool call. This must represent caller-supplied prior tool context
without inventing or dropping the assistant call.

Monoloop has no responsibility for crafting, augmenting, rewriting, ranking,
summarizing, or otherwise deciding prompt content. The caller supplies the
complete canonical input required by the selected Channel. The outbound dialect
encoder only converts that supplied input into provider wire format.

The schema is version-extensible so later contracts may add image, audio, file,
or structured parts. The first implementation accepts text parts only and
rejects unsupported parts explicitly. It remains a caller-built request
product.

Transaction limits such as deadlines, inner-turn ceilings, token limits, tool
call limits, and byte limits belong to transaction/Channel configuration. Their
concrete field set is defined by `TRANSACTION_RUNTIME_IMPLEMENTATION.md`.
Defaults are host configuration, not hard-coded provider policy, and every
effective limit is validated before work begins.

### Live canonical event subscriptions

Canonical transaction events must be subscribable and emitted as they arrive.
Subscribers receive semantic events from the Interpreter/Loop, not a rendered
or reassembled presentation.

```text
external provider bytes
    -> Interpreter
    -> canonical transaction events
    -> subscribed event sink(s)

terminal transaction result
    -> completion callback
```

Requirements:

1. Event subscription is established as part of bounded transaction admission
   so that initial events cannot be missed.
2. Every event carries `transaction_id`, `channel_id`, the effective
   `session_id`, and a monotonic transaction-local sequence.
3. Events are published incrementally as soon as canonical units become
   available.
4. Events from simultaneous sessions must never be mixed or cross-routed.
5. The final presentation format is not part of the transaction event contract.
6. Reassembly, projection, formatting, and UI rendering happen downstream.
7. The existing presentation/report format remains a demonstration of how a
   downstream consumer may reassemble canonical events.
8. Subscribers do not poll the transaction. Delivery is push-based through an
   event sink/callback/subscription adapter.
9. Subscriber count, buffering, and backpressure are bounded by configuration.
10. The terminal result is delivered through the completion callback defined in
    R-003 after the final transaction event is accepted, or after a bounded
    failed final-delivery attempt that is reported truthfully in completion.
11. Terminal event delivery has an independent cleanup deadline and does not
    reuse an expired transaction deadline.

### Entirely ephemeral runtime

The complete Monoloop system is ephemeral.

- No transaction, direct-LLM session, external-session routing entry, event,
  callback, prompt, configuration, or tool-call state is persisted by
  Monoloop.
- All runtime state is bounded and held in memory only.
- Direct-LLM session IDs exist only for in-flow correlation and are discarded
  after completion.
- External-agent session state remains in the external system. Monoloop retains
  only the temporary routing information needed while running.
- Process restart loses all Monoloop-owned state and outstanding callbacks.
- Durable conversation, audit, retry, UI, and recovery state belong entirely to
  the calling system or external provider.

### Acceptance criteria

- [ ] A caller can attach an event subscriber atomically with transaction
  admission.
- [ ] Canonical events are pushed as they are produced.
- [ ] Subscribers receive canonical events rather than presentation output.
- [ ] Event sequence and session correlation remain correct under concurrent
  transactions.
- [ ] The completion callback follows final-event acceptance exactly once, and
  a failed final delivery is reported rather than claimed delivered.
- [ ] Graceful runtime shutdown invokes every admitted transaction's callback
  exactly once, including transactions whose actor requires forced abort.
- [ ] Downstream code can reconstruct presentation independently from the event
  stream.
- [ ] Monoloop contains no persistence implementation or durable recovery path.
- [ ] All in-memory state is released after transaction completion.
- [ ] Direct-LLM generated session IDs disappear after completion.
