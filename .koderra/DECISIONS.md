# Key Project Decisions

Record architectural and product decisions with rationale. Prefer short, dated
entries. Specs under `doc/` remain normative; this file captures *why*.

## 2026-08 — Three-component kernel only

- **Decision**: Monoloop is exactly Connector + Interpreter + an extensible Loop
  component. Its provider-neutral inner runtime remains minimal; the later
  2026-08-17 decision adds production transaction composition inside Component 3.
- **Rationale**: Keep a ground-zero cognitive runtime small, independently testable,
  and free of chat/agent/prompt/memory/task product surface. Hosts compose; the
  kernel does not grow into a monolith.
- **Implication**: Driver, Console Input/Renderer, fixtures, and outbound test
  encoders are **test kit**, never a fourth product component or production dependency.

## 2026-08 — Spec-first, contracts before code

- **Decision**: Architecture is defined under `doc/` with RFC-style MUST/MUST NOT
  language before implementation crates land.
- **Rationale**: Prevent “shaped” or partial qualification and accidental coupling
  from prior combined applications.
- **Implication**: Existing code may supply adapters only after it satisfies these
  contracts. Cross-component behavior requires an explicit contract in `doc/`.

## 2026-08 — No Monoloop-owned durable persistence

- **Decision**: Kernel state is bounded in-memory operational state only.
- **Rationale**: Conversation durability and resumability belong to external systems
  (initially Grok Build). Faking crash-safe exactly-once or durable history here
  would lie about ownership.
- **Implication**: Run teardown destroys run-owned state. Connector may keep a
  bounded in-memory external-session routing table; loss requires explicit reload
  with a known external session ID.

## 2026-08 — External session identity is authoritative

- **Decision**: For Grok Build, Grok’s returned `sessionId` is the session
  correlation identity; Monoloop does not invent a second competing ID.
- **Rationale**: One server hosts many logical sessions; routing must follow the
  protocol’s authority, not ambient UI “current session”.
- **Implication**: No most-recent/current-session heuristics. `session/new` and
  explicit `session/load` only.

## 2026-08 — Canonical stream is provider-neutral and complete-unit only

- **Decision**: Interpreter emits fully assembled canonical units (sentences,
  structures, tool lifecycle), never tokens/deltas/partial tool JSON.
- **Rationale**: Downstream Loop/UI must not race on transport fragmentation or
  invent completeness from prose.
- **Implication**: Fragmentation-invariant determinism; incomplete publication only
  for lifecycle-bearing constructs (e.g. tools waiting), never raw text chunks.

## 2026-08 — Loop dispatches only on ToolRequestReady

- **Decision**: Only complete canonical `ToolRequestReady` may resolve/dispatch tools.
- **Rationale**: Waiting placeholders, Markdown that looks like a tool, and partial
  JSON must never execute.
- **Implication**: Initial empty registry yields deterministic `ToolUnavailable` +
  `OutboundToolResult`; no shell/network/model fallback.

## 2026-08 — Caller selects Channel; no model router inside Monoloop

- **Decision**: Channel is explicit in the request; Monoloop validates and uses it.
- **Rationale**: Identical behavior under deterministic tests and future external
  routing; no silent provider fallback.
- **Implication**: Unavailable/failed/cancelled channel states stay distinct; no
  retry against another provider inside the kernel.

## 2026-08 — Continuation policy is explicit

- **Decision**: `inline_tool_continuation` vs `caller_controlled` are request-level
  immutable policies; Fabled Cognitive Runtime requires `caller_controlled`.
- **Rationale**: Hosts that recompile context each model decision must not get an
  uncompiled auto-continuation inside one run.
- **Implication**: Under `caller_controlled`, tool evidence ends the run as
  `continuation_required`, not another model write.

## 2026-08 — Outbound encoder is a seam, not a product component

- **Decision**: Canonical→dialect encoding is a required supporting seam.
  Deterministic test encoders live in the Driver path; production encoders live
  in Component 3's transaction adapter layer.
- **Rationale**: Encoding must not live inside Connector, Interpreter, Loop, or
  Console Input.
- **Implication**: The inner Loop emits provider-neutral `OutboundToolResult`
  only. The 2026-08-17 transaction-runtime decision supersedes the
  test-kit-only location: production encoders are Component 3 adapters invoked
  by `TransactionRuntime`, never by the inner Loop.

## 2026-08 — Async, multi-thread Tokio, no ambient task-local identity

- **Decision**: First implementation is fully async on a multi-threaded runtime;
  identities travel on events/handles, not task-local “current *” state.
- **Rationale**: Many concurrent runs/sessions; isolation under load is a product
  requirement, not an optimization.
- **Implication**: No blocking I/O on async workers; no sync guards held across
  await; every task owned, joined exactly once.

## 2026-08 — Preferred package boundaries

- **Decision**: Logical packages: `monoloop-contracts`, `-connector`,
  `-interpreter`, `-loop`, `-testkit`, `-conformance`. No required `monoloop-core`
  coordinator facade.
- **Rationale**: Dependency direction and public seams from day one; physical crate
  consolidation allowed only if rules still hold.
- **Implication**: Architecture gates must fail imports that violate product vs
  testkit and component non-responsibilities.

## 2026-08 — Loop empty-tool path + independent fan-out

- **Decision**: First Loop ships with EmptyToolRegistry/NoToolRuntime; composition
  uses an EventDistributor so Console and Loop each get a private subscription.
- **Rationale**: Prove tool reaction without effects; prove Console failure cannot
  steal Loop events.
- **Implication**: Ready tools emit exactly one unavailable OutboundToolResult;
  text never dispatches; duplicate Ready is idempotent.

## 2026-08 — Interpreter reassembles; console only prints complete units

- **Decision**: Interpreter owns dialect framing + sentence/tool assembly; console
  renderer lives in `monoloop-testkit` and only consumes canonical events.
- **Rationale**: Grok ACP streams are rambling deltas; product truth is complete
  sentences and complete tool requests. Presentation must not re-parse or invent
  completeness.
- **Implication**: Partial chunks never become console lines; tool waiting never
  exposes partial arguments; terminator at chunk end waits for next byte or seal.

## 2026-08 — Connector crate split + proxy

- **Decision**: Implement Component 01 as `monoloop-contracts` + `monoloop-connector`
  (abstract, fake, proxy) + `monoloop-connector-grok` (Grok profile).
- **Rationale**: Keep WebSocket/ACP deps out of the abstract connector; allow
  `ConnectorProxy` to route hosts to named backends without ambient state.
- **Implication**: Product interpreters/loop depend on contracts + abstract
  connector traits; Grok is an optional backend crate.

## 2026-08-17 — Component 3 owns production transaction composition

- **Decision**: Monoloop remains exactly three components. Component 3 contains
  a production `TransactionRuntime` composition layer and a separately testable,
  provider-neutral inner `LoopRuntime`.
- **Rationale**: Admission, correlation, event delivery, continuations,
  termination, and completion require one production owner. Treating that owner
  as an unspecified future host leaves the product API unimplemented.
- **Implication**: The transaction layer may compose Connector, Interpreter,
  outbound encoder, MCP adapter, and inner Loop handles. The inner Loop still
  does not encode dialects, write Connector input, or host MCP transport. This
  refines the earlier “minimal Loop” decision without creating a fourth
  component.

## 2026-08-17 — Push completion and one active transaction per SessionKey

- **Decision**: Production submission performs bounded synchronous admission,
  streams canonical events to an attached sink, and invokes one asynchronous
  completion callback. A second request for an active `SessionKey` is rejected
  immediately and is not queued.
- **Rationale**: Calling systems must not poll or block, and mutable session
  routing must remain unambiguous.
- **Implication**: Awaitable run handles remain test-kit internals. Provider
  capability cannot enable concurrent transactions on one session.

## 2026-08-17 — Request-scoped tools use one linked execution path

- **Decision**: The host registry is immutable and linked at startup; requests
  select tools by stable ID. MCP and direct-LLM projections derive from the same
  resolved set and delegate to the same dispatcher and handler.
- **Rationale**: Tool availability may differ per transaction without dynamic
  code loading or schema drift between agents and direct LLMs.
- **Implication**: MCP is a bounded Component 3 adapter, initially implemented
  with a maintained Rust MCP SDK over loopback Streamable HTTP. Profiles that
  cannot provide request-scoped MCP tools reject non-empty tool requests.

## 2026-08-17 — First direct-LLM dialect is Chat Completions

- **Decision**: The first OpenAI-compatible implementation is OpenAI Chat
  Completions v1 over streaming HTTP/SSE.
- **Rationale**: “OpenAI-compatible” is not one protocol. Chat Completions and
  Responses have different request, stream, tool, and terminal semantics.
- **Implication**: Add a distinct `OpenAiChatCompletions` dialect. OpenAI
  Responses, non-streaming JSON, and provider-specific NDJSON require separately
  qualified dialects.

## 2026-08-17 — First canonical input is ordered text messages

- **Decision**: The first input schema is caller-authored ordered messages with
  roles and ordered text parts, plus bounded tool-call correlation fields.
- **Rationale**: A single text string cannot faithfully carry existing
  conversation/tool context, while arbitrary multimodal payloads would create
  unqualified scope.
- **Implication**: Monoloop validates and mechanically encodes this product but
  never authors or rewrites it. New content-part kinds require versioned
  contracts and dialect tests.

## 2026-08-17 — Transaction isolation closes over Channel, exchange, and capability

- **Decision**: Session exclusion uses `SessionKey { ChannelId, SessionId }`;
  every provider cycle has a fresh `ExchangeId`/connection/interpretation; and
  every external-agent transaction receives a newly rotated MCP capability.
- **Rationale**: Provider session strings are not globally unique, completed
  Interpretations cannot decode later HTTP responses, and a stable MCP URL
  cannot distinguish a delayed old call from a current call.
- **Implication**: Session-directed termination includes Channel identity.
  Connector factories produce matched Connector/SessionAdapter instances.
  MCP descriptors are refreshed before every prompt and revoked before terminal
  publication. Profiles unable to refresh or reach the configured MCP transport
  reject tool-enabled requests.

## 2026-08-17 — Finalization and tool execution remain forcibly bounded

- **Decision**: Actor and shutdown supervisor share an exactly-once
  `FinalizationGuard`; final event delivery has its own cleanup deadline; and
  transaction tools must be cooperative, abortable, or isolated-killable.
- **Rationale**: Aborting an actor during shutdown must not lose its callback,
  an expired transaction deadline must not suppress its terminal event, and
  unstoppable in-process work contradicts leak-free bounded teardown.
- **Implication**: Graceful shutdown invokes every admitted callback exactly
  once. Unbounded/non-cancellable handlers are rejected. Tool outputs are
  contract-validated, while domain failures remain distinct from runtime
  failures.

## Template for new entries

- **Decision**: …
  - **Rationale**: …
  - **Implication**: …
  - **Date**: YYYY-MM-DD
  - **Refs**: `doc/….md` §…
