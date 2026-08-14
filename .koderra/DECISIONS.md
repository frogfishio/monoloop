# Key Project Decisions

Record architectural and product decisions with rationale. Prefer short, dated
entries. Specs under `doc/` remain normative; this file captures *why*.

## 2026-08 — Three-component kernel only

- **Decision**: Monoloop is exactly Connector + Interpreter + minimal extensible Loop.
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

- **Decision**: Canonical→dialect encoding is a required supporting seam; initial
  implementation lives in the test kit Driver path.
- **Rationale**: Encoding must not live inside Connector, Interpreter, Loop, or
  Console Input.
- **Implication**: Loop emits provider-neutral `OutboundToolResult` only.

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

## Template for new entries

- **Decision**: …
  - **Rationale**: …
  - **Implication**: …
  - **Date**: YYYY-MM-DD
  - **Refs**: `doc/….md` §…
