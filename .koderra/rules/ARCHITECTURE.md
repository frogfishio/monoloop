---
always: true
priority: 18
---

# Architecture Overview — Monoloop

## Key architectural principles

- **Componentisation, not monolith.** Monoloop is exactly three independently
  testable product components. Responsibilities do not migrate “because the app
  already combines them.”
- **Spec-first.** Normative contracts live under `doc/` (MUST / MUST NOT). Code
  implements contracts; it does not redefine them ad hoc.
- **Explicit identity, no ambient state.** Run, connection, interpretation, tool
  action, and external session IDs travel on envelopes. No process-global or
  task-local “current *”.
- **Bounded, async, fail-closed.** Every queue/table/concurrency limit is bounded.
  Gaps and backpressure are explicit. Actionable events are never silently dropped.
- **In-memory kernel only.** No Monoloop-owned durable conversation, session store,
  or database. External systems own resumable sessions.

## Product definition

```text
Connector → Interpreter → transaction-composing Loop
```

| Component | Spec | Responsibility |
|---|---|---|
| 01 Connector | `doc/CONNECTOR.md` | Dialect-labelled transport, explicit external-session routing, cancel/terminate, one terminal transport outcome |
| 02 Interpreter | `doc/INTERPRETER.md` | Incremental dialect decode + assembly; provider-neutral **complete** canonical units only |
| 03 The Loop | `doc/THE_LOOP.md` | Bounded transaction admission/composition plus a lossless complete-unit tool runtime |

**Not product components:** Driver, Console Input, Console Renderer, deterministic
fixtures, outbound test encoders. They form `monoloop-testkit` and must not become
production dependencies of the three product crates.

**Initial Connector profile:** Grok Build — ACP/JSON-RPC over authenticated
WebSocket; one long-lived server; many sessions keyed by Grok `sessionId`
(`doc/GROK_BUILD_CONNECTOR.md`).

**Supporting seams (not fourth product components):** outbound dialect encoders
and MCP adapters. Production implementations are owned by Component 3's
transaction composition layer; the inner `LoopRuntime` remains dialect- and
transport-neutral.

## Composition flow

```text
caller
  → TransactionRuntime.submit(TransactionRequest)
  → bounded admission + active-session reservation
  → explicit ChannelBinding
  → outbound dialect encoder
  → Connector raw input
  → external channel
  → Connector raw output + DialectBinding
  → Interpreter
  → canonical event distributor
       → caller subscription
       → Console Renderer (test only)
       → inner LoopRuntime (lossless)
           → request-scoped ToolRegistry/ToolRuntime
           → OutboundToolResult
  → continuation policy (inline | caller_controlled)
  → TransactionEnd event
  → destroy transaction state
  → one completion callback
```

## Package boundaries (preferred)

```text
monoloop-contracts      # identities, canonical types, errors, ports
monoloop-connector      # abstract Connector + transport primitives
monoloop-interpreter    # Interpreter + dialect plugins
monoloop-loop           # transaction composition + inner Loop + tool/MCP adapters
monoloop-testkit        # Driver, fixtures, console adapters
monoloop-conformance    # qualification scenarios
```

No required `monoloop-core` coordinator facade. Physical crate consolidation is
allowed only if dependency rules and public seams hold. Product crates **must not**
depend on testkit.

## Non-responsibilities (kernel)

Monoloop is **not**: a chat app, agent, prompt/context engine, memory system, task
system, model router, persistence service, or product UI.

- Connector: moves bytes and routing envelopes; does not interpret semantics.
- Interpreter: assembles canonical units; does not execute tools or complete turns.
- Inner Loop runtime: reacts to complete tool requests; does not encode dialect,
  write Connector input, or contain MCP transport. Component 3's transaction
  coordinator invokes those adapters and composes component handles.

Related systems (Context Engine, host agent, UI, etc.) may **consume** Monoloop;
dependency direction is one-way into the kernel, never reverse.

## Key runtime rules

- Many concurrent runs; no shared mutable request/tool/completion state across runs.
- Connector may retain a **bounded** in-memory external-session routing table only.
- Interpreter: no token/delta/partial-JSON escape; fragmentation-invariant output.
- Loop: independent lossless subscription (never share a receiver with Console);
  empty registry → deterministic `tool_unavailable`, zero effects.
- Exactly one terminal outcome per connection / interpretation / loop / run.
- Cancellation is explicit, bounded, idempotent, and run-scoped.
- One in-flow transaction per SessionId; a duplicate is rejected, never queued.
- Production transaction completion and live events are push-based.

## Authoritative docs

- Index: `doc/README.md`, root `README.md`
- Requirements: `doc/REQUIREMENTS.md`
- Integration: `doc/MONOLOOP.md`
- Transaction architecture: `doc/TRANSACTION_RUNTIME_DESIGN.md`
- Development contract: `doc/TRANSACTION_RUNTIME_IMPLEMENTATION.md`
- Components: `doc/CONNECTOR.md`, `doc/INTERPRETER.md`, `doc/THE_LOOP.md`
- Grok profile: `doc/GROK_BUILD_CONNECTOR.md`
- Test kit: `doc/TEST_KIT.md`, `doc/CONSOLE_INPUT.md`, `doc/CONSOLE_RENDERER.md`

See also: `DECISIONS.md`, `PLAN.md`, `SELFCONFIG.md`, `rules/LAWS.md`, `rules/SECURITY.md`.
