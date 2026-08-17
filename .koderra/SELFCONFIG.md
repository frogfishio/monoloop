---
always: true
priority: 12
maxChars: 4500
---

# Koderra Self-Configuration — Monoloop

## What this project is

**Monoloop** is a ground-zero **async Rust kernel** for multi-channel LLM I/O:

```text
Connector → Interpreter → transaction-composing Loop
```

- **Language / runtime**: Rust, Tokio multi-thread async.
- **Status**: Architecture under construction; authoritative product DNA lives in
  `doc/` (RFC-style MUST/MUST NOT). Implementation crates may lag the specs.
- **Initial Connector profile**: Grok Build — ACP/JSON-RPC 2.0 over authenticated
  WebSocket; one long-lived server, many sessions keyed by Grok `sessionId`.
- **Not in product**: chat app, agent, prompt engine, memory, Kanban, model router,
  persistence service, product UI, Tauri, concrete tools.

Test kit (`doc/TEST_KIT.md`, Console Input/Renderer) proves the three components.
It is **prohibited** as a product dependency.

## Authoritative sources (read these before inventing)

| Concern | Source |
|---|---|
| Product + composition | `doc/MONOLOOP.md` |
| Requirements | `doc/REQUIREMENTS.md` |
| Transaction architecture | `doc/TRANSACTION_RUNTIME_DESIGN.md` |
| Development contract | `doc/TRANSACTION_RUNTIME_IMPLEMENTATION.md` |
| Delivery sequence | `doc/TRANSACTION_RUNTIME_DELIVERY_PLAN.md` |
| Component 01 | `doc/CONNECTOR.md` |
| Component 02 | `doc/INTERPRETER.md` |
| Component 03 | `doc/THE_LOOP.md` |
| Grok profile | `doc/GROK_BUILD_CONNECTOR.md` |
| Test composition | `doc/TEST_KIT.md`, `doc/CONSOLE_*.md` |
| Index | `doc/README.md`, root `README.md` |

Specs are normative. If code and docs disagree, **fix the code or open a
spec revision** — do not silently expand component responsibilities.

## Operating rules for agents in this workspace

1. **Preserve the three-component boundary.** Before adding code or types, ask:
   which of Connector / Interpreter / Loop / testkit owns this? Component 3's
   transaction layer owns production composition and its specified encoder,
   tool, and MCP adapters; unspecified responsibilities remain **out of product**.
2. **No responsibility by accident.** Do not pull in prompt augmentation, session
   history storage, UI, concrete tools, routers, or host-agent modules because a
   demo “needs them.” Cross-component behavior requires an explicit `doc/` contract.
3. **Explicit identities only.** Thread `MonoloopRunId`, `ConnectionId`,
   `InterpretationId`, `ToolActionId`, external `sessionId` on envelopes. Never
   introduce process-global or task-local current session/connection/run/tool.
4. **Bounded + fail-closed.** Every queue, buffer, table, and concurrency limit is
   bounded. Gaps, backpressure, and unsupported dialects fail closed — never
   silently drop actionable Loop events or invent success.
5. **Canonical completeness.** Interpreter: no token/delta/partial-JSON escape.
   Loop: dispatch **only** on complete `ToolRequestReady`. Empty registry is the
   required first path.
6. **Async hygiene.** Non-blocking I/O; no lock held across `.await`; every spawned
   task has owner, cancel, single join, cleanup. No fire-and-forget.
7. **Channel is caller-selected.** No silent Channel/provider fallback or ranking.
8. **Grok profile specifics.** One server instance per deployment; prompts never on
   argv; `sessionId` from Grok is correlation identity; reconnect via explicit
   `session/load` only.
9. **Security defaults.** Untrusted I/O; secrets via refs only; redact session IDs
   and credentials in logs/metrics; no raw body logging by default.
10. **Tests match acceptance suites in the specs.** Prefer deterministic fixtures
    and architecture/import gates over “shaped” partial demos. Console adapters
    optional; deleting them must not change product semantics.

## How to implement (when code lands)

Preferred logical packaging (from `doc/MONOLOOP.md` §32):

```text
monoloop-contracts      # identities, canonical types, errors, ports
monoloop-connector      # abstract Connector + shared transport primitives
monoloop-interpreter    # Interpreter + dialect plugins
monoloop-loop           # Loop + abstract ToolRegistry/ToolRuntime
monoloop-testkit        # Driver, fixtures, console adapters
monoloop-conformance    # qualification scenarios
```

Physical crate merge is allowed only if dependency rules still hold. Product
crates **must not** depend on testkit/console.

Trait spellings in docs may vary; **semantics** (begin_open returns immediately,
exactly one terminal outcome, lossless Loop subscription, etc.) are required.

## How to change the product

- Spec change first (or in the same change set as code) under `doc/`.
- Update DECISIONS.md when the *why* changes.
- Keep LAWS/STYLE/ARCHITECTURE aligned with componentisation: composition over
  monolith; no ambient state; no persistence creep.
- Deferred work lists in each component doc are intentional — do not “finish”
  deferred items (real tools, durable receipts, product UI, context engine)
  inside the kernel without an explicit product decision.

## What “done” means here

Acceptance is the checklists in each component + `MONOLOOP.md` §31, including:

- architecture/import gates;
- isolation under concurrent runs/sessions;
- cancellation races with exactly one terminal;
- empty-tool path with zero effects;
- no provider-native DTO across the canonical boundary.

Partial green tests that violate non-responsibilities are **not** acceptance.

## Koderra brain hygiene (this repo)

- Prefer high-signal updates to `rules/`, DECISIONS, PLAN, and this file over
  pasting large doc excerpts into chat.
- Scope rules to `doc/**` or future crate paths when implementation appears.
- Track work with structured tasks (`executeTaskTool` / project task tools), not
  ad-hoc markdown task lists as source of truth.
- After substantial delivery, use expert/advisor gates against **spec compliance
  and component boundaries**, not only “does it compile.”

## Anti-patterns (reject these)

- Storing conversation history “temporarily” in Monoloop.
- Encoding requests inside Connector; decoding semantics inside Connector.
- Letting Console and Loop share one mpsc receiver.
- Feeding Interpreter fragments to UI or tool execution.
- Hard-coding Grok behavior outside its Connector profile + dialect.
- Treating EOF / “done” text / Interpreter end as run success.
- Adding a DB for diagnostics or resumability.
- Implicit Channel retry or most-recent session selection.
- Making console adapters required for product crates.
- Inventing successful tool output when the registry is empty/unavailable.
