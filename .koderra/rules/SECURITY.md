---
always: true
priority: 20
---

# Project Security Notes

Monoloop is a three-component async Rust kernel (Connector, Interpreter, Loop).
Security is structural: untrusted I/O, opaque credentials, identity isolation,
bounded resources, and zero accidental effects from incomplete tool material.

## Trust model

- All channel output, dialect bytes, tool names, and tool payloads are **untrusted**.
- **Canonical ≠ authorized**. Structural completeness does not grant execution rights.
- Authority for effects lives only in a future explicit policy/admission layer, not
  in Connector, Interpreter, or The Loop.
- Initial product composition uses `EmptyToolRegistry` / `NoToolRuntime`: complete
  tool requests resolve to deterministic `tool_unavailable` with **zero external effects**.

## Credentials and secrets

- Transport secrets (e.g. Grok Build WebSocket server secret) resolve only through
  an injected secret / configuration boundary (`*_secret_ref`, credential reference).
- **Never** log, metric-label, trace, descriptor, error, or terminal-outcome
  credential material, prompts, raw provider bodies, or unrestricted endpoints.
- External session IDs (Grok `sessionId`) are **opaque and redacted** in logs/metrics;
  they grant authority only for explicitly addressed create/load/route operations.
- Monoloop **must not** read, copy, parse, refresh, or log host Grok account
  credential files. Host auth to Grok’s backend is outside this product.

## Session and identity isolation

- No ambient “current session / connection / run / tool / channel”.
- No most-recent-session heuristic after restart or routing-table loss.
- Resume only via **explicit** known external session ID + `session/load` (or
  equivalent profile contract).
- Cross-run injection, cancellation, result write, or event consumption is rejected.
- Connector cancellation/termination is connection-scoped; one connection must not
  close siblings.
- Loop instances are owned by exactly one `MonoloopRunId`; cross-run events fail closed.

## Transport and Grok Build profile

- Default local Grok server binds **loopback**. Non-loopback requires an explicit
  authenticated transport-security policy; fail closed otherwise.
- Prompts and session control travel only over authenticated ACP/JSON-RPC WebSocket —
  never via process argv or one-process-per-session spawn side effects.
- Connector may parse only bounded framing/routing fields (e.g. JSON-RPC id,
  `sessionId`). Semantic payload inspection (text, tools, plans) is prohibited.
- TLS / pipe / socket security requirements fail closed.

## Interpreter and Loop surfaces

- Treat tool names/arguments as **data**, never as code, shell, or dynamic load targets.
- No string-to-shell interpretation; no dynamic code load from tool names.
- Partial JSON, token deltas, and raw fragments must never escape as executable
  or canonical-complete content.
- Avoid terminal escape / control-sequence interpretation of raw model bytes.
- Quoted external content remains untrusted content, not elevated authority.
- Reject cross-action / cross-lane fragment injection without explicit correlation.
- Bound all diagnostic and result material; do not copy secrets or full provider
  error bodies into default diagnostics.

## Resource and DoS bounds

- Every queue, buffer, table, concurrency limit, and deadline is **bounded**.
- Enforce frame/assembly/aggregate limits **before** attacker-controlled allocation growth.
- Backpressure and fail-closed behavior beat unbounded buffering “to stay responsive”.
- Detached fire-and-forget tasks are prohibited; every task has owner, cancel, join, cleanup.

## Persistence boundary

- Monoloop product components open **no** database, history log, session store, or
  checkpoint repository of their own.
- Console JSONL is diagnostic output, not product persistence.
- External apps (e.g. Grok Build) own durable conversation state; local in-memory
  routing tables are not a substitute for durable session ownership.

## Logging and observability

- Never log secrets, PII, prompts, raw I/O bodies, tool payloads, or full provider errors.
- Metrics labels are content-free and bounded (no run/session/project IDs, paths,
  raw text, or credentials).
- Raw input/output logging is prohibited by default.

## Least privilege (implementation)

- Product crates must not depend on `monoloop-testkit` or console adapters.
- No host agent, product UI, Tauri, Kanban, DAL, memory, router, or concrete tool
  modules in product-component dependency graphs.
- When tools are later introduced: authorize **before** `ToolRuntime.start`; keep
  results scoped to owning run/action; never invent success for unavailable tools.
