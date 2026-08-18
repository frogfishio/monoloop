---
always: true
priority: 19
---

# Project Laws

Hard constraints. Violating these is a design failure, not a style preference.
Normative detail lives in `doc/`; this file is the short enforcement list.

## Product shape

1. Monoloop is **exactly three** product components: Connector, Interpreter, Loop.
2. Driver, Console Input/Renderer, fixtures, and outbound test encoders are
   **test kit only** — never a fourth product component or product dependency.
3. Cross-component behavior requires an **explicit contract** in `doc/`. No
   responsibility by accident because a host “already combines them.”
4. Specs under `doc/` are normative (MUST / MUST NOT). Implementation follows them.

## Identity and isolation

5. No ambient or task-local “current” session, connection, run, channel, or tool.
6. No most-recent-session heuristic. Resume only with an **explicit** external
   session ID (Grok: `sessionId` + `session/load`).
7. For Grok Build, Grok’s `sessionId` is the session correlation identity; do not
   invent a competing Monoloop session ID.
8. Runs, connections, Interpretations, and Loops do not share mutable request,
   tool, event, cancellation, or completion state across run boundaries.
9. Cross-run injection, cancellation, result write, or event consumption is rejected.

## Canonical and tools

10. Interpreter emits **complete** canonical units only — never tokens, text deltas,
    or partial tool JSON as canonical content.
11. Loop dispatches tools **only** on complete `ToolRequestReady`. Waiting,
    incomplete, malformed, prose, or Markdown “looking like a tool” never execute.
12. Initial composition uses EmptyToolRegistry / NoToolRuntime: deterministic
    `tool_unavailable`, **zero external effects**, no shell/network/model fallback.
13. Loop never mutates Interpreter state or writes Connector input. Outbound results
    are provider-neutral; dialect encoding is a separate seam.

## Channels and routing

14. Caller selects Channel explicitly. No silent Channel/provider substitution,
    ranking, or fallback retry inside Monoloop.
15. Connector moves dialect-labelled bytes and minimal routing envelopes only —
    no semantic interpretation of assistant text, tools, plans, or turns.
16. Prompts and secrets never go on process argv for the Grok Build path
    (authenticated ACP/JSON-RPC over WebSocket to one long-lived server; not
    one process per session). Headless CLI profiles (Z.ai, Claude Code) that
    require a vendor `-p`/`prompt` argument MAY place the prompt on argv only
    when recorded in `DECISIONS.md`; they MUST NOT place credentials or secrets
    on argv. Prefer ACP/WebSocket or stdin-only transports when the vendor
    supports them.

## State and persistence

17. No Monoloop-owned durable conversation, session store, database, or history log.
18. All run-owned state is bounded in memory and **destroyed** at run terminal.
19. Connector may keep only a bounded in-memory external-session routing table.
20. Console JSONL is diagnostics, not product persistence.

## Async and resources

21. Fully async from the first implementation; no blocking I/O on async workers;
    no synchronization guard held across `.await`.
22. Every queue, buffer, table, and concurrency limit is **bounded**. Exceeding a
    limit fails explicitly — never unbounded growth to stay “responsive.”
23. Every spawned task has an owner, cancellation path, single safe join, and cleanup.
    Detached fire-and-forget is prohibited.
24. Actionable Loop subscription is lossless and gap-detecting. Gaps fail closed.
    Console and Loop never race for one receiver.
25. Exactly one terminal outcome per connection, Interpretation, Loop, and run.
    EOF / text “done” / Interpreter end alone do not mean run success.

## Dependency direction

26. Product crates must not depend on monoloop-testkit, console adapters, host agent,
    product UI, Tauri, Kanban, DAL, Residiuum, context compiler, memory, router, or
    concrete tool modules.
27. Context Engine / hosts may call Monoloop; Monoloop never reaches into their
    internals to build prompts or choose models.

## Security (see also SECURITY.md)

28. All channel output and tool material are untrusted. Canonical ≠ authorized.
29. Secrets and external session IDs never appear in logs, metrics labels, or default
    diagnostics. Credentials resolve only through injected secret boundaries.
