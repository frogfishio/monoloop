# Monoloop — Three-component asynchronous kernel

**Status:** Architecture under construction

**Implementation language:** Rust

**Product specification:** [MONOLOOP.md](MONOLOOP.md)

This directory defines Monoloop from first principles. It does not describe a
refactor of a previous implementation loop. Existing code may later supply
adapters or reusable implementations only after it satisfies these contracts.

Monoloop consists of exactly three product components:

1. a Connector;
2. an Interpreter; and
3. the smallest useful extensible Loop.

The components are asynchronous, non-blocking, independently testable Rust
libraries. A Connector may intentionally retain bounded in-memory state for
externally owned sessions, including correlation and routing state. This does
not make Monoloop the durable owner of those sessions.

The Driver, Console Input, Console Renderer, deterministic transports, fixtures,
and outbound test encoders form the separate test kit. They prove and exercise
the three components; they are not additional Monoloop product components and
must not become production dependencies.

| Product component | Specification | Responsibility |
|---|---|---|
| 01 | [Connector](CONNECTOR.md) | Dialect-labelled transport, explicit external-session routing, cancellation, and termination |
| 02 | [Interpreter](INTERPRETER.md) | Async incremental dialect interpretation and immediate in-memory canonical-unit events |
| 03 | [The Loop](THE_LOOP.md) | Minimal lossless event loop with an empty-capable extension/tool boundary |

Initial Connector implementation profiles:

- [Grok Build Network Connector](GROK_BUILD_CONNECTOR.md) — one authenticated
  Grok Build WebSocket server, multiple independently correlated sessions.
- [Cursor Agent Connector](CURSOR_CONNECTOR.md) — Cursor CLI `agent acp` over
  stdio NDJSON (ACP family; Cursor profile).

Test infrastructure:

- [Test Kit and Driver](TEST_KIT.md)
- [Console Input](CONSOLE_INPUT.md)
- [Console Renderer](CONSOLE_RENDERER.md)

Required supporting seam, contractually defined in
[Monoloop §7](MONOLOOP.md#7-outbound-encoder-seam) and intentionally awaiting a
separate implementation specification: canonical outbound request/result
encoding into the selected channel dialect. In the initial project it is
provided by the test kit Driver. It never becomes a fourth product component.

No component acquires responsibilities merely because the current application
already combines them. Cross-component behavior is added only through an
explicit contract in this directory.
