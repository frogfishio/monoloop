# Monoloop — Three-component asynchronous kernel

**Status:** Architecture under construction

**Implementation language:** Rust

**Product specification:** [MONOLOOP.md](MONOLOOP.md)

**Requirements register:** [REQUIREMENTS.md](REQUIREMENTS.md)

**Transaction runtime design:** [TRANSACTION_RUNTIME_DESIGN.md](TRANSACTION_RUNTIME_DESIGN.md)

**Development specification:** [TRANSACTION_RUNTIME_IMPLEMENTATION.md](TRANSACTION_RUNTIME_IMPLEMENTATION.md)

**Engineering delivery plan:** [TRANSACTION_RUNTIME_DELIVERY_PLAN.md](TRANSACTION_RUNTIME_DELIVERY_PLAN.md)

This directory defines Monoloop from first principles. It does not describe a
refactor of a previous implementation loop. Existing code may later supply
adapters or reusable implementations only after it satisfies these contracts.

Monoloop consists of exactly three product components:

1. a Connector;
2. an Interpreter; and
3. a transaction-composing Loop with a separately testable inner tool runtime.

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
| 03 | [The Loop](THE_LOOP.md) | Bounded transaction composition plus a lossless complete-unit tool runtime |

Initial Connector implementation profiles:

- [Grok Build Network Connector](GROK_BUILD_CONNECTOR.md) — one authenticated
  Grok Build WebSocket server, multiple independently correlated sessions.
- [Cursor Agent Connector](CURSOR_CONNECTOR.md) — Cursor CLI `agent acp` over
  stdio NDJSON (ACP family; Cursor profile).
- [Antigravity (agy) Connector](AGY_CONNECTOR.md) — Google Antigravity via
  stdio ACP (`agy-acp` bridge until native `agy --acp` ships).
- [OpenAI Codex Connector](CODEX_CONNECTOR.md) — Codex via stdio ACP
  (`@agentclientprotocol/codex-acp` adapter over Codex App Server).
- [Z.ai CLI Connector](ZAI_CONNECTOR.md) — Z.ai `zai -p` headless OpenAI-chat
  NDJSON (not ACP; tools execute inside the CLI).
- [Claude Code Connector](CLAUDE_CONNECTOR.md) — Claude Code
  `claude -p --output-format stream-json` (not ACP; native timestamps).

Test infrastructure:

- [Test Kit and Driver](TEST_KIT.md)
- [Console Input](CONSOLE_INPUT.md)
- [Console Renderer](CONSOLE_RENDERER.md)

Required supporting seams—canonical outbound encoding and MCP exposure—are
specified by the transaction design and development specification. Production
implementations are adapters owned by Component 3's transaction layer; test
composition may provide deterministic substitutes. They never become additional
product components.

No component acquires responsibilities merely because the current application
already combines them. Cross-component behavior is added only through an
explicit contract in this directory.
