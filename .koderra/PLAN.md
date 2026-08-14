# Monoloop Development Plan

**Vision**: A three-component async Rust kernel (Connector → Interpreter → Loop)
that hosts compose for multi-channel LLM I/O. Specs in `doc/` are the living
product definition. The test kit proves the kernel without becoming product.

## Core principles

- Exactly three product components; test kit is disposable composition.
- Spec-first; acceptance suites and architecture gates define “done.”
- Explicit identities; bounded in-memory state; no Monoloop-owned persistence.
- Initial real profile: Grok Build ACP/JSON-RPC over WebSocket.
- Empty tool path first; real tools only behind later contracts.

## Current state

- [x] Foundational specs: Connector, Interpreter, Loop, Monoloop composition
- [x] Grok Build connector profile + test kit / console adapter specs
- [x] Rust workspace: `monoloop-contracts`, `monoloop-connector`, `monoloop-connector-grok`
- [x] Abstract Connector + FakeConnector + ConnectorProxy + cancel/terminal tests
- [x] Grok Build ACP/WebSocket connector (initialize, session/new, session/load,
      multi-session demux, secret boundary, mock-server tests)
- [x] monoloop-interpreter — ACP + test dialects; sentence assembly; tool lifecycle;
      fragmentation-invariant; no partial token/tool-arg escape
- [x] monoloop-testkit console renderer (append-only) for printing canonical events
- [x] monoloop-loop — EmptyToolRegistry / NoToolRuntime; ToolRequestReady only
- [x] Event distributor + Driver pipeline (Interpreter → Console ∥ Loop)
- [ ] Full CONNECTOR.md / INTERPRETER.md / THE_LOOP.md acceptance matrices
- [ ] Live Grok integration e2e (optional)

## Implementation roadmap (suggested order)

1. ~~**Workspace + monoloop-contracts**~~ — done.
2. ~~**monoloop-connector**~~ — abstract, Fake, Proxy.
3. ~~**Grok Build connector profile (initial)**~~ — WebSocket ACP; multi-session.
4. ~~**monoloop-interpreter (initial)**~~ — reassemble → complete canonical events.
5. ~~**Console renderer (testkit)**~~ — append-only human projection.
6. ~~**monoloop-loop + distributor + Driver**~~ — empty-tool path end-to-end.
7. **Harden all components** — race/load suites, Markdown structures, permission
   requests, architecture import gates.
8. **Outbound encoder seam** — dialect encoding in testkit first.
9. **monoloop-conformance** — full acceptance matrices.
10. **Later** — real tools, authz, durable receipts, more Channel profiles.

## Non-goals (near term)

- Product UI, Tauri, Kanban, prompt/context engine inside Monoloop
- Durable session DB or crash-safe exactly-once tool effects
- Model router / provider ranking inside the kernel
- Concrete tool catalogue in the first Loop implementation

## Tracking

Use structured tasks (`executeTaskTool` / project task tools) for actionable work.
Record durable *why* in `DECISIONS.md`. Update this plan when roadmap order or
scope changes.

## References

- `doc/README.md`, `doc/MONOLOOP.md`
- `SELFCONFIG.md`, `rules/ARCHITECTURE.md`, `rules/LAWS.md`, `rules/SECURITY.md`
