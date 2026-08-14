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
- [ ] Full CONNECTOR.md / GROK_BUILD_CONNECTOR.md acceptance matrices
- [ ] monoloop-interpreter
- [ ] monoloop-loop + testkit Driver

## Implementation roadmap (suggested order)

1. ~~**Workspace + monoloop-contracts**~~ — done (IDs, dialect, errors, limits).
2. ~~**monoloop-connector**~~ — abstract contract, FakeConnector, ConnectorProxy.
3. ~~**Grok Build connector profile (initial)**~~ — WebSocket ACP; multi-session.
4. **Harden connector** — remaining race/load suites, reconnect policy, permission
   server-request handling, architecture import gates.
5. **monoloop-interpreter** — factory, assembly pipeline, deterministic test dialect;
   fragmentation invariance; no-token contract tests.
6. **Canonical event distribution** — run-scoped bounded fan-out (Loop lossless;
   optional observers independent).
7. **monoloop-loop** — empty registry / no runtime; ToolRequestReady-only dispatch.
8. **monoloop-testkit Driver** — end-to-end composition with fakes; empty-tool path.
9. **Outbound encoder seam** — dialect encoding in testkit first.
10. **monoloop-conformance** — full acceptance matrices from each component doc.
11. **Later (explicit decision required)** — real tools, authz, durable receipts,
    additional Channel profiles, host Context Engine integration (one-way).

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
