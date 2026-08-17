# WP-12 — Current limitations

Honest scope of the Transaction Runtime release candidate. This document
**must not** be read as “everything is production-complete for every profile
path”; it records what is proven, partial, or out of scope.

## Proven (deterministic gates)

- Runtime bootstrap, Channel registry, admission, dual-index SessionKey registry.
- Exactly-one callback / Ended sequencing on FakeConnector + Test dialect paths.
- Global and per-Channel active capacity plus-one rejection and release.
- Multi-Channel same session-string isolation; concurrent transaction isolation.
- Subscriber backpressure isolated to the slow transaction’s sink.
- Shutdown with active work: finalize + zero active/capacity after drain.
- EmptyToolRegistry / NoToolRuntime → `tool_unavailable`, zero effects.
- Host linked tools (dispatcher, capacity, validation) and MCP gateway binding
  lifecycle (loopback, capability redaction, revoke on shutdown).
- Streaming HTTP Connector + credential resolver (secrets not in Debug/errors).
- OpenAI Chat Completions SSE Interpreter + encoder vertical e2e (local scripted).
- Six profile ChannelBinding capability matrices register and validate.
- Architecture: product crates do not depend on `monoloop-testkit`; Connector /
  Interpreter / Loop production dependency direction holds.
- Testkit: chat projection rebuilds from canonical TransactionEvent units only.

## Partial / not release-proven

| Area | Limitation |
|---|---|
| External agent live multi-exchange | SendAndRetain against real Grok/Cursor/Codex/agy is qualification (testkit examples), not a deterministic acceptance gate |
| MCP Refreshable | Not declared; CreationOnly only for external agents |
| Inline continuation + MCP | Explicitly unsupported (CallerControlled only for gateway profiles) |
| Full terminal-race matrix | Covered: cancel vs complete, short deadline, shutdown vs active. Not every combinatorial pair of stale exchange/child/capability races is exhaustively enumerated |
| Paused-time deadline suites | Deadline tests use real short timeouts; Tokio paused-time not required for current Fake paths |
| Forced actor abort supervisor | Shutdown supervisor path is exercised; dedicated panic/abort injection suite is not separate |
| Concurrent tools + concurrent MCP + concurrent direct LLM | Covered in slices (tools, MCP gateway, direct LLM e2e); not one giant multi-path soak |
| Provider malformed corpus | Interpreter/SSE fixtures cover fragmentation and common malformation; not an infinite fuzz corpus |
| Profile prompt-shortcut removal | Encoders own prompt bodies; connector open/session sequencing remains profile-owned (see WP-00 PS-* inventory) |

## Out of scope (by design / plan)

- Durable persistence, callback recovery after process loss.
- Prompt construction, context engine, memory, UI, model routing inside Monoloop.
- Dynamic loading of tool executable code.
- OpenAI Responses API, non-streaming Chat Completions, multimodal.
- Remote MCP transport beyond loopback gateway.
- Ambient “current session” recovery after Connector restart.

## Dependency / MSRV notes

- `rmcp` / `jsonschema` push workspace MSRV to **1.88**.
- Profile crates may depend on `monoloop-loop` for ChannelBinding construction;
  `monoloop-loop` depends on profiles only as **dev-dependencies**.

## When something fails live

Treat live-provider failures as environment/qualification issues unless a
deterministic Fake/scripted test also fails. Prefer adding a Fake or scripted
fixture over expanding live-only acceptance.
