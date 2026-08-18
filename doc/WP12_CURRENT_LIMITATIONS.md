# WP-12 — Current limitations

Honest scope of the Transaction Runtime release candidate. This document
**must not** be read as “everything is production-complete for every profile
path”; it records what is proven, partial, or out of scope.

## Proven (deterministic gates)

- Runtime bootstrap, Channel registry, admission, dual-index SessionKey registry.
- Exactly-one callback / Ended sequencing on FakeConnector + Test dialect paths.
- Runtime-owned `CallbackService`: capacity released while host callbacks run;
  shutdown drains inflight callbacks.
- Global and per-Channel active capacity plus-one rejection and release.
- Distinct-session and encoded-exchange channel limits; event byte budget;
  tool payload/output caps; empty extension allowlist deny.
- Multi-Channel same session-string isolation; concurrent transaction isolation.
- Subscriber backpressure isolated to the slow transaction’s sink.
- Shutdown with active work: finalize + zero active/capacity after drain.
- Cancel during delayed open and during Hang (no provider body) response wait.
- EmptyToolRegistry / NoToolRuntime → `tool_unavailable`, zero effects.
- Host linked tools (dispatcher, capacity, validation) and MCP gateway binding
  lifecycle (loopback, HTTP initialize/list/call, capability redaction, revoke).
- Streaming HTTP Connector + credential resolver (secrets not in Debug/errors).
- OpenAI Chat Completions SSE Interpreter + encoder vertical e2e (local scripted);
  tool Ready only on `tool_calls` finish.
- Six profile ChannelBinding capability matrices register and validate.
- Architecture: product crates do not depend on `monoloop-testkit`; Connector /
  Interpreter / Loop production dependency direction holds.
- Testkit: chat projection rebuilds from canonical TransactionEvent units only.
- Tool registration rejects Abortable handlers that do not `supports_abort`.

## Partial / not release-proven

| Area | Limitation |
|---|---|
| External agent live multi-exchange | SendAndRetain against real Grok/Cursor/Codex/agy is qualification (testkit examples), not a deterministic acceptance gate |
| MCP Refreshable | Not declared; CreationOnly only for external agents |
| Inline continuation + MCP | Explicitly unsupported (CallerControlled only for gateway profiles) |
| IsolatedKillable escalate | Registration validates `supports_isolated_kill`; full kill+join-after-grace suite is residual |
| Provider malformed corpus | Interpreter/SSE fixtures cover fragmentation and common malformation; not an infinite fuzz corpus |
| Profile prompt-shortcut removal | Encoders own prompt bodies; connector open/session sequencing remains profile-owned (see WP-00 PS-* inventory) |

## Out of scope (by design / plan)

- Durable persistence, callback recovery after process loss.
- Prompt construction, context engine, memory, UI, model routing inside Monoloop.
- Dynamic loading of tool executable code.
- OpenAI Responses API, non-streaming Chat Completions, multimodal.
- Remote MCP transport beyond loopback gateway.
- Ambient “current session” recovery after Connector restart.
- Independent security audit process sign-off (organizational, not a Fake test).

## Dependency / MSRV notes

- `rmcp` / `jsonschema` push workspace MSRV to **1.88**.
- Profile crates may depend on `monoloop-loop` for ChannelBinding construction;
  `monoloop-loop` depends on profiles only as **dev-dependencies**.

## When something fails live

Treat live-provider failures as environment/qualification issues unless a
deterministic Fake/scripted test also fails. Prefer adding a Fake or scripted
fixture over expanding live-only acceptance.
