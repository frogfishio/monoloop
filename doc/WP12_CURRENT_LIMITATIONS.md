# WP-12 — Current limitations

Honest scope of the Transaction Runtime release candidate. This document
**must not** be read as “everything is production-complete for every profile
path”; it records what is proven, partial, or out of scope.

## Proven (deterministic gates)

- Runtime bootstrap, Channel registry, admission, dual-index SessionKey registry.
- Exactly-one completion publication on FakeConnector + Test dialect paths
  (`lifecycle/tests.rs` §22.2).
- Host adapters (`adapt_event_sink` / `adapt_completion_callback`) run **outside**
  the core executor; capacity is not held for host callback duration (v2 §7 / M1).
- Global and per-Channel active capacity plus-one rejection and release.
- Distinct-session and encoded-exchange channel limits; event byte budget;
  tool payload/output caps; empty extension allowlist deny.
- Multi-Channel same session-string isolation; concurrent transaction isolation.
- Event delivery fail-closed under item/byte pressure (lifecycle §22.6).
- Shutdown with active work: finalize + zero active/capacity after drain;
  timeout yields `Quiescing`, not false `Stopped`.
- Cancel during delayed open (D-051) and Hang-pinned response wait.
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
- Tool registration requires structural factories per class (`try_new_abortable` /
  `try_new_process_isolated`); dyn boolean self-assert is rejected.

## Partial / not release-proven

| Area | Limitation |
|---|---|
| External agent live multi-exchange | SendAndRetain against real Grok/Cursor/Codex/agy is qualification (testkit examples), not a deterministic acceptance gate |
| MCP Refreshable | **Deferred by DECISIONS D-042**; CreationOnly only for external agents until a superseding decision + vendor proofs |
| Inline continuation + MCP | Explicitly unsupported (CallerControlled only for gateway profiles) |
| Headless CLI argv prompts | Z.ai/Claude vendor CLIs require prompt on argv; LAW 16 clarified in `DECISIONS.md` D-002; secrets must not be on argv |
| Headless CLI in-CLI tools | Tools may run inside the spawned CLI; Monoloop EmptyToolRegistry is observational for those Channels |
| Profile → Loop coupling | Profile crates depend on Loop/Interpreter to build `ChannelBinding` (accepted; not Connector-only packages) |
| Provider malformed corpus | Interpreter/SSE fixtures cover fragmentation and common malformation; not an infinite fuzz corpus |
| Sync admit join | Install-failure path aborts owned tasks but cannot await joins (sync `admit`) |

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
  `monoloop-loop` must not depend on profiles (even as dev-dependencies) so
  crates.io packaging can publish leaf-first. WP-11 binding qualification lives
  in `monoloop-testkit`.

## When something fails live

Treat live-provider failures as environment/qualification issues unless a
deterministic Fake/scripted test also fails. Prefer adding a Fake or scripted
fixture over expanding live-only acceptance.
