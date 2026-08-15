# Auto Conversation Summary

Generated: 2026-08-14T23:46:18.951Z

Session: agent.9757e834-4c97-4de6-855b-d06134839b55
Mode: agents

(Internal continuity summary. Also available via session metadata for prompt augmentation. Not rendered in main chat transcript.)

## Conversation summary

### What was established

**Monoloop** is a three-component async Rust kernel, not an IDE, agent, chat app, or prompt engine:

```text
Connector → Interpreter → Loop
```

| Component | Job |
|---|---|
| **Connector** | Dialect-labelled bytes, explicit external-session routing, cancel/terminate. No semantic interpretation. |
| **Interpreter** | Incremental decode → **complete** provider-neutral canonical events only. Never tokens, deltas, or partial tool JSON. |
| **Loop** | Lossless subscription; dispatch only on complete `ToolRequestReady`. First ship: empty registry → honest `tool_unavailable`, zero effects. |

Driver, Console, fixtures, and outbound encoders are **test kit only**. Hosts (Context Engine, UI, agents) call into Monoloop; Monoloop never reaches into them.

**First real path:** Grok Build — one long-lived server, ACP/JSON-RPC over authenticated WebSocket, sessions keyed by Grok’s `sessionId`. Resume only via explicit `session/load`. Prompts never on argv.

### Key decisions

- Specs under `doc/` are normative. Code follows them; disagreement is a spec revision or a code fix, recorded in `DECISIONS.md`.
- Preferred crates: `monoloop-contracts`, `-connector`, `-connector-grok`, `-interpreter`, `-loop`, `-testkit`, `-conformance`. No required `monoloop-core` facade. Product crates must not depend on testkit.
- `ConnectorProxy` routes named backends (`grok:ws://…`); `FakeConnector` is the deterministic in-process test backend.
- Console and Loop each get a **private** subscription via `EventDistributor`. Never one shared receiver.
- Canonical completeness ≠ authorization. Authority for effects is a future policy layer.
- Secrets only via injected `SecretResolver` / `*_secret_ref`. Session IDs and credentials stay out of logs and metrics.
- Everything bounded, async (Tokio), fail-closed. No Monoloop-owned persistence.

### Significant code / architecture notes

Workspace is **implemented**, not docs-only (that was an earlier misread):

- Grok connector: `initialize` → `session/new` | explicit `session/load`; multi-session demux; non-loopback fail-closed.
- Interpreter: ACP `session/update` + tool-call lifecycle; sentence segmenter waits rather than emitting partials; abrupt EOF does not promote incomplete text.
- Loop: `EmptyToolRegistry` / `NoToolRuntime`; Ready tools emit exactly one unavailable `OutboundToolResult`.
- **Raw dump** (`RawDumpCollector` / `with_raw_dump`): exact inbound WebSocket payloads before demux. Answers “what did Grok send?”
- **HTML review** (`PipelineParams::with_html_dump`): complete `public_response` sentences → Markdown → HTML plus a canonical event timeline. Answers “did we assemble the right story?”
- Live Grok CRUD against this project folder succeeded: wire → demux → complete tool generations → summary sentences → HTML, `unresolved_bytes=0`. Artifacts under `target/`.

### Open tasks

- Harden all components: race/load suites, Markdown structures, permission requests, architecture import gates.
- Outbound encoder seam (testkit first).
- `monoloop-conformance` — full CONNECTOR / INTERPRETER / THE_LOOP acceptance matrices.
- Optional live Grok e2e as a standing qualification, not the default CI path.
- Later, behind new contracts: real tools, authz, durable receipts, more Channel profiles.

**Not near-term:** product UI, Tauri, Kanban, prompt/context engine, session DB, model router, concrete tool catalogue.

### Process note

`.koderra` was rewritten from generic Koderra/IDE boilerplate to Monoloop DNA (`rules/`, `DECISIONS.md`, `SELFCONFIG.md`, `PLAN.md`, `memory/global.md`, agent/expert/advisor gates). Agents should load component boundaries and `doc/` contracts, not the old control-tower framing.
