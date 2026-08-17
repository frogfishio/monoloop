---
always: true
priority: 10
---

# Global Project Memory — Monoloop

Long-term facts that should persist across sessions.

## Product DNA

- **Monoloop** = Connector + Interpreter + transaction-composing Loop only.
- Component 3 contains production `TransactionRuntime` composition and the
  separately testable provider-neutral inner `LoopRuntime`.
- Language: **Rust**; async runtime: **Tokio multi-thread**.
- Authoritative specs: `doc/` (RFC-style MUST/MUST NOT).
- Test kit (Driver, Console, fixtures, outbound test encoder) is **not** product.
- Production events and completion are push-based; one active transaction is
  permitted per `SessionKey { ChannelId, SessionId }`, with duplicates rejected
  rather than queued.
- Initial Connector: **Grok Build** — ACP/JSON-RPC 2.0 over authenticated WebSocket;
  one server, many sessions; correlation ID = Grok `sessionId`.

## Implementation status

- Workspace crates: `monoloop-contracts`, `monoloop-connector`, `monoloop-connector-grok`,
  `monoloop-interpreter`, `monoloop-loop`, `monoloop-testkit` (test-only).
- `ConnectorProxy` routes `endpoint_ref` prefixes (e.g. `grok:ws://…`) to backends.
- `FakeConnector` is deterministic in-process (echo/scripted/pair) for tests.
- Grok profile: `connect` → `initialize` → `session/new` | explicit `session/load`;
  secrets via `SecretResolver` only; non-loopback fail-closed by default.
- Interpreter: raw bytes (any fragmentation) → complete `CanonicalUnitEvent`s only;
  ACP `session/update` text chunks + tool_call lifecycle; sentence segmenter waits
  rather than emitting partials; abrupt EOF does not promote incomplete text.
- Loop: lossless subscription; dispatches only on complete ToolRequestReady;
  EmptyToolRegistry → ToolUnavailable + OutboundToolResult; NoToolRuntime never started.
- Testkit Driver: Interpreter → EventDistributor → independent Console + Loop
  subscriptions (never one shared receiver).
- Raw dump (opt-in): `RawDumpCollector` on `GrokServerConfig::with_raw_dump` captures
  exact inbound WebSocket payloads before demux; pipeline uses `PipelineParams::with_raw_dump()`.
- HTML review (testkit): `PipelineParams::with_html_dump(path)` builds
  complete public_response sentences → Markdown → HTML plus a canonical event
  timeline, for visual checks that interpretation serialises correctly.

## Gotchas

- Do not treat this repo’s older Koderra-template ARCHITECTURE prose as product
  truth; Monoloop kernel is not a Tauri/Angular IDE.
- “Canonical” means structurally complete and provider-neutral, **not** authorized
  or safe to execute.
- `InterpretationEnd` / remote EOF / model text “done” ≠ user-turn or run success.
- Empty tool registry yielding `tool_unavailable` is **required** first qualification,
  not a failure of The Loop.
- Console and Loop must have **independent** subscriptions; never one shared receiver.
- After Connector restart, no ambient session recovery — only explicit `session/load`.

## Dependency direction (remember)

```text
Host / Context Engine / UI  →  Monoloop
Monoloop  -X->  host internals, prompt memory, routers, concrete tools (initial)
Product crates  -X->  monoloop-testkit
```

## When specs and code disagree

Fix code to match `doc/`, or revise the spec deliberately and record in DECISIONS.md.
