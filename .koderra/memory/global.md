---
always: true
priority: 10
---

# Global Project Memory — Monoloop

Long-term facts that should persist across sessions.

## Product DNA

- **Monoloop** = Connector + Interpreter + minimal extensible Loop only.
- Language: **Rust**; async runtime: **Tokio multi-thread**.
- Authoritative specs: `doc/` (RFC-style MUST/MUST NOT).
- Test kit (Driver, Console, fixtures, outbound test encoder) is **not** product.
- Initial Connector: **Grok Build** — ACP/JSON-RPC 2.0 over authenticated WebSocket;
  one server, many sessions; correlation ID = Grok `sessionId`.

## Implementation status (Connector slice)

- Workspace crates: `monoloop-contracts`, `monoloop-connector`, `monoloop-connector-grok`.
- `ConnectorProxy` routes `endpoint_ref` prefixes (e.g. `grok:ws://…`) to backends.
- `FakeConnector` is deterministic in-process (echo/scripted/pair) for tests.
- Grok profile: `connect` → `initialize` → `session/new` | explicit `session/load`;
  secrets via `SecretResolver` only; non-loopback fail-closed by default.

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
