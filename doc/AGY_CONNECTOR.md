# Antigravity (agy) Connector Profile

Status: initial implementation (stdio ACP via bridge).

## Transport

| Item | Value |
|---|---|
| Product | Google Antigravity CLI (`agy`) |
| ACP server (practical) | Community **`agy-acp`** bridge (`npx agy-acp` or `AGY_ACP_BIN`) |
| Native ACP | Not yet on `agy` (upstream request for `agy --acp`); configurable when it lands |
| Envelope | JSON-RPC 2.0 |
| Framing | **NDJSON** over **stdio** |
| Auth | Existing Google / agy login; optional `authenticate` with `agy-login` |
| Session identity | ACP `sessionId` (explicit `session/new` or `session/load`) |

Same monoloop rules as other connectors: no semantic interpretation of tool
payloads in the connector, no ambient current session, bounded resources.

## Flow

```text
spawn agy-acp (or agy --acp)
  → initialize
  → optional authenticate (agy-login)
  → session/new | session/load
  → optional session/set_mode (default | accept-edits | plan)
  → session/prompt
  → session/update*  → Interpreter dialect bytes
  → session/request_permission*  (test config may auto allow-once)
  → session/prompt result { stopReason }
  → process shutdown
```

## Dialect

- Family: `DialectFamily::AgyAcp`
- Descriptor: `DialectDescriptor::agy_acp("1")` (framing `ndjson`, profile `antigravity`)
- Interpreter reuses the shared ACP mapper (same as Cursor/Grok ACP updates).

## Discovery

1. `AGY_ACP_BIN` if set  
2. `agy-acp` on `PATH`  
3. else `npx --yes agy-acp`  

`AgyAgentConfig::with_native_agy_acp()` selects `agy --acp` when native support exists.

`AgyAgentConfig::with_skip_permissions()` appends `--dangerously-skip-permissions`
to the bridge argv for unattended tool runs (test sandboxes only).

## Live qualification (testkit)

```bash
cargo run -p monoloop-testkit --example live_agy_ask
cargo run -p monoloop-testkit --example live_agy_crud
open target/live_agy_crud.html
```

## Relationship to Cursor / Grok

| | Grok Build | Cursor | Antigravity |
|---|---|---|---|
| Transport | WebSocket | stdio process | stdio process (bridge) |
| Framing | WS JSON | NDJSON | NDJSON |
| Profile | grok_build | cursor | antigravity |
| Source clock | often present (`agentTimestampMs`) | rare | rare |
| Stream step | rare | rare | **`_meta.stepIdx`** / numeric `messageId` |

Canonical units are shared. Human chat projection orders by dialect
`source_time` when present, else by `source_step` (Agy step/message sequence),
else emit order. Agy often streams tools then a single summary message — that
tools-first layout is the dialect sequence, not an Interpreter reorder bug.

## Non-responsibilities

- Not the Antigravity IDE UI.
- Does not own durable agy conversations.
- Community bridge is a **compatibility** path until native ACP ships; product hosts
  should pin bridge version for reproducibility.
