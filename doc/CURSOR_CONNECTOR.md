# Cursor Agent Connector Profile

Status: initial implementation (stdio ACP).

## Transport

| Item | Value |
|---|---|
| Binary | `agent acp` (Cursor CLI) |
| Envelope | JSON-RPC 2.0 |
| Framing | **newline-delimited JSON** (NDJSON) over **stdio** |
| Auth | `authenticate` with `methodId: "cursor_login"` (or env `CURSOR_API_KEY`) |
| Session identity | Cursor `sessionId` (explicit `session/new` or `session/load`) |

Normative product rules for Connectors still apply (`doc/CONNECTOR.md`): no semantic
interpretation of assistant text/tools in the connector, no ambient current session,
bounded resources, fail-closed.

## Flow

```text
spawn agent acp
  → initialize
  → authenticate (cursor_login)
  → session/new | session/load (explicit sessionId)
  → optional session/set_mode (agent | plan | ask)
  → optional session/set_config_option (configId=model, …)
  → session/prompt
  → session/update*  (streamed to Interpreter as dialect bytes)
  → session/request_permission*  (test config may auto allow-once)
  → session/prompt result { stopReason }
  → optional session/cancel
  → process shutdown
```

Mode and model are **explicit** on `CursorSessionConfig` (`with_ask_mode`,
`with_agent_mode`, `with_model`). There is no ambient “current mode” in Monoloop.

## Dialect

- Family: `DialectFamily::CursorAcp`
- Descriptor: `DialectDescriptor::cursor_acp("1")` (framing `ndjson`, profile `cursor`)
- Interpreter reuses the ACP mapper (`session/update` text + tool lifecycle).
- Cursor extension methods (`cursor/*`) are observational / not executed by Monoloop.

## Non-responsibilities

- Not a Cursor UI, not IDE integration product code.
- Does not own durable Cursor conversations.
- Does not invent success for tools; host Loop / EmptyToolRegistry apply as usual.
- Permission requests may be auto-answered in test config (`allow-once`); product
  hosts should inject an explicit policy later.

## Relationship to Grok Build

Both speak ACP-shaped `session/update` traffic. Differences:

| | Grok Build | Cursor |
|---|---|---|
| Transport | Authenticated WebSocket | stdio process |
| Framing | WebSocket text frames (JSON) | NDJSON lines |
| Profile tag | `grok_build` / `Acp` | `cursor` / `CursorAcp` |
| Source clock | often `params._meta.agentTimestampMs` | usually absent |

Canonical units are the same. Human chat projection may fall back to emit order
when source times are missing.
