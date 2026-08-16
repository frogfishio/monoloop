# OpenAI Codex Connector Profile

Status: initial implementation (stdio ACP via adapter).

## Transport

| Item | Value |
|---|---|
| Product | OpenAI Codex CLI (`codex`) |
| ACP server (practical) | Official **`@agentclientprotocol/codex-acp`** adapter |
| Envelope | JSON-RPC 2.0 |
| Framing | **NDJSON** over **stdio** |
| Auth | Existing `codex login`, or `OPENAI_API_KEY` / `CODEX_API_KEY` |
| Session identity | ACP `sessionId` (explicit `session/new` or `session/load`) |

Native Codex also exposes **`codex app-server`** (JSON-RPC, not ACP) and the
TypeScript/Python Codex SDK. Monoloop folds Codex in through **ACP** so the
shared Interpreter path (Cursor / Agy / Grok family) applies unchanged.

## Flow

```text
spawn codex-acp (npx @agentclientprotocol/codex-acp)
  → initialize
  → optional authenticate
  → session/new | session/load
  → optional session/set_mode (read-only | agent | agent-full-access)
  → session/prompt
  → session/update*  → Interpreter dialect bytes
  → session/request_permission*  (test config may auto allow-once)
  → session/prompt result { stopReason }
  → process shutdown
```

## Dialect

- Family: `DialectFamily::CodexAcp`
- Descriptor: `DialectDescriptor::codex_acp("1")` (framing `ndjson`, profile `codex`)
- Interpreter reuses the shared ACP mapper (text chunks + tool_call lifecycle).

## Discovery

1. `CODEX_ACP_BIN` if set  
2. `codex-acp` on `PATH`  
3. else `npx --yes @agentclientprotocol/codex-acp`  

The adapter starts Codex App Server internally. Set `CODEX_PATH` in the
environment when the adapter should use a specific `codex` binary.

## Modes (`session/set_mode`)

| Mode id | Intent |
|---|---|
| `read-only` | Plan / review style |
| `agent` | Default agent (workspace write sandbox typical) |
| `agent-full-access` | Elevated sandbox (test sandboxes only) |

## Live qualification (testkit)

```bash
# requires: codex installed + authenticated
cargo run -p monoloop-testkit --example live_codex_ask
cargo run -p monoloop-testkit --example live_codex_crud
open target/live_codex_crud.html
```

## Relationship to other connectors

| | Grok Build | Cursor | Antigravity | Codex |
|---|---|---|---|---|
| Transport | WebSocket | stdio process | stdio process (bridge) | stdio process (adapter) |
| Framing | WS JSON | NDJSON | NDJSON | NDJSON |
| Profile | grok_build | cursor | antigravity | codex |
| Source clock | often present | rare | rare | rare |
| Stream step | rare | rare | `stepIdx` / `messageId` | `messageId` + `_meta.codex.*` |

Canonical units are shared. Human chat projection uses dialect `source_time` /
`source_step` when present, else emit order.

## Non-responsibilities

- Not the Codex IDE / ChatGPT UI.
- Does not own durable Codex threads; external systems own resume.
- Does not invent tool success; EmptyToolRegistry / host Loop apply as usual.
- Permission auto-allow is test-config only; product hosts inject policy later.
- Does not embed the TypeScript/Python Codex SDK as a product dependency.
