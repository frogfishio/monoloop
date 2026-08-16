# Claude Code Connector Profile

Status: initial implementation (headless print / stream-json, **not ACP**).

## Transport

| Item | Value |
|---|---|
| Product | Anthropic Claude Code (`claude` CLI) |
| Mode | Headless: `claude -p <prompt> --output-format stream-json --verbose` |
| Envelope | Claude Code **stream-json** events |
| Framing | **NDJSON** on **stdout** (realtime while the agent runs) |
| Auth | Existing Claude Code login / `ANTHROPIC_API_KEY` (never logged) |
| Session identity | `session_id` from stream `system`/`init` (else synthetic) |

Optional ACP adapters exist (`@zed-industries/claude-code-acp`, etc.). Monoloop
uses the **official print/stream-json surface** for fewer moving parts and native
timestamps on events.

## Flow

```text
spawn claude -p <prompt> --output-format stream-json --verbose
  [optional --dangerously-skip-permissions for unattended tools]
  → stdout NDJSON: system / assistant / user(tool_result) / result
  → Interpreter DialectFamily::ClaudeCode
  → complete text sentences + tool Ready/Resolved (+ source_time from timestamps)
  → process exit
```

## Dialect

- Family: `DialectFamily::ClaudeCode`
- Descriptor: `DialectDescriptor::claude_code("1")` (framing `ndjson`, profile `claude_code`)
- Maps:
  - `assistant` + `content[].type=text` → public response (with `timestamp` → `source_time`)
  - `assistant` + `tool_use` → complete `ToolRequestReady`
  - `user` + `tool_result` → `Resolved`
  - `thinking` blocks suppressed (private CoT)
  - `result` → response finished boundary

## Discovery

1. `CLAUDE_BIN` if set  
2. else `claude` on `PATH`

Optional: `CLAUDE_MODEL` → `--model`.

## Live qualification (testkit)

```bash
cargo run -p monoloop-testkit --example live_claude_ask
cargo run -p monoloop-testkit --example live_claude_crud
open target/live_claude_crud.html
```

## Relationship to other connectors

| | ACP family | Z.ai CLI | Claude Code |
|---|---|---|---|
| Wire | session/update | batch chat NDJSON | stream-json NDJSON |
| Tools | observed ACP lifecycle | inside CLI | inside Claude Code |
| Source clock | Grok often; others rare | none | **event `timestamp`** |

## Non-responsibilities

- Not the Claude Code TUI / IDE extension.
- Does not re-execute tools in Monoloop Loop (wire tools already completed).
- Permission skip is test-config only.
- Does not require a community ACP bridge for the initial profile.
