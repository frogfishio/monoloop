# Z.ai CLI Connector Profile

Status: initial implementation (headless process, **not ACP**).

## Transport

| Item | Value |
|---|---|
| Product | Z.ai CLI (`@guizmo-ai/zai-cli`, binary `zai`) |
| Mode | Headless: `zai -p <prompt> --no-color -d <cwd>` |
| Envelope | OpenAI-compatible **chat message** objects |
| Framing | **NDJSON** on **stdout** (one message per line after the turn) |
| Auth | CLI config / `ZAI_API_KEY` / optional `-k` (never logged) |
| API | `https://api.z.ai/api/coding/paas/v4` (OpenAI chat completions style) |
| Session identity | Synthetic `zai-<uuid>` per headless run (no ambient resume) |

This profile is intentionally “left field” relative to Grok/Cursor/Agy/Codex:
there is **no ACP** server. Tools execute **inside** the CLI agent (auto-approved
in headless mode). Monoloop observes the final transcript only.

## Flow

```text
spawn zai -p <prompt> --no-color -d <cwd>
  → (CLI runs model + local tools)
  → stdout NDJSON: user / assistant(+tool_calls) / tool / …
  → Interpreter DialectFamily::ZaiCli
  → complete text sentences + tool Ready/Resolved units
  → process exit
```

## Dialect

- Family: `DialectFamily::ZaiCli`
- Descriptor: `DialectDescriptor::zai_cli("1")` (framing `ndjson`, profile `zai`)
- Mapper skips `user` lines, drops the `"Using tools to help you..."` placeholder,
  maps `tool_calls` → complete `ToolRequestReady`, `role=tool` → `Resolved`.

## Discovery

1. `ZAI_BIN` if set  
2. else `zai` on `PATH`

Optional: `ZAI_MODEL`, `ZAI_BASE_URL`, `ZAI_API_KEY` (CLI also reads its own config).

## Live qualification (testkit)

```bash
zai config --show   # API key configured
cargo run -p monoloop-testkit --example live_zai_ask
cargo run -p monoloop-testkit --example live_zai_crud
open target/live_zai_crud.html
```

## Relationship to other connectors

| | ACP family (Grok/Cursor/Agy/Codex) | Z.ai CLI |
|---|---|---|
| Transport | stdio/WS ACP JSON-RPC | one-shot process |
| Tools | observed as ACP tool_call lifecycle | executed inside CLI; observed as chat tool_calls |
| Streaming | mid-turn session/update | batch NDJSON after turn |
| Source clock / step | varies | none in transcript |

## Non-responsibilities

- Not a Z.ai product UI or session store.
- Does not re-execute CLI tools in Monoloop Loop (EmptyToolRegistry still applies
  if hosts subscribe; wire tools already completed inside `zai`).
- Does not invent ACP when the product only offers headless chat NDJSON.
