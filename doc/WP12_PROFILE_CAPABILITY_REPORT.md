# WP-12 — Profile capability report (from tested declarations)

**Status:** Generated from WP-11 ChannelBinding constructors and
`crates/monoloop-testkit/tests/profile_bindings.rs` + WP-00 worksheet evidence.  
**Not a live multi-session MCP proof.** Limitations: see `WP12_CURRENT_LIMITATIONS.md`.

| Profile | Crate | Kind | Session mode | MCP config | Exchange | Continuation | Tool mode | Binding test |
|---|---|---|---|---|---|---|---|---|
| Grok Build | `monoloop-connector-grok` | ExternalAgent | Explicit create/load | McpGateway + CreationOnly | Bidirectional / SendAndRetain | CallerControlled only | ModelToolCalls | `six_profile_bindings_register_and_validate` |
| Cursor | `monoloop-connector-cursor` | ExternalAgent | Explicit create/load | McpGateway + CreationOnly | Bidirectional | CallerControlled only | ModelToolCalls | same |
| Codex | `monoloop-connector-codex` | ExternalAgent | Explicit create/load | McpGateway + CreationOnly | Bidirectional | CallerControlled only | ModelToolCalls | same |
| Antigravity (agy) | `monoloop-connector-agy` | ExternalAgent | Explicit create/load | McpGateway + CreationOnly | Bidirectional | CallerControlled only | ModelToolCalls | same |
| Z.ai CLI | `monoloop-connector-zai` | DirectLlm (headless CLI) | Stateless / synthetic | None | RequestResponse / SendAndFinish | CallerControlled | ModelToolCalls | same |
| Claude Code | `monoloop-connector-claude` | DirectLlm (headless CLI) | Stateless / synthetic | None | RequestResponse / SendAndFinish | CallerControlled | ModelToolCalls | same |

## Capability validation rules (runtime)

- `None` + MCP tools selected → reject at admission / binding validate.
- `CreationOnly` + MCP: only when creating a new external session (not refresh).
- `Inline` continuation + `McpGateway` is **invalid** for these profiles; only
  `CallerControlled` is declared.
- Direct-LLM headless (Z.ai / Claude) reject non-empty linked tools on the
  encoder path (provider tools stay inside the CLI).

## Encoder ownership

| Family | Encoder | Prompt ownership |
|---|---|---|
| ACP agents | `AcpPromptEncoder` (grok/cursor/codex/agy shapes) | Encoder emits dialect-complete prompt body |
| Headless CLI | `HeadlessPromptEncoder` (zai/claude) | Encoder owns prompt text for argv/print mode |

Qualification: `external_encoders_reject_nonempty_tools_and_own_prompt_text`.

## Direct-LLM OpenAI path (not a profile crate)

Separate from the six external profiles: `StreamingHttpConnector` +
`OpenAiChatCompletionsEncoder` + OpenAI Chat Completions Interpreter dialect.
Covered by WP-09/WP-10 (`openai_chat_sse`, `direct_llm_e2e`).

## Honest residual gaps

| Gap | Status |
|---|---|
| Refreshable MCP on retained external sessions | Deferred (**DECISIONS D-042**); CreationOnly only for initial profiles |
| Live SendAndRetain multi-exchange proof against real agents | Testkit live examples only; not deterministic gate |
| Unified attach/open without connector-local sequencing | Partial (encoders own prompt; open still profile-owned) |
| Inline continuation with MCP | Explicitly unsupported for external agents |
