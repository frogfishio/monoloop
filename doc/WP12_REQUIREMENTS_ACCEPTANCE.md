# WP-12 — Requirements acceptance checklist

Maps `REQUIREMENTS.md` R-000–R-004 to **direct tests or explicit limitations**.
Unchecked items remain incomplete; partial rows cite evidence and residual gaps.

Legend: **Pass** = direct non-conditional test evidence · **Partial** = slice
proven · **Open** = not acceptance-complete.

## R-000 Engineering quality

| Practice | Status | Evidence |
|---|---|---|
| Advertised paths work end to end | Partial | Fake + OpenAI scripted + empty-tool + linked tools; live profiles qualification only |
| Typed truthful errors | Pass | Admission/tool/MCP/HTTP error kinds in component tests |
| Cancel/timeout/completion races single terminal | Partial | `hardening::complete_versus_cancel_single_terminal`, `short_deadline_terminal`, admission shutdown |
| Bounded concurrency under load | Pass | `hardening::thousands_of_fake_transactions_within_limits`, capacity plus-one |
| Enforced bounds | Pass | Capacity managers, tool capacity, TransactionLimits validate, encoder limits |
| No leaks after completion/shutdown | Pass | `shutdown_with_active_zero_owned`, active_count/capacity zero checks |
| Backpressure explicit | Pass | `subscriber_backpressure_isolated` |
| Security fail-closed | Partial | Non-loopback Grok, credential redaction, capability redaction, empty tools zero effects |
| No cross-route under concurrency | Pass | concurrent isolation + SessionKey tests |
| No todo!/unimplemented production paths | Partial | Workspace gate; residual profile shortcuts documented not advertised as complete |
| Docs state actual limitations | Pass | `WP12_CURRENT_LIMITATIONS.md` |

### R-000 verification gate commands

```text
cargo test --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
cargo doc --workspace --no-deps
```

## R-001 Configurable direct-LLM channels

| Criterion | Status | Evidence |
|---|---|---|
| Explicit Channel select | Pass | Admission unknown channel; no fallback |
| Shared OpenAI path for compatible providers | Pass | HTTP + encoder + Interpreter reused (`direct_llm_e2e`, `openai_chat_sse`) |
| Canonical input + invocation config | Pass | contracts input + merge_effective_config |
| Credentials via resolver only | Pass | `streaming_http`, credential Debug redaction |
| Streaming SSE assembly | Pass | interpreter OpenAI dialect tests |

## R-002 Session / multi-channel transactions

| Criterion | Status | Evidence |
|---|---|---|
| SessionKey isolation | Pass | duplicate same channel rejected; same string different channels OK |
| Generated + supplied DirectLlm sessions | Pass | `admission_lifecycle::generated_and_supplied_direct_llm_sessions` |
| Shutdown finalizes callbacks | Pass | `shutdown_with_active_*` |
| External create/reuse | Partial | Binding declares create/load; live proof qualification |
| All terminal races | Partial | Subset in WP-04/WP-12 hardening |

## R-003 Tools and completion

| Criterion | Status | Evidence |
|---|---|---|
| Empty registry unavailable zero effects | Pass | `empty_loop` |
| Linked host tools | Pass | `linked_tools` |
| MCP capability lifecycle | Pass | `mcp_gateway` |
| Exactly one completion callback | Pass | hardening + admission lifecycle |
| Tool cancel / capacity | Partial | capacity plus-one; cancel policies in tool tests |

## R-004 Ephemeral events and presentation

| Criterion | Status | Evidence |
|---|---|---|
| Push events with transaction admission | Pass | event sink on TransactionRequest |
| Canonical not presentation | Pass | TransactionEventPayload::CanonicalUnit |
| Concurrent sequence/session | Pass | hardening + exchange_e2e |
| Reconstruct presentation downstream | Pass | `canonical_event_presentation` testkit test |
| No persistence | Pass | no product DB; docs + architecture |
| State released after completion | Pass | active_count/capacity zero |

## Architecture import gates

| Gate | Status | Evidence |
|---|---|---|
| Product ↛ testkit | Pass | `monoloop-contracts` tests/architecture.rs |
| Contracts leaf | Pass | same |
| Connector ↛ Interpreter/Loop | Pass | same |
| Interpreter ↛ Connector/Loop | Pass | same |
| Loop production ↛ profiles | Pass | same |

## Open items (do not claim complete)

1. Exhaustive stale exchange/child/capability race matrix.
2. Refreshable MCP on retained external sessions.
3. Deterministic multi-exchange SendAndRetain against each live agent profile.
4. Full paused-time deadline library across all actor waits.
5. Independent security audit sign-off (process, not code gate).

When closing an open item, add a direct test and update this checklist in the
same change.
