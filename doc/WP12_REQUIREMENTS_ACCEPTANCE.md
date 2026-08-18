# WP-12 — Requirements acceptance checklist

Maps `REQUIREMENTS.md` R-000–R-004 to **direct tests or explicit limitations**.
Unchecked items remain incomplete; partial rows cite evidence and residual gaps.

Legend: **Pass** = direct non-conditional test evidence · **Partial** = slice
proven · **Open** = not acceptance-complete.

## R-000 Engineering quality

| Practice | Status | Evidence |
|---|---|---|
| Advertised paths work end to end | Pass | Fake + OpenAI scripted + empty-tool + linked tools + MCP HTTP initialize/list/call; live profiles remain qualification |
| Typed truthful errors | Pass | Admission/tool/MCP/HTTP error kinds in component tests |
| Cancel/timeout/completion races single terminal | Pass | `hardening::complete_versus_cancel_single_terminal`, `short_deadline_terminal`, `cancel_during_slow_open_*`, `cancel_during_response_wait_*`, admission shutdown |
| Bounded concurrency under load | Pass | `hardening::thousands_of_fake_transactions_within_limits`, capacity plus-one |
| Enforced bounds | Pass | Capacity managers, tool capacity, TransactionLimits validate, event byte budget, distinct sessions, encoded exchange, extension deny |
| No leaks after completion/shutdown | Pass | `shutdown_with_active_zero_owned`, active_count/capacity zero checks; CallbackService drain |
| Backpressure explicit | Pass | `subscriber_backpressure_isolated` |
| Security fail-closed | Pass | Non-loopback Grok, credential/capability redaction, empty tools zero effects, unknown extension deny |
| No cross-route under concurrency | Pass | concurrent isolation + SessionKey tests |
| No todo!/unimplemented production paths | Pass | Workspace gate; unsupported capabilities honestly disabled |
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
| Shutdown finalizes callbacks | Pass | `shutdown_with_active_*`; CallbackService drain |
| External create/reuse | Pass | FakeSessionAdapter create/load + actor attach (D-013); live multi-exchange is qualification |
| Terminal race coverage | Pass | cancel vs complete, hang response-wait, short deadline, shutdown vs active |

## R-003 Tools and completion

| Criterion | Status | Evidence |
|---|---|---|
| Empty registry unavailable zero effects | Pass | `empty_loop` |
| Linked host tools | Pass | `linked_tools` |
| MCP capability lifecycle | Pass | `mcp_gateway` including HTTP initialize/list/call |
| Exactly one completion callback | Pass | hardening + admission lifecycle; CallbackService |
| Tool cancel / capacity | Pass | capacity plus-one; `abortable_requires_supports_abort_handler` (D-024 registration) |

## R-004 Ephemeral events and presentation

| Criterion | Status | Evidence |
|---|---|---|
| Push events with transaction admission | Pass | event sink on TransactionRequest |
| Canonical not presentation | Pass | TransactionEventPayload::CanonicalUnit |
| Concurrent sequence/session | Pass | hardening + exchange_e2e |
| Reconstruct presentation downstream | Pass | `canonical_event_presentation` testkit test |
| No persistence | Pass | no product DB; docs + architecture |
| State released after completion | Pass | active_count/capacity zero; slow callback does not hold capacity |

## Architecture import gates

| Gate | Status | Evidence |
|---|---|---|
| Product ↛ testkit | Pass | `monoloop-contracts` tests/architecture.rs |
| Contracts leaf | Pass | same |
| Connector ↛ Interpreter/Loop | Pass | same |
| Interpreter ↛ Connector/Loop | Pass | same |
| Loop production ↛ profiles | Pass | same |

## Open items (honest residual — not unmarked required Fake gates)

1. Live SendAndRetain multi-exchange against each external agent profile
   (qualification / environment, not Fake acceptance).
2. Refreshable MCP on retained external sessions (not declared; CreationOnly only).
3. IsolatedKillable escalate + join-after-grace proof suite (D-024 residual).
4. Independent security audit sign-off (process gate, not code).

When closing an open item, add a direct test and update this checklist in the
same change.
