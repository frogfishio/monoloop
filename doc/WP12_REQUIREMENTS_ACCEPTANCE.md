# WP-12 — Requirements acceptance checklist

Maps `REQUIREMENTS.md` R-000–R-004 to **direct tests or explicit limitations**.
Unchecked items remain incomplete; partial rows cite evidence and residual gaps.

Legend: **Pass** = direct non-conditional test evidence · **Partial** = slice
proven · **Open** = not acceptance-complete.

## R-000 Engineering quality

| Practice | Status | Evidence |
|---|---|---|
| Advertised Fake/scripted paths work end to end | Pass | Fake + OpenAI scripted + empty-tool + linked tools + MCP HTTP initialize/list/call |
| Typed truthful errors | Pass | Admission/tool/MCP/HTTP error kinds in component tests |
| Cancel/timeout/completion races single terminal | Pass | hardening cancel/hang/deadline + admission shutdown |
| Bounded concurrency under load | Pass | capacity plus-one; thousands-of-fake |
| Enforced bounds | Pass | limits validate; event bytes; distinct sessions; encoded exchange; extension deny |
| No leaks after completion/shutdown | Pass | active_count/capacity zero; CallbackService drain; admission abort on install fail |
| Backpressure explicit | Pass | `subscriber_backpressure_isolated` |
| Security fail-closed (Grok) | Pass | non-loopback deny; non-loopback + allow still requires `wss`; credential redaction |
| No cross-route under concurrency | Pass | SessionKey + concurrent isolation |
| Docs state actual limitations | Pass | this file + `WP12_CURRENT_LIMITATIONS.md` + `DECISIONS.md` D-002 |
| Six-profile live release candidate | Partial | Bindings register/validate; Z.ai/Claude argv prompt exception + in-CLI tools documented |

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
| Shared OpenAI path for compatible providers | Pass | `direct_llm_e2e`, interpreter OpenAI SSE |
| Canonical input + invocation config | Pass | contracts + merge_effective_config |
| Credentials via resolver only | Pass | streaming HTTP; Debug redaction; Z.ai `-k` removed |
| Streaming SSE assembly | Pass | interpreter OpenAI dialect tests |

## R-002 Session / multi-channel transactions

| Criterion | Status | Evidence |
|---|---|---|
| SessionKey isolation | Pass | duplicate reject; same string different channels |
| Generated + supplied DirectLlm sessions | Pass | admission lifecycle |
| Shutdown finalizes callbacks | Pass | shutdown + CallbackService drain |
| External create/reuse (Fake) | Pass | FakeSessionAdapter + actor attach |
| Live multi-exchange per agent profile | Partial | Qualification / environment (not Fake gate) |

## R-003 Tools and completion

| Criterion | Status | Evidence |
|---|---|---|
| Empty registry unavailable zero effects | Pass | `empty_loop` |
| Linked host tools | Pass | `linked_tools` including IsolatedKillable escalate |
| MCP capability lifecycle | Pass | `mcp_gateway` HTTP initialize/list/call |
| Exactly one completion callback | Pass | hardening + CallbackService |
| Headless CLI in-process tools | Partial | Z.ai/Claude execute tools inside CLI; kernel EmptyToolRegistry observational only (`DECISIONS.md` D-002) |

## R-004 Ephemeral events and presentation

| Criterion | Status | Evidence |
|---|---|---|
| Push events with transaction admission | Pass | event sink on TransactionRequest |
| Canonical not presentation | Pass | TransactionEventPayload::CanonicalUnit |
| Concurrent sequence/session | Pass | hardening + exchange_e2e |
| Reconstruct presentation downstream | Pass | testkit `canonical_event_presentation` |
| No persistence | Pass | no product DB |
| State released after completion | Pass | capacity zero; slow callback does not hold capacity |

## Architecture import gates

| Gate | Status | Evidence |
|---|---|---|
| Product ↛ testkit | Pass | architecture tests |
| Contracts leaf | Pass | same |
| Connector ↛ Interpreter/Loop | Pass | abstract connector crate |
| Interpreter ↛ Connector/Loop | Pass | same |
| Loop production ↛ profiles | Pass | profiles not in Loop deps (even as dev-deps); WP-11 binding tests in testkit |
| Profile → Loop/Interpreter | Partial | Accepted coupling for `ChannelBinding` construction (`DECISIONS.md` D-002) |

## Open items (honest residual)

1. Live SendAndRetain multi-exchange against each external agent profile.
2. Refreshable MCP (not declared; CreationOnly only).
3. Headless CLI non-argv prompt transport (or host acceptance of D-002).
4. Independent security audit sign-off (process gate).
5. Sync admit cannot `await` aborted joins after install failure (abort only).

When closing an open item, add a direct test and update this checklist in the
same change.
