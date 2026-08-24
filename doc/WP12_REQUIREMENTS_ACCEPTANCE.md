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
| Cancel/timeout/completion races single terminal | Pass | `lifecycle/tests.rs` cancel/hang/deadline + shutdown (`s22_2_*`) |
| Bounded concurrency under load | Pass | capacity plus-one; lifecycle admit/claim stress cells |
| Enforced bounds | Pass | limits validate; event bytes; distinct sessions; encoded exchange; extension deny |
| No leaks after completion/shutdown | Pass | active/capacity zero after `Stopped`; host `adapt_*` drain outside core; admit abort on install fail |
| Backpressure explicit | Pass | lifecycle event delivery fail-closed / byte+item plus-one |
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
| Shared OpenAI path for compatible providers | Pass | interpreter OpenAI SSE; lifecycle Fake DirectLlm (`fake_echo_exchange_*`); see `doc/D053_COVERAGE_REPLACEMENT.md` |
| Canonical input + invocation config | Pass | contracts + sync admission `merge_effective_config` (`unknown_extension_rejected_at_admission`) |
| Credentials via resolver only | Pass | streaming HTTP; Debug redaction; Z.ai `-k` removed |
| Streaming SSE assembly | Pass | interpreter OpenAI dialect tests |

## R-002 Session / multi-channel transactions

| Criterion | Status | Evidence |
|---|---|---|
| SessionKey isolation | Pass | duplicate reject; same string different channels |
| Generated + supplied DirectLlm sessions | Pass | `lifecycle/tests/` admission + session isolation |
| Shutdown finalizes callbacks | Pass | lifecycle shutdown / `Stopped` proofs |
| External create/reuse (Fake) | Pass | FakeSessionAdapter + actor attach |
| Live multi-exchange per agent profile | Partial | Qualification / environment (not Fake gate) |

## R-003 Tools and completion

| Criterion | Status | Evidence |
|---|---|---|
| Empty registry unavailable zero effects | Pass | `empty_loop` |
| Linked host tools | Pass | `linked_tools` including IsolatedKillable escalate |
| MCP capability lifecycle | Pass | `mcp_gateway` HTTP initialize/list/call |
| Exactly one completion callback | Pass | lifecycle §22.2 one-completion proofs |
| Headless CLI in-process tools | Partial | Z.ai/Claude execute tools inside CLI; kernel EmptyToolRegistry observational only (`DECISIONS.md` D-002) |

## R-004 Ephemeral events and presentation

| Criterion | Status | Evidence |
|---|---|---|
| Push events with transaction admission | Pass | `TransactionSubmitRequest.delivery` / `transaction_delivery` (push ports) |
| Canonical not presentation | Pass | TransactionEventPayload::CanonicalUnit |
| Concurrent sequence/session | Pass | `lifecycle/tests.rs` `s22_6_concurrent_producers_contiguous_sequence` + SessionKey isolation |
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
2. Refreshable MCP — deferred by **DECISIONS D-042** (CreationOnly only for
   initial ExternalAgent profiles; enum variant retained for future profiles).
3. Headless CLI non-argv prompt transport (or host acceptance of D-002).
4. Independent security audit sign-off (process gate).
5. Sync admit cannot `await` aborted joins after install failure (abort only).

When closing an open item, add a direct test and update this checklist in the
same change.
