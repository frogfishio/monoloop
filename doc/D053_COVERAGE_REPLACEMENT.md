# D-053 coverage replacement map

Legacy `monoloop-loop` integration suites imported deleted
`DefaultTransactionRuntime` and were excluded from `--all-targets`
(`autotests = false`). They were removed (2026-08-23) rather than left as
uncompiled dead weight. Replacement coverage:

| Deleted file | Intent | Replacement (registered / compiled) |
|---|---|---|
| `tests/hardening.rs` | Capacity, load, race, shutdown, isolation | `src/transaction/lifecycle/tests.rs` (§22.1–§22.3 admit/shutdown/race), `tests/s22_*`, `tests/s23_forbidden_patterns.rs` |
| `tests/admission_lifecycle.rs` | Admission, events, terminate, shutdown | `lifecycle/tests.rs` admission + completion + shutdown proofs |
| `tests/exchange_e2e.rs` | Exchange pump / join | `lifecycle/tests.rs` `s22_3_exchange_pumps_joined_not_detached`, `tests/empty_loop.rs` |
| `tests/direct_llm_e2e.rs` | DirectLlm FakeConnector + HTTP/OpenAI SSE composition, continuation, tool second-exchange, call-ID reuse, concurrency | **Phase A+B partial replacement:** `tests/direct_llm_openai_e2e.rs` — HTTP/OpenAI text-only + concurrent admits; CallerControlled tool path (`caller_controlled_tool_exchange_ends_continuation_required_without_second_open`); InlineToolContinuation one-round second exchange (`inline_tool_continuation_second_exchange_emits_text`); call-ID reuse across sequential admits (`reused_provider_call_id_across_exchanges_distinct_action_ids`). Retained Fake smoke unchanged. **Still open Golden residual:** multi-round inline continuation (N>1), FakeConnector parity suites from deleted `direct_llm_e2e.rs`, full §25 / D-025 Golden sign-off. |
| `tests/runtime_startup.rs` | Start / reject-after-shutdown / config | `lifecycle/tests.rs` startup + `Stopped` / admit-after-shutdown paths |
| `tests/claim_gate.rs` (v1) | Omit provider sessionId → InvariantFailed | **Ported** to v2 `tests/claim_gate.rs` (`StartedRuntime`) |
| `examples/fake_echo.rs` (v1) | Loop smoke example | **Rewritten** on `StartedRuntime` (registered via `autoexamples`) |

After deletion, `monoloop-loop` sets `autotests = true` and `autoexamples = true`
so `cargo test --workspace --all-targets --all-features` compiles every on-disk
suite and example in this crate.
