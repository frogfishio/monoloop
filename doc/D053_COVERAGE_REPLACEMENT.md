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
| `tests/direct_llm_e2e.rs` | DirectLlm FakeConnector + HTTP/OpenAI SSE composition, continuation, tool second-exchange, call-ID reuse, concurrency | **Not equivalently replaced.** Retained smoke only: `lifecycle/tests.rs` `fake_echo_exchange_emits_canonical_text_unit`, `tests/empty_loop.rs`, `examples/fake_echo.rs`. Connector HTTP + Interpreter OpenAI SSE unit suites prove components in isolation, **not** their production composition through `StartedRuntime`. Treat HTTP/OpenAI DirectLlm vertical e2e as an open Golden residual (D-053 honesty). |
| `tests/runtime_startup.rs` | Start / reject-after-shutdown / config | `lifecycle/tests.rs` startup + `Stopped` / admit-after-shutdown paths |
| `tests/claim_gate.rs` (v1) | Omit provider sessionId → InvariantFailed | **Ported** to v2 `tests/claim_gate.rs` (`StartedRuntime`) |
| `examples/fake_echo.rs` (v1) | Loop smoke example | **Rewritten** on `StartedRuntime` (registered via `autoexamples`) |

After deletion, `monoloop-loop` sets `autotests = true` and `autoexamples = true`
so `cargo test --workspace --all-targets --all-features` compiles every on-disk
suite and example in this crate.
