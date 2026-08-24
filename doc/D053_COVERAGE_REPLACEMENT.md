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
| `tests/direct_llm_e2e.rs` | DirectLlm FakeConnector + HTTP/OpenAI SSE composition, continuation, tool second-exchange, call-ID reuse, concurrency | **Phase A+B + Fake parity + independent bound e2e replacement:** `tests/direct_llm_openai_e2e.rs` and `tests/direct_llm_fake_e2e.rs` cover text, concurrency, CallerControlled, one-/multi-round InlineToolContinuation, call-ID reuse, and fail-closed `LimitExceeded` for `max_continuations` (0/1), `max_continuation_context_bytes`, `max_provider_exchanges` (1/2 exact), and `max_total_provider_{input,output}_bytes` (first-exchange input-before-open + output-during-pump; cumulative remaining-output exact + plus-one Fake+HTTP; Fake continuation remaining-input `== 0`). Retained Fake smoke unchanged. Fake **20/20** and HTTP **16/16** are the DirectLlm replacement row (includes mixed text+tool + context exact−1 needles), **not** Golden. **Still open for full Golden:** remaining §23 extras (exhaustive public-limit exact/plus-one matrix, race/load, live Grok multi-session) and independent §25 / D-025 sign-off (agents must not self-sign; unsigned pointers in `doc/D025_EVIDENCE_PACK.md`). D-054 deprecated-alias cut **closed** by D-060. |
| `tests/runtime_startup.rs` | Start / reject-after-shutdown / config | `lifecycle/tests.rs` startup + `Stopped` / admit-after-shutdown paths |
| `tests/claim_gate.rs` (v1) | Omit provider sessionId → InvariantFailed | **Ported** to v2 `tests/claim_gate.rs` (`StartedRuntime`) |
| `examples/fake_echo.rs` (v1) | Loop smoke example | **Rewritten** on `StartedRuntime` (registered via `autoexamples`) |

After deletion, `monoloop-loop` sets `autotests = true` and `autoexamples = true`
so `cargo test --workspace --all-targets --all-features` compiles every on-disk
suite and example in this crate.
