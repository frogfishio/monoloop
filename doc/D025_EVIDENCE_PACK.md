# D-025 / §25 evidence pack (unsigned)

Agent-prepared pointers for an **independent** human or contracted reviewer.
This file does **not** fill `doc/SECURITY_REVIEW_CHECKLIST.md` Sign-off and
does **not** claim Golden / §25 / D-025 complete.

## How to use

1. Reviewer runs the commands below on a clean tree.
2. Reviewer fills the Sign-off table in `doc/SECURITY_REVIEW_CHECKLIST.md`.
3. Any new P0/P1/P2 findings go in `DEFECTS.md` under a new id.
4. Agents must **not** self-sign the Sign-off table.

## Product shape (standing)

| Check | Evidence |
|---|---|
| Exactly three product components | `rules/ARCHITECTURE.md`, crate graph |
| Product crates ↛ testkit | `cargo` architecture / forbidden-dep gates |
| Spec-first (`doc/` MUST/MUST NOT) | `doc/README.md`, component RFCs |
| No ambient session/run/tool | LAWS 5–9; identity on envelopes |

## §23 public-limit matrix honesty (2026-08-24)

Authoritative inventory: `doc/S23_PUBLIC_LIMIT_MATRIX.md`.

| Class | Fields (summary) |
|---|---|
| **Covered** | Active capacity; actor-command **items**; event-queue ceilings (D-055); input/messages/parts/tools; tool schema (D-056); tool payload/output/concurrent/queued; continuations / provider budgets; `transaction_deadline`; `terminal_event_delivery_deadline` |
| **Retired** | `max_actor_command_bytes` (D-057 — closed-enum `ControlCommand`) |
| **Open (deferred)** | `max_diagnostic_*` (D-058); `callback_deadline` (D-059) |
| **Partial** | `cleanup_deadline` (wired wait + quiesce `max(2s)` floor; no distinct fail-closed completion code) |

Inventory gate: `cargo test -p monoloop-loop --test s23_forbidden_patterns`.

## DirectLlm replacement row (D-053)

| Suite | On-disk `#[test]` count | Command |
|---|---|---|
| Fake | 20 | `cargo test -p monoloop-loop --test direct_llm_fake_e2e -- --test-threads=1` |
| HTTP/OpenAI | 16 | `cargo test -p monoloop-loop --test direct_llm_openai_e2e` |

Coverage map: `doc/D053_COVERAGE_REPLACEMENT.md`.

Includes: text, concurrency, CallerControlled, one-/multi-round InlineToolContinuation,
call-ID reuse, `max_continuations` 0/1, context-byte ceiling, `max_provider_exchanges`
1/2, total provider input/output (first-exchange + cumulative exact/plus-one), Fake
continuation remaining-input `== 0`. HTTP harness uses shared Tokio runtime + graceful
SSE shutdown (`finish_http_test`).

## Named Fake race / load (not exhaustive)

Full table: `doc/S23_RACE_LOAD_INVENTORY.md`.

Includes capacity races, multi-channel multi-session load, submit-vs-shutdown
races (including begin-shutdown), duplicate-session race, Hang terminate
storms, and same-tx Cancel vs ForceTerminate:

- `concurrent_hang_terminate_storm_all_cancelled` (Cancel → `Cancelled`)
- `concurrent_hang_force_terminate_storm_all_terminated` (ForceTerminate →
  `Terminated`)
- `concurrent_hang_cancel_versus_force_terminate_one_terminal` (one Hang id;
  barrier Cancel×Force → one `{Cancelled, Terminated}`)

Inventory gate: `s23_race_load_inventory_present` (s23 suite).

These are **named** proofs. Broader load/race remains open. Live Grok
multi-session concurrent `session/new` + isolation is exercised via
`monoloop-testkit` example `live_grok_multi_session` (default secret
`monoloop-live-test` on preauthorized hosts). Explicit live `session/load`
after a short session remains a standing residual on some agent builds.

## Core §23 / gates (re-run)

Prefer the project’s documented gate entrypoint (e.g. `make gates` / workspace
`--all-targets` as recorded in DEFECTS §23 hygiene). At minimum:

```bash
cargo test -p monoloop-loop --lib -- --test-threads=1
cargo test -p monoloop-loop --test s23_forbidden_patterns -- --test-threads=1
cargo test -p monoloop-loop --test linked_tools -- --test-threads=1
cargo test -p monoloop-loop --test direct_llm_fake_e2e --test direct_llm_openai_e2e
```

## Agent Golden-ready (2026-08-24) — still unsigned

Closable residuals landed for independent review:

- Cancel invent-Cancelled race fixed (`RawOutputHandle` / HTTP `wait_control`)
- Absolute Instant through Loop + Host tools; MCP handler carries tx Instant
- Encoded context exact−1 / padded-exact: `fake_inline_continuation_context_bytes_exact_admits_plus_one_rejects`
- Mixed text+tool continuation: `fake_inline_mixed_text_and_tool_call_preserved_in_continuation`
- Loop past Instant: `supervised_empty_loop_past_instant_is_deadline_exceeded`
- Live concurrent `session/new` + isolation; live `session/load` residual = **D-061**

**This is not Golden.** Reviewer must still Sign-off below’s checklist.

## Explicitly still open (do not waive)

- Sign-off table unsigned (this pack does not close it)
- Matrix **Open** rows: `max_diagnostic_*` (D-058), `callback_deadline` (D-059)
- Matrix **Partial**: `cleanup_deadline` (no distinct fail-closed completion cell)
- Full concurrent/race/load beyond named Fake proofs
- Live Grok `session/load` residual (**DECISIONS D-061**); concurrent new+isolation landed
- Refreshable MCP deferred (DECISIONS D-042) — do not treat as shipped
- D-054 / D-060: deprecated-only breaking cut **executed**; `adapt_*` retained

## Defect / decision index for this Golden pursuit

| Id | Role |
|---|---|
| D-046–D-054 | Silver Fixed; D-054 deprecated-alias cut closed by D-060 |
| D-053 | Coverage replacement map; DirectLlm Fake+HTTP+bounds |
| D-055–D-060 | Event-queue ceilings; tool schema; actor-command-bytes Retired; diagnostic/callback deferrals; alias cut |
| D-025 | Process residual: independent security/acceptance sign-off |
| D-042 | Refreshable MCP deferred |

## Suggested reviewer focus

1. Confirm §23 matrix honesty: Covered cells set **TransactionLimits** fields;
   Retired/Open deferred rows cite DECISIONS; no shaped Covered.
2. Confirm DirectLlm HTTP suite is deterministic after harness harden.
3. Confirm no product→testkit dependency and three-component boundaries.
4. Confirm D-054 / D-060 alias inventory: deprecated-only cut **executed**; `adapt_*` retained as host helpers.
5. Do **not** treat agent PASS Silver / Covered stamps as Golden / §25.

## Sign-off

Not filled here. Use `doc/SECURITY_REVIEW_CHECKLIST.md` § Sign-off.
