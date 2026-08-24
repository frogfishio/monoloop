# §23 named Fake race / load inventory

Honest inventory of **in-tree** concurrent / race / load proofs under Fake /
Hang. This is **not** exhaustive load testing and **not** live Grok
multi-session qualification.

## Named proofs (`monoloop-loop` lifecycle tests)

| Needle | What it proves |
|---|---|
| `concurrent_global_capacity_exhaustion_admits_exactly_max` | Global active capacity exact under barrier race |
| `concurrent_per_channel_capacity_exhaustion_admits_exactly_channel_max` | Per-channel capacity exact under barrier race |
| `multi_channel_multi_session_concurrent_load` | ≥3 Channels, shared external strings across Channels (SessionKey isolation), duplicate reject with headroom, fill-to-capacity, one shutdown for every admission |
| `submit_versus_shutdown_barrier_race_two_outcomes` | Submit vs shutdown: admit **or** `RuntimeShuttingDown` (silent reject) |
| `submit_versus_shutdown_hang_barrier_both_outcomes` | Same with pre-admitted Hang still live through Quiescing |
| `submit_versus_begin_shutdown_two_outcomes` | Begin-shutdown race outcomes |
| `duplicate_session_race_admits_exactly_one` | Concurrent duplicate `SessionKey` → exactly one admit |
| `concurrent_hang_terminate_storm_all_cancelled` | Barrier concurrent Cancel on N distinct Hang sessions → all `Accepted`, all `Cancelled`, N completions |
| `concurrent_hang_force_terminate_storm_all_terminated` | Barrier concurrent ForceTerminate on N distinct Hang sessions → all `Accepted`, all `Terminated`, N completions |
| `concurrent_hang_cancel_versus_force_terminate_one_terminal` | Barrier Cancel vs ForceTerminate on **one** Hang id → dispositions `{Accepted, AlreadyTerminal}` (≥1 Accepted); exactly one completion in `{Cancelled, Terminated}` |

Hang storms and the Cancel×Force race wait for
`RuntimeOwner::live_connector_owners() >= N` (storms) / `>= 1` (same-tx race)
before the barrier (per-class ConnectorOwner count; D-051 register-before-I/O).
Aggregate `owned_task_count` is **not** used — InterpreterOwner inflation made
`>= 3N` an early-fire hole.

Inventory gate: `crates/monoloop-loop/tests/s23_forbidden_patterns.rs` →
`s23_race_load_inventory_present` requires this file **and** the named race
needles to remain under `lifecycle/tests/` (composed modules; deleting a listed
fn fails the gate).

## Explicitly not claimed

- Exhaustive scheduler / OS-load fuzzing
- Live Grok multi-session (requires `GROK_AGENT_SECRET` / agent env)
- Product→testkit race harnesses as Golden evidence

## Related

- `doc/S23_PUBLIC_LIMIT_MATRIX.md` (still-open: race/load beyond named proofs)
- `doc/D025_EVIDENCE_PACK.md` (unsigned)
