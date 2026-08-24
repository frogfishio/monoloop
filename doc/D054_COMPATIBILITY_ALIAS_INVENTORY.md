# D-054 compatibility-alias inventory

**Status (D-060, 2026-08-24):** Declared deprecated-only breaking cut
**executed**. Host `adapt_*` helpers retained (not deprecated). This file is
historical + residual checklist — not a claim of Golden / §25 / D-025.

## Cut executed (D-060)

| Symbol | Action |
|---|---|
| `TransactionRequest` | **Removed** from `monoloop-contracts` |
| `TransactionRuntime` trait | **Removed** (no live `impl` existed) |
| `RuntimeToolSpill` | **Removed** alias; use `OrphanToolPermitSet` only |
| `HostCompletionAdapter` / `HostEventAdapter` | **Removed** empty markers |
| `TransactionToolDispatcher::reap_vault` | **Removed** |
| `OrphanToolPermitSet::reap_finished` | **Removed** no-op |

## Retained host helpers (outside kernel executor)

| Symbol | Crate | Notes |
|---|---|---|
| `adapt_event_sink` | `monoloop-loop` | Host-task drain; §22.7 / `s22_7_host_adapters` |
| `adapt_completion_callback` | `monoloop-loop` | Host-task oneshot; does not read `callback_deadline` (D-059) |
| `TransactionEventSink` / `CompletionCallback` / `Fn*` | `monoloop-contracts` | Host traits for adapters |

## Residual (not this cut)

- Live Grok multi-session
- Exhaustive race/load beyond named Fake proofs
- Independent D-025 / §25 Sign-off
- Refreshable MCP (DECISIONS D-042)
- Optional: move `adapt_*` out of `monoloop-loop` into a host/support crate

## Related

- `DECISIONS.md` D-060
- `DEFECTS.md` D-054
- `doc/D025_EVIDENCE_PACK.md` (unsigned)
