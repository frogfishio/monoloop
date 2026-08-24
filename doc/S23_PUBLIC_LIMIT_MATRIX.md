# §23 public-limit exact / plus-one matrix

Honest inventory of `TransactionLimits` fields vs in-tree exact/plus-one (or
equivalent fail-closed) proofs. This is **not** a claim that the matrix is
exhaustive enough for Golden / §25.

Status legend:

| Status | Meaning |
|---|---|
| **Covered** | Proof sets **this** `TransactionLimits` field and fails closed at the bound |
| **Partial** | Field is wired or adjacent proof exists; not a full field-exact+plus-one cell |
| **Open** | Unwired and/or no dedicated field proof |
| **Retired** | Deliberately not a product-enforced bound; recorded in `DECISIONS.md` |

## TransactionLimits

| Field | Status | Proof needle(s) |
|---|---|---|
| `max_active_transactions` | Covered | `capacity_plus_one_rejects`; `concurrent_global_capacity_exhaustion_admits_exactly_max` |
| `max_active_per_channel` | Covered | `concurrent_per_channel_capacity_exhaustion_admits_exactly_channel_max` |
| `max_actor_commands` | Covered | `transaction_limits_max_actor_commands_plus_one_rejects` (sets `TransactionLimits.max_actor_commands=1` on `StartedRuntime`; hold control drain; exact-admit Cancel then plus-one `ControlCapacityExceeded`) |
| `max_actor_command_bytes` | Retired | D-057: supervisor `ControlCommand` is a closed enum; product bound is item capacity `max_actor_commands` only. Field retained for validate/ABI; no byte accounting use site |
| `max_event_queue` | Covered | `transaction_limits_max_event_queue_exact_admits_plus_one_rejects` (runtime ceiling over caller `DeliveryLimits.max_event_items` at admit). Adjacent enqueue: `s22_6_event_item_plus_one_fails_closed` (DeliveryLimits) |
| `max_event_queue_bytes` | Covered | `transaction_limits_max_event_queue_bytes_exact_admits_plus_one_rejects` (runtime ceiling over caller `DeliveryLimits.max_event_bytes` at admit). Adjacent enqueue: `s22_6_event_byte_plus_one_fails_closed` (DeliveryLimits) |
| `max_input_bytes` | Covered | `max_input_bytes_exact_admits_plus_one_rejects`; `max_input_bytes_plus_one_rejected_at_admit` |
| `max_messages` | Covered | `max_messages_exact_admits_plus_one_rejects`; `max_messages_plus_one_rejected_at_admit` |
| `max_content_parts` | Covered | `max_content_parts_exact_admits_plus_one_rejects`; `max_content_parts_plus_one_rejected_at_admit` |
| `max_tools_per_transaction` | Covered | `max_tools_per_transaction_exact_admits_plus_one_rejects` |
| `max_tool_schema_bytes` | Covered | `transaction_limits_max_tool_schema_bytes_exact_admits_plus_one_rejects` (sets `TransactionLimits.max_tool_schema_bytes` on `StartedRuntime`; exact schema size admits, size−1 rejects at start) |
| `max_tool_payload_bytes` | Covered | `fake_transaction_limits_max_tool_payload_bytes_plus_one_rejects` (sets `TransactionLimits.max_tool_payload_bytes` through `StartedRuntime` / `limits_from_transaction`) |
| `max_tool_output_bytes` | Covered | `fake_transaction_limits_max_tool_output_bytes_plus_one_fails_closed` (sets `TransactionLimits.max_tool_output_bytes` through `StartedRuntime` / `limits_from_transaction`). Adjacent: `max_tool_output_bytes_plus_one_fails_closed` (DispatcherLimits-only) |
| `max_concurrent_tools_per_transaction` | Covered | `transaction_limits_max_concurrent_tools_plus_one_rejects` (sets `TransactionLimits.max_concurrent_tools_per_transaction` then `limits_from_transaction`) |
| `max_queued_tools_per_transaction` | Covered | `transaction_limits_max_queued_tools_plus_one_rejects` (sets `TransactionLimits.max_queued_tools_per_transaction` then `limits_from_transaction`; hold occupies concurrency, second occupies queue slot, third → `tool_queue_full`) |
| `max_continuations` | Covered | `fake_inline_max_continuations_zero_ends_limit_exceeded`; `fake_inline_max_continuations_one_exhausted_ends_limit_exceeded`; HTTP twins |
| `max_provider_exchanges` | Covered | `fake_inline_max_provider_exchanges_one_ends_limit_exceeded`; `fake_inline_max_provider_exchanges_two_exact_then_limit_exceeded`; HTTP twins |
| `max_continuation_context_bytes` | Covered | Encoded-byte enforcement in `run_direct_llm_continuation`; gross overflow `fake_inline_continuation_context_bytes_limit_exceeded` / HTTP twin; exact−1 reject + padded-exact open: `fake_inline_continuation_context_bytes_exact_admits_plus_one_rejects` |
| `max_total_provider_input_bytes` | Covered | `fake_total_provider_input_bytes_limit_exceeded_before_open`; `fake_inline_cumulative_input_budget_exhausted_blocks_second_open`; HTTP first-exchange twin |
| `max_total_provider_output_bytes` | Covered | `fake_total_provider_output_bytes_limit_exceeded`; cumulative exact/plus-one Fake+HTTP |
| `max_diagnostic_count` | Open | Deferred (D-058): no production `TransactionDiagnostic` emission; ledger count unused; do not invent shaped Covered |
| `max_diagnostic_bytes` | Open | Deferred (D-058): `SafeDiagnostic::try_new_default` uses `TransactionLimits::default()`, not runtime field |
| `transaction_deadline` | Covered | Hang exchange + invocation override Instant; Loop/tool wait races absolute Instant (`LoopDispatchError::DeadlineExceeded`); tool budget `min(execution_deadline, remaining tx Instant)`. Startup rejects Instant-unrepresentable durations. |
| `cleanup_deadline` | Partial | Wired to exchange `children.wait(cleanup_deadline)` and quiesce hard-grace (`cleanup_deadline.max(2s)` floor on that path only). **No** distinct fail-closed completion code / exact-admit cell — keep Partial for Golden (do not invent a new end kind in agent Golden push) |
| `terminal_event_delivery_deadline` | Covered | `transaction_limits_terminal_event_delivery_deadline_seal_fails_closed` (sets field on `StartedRuntime`; full host mailbox + Seal budget → `TerminalEventDelivery::DeadlineExceeded`). Adjacent: `d047_seal_uses_terminal_deadline_not_transaction_deadline` |
| `callback_deadline` | Open | Deferred (D-059): validate-only; M7 push completion has no core callback wait site |

## Inventory gate

`crates/monoloop-loop/tests/s23_forbidden_patterns.rs` →
`s23_exact_limit_plus_one_inventory_present` requires listed **Covered** needles
to remain on disk, and requires this matrix file to name every
`TransactionLimits` field above.

## Still open for Golden (do not waive)

- Every **Open** / **Partial** row above (**Retired** rows are closed by decision, not Covered proofs)
- Race/load beyond named Fake proofs (inventory: `doc/S23_RACE_LOAD_INVENTORY.md`; still not exhaustive; live concurrent new+isolation example landed, live session/load residual)
- Live Grok: concurrent new+isolation landed; `session/load` residual (D-061)
- Independent D-025 / §25 Sign-off (`doc/D025_EVIDENCE_PACK.md`, unsigned)
- Optional later: move `adapt_*` host helpers out of `monoloop-loop` (D-060 retained them; not a Golden blocker by itself)
