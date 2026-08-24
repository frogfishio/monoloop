# Independent security review checklist (D-025 process residual)

Organizational gate — **not** a Fake/scripted acceptance suite and **not**
satisfied by in-session agent triage. Closing D-025’s residual requires a
human (or contracted) reviewer to sign this checklist and record the
sign-off date in `DEFECTS.md` / release notes.

## Scope that must be covered

1. **Trust boundaries** — all Connector I/O, Interpreter assembly output, and
   tool names/payloads treated as untrusted; Canonical ≠ authorized
   (`rules/SECURITY.md`).
2. **Secrets** — no argv secrets on Grok path; `SecretResolver` only; no
   secrets in logs/metrics/default diagnostics.
3. **Identity isolation** — no ambient current session/run/tool; explicit
   resume only; cross-run injection rejected (LAWS 5–9).
4. **Tool effects** — EmptyToolRegistry / NoToolRuntime zero effects;
   dispatch only on complete `ToolRequestReady`; no shell/dynamic load from
   tool names.
5. **Bounds / DoS** — every public queue/table/concurrency/deadline fail-closed
   (Law 22); spot-check Connector HTTP, MCP gateway, Loop admission, event
   delivery.
6. **Ownership** — no detached tasks; RuntimeOwner Drop joins (§18.4);
   TaskSupervisor owns joins; product crates do not depend on testkit.
7. **MCP** — loopback default; CreationOnly vs Refreshable posture matches
   **DECISIONS D-042** until superseded.
8. **Profiles** — headless argv prompt exception only as recorded in
   DECISIONS; credentials never on argv.

## Evidence map (pointers for the independent reviewer — not a sign-off)

These are deterministic entry points a reviewer can run/read. Presence here
does **not** close D-025.

| Checklist item | Suggested evidence |
|---|---|
| 1 Trust boundaries | `rules/SECURITY.md`; Interpreter fragmentation/EOF suites; tool validation reject paths in `linked_tools` / dispatcher |
| 2 Secrets | Grok non-loopback fail-closed; `StreamingHttp` / Grok tests that secrets absent from Debug/errors; `InMemorySecretResolver` / `SecretResolver` seams |
| 3 Identity isolation | No most-recent session; explicit `session/load`; `duplicate_session_race_admits_exactly_one`; DECISIONS D-004 sessionless tool `SessionKey` |
| 4 Tool effects | `empty_loop` EmptyToolRegistry / NoToolRuntime; dispatch only on complete Ready; ProcessIsolated / cancel_only paths |
| 5 Bounds / DoS | MCP plus-one (`mcp_*_plus_one_*`); HTTP D-033/D-019 proofs; admit `max_*_exact_*` / plus-one; event byte/item plus-one (`s22_6_*`) |
| 6 Ownership | `runtime_owner_drop_joins_executor_thread_reaches_stopped`; TaskSupervisor abort-then-join proofs; architecture product↛testkit gates |
| 7 MCP posture | **DECISIONS D-042**; `six_profile_bindings_register_and_validate` (no `Refreshable`); CreationOnly reuse reject at admit |
| 8 Profiles | DECISIONS D-002 argv exception; profile capability report; headless bindings use `McpConfigurationCapability::None` |

### Recent load/race proofs relevant to item 5–6

- `submit_versus_shutdown_barrier_race_two_outcomes` (Echo barrier)
- `submit_versus_shutdown_hang_barrier_both_outcomes` (Hang; both outcomes pinned)
- `concurrent_global_capacity_exhaustion_admits_exactly_max` (Hang; N+1 at
  exact `max_active`)
- `concurrent_per_channel_capacity_exhaustion_admits_exactly_channel_max`
  (Hang; N+1 at exact `max_active_per_channel`)
- `multi_channel_multi_session_concurrent_load` (Hang; ≥3 Channels; shared
  session-string SessionKey isolation + duplicate / capacity rejects)
- `concurrent_hang_terminate_storm_all_cancelled` (Hang; N concurrent Cancel →
  all `Cancelled`)
- `concurrent_hang_force_terminate_storm_all_terminated` (Hang; N concurrent
  ForceTerminate → all `Terminated`)
- `concurrent_hang_cancel_versus_force_terminate_one_terminal` (Hang; barrier
  Cancel vs ForceTerminate on one id → one `{Cancelled, Terminated}`)
- `max_content_parts_exact_admits_plus_one_rejects` (D-035 matrix cell)
- `max_tools_per_transaction_exact_admits_plus_one_rejects` (StartedRuntime
  port of unregistered v1 hardening cell; `InvalidConfiguration`)
- `max_distinct_sessions_exact_admits_plus_one_rejects` (v2 ledger + admit;
  Hang-pinned; session-less does not consume slot at admit)
- `external_agent_claim_time_distinct_sessions_plus_one_limit_exceeded`
  (claim-time `bind_session` → `LimitExceeded`, not `InvariantFailed`)
- `concurrent_session_new_and_explicit_load` (Grok mock; not live)
- Quiescing CAS under ledger lock (`owner.rs` / `supervisor.rs`); late Start
  terminalizes while `stopping`

### Explicitly still open for Golden / §25 (reviewer must not waive)

- This checklist’s **Sign-off** table below unsigned
- Exhaustive public-limit exact/plus-one matrix vs §23 wording (honest inventory
  in `doc/S23_PUBLIC_LIMIT_MATRIX.md`; Open/Partial rows still open — not every
  public `TransactionLimits` field has exact+plus-one)
- Full concurrent/race/load suites beyond named proofs (Fake multi-channel
  load landed; not exhaustive WP-12 race matrix)
- Live Grok multi-session: `live_grok_multi_session` example (concurrent
  `session/new` + isolation; default secret on preauthorized hosts). Explicit
  live `session/load` after short session still residual; mock concurrent
  new/load remains
- Refreshable MCP (deferred; do not treat as shipped)

## Sign-off

| Field | Value |
|---|---|
| Reviewer | _TBD_ |
| Date | _TBD_ |
| Scope notes | _TBD_ |
| Findings filed | _none / link to DEFECTS_ |
| Result | _PASS / FAIL_ |

Until this table is filled by an **independent** reviewer (not the implementing
agent session), D-025 process residual and §23 “independent review finds no
unresolved P0/P1/P2” remain **open** for Golden / §25.

Agent-prepared (unsigned) evidence pointers for reviewers:
`doc/D025_EVIDENCE_PACK.md`. That pack does **not** constitute Sign-off.
