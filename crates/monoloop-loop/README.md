# monoloop-loop

**Component 03 — The Loop:** transaction admission/composition plus the inner
lossless tool runtime.

**Lifecycle status:** Runtime v2 migration in progress
(`doc/TRANSACTION_RUNTIME_V2_SPEC.md`, D-003). The v1 lifecycle modules were
removed; replacement code lives under `src/transaction/lifecycle/`.

Owns Channel registry, outbound encoders, linked-tool types, and the emerging
`lifecycle` owner/handle surface. Inner `LoopRuntime` stays dialect-neutral.

Prefer the façade crate `monoloop` unless you need this crate as a direct dependency.

**License:** AGPL-3.0-or-later. Commercial: <https://frogfish.io>

## What this crate is / is not

| Is | Is not |
|---|---|
| `lifecycle::RuntimeOwner` / `StartedRuntime` (v2, staging) | Context/prompt engine |
| `ChannelBinding` / `ChannelRegistry` | Test Driver / Console (→ `monoloop-testkit`) |
| Empty-tool path (`tool_unavailable`, zero effects) | Ambient “current session” |
| Production profile encoders + `TestTextEncoder` smoke helper | Host UI |

## Migration note

Do not recreate the deleted v1 files (`runtime`, `admission`, `actor`,
`finalization`, `callback_service`, `executor_spawn`, `tool_join_vault`).
Follow the seven-stage plan in the v2 spec. M5.4 delete-vaults: joins are
TaskSupervisor-owned (not vaulted). `OrphanToolPermitSet` (alias
`OrphanToolPermitSet`; deprecated alias `RuntimeToolSpill`) holds only capacity orphans for §22.4 non-ack / Process
mid-drop — not JoinHandles. JoinOnly Stopped inject is supervisor
park/unpark. Production handlers drive inline. §23 gates live in
`tests/s23_forbidden_patterns.rs`.

**M0–M5 landed** (DEFECTS D-042 / D-043 / D-044). **M6 §22 closed enough**
(D-045): §22.1–§22.7 proofs landed (host-adapter proofs outside core);
MCP RuntimeService + CreationOnly + `TaskClass::McpRequest` ownership landed.
Join vault retired to `OrphanToolPermitSet` (`RuntimeToolSpill` deprecated alias);
`Stopped` is TaskSupervisor-empty after orphan release. Refreshable MCP
**deferred** for initial profiles (**DECISIONS D-042** / WP12).

**M7 façade landed (D-038 Fixed):** `StartedRuntime` + `TransactionSubmitRequest`
is the assembler recipe; deprecated sink-shaped `TransactionRequest` /
`TransactionRuntime` trait are not core submit APIs. Host adapters
`adapt_event_sink` / `adapt_completion_callback` stay (outside the kernel).
**D-053 Fixed:** legacy v1 integration suites were deleted (coverage map
`doc/D053_COVERAGE_REPLACEMENT.md`); `autotests` / `autoexamples` are enabled
so every on-disk suite and example is compiled by `--all-targets`.
**Not Golden / §25 DoD** while remaining §23 extras, independent review, and
compatibility-alias cleanup remain open. D-039 / D-040 / D-041 Fixed.

**D-054 (partial):** obsolete uncompiled v1 modules (`active_registry`,
`events`, `exchange`, `spawn_gate`) **deleted**. Host adapters
`adapt_event_sink` / `adapt_completion_callback` and deprecated aliases
(`RuntimeToolSpill`, sink-shaped `TransactionRequest`) remain as an explicit
compatibility phase — not claimed as full M7 deletion.

## Agent assembly recipe (v2 / M2)

```text
1. Build ChannelBinding { … }
2. ChannelRegistry::build(vec![binding])
3. StartedRuntime::start(RuntimeBootstrap { config, channels, tools })
     // no external Tokio Handle — runtime owns its executor
4. let (delivery, receiver) = transaction_delivery(limits)?;
5. handle.submit(TransactionSubmitRequest { …, delivery })
6. Host drains receiver outside the runtime executor
7. owner.begin_shutdown(); owner.wait_stopped(deadline) // timeout ⇒ Quiescing
```
