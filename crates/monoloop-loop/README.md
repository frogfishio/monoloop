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
Follow the seven-stage plan in the v2 spec. Runtime-scoped
`RuntimeToolSpill` (in `dispatcher.rs`) is the interim honesty fix for
parked tool joins — not a revived v1 vault module; M5 end state still
deletes join vaults once handler workers are TaskSupervisor-owned.

**M0–M5 landed** (D-042 / D-043 / D-044). **M6 §22 closed enough** (D-045):
§22.1–§22.7 proofs landed (host-adapter proofs outside core);
MCP RuntimeService + CreationOnly + `TaskClass::McpRequest` ownership landed.
Process-global tool-join pending set removed (`RuntimeToolSpill` + Stopped
spill-empty gate). Refreshable MCP undeclared (WP12).

**M7 façade landed (D-038 Fixed):** `StartedRuntime` + `TransactionSubmitRequest`
is the assembler recipe; deprecated sink-shaped `TransactionRequest` /
`TransactionRuntime` trait are not core submit APIs. Host adapters
`adapt_event_sink` / `adapt_completion_callback` stay (outside the kernel).
Unregistered v1 integration `.rs` files remain on disk until rewritten.
**Not Golden / §25 DoD** while §23 workspace gates and independent review
remain open. D-039 / D-040 / D-041 Fixed.

Deferred on-disk modules (`active_registry`, `spawn_gate`, …) stay uncompiled
until Loop-machine consolidation — deleting them is not part of façade cutover.

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
