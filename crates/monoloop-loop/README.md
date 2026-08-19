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
Follow the seven-stage plan in the v2 spec. **M0–M4 connection-open landed**
(D-042 tracks remaining process-core joins): owned executor, task supervisor,
ledger, RAII reservations, sync admission, separate start / control / worker /
spawn queues, EventPublisher + Seal, Fake exchange under `TransactionTaskSpawner`,
and joinable `ConnectionOwnerWork` on Fake/HTTP/ACP `begin_open` paths. ACP
update pumps are JoinSet-owned inside owner work (not fused with prompt RPC).
Process-lifetime pumps / Grok `connect()` demux remain open. Deferred on-disk
modules stay uncompiled until their stage. Legacy suites need
`--features legacy_runtime_tests`.

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
