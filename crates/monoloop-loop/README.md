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
Follow the seven-stage plan in the v2 spec. Deferred on-disk modules
(`dispatcher`, `exchange`, `mcp`, …) stay uncompiled until their stage.

## Agent assembly recipe (v2 target)

```text
1. Build ChannelBinding { … }
2. ChannelRegistry::build(vec![binding])
3. start → StartedRuntime { owner, handle }   // M2: owned executor
4. handle.submit with TransactionDelivery ports
5. Host drains TransactionReceiver outside the runtime executor
6. owner.begin_shutdown(); owner.wait_stopped(deadline)
```

Until M7 cutover, integration tests that still expect `DefaultTransactionRuntime`
will not compile against this crate.
