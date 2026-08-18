# monoloop-contracts

Shared **identities, ports, limits, errors, and canonical types** for Monoloop.

Hosts and all three product components depend on this crate. It has **no I/O**
and **no Tokio runtime** of its own.

**License:** AGPL-3.0-or-later. Commercial: <https://frogfish.io>

## What this crate is / is not

| Is | Is not |
|---|---|
| Shared vocabulary (`SessionKey`, `TransactionRequest`, canonical units, …) | The Monoloop product façade (use `monoloop`) |
| Traits/ports (`TransactionRuntime`, event sink, completion callback) | Connector / Interpreter / Loop implementations |
| Config merge + option policy types | Persistence, UI, or tools |

## How an assembler uses it

This crate alone does **not** run a transaction. Prefer the façade:

1. Depend on `monoloop` (or wire `monoloop-loop` + connector + interpreter).
2. Build `CanonicalInput` (e.g. `user_text_input("…")`).
3. Build `TransactionRequest` with:
   - explicit `channel_id` (and optional `session_id`)
   - `invocation_config` (deadline, continuation policy, …)
   - push `events: Arc<dyn TransactionEventSink>` (e.g. `FnEventSink`)
   - one `completion: Box<dyn CompletionCallback>` (e.g. `FnCompletionCallback`)
4. Call `TransactionRuntime::submit` on a started `DefaultTransactionRuntime`.

```rust
use monoloop_contracts::user_text_input;

let input = user_text_input("hello")?;
```

Full wiring: `monoloop` / `monoloop-loop` `examples/fake_echo.rs`.

## Key modules

| Module area | Examples |
|---|---|
| Identity | `ChannelId`, `SessionId`, `SessionKey`, `TransactionId`, `ExchangeId` |
| Transaction ports | `TransactionRuntime`, `TransactionRequest`, `TransactionEventSink`, `CompletionCallback` |
| Push adapters | `FnEventSink`, `FnCompletionCallback`, `EventDelivery`, `CompletionDelivery` |
| Canonical | `CanonicalUnitEvent`, `ToolRequestState`, `CanonicalInput` |
| Config | `InvocationConfig`, `EffectiveConfig`, `OptionPolicy`, `ChannelCapabilities` |
| Dialects | `DialectDescriptor`, `DialectFamily` |
| Outcomes | `TransactionEvent`, `TransactionEventPayload`, `TransactionEnd`, `TransactionEndKind` |

Normative: `doc/REQUIREMENTS.md`, `doc/TRANSACTION_RUNTIME_IMPLEMENTATION.md`.
