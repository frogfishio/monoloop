# monoloop-contracts

Shared **identities, ports, limits, errors, and canonical types** for Monoloop.

Hosts and all three product components depend on this crate. It has **no I/O**
and **no Tokio runtime** of its own.

**License:** AGPL-3.0-or-later. Commercial: <https://frogfish.io>

## What this crate is / is not

| Is | Is not |
|---|---|
| Shared vocabulary (`SessionKey`, `TransactionSubmitRequest`, canonical units, …) | The Monoloop product façade (use `monoloop`) |
| Push delivery ports (`transaction_delivery`, `TransactionDelivery`) | Connector / Interpreter / Loop implementations |
| Host-side sink/callback **adapters** (`TransactionEventSink`, `CompletionCallback`) | Persistence, UI, or tools |

## How an assembler uses it (Runtime v2)

This crate alone does **not** run a transaction. Prefer the façade:

1. Depend on `monoloop` (or wire `monoloop-loop` + connector + interpreter).
2. Build `CanonicalInput` (e.g. `user_text_input("…")`).
3. `let (delivery, receiver) = transaction_delivery(limits)?;`
4. `StartedRuntime::start(RuntimeBootstrap { … })` — runtime owns its executor.
5. `handle.submit(TransactionSubmitRequest { …, delivery })`.
6. Drain `receiver.events` / await `receiver.completion` on the host side.
7. Optional: `adapt_event_sink` / `adapt_completion_callback` (in `monoloop-loop`)
   to bridge push receivers into host sinks **outside** the runtime.

```rust
use monoloop_contracts::user_text_input;

let input = user_text_input("hello")?;
```

Full wiring: `monoloop` `examples/fake_echo.rs`.

Core submits use `TransactionSubmitRequest` + `transaction_delivery` only.
The former sink-shaped `TransactionRequest` / `TransactionRuntime` trait were
**removed** (DECISIONS D-060). Host traits `TransactionEventSink` /
`CompletionCallback` remain for out-of-kernel adapters.

## Key modules

| Module area | Examples |
|---|---|
| Identity | `ChannelId`, `SessionId`, `SessionKey`, `TransactionId`, `ExchangeId` |
| Core submit (v2) | `TransactionSubmitRequest`, `transaction_delivery`, `TransactionDelivery` |
| Host adapters (outside core) | `TransactionEventSink`, `CompletionCallback`, `FnEventSink`, `FnCompletionCallback` |
| Canonical | `CanonicalUnitEvent`, `ToolRequestState`, `CanonicalInput` |
| Config | `InvocationConfig`, `EffectiveConfig`, `OptionPolicy`, `ChannelCapabilities` |
| Dialects | `DialectDescriptor`, `DialectFamily` |
| Outcomes | `TransactionEvent`, `TransactionEventPayload`, `TransactionEndEvent`, `TransactionEndKind` |

Normative: `doc/TRANSACTION_RUNTIME_V2_SPEC.md`, `doc/REQUIREMENTS.md`.
