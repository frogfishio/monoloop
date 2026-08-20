# monoloop

**Product façade** for Monoloop: Connector + Interpreter + transaction-composing Loop.

Hosts should depend on this crate for plug-and-play assembly (`cargo add monoloop`).
Profile connectors (Grok, Cursor, …) are optional Cargo features.

**License:** AGPL-3.0-or-later. Commercial: <https://frogfish.io>

| Resource | URL |
|---|---|
| docs.rs | <https://docs.rs/monoloop> |
| Repository / normative `doc/` | <https://github.com/frogfishio/monoloop> |
| Homepage | <https://frogfish.io> |

## What this crate is / is not

| Is | Is not |
|---|---|
| Re-exports of the three product components + contracts | A fourth runtime component |
| Thin CLI (`monoloop --version` / `--copyright`) | A chat UI or agent framework |
| Assembly entry for hosts | Test kit (see `monoloop-testkit`) |

## Host integration (product hosts)

### 1. Smoke assembly (FakeConnector, no network)

```bash
cargo run -p monoloop --example fake_echo
```

Shape: `ChannelBinding` → `ChannelRegistry` → `RuntimeBootstrap` → `submit`.

### 2. Grok Build wiring (no testkit)

```bash
cargo run -p monoloop --example host_grok_wiring --features grok
```

Public binding signature (also on docs.rs / `monoloop-connector-grok`):

```rust
pub fn grok_channel_binding(
    id: impl AsRef<str>,
    endpoint_ref: impl Into<String>,     // "ws://127.0.0.1:2419"
    credential_ref: impl Into<String>,   // SecretResolver key name
    secrets: Arc<dyn SecretResolver>,    // host keychain adapter
    encoder: Arc<dyn OutboundDialectEncoder>, // AcpPromptEncoder::grok()
    interpreter: Arc<dyn InterpreterFactory>, // DefaultInterpreterFactory::new()
) -> ChannelBinding
```

Façade imports with `--features grok`:

- `monoloop::connector_grok::{grok_channel_binding, SecretResolver, SecretRef, …}`
- `monoloop::loop_runtime::AcpPromptEncoder`
- `monoloop::interpreter::DefaultInterpreterFactory`

Live Driver/Console qualification stays in **`monoloop-testkit`** (`live_grok_*`).
Product hosts must not depend on testkit.

### 3. Multi-turn history (host-built)

`user_text_input("…")` is a **one-line** helper. For chat journals, build
`CanonicalInput` yourself:

```rust
use monoloop::contracts::{CanonicalInput, CanonicalMessage, InputLimits, TextPart};

let limits = InputLimits::default();
let input = CanonicalInput::try_new(
    vec![
        CanonicalMessage::User { content: vec![TextPart::try_new("Hi", limits.max_text_part_bytes)?], name: None },
        CanonicalMessage::Assistant { content: vec![TextPart::try_new("Hello!", limits.max_text_part_bytes)?], tool_calls: vec![] },
        CanonicalMessage::User { content: vec![TextPart::try_new("Continue.", limits.max_text_part_bytes)?], name: None },
    ],
    &limits,
)?;
```

Monoloop does **not** own durable history. The host maps journal →
`CanonicalMessage::{System,User,Assistant,Tool}` and submits one transaction at a time.
Resume Grok with explicit `session_id: Some(SessionId::from_external(&ExternalSessionId::try_new(grok_session_id)?))`
(never ambient “last session”).

### 4. Live text path = complete canonical units only

There is **no token / delta stream API**. UI should render from push events:

`TransactionEventPayload::CanonicalUnit(CanonicalUnitEvent)` — complete sentences /
structures / tool lifecycle units. `InterpretationEnd` / EOF alone ≠ turn success;
wait for the completion callback / `Ended` payload.

### 5. Executor ownership (v2)

`StartedRuntime::start` owns a dedicated multi-thread Tokio executor. Hosts do
**not** pass a bare `Handle` into `RuntimeBootstrap`. Pattern:

1. Call `StartedRuntime::start(...)` once at process setup; keep `owner` + `handle`.
2. `handle.submit(...)` is synchronous (no executor wait on the caller).
3. Drain `TransactionReceiver` on a host runtime of your choice.
4. Shutdown: `owner.begin_shutdown()` then `wait_stopped` until `Stopped`.

Set `RuntimeConfig { enable_mcp_listener: false, ..Default::default() }` unless you want the MCP shell.

### Hard rules

- **Do not** depend on `monoloop-testkit` from product crates.
- **Do not** invent ambient “current session”.
- Empty tools (`HostToolRegistry::empty()`, `tools: vec![]`) → `tool_unavailable`, zero effects.
- Canonical completeness ≠ authorization.

## Agent assembly recipe (copy this shape)

```text
1. ChannelBinding { connector_factory, encoder, interpreter, capabilities, … }
2. ChannelRegistry::build(vec![binding])
3. StartedRuntime::start(RuntimeBootstrap {
     config: RuntimeConfig { enable_mcp_listener: false, ..Default::default() },
     channels, tools: HostToolRegistry::empty()
   })   // owns its executor — no bare Handle
4. let (delivery, receiver) = transaction_delivery(limits)?;
5. handle.submit(TransactionSubmitRequest { …, delivery })
6. Drain receiver.events / await receiver.completion (host side)
7. owner.begin_shutdown(); owner.wait_stopped(deadline)  // TimedOut ⇒ Quiescing
```

### Module map

| Use | From |
|---|---|
| `monoloop::contracts::*` | identities, ports, `CanonicalInput`, sinks |
| `monoloop::connector::*` | FakeConnector / abstract Connector |
| `monoloop::interpreter::*` | `DefaultInterpreterFactory` |
| `monoloop::loop_runtime::*` | runtime, registry, encoders |
| `monoloop::connector_grok::*` | Grok profile (`features = ["grok"]`) |

### `TestTextEncoder` (smoke only)

Loop-owned deterministic encoder for FakeConnector + `test_raw`. Not a production
Channel encoder. Live hosts use profile `*_channel_binding`.

## Optional Channel profiles

```toml
monoloop = { version = "0.1", features = ["grok"] }
# also: cursor, codex, agy, zai, claude
```

## Version / license helpers

```rust
use monoloop::{version_string, copyright_notice};
println!("{}", version_string());
println!("{}", copyright_notice());
```

Normative specs (public repo): `doc/README.md`, `doc/MONOLOOP.md`,
`doc/TRANSACTION_RUNTIME_IMPLEMENTATION.md`, `doc/GROK_BUILD_CONNECTOR.md`.
