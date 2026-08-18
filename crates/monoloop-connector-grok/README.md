# monoloop-connector-grok

Grok Build ACP/WebSocket profile.

**Component role:** Channel profile for Component 01 (Connector). Pair with
`monoloop-loop` ChannelBinding / the `monoloop` façade (`features = ["grok"]`).

**License:** AGPL-3.0-or-later. Commercial: <https://frogfish.io>

## Transport

Authenticated ACP/JSON-RPC over WebSocket; correlation id = Grok `sessionId`.

## Public assembly API

```rust
pub fn grok_channel_binding(
    id: impl AsRef<str>,                 // ChannelId, e.g. "grok"
    endpoint_ref: impl Into<String>,     // "ws://127.0.0.1:2419"
    credential_ref: impl Into<String>,   // SecretResolver key name (not the secret)
    secrets: Arc<dyn SecretResolver>,    // host keychain / EnvSecretResolver / …
    encoder: Arc<dyn OutboundDialectEncoder>, // AcpPromptEncoder::grok()
    interpreter: Arc<dyn InterpreterFactory>, // DefaultInterpreterFactory::new()
) -> ChannelBinding
```

Related types in this crate:

| Type | Role |
|---|---|
| `SecretResolver` | Host trait: `resolve(&SecretRef) -> Result<String, _>` |
| `SecretRef` | Opaque credential **name** |
| `InMemorySecretResolver` | Test / smoke only |
| `EnvSecretResolver` | `credential_ref` = env var name |
| `GrokServerConfig` | Optional endpoint security validation helper |

Façade path when `monoloop` is built with `--features grok`:

`monoloop::connector_grok::{grok_channel_binding, SecretResolver, …}`

## Host wiring (no testkit)

```bash
cargo run -p monoloop --example host_grok_wiring --features grok
```

Optional live submit:

```bash
MONOLOOP_GROK_SUBMIT=1 \
MONOLOOP_GROK_ENDPOINT=ws://127.0.0.1:2419 \
MONOLOOP_GROK_SECRET='…' \
  cargo run -p monoloop --example host_grok_wiring --features grok
```

Live Driver/Console qualification examples stay in **`monoloop-testkit`**
(`live_grok_*`) and must not become product dependencies.

## Secrets

Resolve only through `SecretResolver`. Never put credentials on argv or in logs.
Non-loopback requires `wss` (fail-closed).

Normative: `doc/GROK_BUILD_CONNECTOR.md` (repo now public:
<https://github.com/frogfishio/monoloop>).
