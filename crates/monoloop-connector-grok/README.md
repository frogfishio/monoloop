# monoloop-connector-grok

Grok Build ACP/WebSocket profile.

**Component role:** Channel profile for Component 01 (Connector). Pair with
`monoloop-loop` ChannelBinding / the `monoloop` façade.

**License:** AGPL-3.0-or-later. Commercial: <https://frogfish.io>

## Transport

authenticated ACP/JSON-RPC over WebSocket; correlation id = Grok sessionId

## Assemble

1. Construct profile config + secret resolver (if required).
2. Call `grok_channel_binding(...)` to get a `monoloop_loop::ChannelBinding`.
3. Insert into `ChannelRegistry` and start `DefaultTransactionRuntime` (see
   `monoloop` / `monoloop-loop` READMEs and `fake_echo` examples).

```rust
use monoloop_connector_grok::grok_channel_binding;
// → ChannelBinding ready for ChannelRegistry::build
```

Secrets via SecretResolver only; non-loopback requires wss (fail-closed).

Live end-to-end qualification uses **`monoloop-testkit`** examples (not a product
dependency of this crate).

Normative: `doc/GROK_BUILD_CONNECTOR.md`.
