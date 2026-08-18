# monoloop-connector-agy

Google Antigravity (agy) ACP profile.

**Component role:** Channel profile for Component 01 (Connector). Pair with
`monoloop-loop` ChannelBinding / the `monoloop` façade.

**License:** AGPL-3.0-or-later. Commercial: <https://frogfish.io>

## Transport

JSON-RPC 2.0 over stdio NDJSON

## Assemble

1. Construct profile config + secret resolver (if required).
2. Call `agy_channel_binding(...)` to get a `monoloop_loop::ChannelBinding`.
3. Insert into `ChannelRegistry` and start `DefaultTransactionRuntime` (see
   `monoloop` / `monoloop-loop` READMEs and `fake_echo` examples).

```rust
use monoloop_connector_agy::agy_channel_binding;
// → ChannelBinding ready for ChannelRegistry::build
```

Live ask/crud examples: monoloop-testkit `live_agy_*`.

Live end-to-end qualification uses **`monoloop-testkit`** examples (not a product
dependency of this crate).

Normative: `doc/AGY_CONNECTOR.md`.
