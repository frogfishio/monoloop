# monoloop-connector-codex

OpenAI Codex ACP profile.

**Component role:** Channel profile for Component 01 (Connector). Pair with
`monoloop-loop` ChannelBinding / the `monoloop` façade.

**License:** AGPL-3.0-or-later. Commercial: <https://frogfish.io>

## Transport

JSON-RPC 2.0 over stdio NDJSON (codex-acp)

## Assemble

1. Construct profile config + secret resolver (if required).
2. Call `codex_channel_binding(...)` to get a `monoloop_loop::ChannelBinding`.
3. Insert into `ChannelRegistry` and start `StartedRuntime` (v2; no bare Handle) (see
   `monoloop` / `monoloop-loop` READMEs and `fake_echo` examples).

```rust
use monoloop_connector_codex::codex_channel_binding;
// → ChannelBinding ready for ChannelRegistry::build
```

Live ask/crud examples: monoloop-testkit `live_codex_*`.

Live end-to-end qualification uses **`monoloop-testkit`** examples (not a product
dependency of this crate).

Normative: `doc/CODEX_CONNECTOR.md`.
