# monoloop-connector-claude

Claude Code headless CLI profile.

**Component role:** Channel profile for Component 01 (Connector). Pair with
`monoloop-loop` ChannelBinding / the `monoloop` façade.

**License:** AGPL-3.0-or-later. Commercial: <https://frogfish.io>

## Transport

`claude` CLI stream-json NDJSON on stdout; not ACP

## Assemble

1. Construct profile config + secret resolver (if required).
2. Call `claude_channel_binding(...)` to get a `monoloop_loop::ChannelBinding`.
3. Insert into `ChannelRegistry` and start `DefaultTransactionRuntime` (see
   `monoloop` / `monoloop-loop` READMEs and `fake_echo` examples).

```rust
use monoloop_connector_claude::claude_channel_binding;
// → ChannelBinding ready for ChannelRegistry::build
```

Prompt-on-argv is a documented exception. Live examples: monoloop-testkit `live_claude_*`.

Live end-to-end qualification uses **`monoloop-testkit`** examples (not a product
dependency of this crate).

Normative: `doc/CLAUDE_CONNECTOR.md`.
