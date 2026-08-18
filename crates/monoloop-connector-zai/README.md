# monoloop-connector-zai

Z.ai headless CLI profile.

**Component role:** Channel profile for Component 01 (Connector). Pair with
`monoloop-loop` ChannelBinding / the `monoloop` façade.

**License:** AGPL-3.0-or-later. Commercial: <https://frogfish.io>

## Transport

CLI stdout NDJSON (OpenAI-chat shaped); not ACP

## Assemble

1. Construct profile config + secret resolver (if required).
2. Call `zai_channel_binding(...)` to get a `monoloop_loop::ChannelBinding`.
3. Insert into `ChannelRegistry` and start `DefaultTransactionRuntime` (see
   `monoloop` / `monoloop-loop` READMEs and `fake_echo` examples).

```rust
use monoloop_connector_zai::zai_channel_binding;
// → ChannelBinding ready for ChannelRegistry::build
```

Prompt-on-argv for this vendor CLI is a documented Law 16 exception
(`DECISIONS.md` D-002). Secrets must not appear on argv.

Live end-to-end qualification uses **`monoloop-testkit`** `live_zai_*` examples
(not a product dependency of this crate).

Normative: `doc/ZAI_CONNECTOR.md`, `doc/REQUIREMENTS.md`.
