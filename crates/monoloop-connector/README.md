# monoloop-connector

**Component 01 — Connector:** dialect-labelled transport, session attach/open,
cancel/terminate, one terminal transport outcome.

Does **not** interpret assistant text, tools, or turns.

**License:** AGPL-3.0-or-later. Commercial: <https://frogfish.io>

## What this crate is / is not

| Is | Is not |
|---|---|
| `Connector` trait, open/pending/opened handles | Semantic Interpreter |
| `FakeConnector` / `FakeConnectorFactory` for deterministic tests | Product UI |
| `StreamingHttpConnector`, `ConnectorProxy` | Durable session store |
| SessionAdapter ownership seams | Tool execution |

## How an assembler uses it

Hosts usually do **not** call Connector directly. They put a `ConnectorFactory`
on a `monoloop_loop::ChannelBinding`; the transaction runtime opens connections.

For local tests:

```rust
use monoloop_connector::FakeConnectorFactory;
use std::sync::Arc;

let factory = Arc::new(FakeConnectorFactory::direct_llm());
// → ChannelBinding.connector_factory = factory
```

Live profiles (Grok/Cursor/…) live in `monoloop-connector-*` crates.

Normative: `doc/CONNECTOR.md`.
