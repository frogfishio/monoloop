# Monoloop

Monoloop is a Rust project built from exactly three asynchronous product
components:

1. Connector
2. Interpreter
3. the smallest extensible Loop

The initial Connector talks to one authenticated Grok Build server using
ACP/JSON-RPC over WebSocket. That single Grok instance hosts multiple logical
sessions. Grok's returned `sessionId` is used directly as Monoloop's session
correlation identity, while bounded routing and pending-operation state remain
in memory.

The Driver, stdin/stdout adapters, deterministic fixtures, and console renderer
are a separate test kit. They are not additional product components.

See the [documentation index](doc/README.md), the [Grok Build Connector
profile](doc/GROK_BUILD_CONNECTOR.md), and the [Test Kit and
Driver](doc/TEST_KIT.md).

## Workspace (implementation in progress)

```text
crates/
  monoloop-contracts        # identities, dialect, errors, limits
  monoloop-connector        # abstract Connector, FakeConnector, ConnectorProxy
  monoloop-connector-grok   # Grok Build ACP/WebSocket profile
```

```bash
cargo test --workspace
```

### Connector usage sketch

```rust
// Multi-session Grok profile
let secrets = Arc::new(InMemorySecretResolver::new());
secrets.insert("GROK_WS_SECRET", std::env::var("GROK_WS_SECRET")?);
let grok = GrokConnector::new(secrets);
let server = grok.connect(GrokServerConfig::loopback(2419, SecretRef::new("GROK_WS_SECRET"))?)?
    .opened.await??;
let session = server.sessions.begin_new(GrokSessionConfig::default())?.opened.await??;

// Or route via ConnectorProxy + abstract Connector trait
let proxy = ConnectorProxy::builder()
    .register("grok", Arc::new(grok))
    .build()?;
// endpoint_ref: "grok:ws://127.0.0.1:2419" with credential_ref set
```
