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
  monoloop-contracts        # identities, dialect, errors, limits, canonical events
  monoloop-connector        # abstract Connector, FakeConnector, ConnectorProxy
  monoloop-connector-grok   # Grok Build ACP/WebSocket profile
  monoloop-interpreter      # reassemble raw dialect bytes → complete canonical events
  monoloop-loop             # lossless subscription; empty-tool path (ToolUnavailable)
  monoloop-testkit          # distributor, console, Driver (NOT a product dependency)
```

```bash
cargo test --workspace
```

### Raw dump (exact Grok / dialect bytes)

Opt-in only — captures **exact** inbound WebSocket payloads from Grok before demux:

```rust
use monoloop_connector_grok::{GrokServerConfig, RawDumpCollector, SecretRef};
use std::sync::Arc;

let dump = Arc::new(RawDumpCollector::enabled());
let config = GrokServerConfig::loopback(2419, SecretRef::new("GROK_WS_SECRET"))?
    .with_raw_dump(Arc::clone(&dump));
// … connect, session/new, session/prompt …
println!("{}", dump.snapshot().format_text());
```

Pipeline / Driver (`--params` style):

```rust
use monoloop_testkit::{run_bytes_pipeline_with_params, PipelineParams, acp_binding};

let report = run_bytes_pipeline_with_params(
    acp_binding(),
    &chunks,
    PipelineParams::with_raw_dump(), // render_console + dump_raw
).await;
println!("{}", report.raw_dump.unwrap().format_text());
```

### Live Grok Build CRUD (event sequence)

```bash
# Terminal A
grok agent --always-approve serve --bind 127.0.0.1:2419 --secret monoloop-live-test

# Terminal B (repo root)
export GROK_AGENT_SECRET=monoloop-live-test
cargo run -p monoloop-testkit --example live_grok_crud
open target/live_grok_crud.html
# also: target/live_grok_crud.sequence.txt  target/live_grok_crud.raw.txt

# Re-assemble HTML from a saved raw dump (no live Grok needed):
cargo run -p monoloop-testkit --example replay_raw_html -- target/live_grok_crud.raw.txt
open target/live_grok_crud.replay.html
```

Connects to `ws://127.0.0.1:2419/ws`, creates a session with `cwd` = this project,
asks Grok to CRUD `monoloop_live_crud_test.txt`, and dumps the canonical event
sequence + raw wire frames + HTML review.

### HTML interpretation review (canonical → Markdown → HTML)

Visual check that complete sentences/tools serialise correctly (test kit only):

```rust
use monoloop_testkit::{run_bytes_pipeline_with_params, PipelineParams, acp_binding};

let report = run_bytes_pipeline_with_params(
    acp_binding(),
    &chunks,
    PipelineParams::with_html_dump("target/interpretation.html"),
).await;
// Open target/interpretation.html in a browser:
// - Interleaved stream (tools + text in event order)
// - Text-only assembly (sentences → MD → HTML; list markers attached)
// - Canonical event timeline (every unit generation)
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
