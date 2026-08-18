# Monoloop

Monoloop is a Rust project built from exactly three asynchronous product
components:

1. Connector
2. Interpreter
3. a transaction-composing Loop with a separately testable inner tool runtime

The initial Connector talks to one authenticated Grok Build server using
ACP/JSON-RPC over WebSocket. That single Grok instance hosts multiple logical
sessions. Grok's returned `sessionId` is used directly as Monoloop's session
correlation identity, while bounded routing and pending-operation state remain
in memory.

The Driver, stdin/stdout adapters, deterministic fixtures, and console renderer
are a separate test kit. They are not additional product components.

See the [documentation index](doc/README.md), [requirements
register](doc/REQUIREMENTS.md), [transaction design](doc/TRANSACTION_RUNTIME_DESIGN.md),
[development specification](doc/TRANSACTION_RUNTIME_IMPLEMENTATION.md),
[delivery plan](doc/TRANSACTION_RUNTIME_DELIVERY_PLAN.md), [Grok Build
Connector profile](doc/GROK_BUILD_CONNECTOR.md), and [Test Kit and
Driver](doc/TEST_KIT.md).

## Workspace

```text
crates/
  monoloop-contracts          # identities, dialect, errors, limits, canonical events
  monoloop-connector          # abstract Connector, FakeConnector, ConnectorProxy
  monoloop-connector-grok     # Grok Build ACP/WebSocket profile
  monoloop-connector-*        # additional Channel profiles (Cursor, Codex, …)
  monoloop-interpreter        # raw dialect bytes → complete canonical events
  monoloop-loop               # transaction runtime; inner tool loop; MCP/tool adapters
  monoloop-testkit            # distributor, console, Driver (NOT a product dependency)
```

Fake + scripted OpenAI paths are the deterministic acceptance surface. Live
multi-profile qualification and headless CLI exceptions are documented in
`doc/WP12_CURRENT_LIMITATIONS.md` and `DECISIONS.md`.

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
# Managed live run (driver starts Grok serve, runs prompt, stops serve):
export GROK_AGENT_SECRET=monoloop-live-test   # optional; default monoloop-live-test
cargo run -p monoloop-testkit --example live_grok_ask -- --preset crud
cargo run -p monoloop-testkit --example live_grok_ask -- --preset analyze
cargo run -p monoloop-testkit --example live_grok_ask -- "Your free-form question"
open target/live_grok_ask.html   # or live_grok_crud / live_grok_analyze

# Optional safety ceiling only (default: wait until Grok finishes, ≤ 2h RPC deadline):
# GROK_PROMPT_CEILING_SECS=3600 cargo run -p monoloop-testkit --example live_grok_ask -- --preset analyze

# Re-assemble HTML from a saved raw dump (no live Grok needed):
cargo run -p monoloop-testkit --example replay_raw_html -- target/live_grok_crud.raw.txt
open target/live_grok_crud.replay.html

# Deterministic qualification (Interpreter + projection matrix; optional live dump replay):
./scripts/qualify-interpreter-projection.sh
./scripts/qualify-interpreter-projection.sh --with-replay-html
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
// - Chat projection (human-digestible reassembly — report, not ground truth)
// - Interleaved stream (tools + text in event order — ground truth)
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

## License

Copyright (C) Alexander R. Croft

Monoloop is licensed under the **GNU Affero General Public License v3.0 or later**
(`AGPL-3.0-or-later`). See [`LICENSE`](LICENSE).

A **commercial license** is available from [frogfish.io](https://frogfish.io).
See [`LICENSE-COMMERCIAL.md`](LICENSE-COMMERCIAL.md) and [`LICENSING.md`](LICENSING.md).

External pull requests and contributed copyrightable material are **not** accepted;
see [`CONTRIBUTING.md`](CONTRIBUTING.md).

## Versioning

- `VERSION` — semver (e.g. `0.1.0`); bump with `make bump` (or edit by hand)
- `BUILD` — monotonic build id; `make dist` increments it (CI may set `BUILD` from
  `GITHUB_RUN_NUMBER`)

```bash
make bump          # 0.1.0 -> 0.1.1 and sync Cargo.toml
make dist          # BUILD++, release build, cargo package dry-run, CLI checks
cargo run -p monoloop -- --version
cargo run -p monoloop -- --copyright
```

## crates.io

Publish leaf crates first (`monoloop-contracts`, then connector/interpreter, then
`monoloop-loop`, profile crates, `monoloop-testkit`, finally the `monoloop` façade).

```bash
make package       # dry-run for the publish order
# then, when ready (requires crates.io token):
# cargo publish -p monoloop-contracts
# ...
# cargo publish -p monoloop
```

