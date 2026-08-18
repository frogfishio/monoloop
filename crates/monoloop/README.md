# monoloop

**Product façade** for Monoloop: Connector + Interpreter + transaction-composing Loop.

Hosts should depend on this crate for plug-and-play assembly (`cargo add monoloop`).
Profile connectors (Grok, Cursor, …) are optional Cargo features.

**License:** AGPL-3.0-or-later. Commercial: <https://frogfish.io>

## What this crate is / is not

| Is | Is not |
|---|---|
| Re-exports of the three product components + contracts | A fourth runtime component |
| Thin CLI (`monoloop --version` / `--copyright`) | A chat UI or agent framework |
| Assembly entry for hosts | Test kit (see `monoloop-testkit`) |

## Agent assembly recipe (copy this shape)

Monoloop is assembled, not “started with a prompt.” Wire one Channel, start the
runtime, then `submit` push-based transactions.

```text
1. ChannelBinding {
     connector_factory,   // FakeConnectorFactory OR profile helper
     encoder,             // profile encoder OR TestTextEncoder (smoke only)
     interpreter,         // DefaultInterpreterFactory (or profile-provided)
     capabilities,        // must include option_policy + dialects
     …                    // id, kind, tool_mode, endpoint_ref, limits
   }
2. ChannelRegistry::build(vec![binding])
3. DefaultTransactionRuntime::start(RuntimeBootstrap {
     config: RuntimeConfig { enable_mcp_listener: false, ..Default::default() },
     channels, tools: HostToolRegistry::empty(), executor
   })
4. TransactionRuntime::submit(TransactionRequest {
     channel_id, input: user_text_input(…),
     events: FnEventSink(…), completion: FnCompletionCallback(…),
     invocation_config, tools: vec![], …
   })
5. Await the one completion callback; run-owned state is destroyed at terminal
```

**Hosts with a live profile** should call that profile’s `*_channel_binding(...)`
(see Optional Channel profiles). Do not hand-roll Grok/Cursor/… bindings from the
smoke sample.

**Offline smoke sample** (FakeConnector + `test_raw` dialect, no network, no testkit):

```bash
cargo run -p monoloop --example fake_echo
```

See [`examples/fake_echo.rs`](examples/fake_echo.rs). A component-level twin lives
under `monoloop-loop` (same wiring, direct crate imports).

### `TestTextEncoder` (smoke only)

`TestTextEncoder` is a **loop-owned deterministic encoder** for FakeConnector +
`DialectDescriptor::test_raw()`. It is **not** the production host path and is
**not** the testkit ACP/OpenAI fixture encoder family. Live Channels use the
encoder supplied by `*_channel_binding`.

### Module map (façade paths)

| Use | From |
|---|---|
| `monoloop::contracts::*` | identities, ports, `user_text_input`, sinks |
| `monoloop::connector::FakeConnectorFactory` | deterministic DirectLlm transport (smoke) |
| `monoloop::interpreter::DefaultInterpreterFactory` | bytes → complete canonical units |
| `monoloop::loop_runtime::{ChannelBinding, ChannelRegistry, DefaultTransactionRuntime, HostToolRegistry, RuntimeBootstrap, RuntimeConfig, TestTextEncoder}` | composition |

### Hard rules agents must not break

- **Do not** depend on `monoloop-testkit` from product/host product crates.
- **Do not** invent ambient “current session”; pass explicit IDs on the request.
- **Empty tools** (`HostToolRegistry::empty()`, `tools: vec![]`) is required first
  qualification: complete tool requests become `tool_unavailable` with **zero effects**.
  (`fake_echo` does not emit tool calls; see `monoloop-loop` `tests/empty_loop.rs`.)
- Set `RuntimeConfig.enable_mcp_listener: false` unless you intentionally start the
  MCP shell (`Default` enables it).
- Events and completion are **push** callbacks on `TransactionRequest`, not a pull API.
- One in-flow transaction per `SessionKey`; duplicates are rejected, never queued.
- Canonical completeness ≠ authorization; never treat Interpreter output as safe to execute.

## Optional Channel profiles

```toml
monoloop = { version = "0.1", features = ["grok"] }
# also: cursor, codex, agy, zai, claude
```

Each profile crate exports a `*_channel_binding(...)` helper. Live qualification
examples live in **`monoloop-testkit`**, not here.

## Version / license helpers

```rust
use monoloop::{version_string, copyright_notice};
println!("{}", version_string());   // e.g. 0.1.0+build-42
println!("{}", copyright_notice());
```

Normative specs: repository `doc/README.md`, `doc/MONOLOOP.md`,
`doc/TRANSACTION_RUNTIME_IMPLEMENTATION.md`.
