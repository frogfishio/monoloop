# monoloop-loop

**Component 03 — The Loop:** transaction admission/composition plus the inner
lossless tool runtime.

Owns `DefaultTransactionRuntime`, Channel registry, outbound encoders, MCP
gateway shell, and linked-tool dispatch. Inner `LoopRuntime` stays dialect-neutral.

Prefer the façade crate `monoloop` unless you need this crate as a direct dependency.

**License:** AGPL-3.0-or-later. Commercial: <https://frogfish.io>

## What this crate is / is not

| Is | Is not |
|---|---|
| `RuntimeBootstrap` → `DefaultTransactionRuntime` | Context/prompt engine |
| `ChannelBinding` / `ChannelRegistry` | Test Driver / Console (→ `monoloop-testkit`) |
| Empty-tool path (`tool_unavailable`, zero effects) | Ambient “current session” |
| Production profile encoders + `TestTextEncoder` smoke helper | Host UI |

## Agent assembly recipe

```text
1. Build ChannelBinding {
     id, kind, tool_mode,
     connector_factory, encoder, interpreter,
     endpoint_ref, credential_ref,
     defaults, capabilities (incl. option_policy + dialects), limits
   }
2. ChannelRegistry::build(vec![binding])  // fails closed on duplicate ids
3. DefaultTransactionRuntime::start(RuntimeBootstrap {
     config: RuntimeConfig { enable_mcp_listener: false, ..Default::default() },
     channels, tools: HostToolRegistry::empty(), executor: Handle::current()
   })
4. TransactionRuntime::submit(TransactionRequest { … })
5. Await completion callback; destroy is automatic at terminal
```

`RuntimeConfig::default()` enables the MCP listener — turn it off for smoke hosts
unless you want that shell.

Minimal DirectLlm smoke sample (FakeConnector, no network):

```bash
cargo run -p monoloop-loop --example fake_echo
# preferred for hosts:
cargo run -p monoloop --example fake_echo
```

[`examples/fake_echo.rs`](examples/fake_echo.rs) is the **component-level** twin of
the façade example (same behaviour, direct imports).

### Pieces for FakeConnector / `test_raw` smoke

| Field | Smoke value |
|---|---|
| `connector_factory` | `Arc::new(FakeConnectorFactory::direct_llm())` |
| `encoder` | `Arc::new(TestTextEncoder)` — loop-owned deterministic encoder for `test_raw` only |
| `interpreter` | `Arc::new(DefaultInterpreterFactory::new())` |
| `capabilities.option_policy` | `OptionPolicy::direct_llm()` |
| `capabilities.input/output_dialect` | `DialectDescriptor::test_raw()` |
| `tool_mode` | `ToolExecutionMode::ModelToolCalls` |

Live profiles replace factory/encoder/dialects via `*_channel_binding` helpers
in `monoloop-connector-*` (also behind `monoloop` features). Do not treat
`TestTextEncoder` as a production dialect encoder; it is not the testkit fixture
encoder family either — it stays in this crate so product smoke/tests need no
testkit dependency.

## Empty tools

With `HostToolRegistry::empty()` and model tool calls, Monoloop must still
complete without external effects (unavailable / rejection paths). That is required
qualification, not a failure of The Loop. See `tests/empty_loop.rs` for the
`tool_unavailable` exercise; `fake_echo` only proves text completion with empty tools.

## Dependency rule

This product crate **must not** depend on `monoloop-testkit`. Hosts that only need
assembly use `monoloop` or this crate + connector/interpreter/contracts.

Normative: `doc/THE_LOOP.md`, `doc/TRANSACTION_RUNTIME_IMPLEMENTATION.md`.
