# monoloop-interpreter

**Component 02 — Interpreter:** ordered raw Connector bytes + dialect binding →
**complete** provider-neutral `CanonicalUnitEvent`s only.

Never emits tokens, text deltas, or partial tool JSON as canonical content.

**License:** AGPL-3.0-or-later. Commercial: <https://frogfish.io>

## What this crate is / is not

| Is | Is not |
|---|---|
| Incremental dialect decode + assembly | Tool execution |
| `DefaultInterpreterFactory` + dialect plugins (ACP, OpenAI SSE, …) | Turn completion / “run success” |
| Fragmentation-invariant output | Persistence |

## How an assembler uses it

Put an `InterpreterFactory` on the Channel binding; the transaction runtime
starts interpretations per exchange.

```rust
use monoloop_interpreter::DefaultInterpreterFactory;
use std::sync::Arc;

let interpreter = Arc::new(DefaultInterpreterFactory::new());
// → ChannelBinding.interpreter = interpreter
```

Standalone (bytes in → units out) is for tests; production path is via Loop composition.

Normative: `doc/INTERPRETER.md`.
