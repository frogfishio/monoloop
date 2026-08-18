# monoloop-testkit

**Test kit only** — Driver, Console, fixtures, live profile examples, HTML/raw dump helpers.

**Not a product component.** Product crates must not depend on this crate.

**License:** AGPL-3.0-or-later. Commercial: <https://frogfish.io>

## What this crate is / is not

| Is | Is not |
|---|---|
| Deterministic + live qualification harness | Fourth product component |
| Console renderer / event distributor for review | Something hosts embed in production |
| `examples/live_*` against real agents | Required for `cargo add monoloop` |

## Examples (live)

```bash
cargo run -p monoloop-testkit --example live_grok_ask -- --preset crud
# also: live_cursor_*, live_codex_*, live_agy_*, live_zai_*, live_claude_*
# review: write_html_review, replay_raw_html
```

For **library assembly without live agents**, use:

```bash
cargo run -p monoloop --example fake_echo
```

Normative: `doc/TEST_KIT.md`, `doc/CONSOLE_RENDERER.md`.
