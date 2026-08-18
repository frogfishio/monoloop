# Publishing Monoloop to crates.io

Copyright (C) Alexander R. Croft  
SPDX-License-Identifier: AGPL-3.0-or-later

## Prerequisites

1. A crates.io account and API token (`cargo login`).
2. `VERSION` / workspace version aligned (`make sync-version`).
3. Clean `cargo test --workspace --all-targets --all-features` and clippy/fmt gates.
4. Understand AGPL-3.0-or-later obligations; commercial terms are separate
   (`LICENSE-COMMERCIAL.md`, <https://frogfish.io>).

## Publish order

Internal crates use `{ path, version }` so the first upload of each crate must
happen **before** dependents can resolve that version from crates.io.

```text
1. monoloop-contracts
2. monoloop-connector
   monoloop-interpreter
3. monoloop-loop
4. monoloop-connector-grok
   monoloop-connector-cursor
   monoloop-connector-codex
   monoloop-connector-agy
   monoloop-connector-zai
   monoloop-connector-claude
5. monoloop-testkit
6. monoloop          # façade + `monoloop` CLI binary
```

Example:

```bash
make sync-version
cargo publish -p monoloop-contracts
cargo publish -p monoloop-connector
cargo publish -p monoloop-interpreter
cargo publish -p monoloop-loop
# … profile crates …
cargo publish -p monoloop-testkit
cargo publish -p monoloop
```

`cargo package -p <dependent> --dry-run` **before** its dependencies exist on
crates.io will fail resolution; that is expected. Dry-run the leaf
(`monoloop-contracts`) any time; dry-run the rest after parents are published
or use `--no-verify` only after registry deps resolve.

## Local release checklist

```bash
make bump          # optional semver bump
make dist          # BUILD++, sync VERSION + copy BUILD into crates/monoloop/, release build, package dry-run, CLI checks
make test
```

The façade embeds `crates/monoloop/BUILD` (kept in sync from the workspace `BUILD`
by `make sync-version` / `make dist`) so published crates.io sources compile
without the workspace root.

## CI build numbers

GitHub Actions writes `BUILD` from `GITHUB_RUN_NUMBER` and uploads it as an
artifact (see `.github/workflows/ci.yml`). Local `make dist` increments `BUILD`
by one.

## Dev-dependency cycle note

`monoloop-loop` must **not** list profile crates as Cargo dependencies (even
dev-dependencies): profiles depend on `monoloop-loop`, and crates.io packaging
resolves all declared deps from the registry. WP-11 profile binding qualification
lives in `monoloop-testkit` (`tests/profile_bindings.rs`).

