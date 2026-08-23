# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (C) Alexander R. Croft
#
# Versioning per LICENSING.md:
#   VERSION  — semver (e.g. 0.1.0), bumped by `make bump` or hand-edited
#   BUILD    — monotonic build number; `make dist` increments (or CI sets it)

.PHONY: help bump dist sync-version check test package publish-dry-run fmt clippy doc gates cli-check

VERSION_FILE := VERSION
BUILD_FILE := BUILD
VERSION := $(shell cat $(VERSION_FILE) 2>/dev/null || echo 0.0.0)
BUILD := $(shell cat $(BUILD_FILE) 2>/dev/null || echo 0)

help:
	@echo "Monoloop $(VERSION)+build-$(BUILD)"
	@echo ""
	@echo "Targets:"
	@echo "  make bump            Bump VERSION patch (X.Y.Z -> X.Y.(Z+1)); sync Cargo.toml"
	@echo "  make dist            Increment BUILD (or use CI run number), sync, release build + package dry-run"
	@echo "  make sync-version    Copy VERSION into workspace Cargo.toml"
	@echo "  make check           cargo check --workspace"
	@echo "  make test            cargo test --workspace --all-targets --all-features"
	@echo "  make package         cargo package dry-run for publishable crates (ordered)"
	@echo "  make publish-dry-run Alias of package"
	@echo "  make cli-check       Verify monoloop --help/--version/--copyright"
	@echo "  make fmt / clippy / doc / gates"
	@echo "  make gates           §23: fmt + clippy -D + test --all-targets + rustdoc -D"
	@echo ""
	@echo "CI: set CI=1 and optionally GITHUB_RUN_NUMBER so dist records the CI build id in BUILD."
	@echo "Release blocking: run \`make gates\` (or the four §23 commands) before dist/publish."

sync-version:
	@test -f $(VERSION_FILE) || (echo "missing $(VERSION_FILE)" && exit 1)
	@python3 scripts/sync_version.py "$(VERSION)"
	@# Façade embeds BUILD for crates.io; keep crate copy aligned with workspace.
	@cp "$(BUILD_FILE)" crates/monoloop/BUILD

bump:
	@python3 scripts/bump_version.py
	@$(MAKE) sync-version
	@echo "VERSION is now $$(cat $(VERSION_FILE))"

dist:
	@# Record build number: local increment, or CI run number when provided.
ifeq ($(CI),1)
ifdef GITHUB_RUN_NUMBER
	@echo "$(GITHUB_RUN_NUMBER)" > $(BUILD_FILE)
	@echo "BUILD set from GITHUB_RUN_NUMBER=$(GITHUB_RUN_NUMBER)"
else
	@n=$$(($$(cat $(BUILD_FILE)) + 1)); echo $$n > $(BUILD_FILE); echo "BUILD -> $$n (CI without GITHUB_RUN_NUMBER)"
endif
else
	@n=$$(($$(cat $(BUILD_FILE)) + 1)); echo $$n > $(BUILD_FILE); echo "BUILD -> $$n"
endif
	@cp "$(BUILD_FILE)" crates/monoloop/BUILD
	@$(MAKE) sync-version
	@$(MAKE) gates
	cargo build --workspace --release
	@$(MAKE) package
	@$(MAKE) cli-check
	@echo "dist complete: $$(cat $(VERSION_FILE))+build-$$(cat $(BUILD_FILE))"

check:
	cargo check --workspace --all-targets --all-features

test:
	cargo test --workspace --all-targets --all-features

fmt:
	cargo fmt --all -- --check

clippy:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps

# TRANSACTION_RUNTIME_V2_SPEC.md §23 core commands (release-blocking).
gates: fmt clippy test doc
	@echo "§23 gates passed (fmt / clippy -D / test --all-targets / rustdoc -D)"

# Dry-run package for the leaf crate (always works). Dependents need parents on
# crates.io first — see PUBLISHING.md.
package publish-dry-run:
	@echo "==> cargo package -p monoloop-contracts --allow-dirty"
	cargo package -p monoloop-contracts --allow-dirty --no-verify
	@echo "Note: package other crates after publishing their dependencies (PUBLISHING.md)."

cli-check:
	cargo build -p monoloop --bin monoloop
	@./target/debug/monoloop --help >/dev/null
	@./target/debug/monoloop --version
	@./target/debug/monoloop --copyright
	@./target/debug/monoloop --coopyrigght >/dev/null
