//! Architecture guards for monoloop-contracts and product crates.

use std::fs;
use std::path::PathBuf;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .parent()
        .expect("workspace")
        .to_path_buf()
}

fn cargo_toml(crate_name: &str) -> String {
    let path = workspace_root()
        .join("crates")
        .join(crate_name)
        .join("Cargo.toml");
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

fn asserts_no_testkit_dep(crate_name: &str) {
    let toml = cargo_toml(crate_name);
    assert!(
        !toml.contains("monoloop-testkit"),
        "{crate_name} must not depend on monoloop-testkit"
    );
}

#[test]
fn contracts_do_not_depend_on_product_or_testkit() {
    let toml = cargo_toml("monoloop-contracts");
    for forbidden in [
        "monoloop-connector",
        "monoloop-interpreter",
        "monoloop-loop",
        "monoloop-testkit",
        "monoloop-connector-grok",
        "monoloop-connector-cursor",
    ] {
        assert!(
            !toml.contains(forbidden),
            "monoloop-contracts must not depend on {forbidden}"
        );
    }
}

#[test]
fn product_crates_do_not_depend_on_testkit() {
    for crate_name in [
        "monoloop-contracts",
        "monoloop-connector",
        "monoloop-connector-grok",
        "monoloop-connector-cursor",
        "monoloop-connector-agy",
        "monoloop-connector-codex",
        "monoloop-connector-zai",
        "monoloop-connector-claude",
        "monoloop-interpreter",
        "monoloop-loop",
    ] {
        asserts_no_testkit_dep(crate_name);
    }
}
