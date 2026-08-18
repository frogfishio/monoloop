//! Architecture import/dependency gates for monoloop product crates.
//!
//! WP-12: prove dependency direction and three-component boundaries from
//! Cargo.toml graphs (no product → testkit; contracts stays leaf).

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

/// Extract dependency package names listed under a Cargo.toml section header.
fn deps_under_section(toml: &str, section: &str) -> Vec<String> {
    let mut in_section = false;
    let mut deps = Vec::new();
    for line in toml.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_section = trimmed == section
                || trimmed.starts_with(&format!("{section}."))
                || (section == "[dependencies]"
                    && (trimmed == "[dependencies]" || trimmed.starts_with("[dependencies.")));
            if section == "[dev-dependencies]" {
                in_section =
                    trimmed == "[dev-dependencies]" || trimmed.starts_with("[dev-dependencies.");
            }
            if section == "[build-dependencies]" {
                in_section = trimmed == "[build-dependencies]"
                    || trimmed.starts_with("[build-dependencies.");
            }
            continue;
        }
        if !in_section || trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if let Some((name, _)) = trimmed.split_once('=') {
            let name = name.trim().trim_matches('"');
            if !name.is_empty() {
                deps.push(name.to_string());
            }
        }
    }
    deps
}

fn production_deps(crate_name: &str) -> Vec<String> {
    deps_under_section(&cargo_toml(crate_name), "[dependencies]")
}

fn all_deps_including_dev(crate_name: &str) -> Vec<String> {
    let toml = cargo_toml(crate_name);
    let mut deps = deps_under_section(&toml, "[dependencies]");
    deps.extend(deps_under_section(&toml, "[dev-dependencies]"));
    deps.extend(deps_under_section(&toml, "[build-dependencies]"));
    deps
}

const PRODUCT_CRATES: &[&str] = &[
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
    "monoloop", // product façade (not a fourth component; still must not depend on testkit)
];

const PROFILE_CRATES: &[&str] = &[
    "monoloop-connector-grok",
    "monoloop-connector-cursor",
    "monoloop-connector-agy",
    "monoloop-connector-codex",
    "monoloop-connector-zai",
    "monoloop-connector-claude",
];

#[test]
fn contracts_do_not_depend_on_product_or_testkit() {
    let deps = all_deps_including_dev("monoloop-contracts");
    for forbidden in [
        "monoloop",
        "monoloop-connector",
        "monoloop-interpreter",
        "monoloop-loop",
        "monoloop-testkit",
        "monoloop-connector-grok",
        "monoloop-connector-cursor",
        "monoloop-connector-agy",
        "monoloop-connector-codex",
        "monoloop-connector-zai",
        "monoloop-connector-claude",
    ] {
        assert!(
            !deps.iter().any(|d| d == forbidden),
            "monoloop-contracts must not depend on {forbidden} (found in {deps:?})"
        );
    }
}

#[test]
fn product_crates_do_not_depend_on_testkit() {
    for crate_name in PRODUCT_CRATES {
        let deps = all_deps_including_dev(crate_name);
        assert!(
            !deps.iter().any(|d| d == "monoloop-testkit"),
            "{crate_name} must not depend on monoloop-testkit (including dev/build); deps={deps:?}"
        );
    }
}

#[test]
fn product_crates_do_not_depend_on_host_ui_or_tauri() {
    for crate_name in PRODUCT_CRATES {
        let deps = all_deps_including_dev(crate_name);
        for forbidden in ["tauri", "dioxus", "egui", "iced", "axum-template"] {
            assert!(
                !deps.iter().any(|d| d == forbidden),
                "{crate_name} must not depend on host UI crate {forbidden}"
            );
        }
    }
}

#[test]
fn connector_does_not_depend_on_interpreter_or_loop() {
    let deps = production_deps("monoloop-connector");
    for forbidden in ["monoloop-interpreter", "monoloop-loop", "monoloop-testkit"] {
        assert!(
            !deps.iter().any(|d| d == forbidden),
            "monoloop-connector production deps must not include {forbidden}; deps={deps:?}"
        );
    }
}

#[test]
fn interpreter_does_not_depend_on_connector_or_loop() {
    let deps = production_deps("monoloop-interpreter");
    for forbidden in [
        "monoloop-connector",
        "monoloop-loop",
        "monoloop-testkit",
        "monoloop-connector-grok",
    ] {
        assert!(
            !deps.iter().any(|d| d == forbidden),
            "monoloop-interpreter production deps must not include {forbidden}; deps={deps:?}"
        );
    }
}

#[test]
fn loop_production_does_not_depend_on_profile_or_testkit() {
    let deps = production_deps("monoloop-loop");
    for forbidden in PROFILE_CRATES.iter().chain(["monoloop-testkit"].iter()) {
        assert!(
            !deps.iter().any(|d| d == *forbidden),
            "monoloop-loop production deps must not include {forbidden}; deps={deps:?}"
        );
    }
    // Loop may compose connector + interpreter. Profiles must not appear even as
    // dev-deps: crates.io packaging resolves all declared deps from the registry,
    // and profiles already depend on monoloop-loop (publish cycle).
    assert!(
        deps.iter().any(|d| d == "monoloop-contracts"),
        "loop must depend on contracts"
    );
}

#[test]
fn loop_dev_deps_do_not_include_profiles() {
    let deps = all_deps_including_dev("monoloop-loop");
    for forbidden in PROFILE_CRATES {
        assert!(
            !deps.iter().any(|d| d == *forbidden),
            "monoloop-loop must not depend on {forbidden} (incl. dev); deps={deps:?}"
        );
    }
}

#[test]
fn profile_crates_depend_on_connector_not_testkit() {
    for crate_name in PROFILE_CRATES {
        let deps = all_deps_including_dev(crate_name);
        assert!(
            !deps.iter().any(|d| d == "monoloop-testkit"),
            "{crate_name} must not depend on testkit"
        );
        let prod = production_deps(crate_name);
        assert!(
            prod.iter().any(|d| d == "monoloop-contracts")
                || prod.iter().any(|d| d == "monoloop-connector"),
            "{crate_name} should depend on contracts and/or connector; prod={prod:?}"
        );
    }
}

#[test]
fn testkit_may_depend_on_product_but_not_reverse() {
    let testkit = production_deps("monoloop-testkit");
    assert!(
        testkit.iter().any(|d| d == "monoloop-contracts"),
        "testkit should compose contracts"
    );
    // Reverse already enforced by product_crates_do_not_depend_on_testkit.
    let _ = testkit;
}

#[test]
fn facade_reexports_three_components_without_testkit() {
    let prod = production_deps("monoloop");
    for required in [
        "monoloop-contracts",
        "monoloop-connector",
        "monoloop-interpreter",
        "monoloop-loop",
    ] {
        assert!(
            prod.iter().any(|d| d == required),
            "façade must depend on {required}; prod={prod:?}"
        );
    }
    let all = all_deps_including_dev("monoloop");
    assert!(
        !all.iter().any(|d| d == "monoloop-testkit"),
        "façade must not depend on monoloop-testkit; deps={all:?}"
    );
}
