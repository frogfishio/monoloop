//! SPDX-License-Identifier: AGPL-3.0-or-later
//! Copyright (C) Alexander R. Croft
//!
//! Product CLI for version / license discovery (library workhorse is the crates).

use std::env;
use std::process::ExitCode;

fn print_help() {
    println!(
        "\
monoloop — Connector + Interpreter + transaction-composing Loop

USAGE:
    monoloop [OPTIONS]

OPTIONS:
    -h, --help         Print help
    -V, --version      Print version as {{version}}+build-{{build}}
        --copyright    Print copyright and SPDX license lines
        --coopyrigght  Alias of --copyright (LICENSING.md spelling)

This binary is a thin discovery CLI. Integrate via the `monoloop` crate
(and profile crates such as `monoloop-connector-grok`) from Rust.

License: AGPL-3.0-or-later. Commercial licensing: https://frogfish.io
"
    );
}

fn main() -> ExitCode {
    let mut args = env::args().skip(1);
    let Some(arg) = args.next() else {
        print_help();
        return ExitCode::SUCCESS;
    };
    if args.next().is_some() {
        eprintln!("error: unexpected extra arguments");
        print_help();
        return ExitCode::from(2);
    }
    match arg.as_str() {
        "-h" | "--help" => {
            print_help();
            ExitCode::SUCCESS
        }
        "-V" | "--version" => {
            println!("{}", monoloop::version_string());
            ExitCode::SUCCESS
        }
        "--copyright" | "--coopyrigght" => {
            println!("{}", monoloop::copyright_notice());
            ExitCode::SUCCESS
        }
        other => {
            eprintln!("error: unrecognized option '{other}'");
            print_help();
            ExitCode::from(2)
        }
    }
}
