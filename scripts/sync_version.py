#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (C) Alexander R. Croft
"""Write VERSION into workspace Cargo.toml and workspace.dependencies crate versions."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    version = sys.argv[1] if len(sys.argv) > 1 else (ROOT / "VERSION").read_text().strip()
    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        print(f"invalid semver: {version!r}", file=sys.stderr)
        return 1

    cargo = ROOT / "Cargo.toml"
    text = cargo.read_text()
    text2, n = re.subn(
        r'(?m)^version = "[^"]+"',
        f'version = "{version}"',
        text,
        count=1,
    )
    if n != 1:
        print("failed to patch workspace.package version", file=sys.stderr)
        return 1

    # Keep internal workspace.dependency versions aligned.
    text2 = re.sub(
        r'(monoloop-[\w-]+ = \{ path = "[^"]+", version = ")[^"]+(" \})',
        rf"\g<1>{version}\2",
        text2,
    )
    cargo.write_text(text2)
    print(f"synced Cargo.toml to {version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
