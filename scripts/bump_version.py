#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (C) Alexander R. Croft
"""Bump VERSION patch component (X.Y.Z -> X.Y.(Z+1))."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERSION_FILE = ROOT / "VERSION"


def main() -> int:
    raw = VERSION_FILE.read_text().strip()
    m = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", raw)
    if not m:
        print(f"invalid VERSION: {raw!r}", file=sys.stderr)
        return 1
    major, minor, patch = map(int, m.groups())
    new = f"{major}.{minor}.{patch + 1}"
    VERSION_FILE.write_text(new + "\n")
    print(f"{raw} -> {new}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
