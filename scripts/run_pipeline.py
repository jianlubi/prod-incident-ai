#!/usr/bin/env python3
"""CLI wrapper for prod_incident_ai.run_pipeline."""

from __future__ import annotations

import sys
from pathlib import Path


def _bootstrap() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    src_dir = repo_root / "src"
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))


def main() -> None:
    _bootstrap()
    from prod_incident_ai.run_pipeline import main as package_main

    package_main()


if __name__ == "__main__":
    main()

