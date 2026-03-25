#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import argparse

from treasury_sector_maturity.utils import read_table


def main() -> None:
    parser = argparse.ArgumentParser(description="Run simple sanity checks on a built sector panel.")
    parser.add_argument("--z1-panel", dest="z1_panel", required=True)
    args = parser.parse_args()

    panel = read_table(args.z1_panel)

    if "identity_error" in panel.columns:
        summary = panel.groupby("sector_key")["identity_error"].agg(["count", "mean", "max", "min"])
        print(summary.to_string())
    else:
        print("identity_error column not found")

    if {"sector_key", "date", "level"}.issubset(panel.columns):
        duplicate_count = int(panel.duplicated(["sector_key", "date"]).sum())
        print(f"duplicates on sector/date: {duplicate_count}")
    else:
        print("Missing columns needed for duplicate check.")


if __name__ == "__main__":
    main()
