#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import argparse

from treasury_sector_maturity.reporting import (
    DEFAULT_FED_SUMMARY_FILE,
    DEFAULT_FOREIGN_NOWCAST_FILE,
    DEFAULT_SECTOR_FILE,
    build_output_metadata_report,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a markdown report summarizing sector metadata and foreign support flags.")
    parser.add_argument("--sector-file", default=str(DEFAULT_SECTOR_FILE))
    parser.add_argument("--foreign-nowcast-file", default=str(DEFAULT_FOREIGN_NOWCAST_FILE))
    parser.add_argument("--fed-summary-file", default=str(DEFAULT_FED_SUMMARY_FILE))
    parser.add_argument("--out", default="outputs/output_metadata_report.md")
    args = parser.parse_args()

    out_path = build_output_metadata_report(
        sector_file=args.sector_file,
        foreign_nowcast_file=args.foreign_nowcast_file,
        fed_summary_file=args.fed_summary_file,
        out=args.out,
    )
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
