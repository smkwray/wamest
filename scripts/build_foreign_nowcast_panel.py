#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import argparse

from treasury_sector_maturity.tic import (
    DEFAULT_SHL_HISTORICAL_URL,
    DEFAULT_SLT_TABLE3_URL,
    build_foreign_monthly_nowcast,
    build_slt_foreign_holder_panel,
    extract_shl_total_foreign_benchmark,
    load_extracted_shl_issue_mix,
    load_shl_historical_treasury_benchmark,
    load_slt_short_long,
    load_slt_table3,
)
from treasury_sector_maturity.utils import write_table


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a monthly foreign Treasury nowcast panel from SHL and SLT inputs.")
    parser.add_argument("--shl-file")
    parser.add_argument("--slt-file")
    parser.add_argument("--source-provider", choices=["manual", "official", "auto"], default="auto")
    parser.add_argument("--shl-url", default=DEFAULT_SHL_HISTORICAL_URL)
    parser.add_argument("--slt-url", default=DEFAULT_SLT_TABLE3_URL)
    parser.add_argument("--out", default="data/processed/foreign_nowcast_panel.csv")
    args = parser.parse_args()

    use_official = args.source_provider == "official" or (
        args.source_provider == "auto" and not args.shl_file and not args.slt_file
    )

    if use_official:
        shl = extract_shl_total_foreign_benchmark(
            load_shl_historical_treasury_benchmark(args.shl_file or args.shl_url)
        )
        slt = build_slt_foreign_holder_panel(load_slt_table3(args.slt_file or args.slt_url))
    else:
        if not args.shl_file:
            raise SystemExit("--shl-file is required for manual mode")
        shl = load_extracted_shl_issue_mix(args.shl_file)
        slt = load_slt_short_long(args.slt_file) if args.slt_file else None

    panel = build_foreign_monthly_nowcast(shl, slt)
    write_table(panel, args.out)

    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
