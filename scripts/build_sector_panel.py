#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import argparse

from treasury_sector_maturity.providers import fetch_z1_series
from treasury_sector_maturity.estimation import attach_revaluation_returns
from treasury_sector_maturity.utils import write_table
from treasury_sector_maturity.z1 import (
    build_sector_panel,
    compute_identity_errors,
    load_series_catalog,
    materialize_series_panel,
    parse_z1_ddp_csv,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a quarterly sector panel from a Z.1 DDP CSV.")
    parser.add_argument("--z1-file", help="Path to a local Z.1 CSV or normalized long file.")
    parser.add_argument("--source-provider", default="auto", choices=["auto", "fed", "fred"])
    parser.add_argument("--catalog", default="configs/z1_series_catalog.yaml")
    parser.add_argument("--sector-defs", default="configs/sector_definitions.yaml")
    parser.add_argument("--series-out", default="data/interim/z1_series_panel.csv")
    parser.add_argument("--out", default="data/interim/z1_sector_panel.csv")
    args = parser.parse_args()

    z1_file = args.z1_file
    if z1_file is None:
        artifact = fetch_z1_series(
            provider=args.source_provider,
            series_catalog_path=args.catalog,
            normalized_out=f"data/external/normalized/z1_series_{args.source_provider}.csv",
        )
        z1_file = str(artifact.normalized_path)

    long_df = parse_z1_ddp_csv(z1_file)
    catalog = load_series_catalog(args.catalog)
    series_panel = materialize_series_panel(long_df, catalog)
    series_panel = compute_identity_errors(series_panel)
    write_table(series_panel, args.series_out)

    sector_panel = build_sector_panel(series_panel, args.sector_defs)
    sector_panel = compute_identity_errors(
        sector_panel.rename(columns={"sector_key": "series_key"}).copy()
    ).rename(columns={"series_key": "sector_key"})
    sector_panel = attach_revaluation_returns(sector_panel, group_col="sector_key")
    write_table(sector_panel, args.out)

    print(f"Wrote {args.series_out}")
    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
