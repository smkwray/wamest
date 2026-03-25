#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import argparse

from treasury_sector_maturity.providers import fetch_h15_curves
from treasury_sector_maturity.h15 import build_benchmark_panel, curve_block_config, load_h15_curve_file
from treasury_sector_maturity.utils import write_table


def main() -> None:
    parser = argparse.ArgumentParser(description="Build benchmark Treasury price-return series from H.15 curve data.")
    parser.add_argument("--h15-file")
    parser.add_argument("--source-provider", default="auto", choices=["auto", "fed", "fred"])
    parser.add_argument("--series-config", default="configs/h15_series.yaml")
    parser.add_argument("--curve-key", default="nominal_treasury_constant_maturity")
    parser.add_argument("--out", default="data/interim/benchmark_returns.csv")
    parser.add_argument("--zero-coupon", action="store_true")
    args = parser.parse_args()

    curve_block = curve_block_config(args.series_config, args.curve_key)
    source_curve_key = str(curve_block.get("source_curve_key", args.curve_key))
    h15_file = args.h15_file
    if h15_file is None:
        artifact = fetch_h15_curves(
            provider=args.source_provider,
            series_config_path=args.series_config,
            curve_key=source_curve_key,
            normalized_out=f"data/external/normalized/h15_curves_{source_curve_key}_{args.source_provider}.csv",
        )
        h15_file = str(artifact.normalized_path)

    curves = load_h15_curve_file(h15_file, series_config_path=args.series_config, curve_key=source_curve_key)
    benchmark = build_benchmark_panel(curves, curve_block=curve_block, zero_coupon=args.zero_coupon)
    write_table(benchmark, args.out)

    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
