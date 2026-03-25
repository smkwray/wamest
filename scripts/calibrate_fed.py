#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import argparse

import pandas as pd

from treasury_sector_maturity.benchmark_sets import (
    build_estimation_benchmark_blocks,
    normalized_family_list,
    parse_curve_file_overrides,
)
from treasury_sector_maturity.calibration import (
    build_fed_interval_calibration,
    calibrate_fed_revaluation_mapping,
    summarize_interval_calibration,
)
from treasury_sector_maturity.estimation import EstimationSettings, attach_revaluation_returns
from treasury_sector_maturity.h15 import load_h15_curve_file
from treasury_sector_maturity.providers import fetch_h15_curves, fetch_soma_holdings
from treasury_sector_maturity.soma import read_soma_holdings, summarize_soma_quarterly
from treasury_sector_maturity.utils import dump_json, load_yaml, read_table, write_table


def main() -> None:
    parser = argparse.ArgumentParser(description="Calibrate Fed Z.1 revaluation mapping against exact SOMA metrics.")
    parser.add_argument("--z1-panel", required=True)
    parser.add_argument("--soma-file")
    parser.add_argument("--h15-file")
    parser.add_argument(
        "--curve-file",
        action="append",
        default=None,
        help="Optional CURVE_KEY=PATH override. Use this for non-nominal families such as tips_real_yield_constant_maturity.",
    )
    parser.add_argument(
        "--benchmark-family",
        action="append",
        default=None,
        help="Optional repeated holdings benchmark family key. Defaults to the model config.",
    )
    parser.add_argument(
        "--factor-family",
        action="append",
        default=None,
        help="Optional repeated factor benchmark family key such as key_rate_buckets_from_nominal.",
    )
    parser.add_argument("--source-provider", default="auto", choices=["auto", "fed", "fred"])
    parser.add_argument("--fed-sector", default="fed")
    parser.add_argument("--series-config", default="configs/h15_series.yaml")
    parser.add_argument("--model-config", default="configs/model_defaults.yaml")
    parser.add_argument("--soma-start", default=None, help="Optional lower bound for fetched SOMA as-of dates.")
    parser.add_argument("--soma-end", default=None, help="Optional upper bound for fetched SOMA as-of dates.")
    parser.add_argument(
        "--soma-max-quarters",
        type=int,
        default=40,
        help="If auto-fetching SOMA, keep at most this many most-recent quarter dates after other filters.",
    )
    parser.add_argument("--exact-out", default="data/processed/fed_exact_metrics.csv")
    parser.add_argument("--interval-calibration-out", default="data/processed/fed_interval_calibration.csv")
    parser.add_argument("--summary-out", default="outputs/fed_calibration_summary.json")
    args = parser.parse_args()

    z1_panel = read_table(args.z1_panel)
    fed_panel = z1_panel[z1_panel["sector_key"] == args.fed_sector].copy()
    fed_panel = attach_revaluation_returns(fed_panel, group_col="sector_key")

    curve_files = parse_curve_file_overrides(args.curve_file)
    if args.h15_file and "nominal_treasury_constant_maturity" not in curve_files:
        curve_files["nominal_treasury_constant_maturity"] = Path(args.h15_file)

    h15_file = curve_files.get("nominal_treasury_constant_maturity")
    if h15_file is None:
        h15_artifact = fetch_h15_curves(
            provider=args.source_provider,
            series_config_path=args.series_config,
            curve_key="nominal_treasury_constant_maturity",
            normalized_out=f"data/external/normalized/h15_curves_{args.source_provider}.csv",
        )
        h15_file = str(h15_artifact.normalized_path)

    soma_file = args.soma_file
    if soma_file is None:
        requested_dates = sorted(pd.to_datetime(fed_panel["date"]).dropna().unique())
        if args.soma_start:
            requested_dates = [date for date in requested_dates if pd.Timestamp(date) >= pd.Timestamp(args.soma_start)]
        if args.soma_end:
            requested_dates = [date for date in requested_dates if pd.Timestamp(date) <= pd.Timestamp(args.soma_end)]
        if args.soma_max_quarters is not None and len(requested_dates) > args.soma_max_quarters:
            requested_dates = requested_dates[-args.soma_max_quarters :]
        if not requested_dates:
            raise SystemExit("No SOMA dates remain after applying the auto-fetch filters.")
        soma_artifact = fetch_soma_holdings(
            as_of_dates=requested_dates,
            normalized_out="data/external/normalized/soma_holdings_fed.csv",
        )
        soma_file = str(soma_artifact.normalized_path)

    curves = load_h15_curve_file(
        h15_file,
        series_config_path=args.series_config,
        curve_key="nominal_treasury_constant_maturity",
    )
    soma = read_soma_holdings(soma_file)
    exact_metrics = summarize_soma_quarterly(soma, curve_df=curves)
    write_table(exact_metrics, args.exact_out)

    model_cfg = load_yaml(args.model_config)
    est_cfg = model_cfg.get("estimation", {})
    interval_cfg = model_cfg.get("interval_calibration", {})
    holdings_families = normalized_family_list(
        args.benchmark_family,
        default=list(est_cfg.get("holdings_benchmark_families") or ["nominal_treasury_constant_maturity"]),
    )
    factor_families = normalized_family_list(
        args.factor_family,
        default=list(est_cfg.get("factor_benchmark_families") or []),
    )
    benchmark, factor_benchmark = build_estimation_benchmark_blocks(
        series_config_path=args.series_config,
        provider=args.source_provider,
        holdings_families=holdings_families,
        factor_families=factor_families,
        curve_file_overrides=curve_files,
    )
    settings = EstimationSettings(
        rolling_window_quarters=int(est_cfg.get("rolling_window_quarters", 12)),
        smoothness_penalty=float(est_cfg.get("smoothness_penalty", 10.0)),
        turnover_penalty=float(est_cfg.get("turnover_penalty", 2.0)),
        ridge_penalty=float(est_cfg.get("ridge_penalty", 0.01)),
        bill_share_penalty=float(est_cfg.get("bill_share_penalty", 0.0)),
        factor_ridge_penalty=float(est_cfg.get("factor_ridge_penalty", 0.1)),
        factor_turnover_penalty=float(est_cfg.get("factor_turnover_penalty", 0.0)),
    )

    summary = calibrate_fed_revaluation_mapping(
        fed_panel,
        exact_metrics,
        benchmark,
        factor_returns=factor_benchmark,
        smoothness_penalty=settings.smoothness_penalty,
        ridge_penalty=settings.ridge_penalty,
        factor_ridge_penalty=settings.factor_ridge_penalty,
    )
    interval_calibration = build_fed_interval_calibration(
        fed_panel,
        exact_metrics,
        benchmark,
        factor_returns=factor_benchmark,
        settings=settings,
    )
    write_table(interval_calibration, args.interval_calibration_out)
    summary["interval_calibration"] = summarize_interval_calibration(interval_calibration, settings=interval_cfg)
    dump_json(summary, args.summary_out)

    print(f"Wrote {args.exact_out}")
    print(f"Wrote {args.interval_calibration_out}")
    print(f"Wrote {args.summary_out}")


if __name__ == "__main__":
    main()
