#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import argparse

from treasury_sector_maturity.benchmark_sets import (
    build_estimation_benchmark_blocks,
    normalized_family_list,
    parse_curve_file_overrides,
)
from treasury_sector_maturity.estimation import EstimationSettings, estimate_effective_maturity_panel
from treasury_sector_maturity.utils import load_yaml, read_table, write_table


def main() -> None:
    parser = argparse.ArgumentParser(description="Estimate effective maturity metrics from sector revaluation returns.")
    parser.add_argument("--z1-panel", required=True)
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
    parser.add_argument("--series-config", default="configs/h15_series.yaml")
    parser.add_argument("--model-config", default="configs/model_defaults.yaml")
    parser.add_argument("--sector-defs", default="configs/sector_definitions.yaml")
    parser.add_argument("--interval-calibration-file", default="data/processed/fed_interval_calibration.csv")
    parser.add_argument("--foreign-nowcast-file", default="data/processed/foreign_nowcast_panel.csv")
    parser.add_argument("--bank-constraint-file", default="data/processed/bank_constraint_panel.csv")
    parser.add_argument("--out", default="data/processed/sector_effective_maturity.csv")
    args = parser.parse_args()

    sector_panel = read_table(args.z1_panel)

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
    curve_files = parse_curve_file_overrides(args.curve_file)
    if args.h15_file and "nominal_treasury_constant_maturity" not in curve_files:
        curve_files["nominal_treasury_constant_maturity"] = Path(args.h15_file)
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

    interval_calibration = None
    calibration_path = Path(args.interval_calibration_file)
    if calibration_path.exists():
        interval_calibration = read_table(calibration_path)

    foreign_nowcast = None
    foreign_nowcast_path = Path(args.foreign_nowcast_file)
    if foreign_nowcast_path.exists():
        foreign_nowcast = read_table(foreign_nowcast_path)

    bank_constraints = None
    bank_constraint_path = Path(args.bank_constraint_file)
    if bank_constraint_path.exists():
        bank_constraints = read_table(bank_constraint_path)

    result = estimate_effective_maturity_panel(
        sector_panel,
        benchmark,
        factor_returns=factor_benchmark,
        settings=settings,
        interval_calibration=interval_calibration,
        interval_settings=interval_cfg,
        foreign_nowcast=foreign_nowcast,
        bank_constraints=bank_constraints,
        sector_config_path=args.sector_defs,
    )
    write_table(result, args.out)

    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
