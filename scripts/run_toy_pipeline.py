#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import argparse
import subprocess


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the pipeline on bundled toy data.")
    parser.add_argument("--root", default=None, help="Optional project root override.")
    args = parser.parse_args()

    root = Path(args.root) if args.root else PROJECT_ROOT
    python_cmd = [sys.executable, "-B"]

    cmds = [
        [
            *python_cmd,
            str(root / "scripts" / "build_sector_panel.py"),
            "--z1-file",
            str(root / "data" / "examples" / "toy_z1_selected_series.csv"),
            "--series-out",
            str(root / "data" / "interim" / "toy_z1_series_panel.csv"),
            "--out",
            str(root / "data" / "interim" / "toy_z1_sector_panel.csv"),
        ],
        [
            *python_cmd,
            str(root / "scripts" / "build_benchmark_returns.py"),
            "--h15-file",
            str(root / "data" / "examples" / "toy_h15_curves.csv"),
            "--out",
            str(root / "data" / "interim" / "toy_benchmark_returns.csv"),
        ],
        [
            *python_cmd,
            str(root / "scripts" / "build_benchmark_returns.py"),
            "--h15-file",
            str(root / "data" / "examples" / "toy_tips_real_yields.csv"),
            "--curve-key",
            "tips_real_yield_constant_maturity",
            "--out",
            str(root / "data" / "interim" / "toy_tips_benchmark_returns.csv"),
        ],
        [
            *python_cmd,
            str(root / "scripts" / "build_benchmark_returns.py"),
            "--h15-file",
            str(root / "data" / "examples" / "toy_h15_curves.csv"),
            "--curve-key",
            "frn_proxy_from_nominal",
            "--out",
            str(root / "data" / "interim" / "toy_frn_benchmark_returns.csv"),
        ],
        [
            *python_cmd,
            str(root / "scripts" / "build_benchmark_returns.py"),
            "--h15-file",
            str(root / "data" / "examples" / "toy_h15_curves.csv"),
            "--curve-key",
            "key_rate_buckets_from_nominal",
            "--out",
            str(root / "data" / "interim" / "toy_key_rate_benchmark_returns.csv"),
        ],
        [
            *python_cmd,
            str(root / "scripts" / "calibrate_fed.py"),
            "--z1-panel",
            str(root / "data" / "interim" / "toy_z1_sector_panel.csv"),
            "--soma-file",
            str(root / "data" / "examples" / "toy_soma_holdings.csv"),
            "--model-config",
            str(root / "configs" / "model_toy_multifamily.yaml"),
            "--curve-file",
            f"nominal_treasury_constant_maturity={root / 'data' / 'examples' / 'toy_h15_curves.csv'}",
            "--curve-file",
            f"tips_real_yield_constant_maturity={root / 'data' / 'examples' / 'toy_tips_real_yields.csv'}",
            "--exact-out",
            str(root / "data" / "processed" / "toy_fed_exact_metrics.csv"),
            "--interval-calibration-out",
            str(root / "data" / "processed" / "toy_fed_interval_calibration.csv"),
            "--summary-out",
            str(root / "outputs" / "toy_fed_calibration_summary.json"),
        ],
        [
            *python_cmd,
            str(root / "scripts" / "build_foreign_anchor_panel.py"),
            "--shl-file",
            str(root / "data" / "examples" / "toy_shl_issue_mix.csv"),
            "--slt-file",
            str(root / "data" / "examples" / "toy_slt_short_long.csv"),
            "--out",
            str(root / "data" / "processed" / "toy_foreign_anchor_panel.csv"),
        ],
        [
            *python_cmd,
            str(root / "scripts" / "build_foreign_nowcast_panel.py"),
            "--shl-file",
            str(root / "data" / "examples" / "toy_shl_issue_mix.csv"),
            "--slt-file",
            str(root / "data" / "examples" / "toy_slt_short_long.csv"),
            "--source-provider",
            "manual",
            "--out",
            str(root / "data" / "processed" / "toy_foreign_nowcast_panel.csv"),
        ],
        [
            *python_cmd,
            str(root / "scripts" / "estimate_effective_maturity.py"),
            "--z1-panel",
            str(root / "data" / "interim" / "toy_z1_sector_panel.csv"),
            "--model-config",
            str(root / "configs" / "model_toy_multifamily.yaml"),
            "--curve-file",
            f"nominal_treasury_constant_maturity={root / 'data' / 'examples' / 'toy_h15_curves.csv'}",
            "--curve-file",
            f"tips_real_yield_constant_maturity={root / 'data' / 'examples' / 'toy_tips_real_yields.csv'}",
            "--interval-calibration-file",
            str(root / "data" / "processed" / "toy_fed_interval_calibration.csv"),
            "--foreign-nowcast-file",
            str(root / "data" / "processed" / "toy_foreign_nowcast_panel.csv"),
            "--bank-constraint-file",
            str(root / "data" / "examples" / "toy_bank_constraint_panel.csv"),
            "--out",
            str(root / "data" / "processed" / "toy_sector_effective_maturity.csv"),
        ],
        [
            *python_cmd,
            str(root / "scripts" / "build_output_metadata_report.py"),
            "--sector-file",
            str(root / "data" / "processed" / "toy_sector_effective_maturity.csv"),
            "--foreign-nowcast-file",
            str(root / "data" / "processed" / "toy_foreign_nowcast_panel.csv"),
            "--fed-summary-file",
            str(root / "outputs" / "toy_fed_calibration_summary.json"),
            "--out",
            str(root / "outputs" / "output_metadata_report.md"),
        ],
    ]

    for cmd in cmds:
        print("Running:", " ".join(cmd))
        subprocess.run(cmd, check=True, cwd=root)

    print("Toy pipeline finished.")


if __name__ == "__main__":
    main()
