#!/usr/bin/env python
from __future__ import annotations

import shlex
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import argparse

from treasury_sector_maturity.full_coverage_release import build_full_coverage_release


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build the full-coverage research release bundle and machine-readable outputs "
            "(canonical_sector_maturity.csv, latest_sector_snapshot.csv, "
            "high_confidence_sector_maturity.csv, reconciliation_nodes.csv, fed_exact_overlay.csv, required_sector_inventory.csv, "
            "full_coverage_report.md, run_manifest.json, full_coverage_summary.json)."
        )
    )
    parser.add_argument("--out-dir", default="outputs/full_coverage_release")
    parser.add_argument("--coverage-scope", default="full", choices=["full"])
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--quarters", type=int, default=None)
    parser.add_argument("--summary-json-out", default=None)
    parser.add_argument("--source-provider", default="fed", choices=["auto", "fed", "fred"])
    parser.add_argument("--z1-file", default=None)
    parser.add_argument("--h15-file", default=None)
    parser.add_argument(
        "--curve-file",
        action="append",
        default=None,
        help="Optional CURVE_KEY=PATH override. Use this for non-nominal families such as tips_real_yield_constant_maturity.",
    )
    parser.add_argument("--soma-file", default=None)
    parser.add_argument("--foreign-shl-file", default=None)
    parser.add_argument("--foreign-slt-file", default=None)
    parser.add_argument("--bank-constraint-file", default=None)
    parser.add_argument("--ffiec-file", default=None)
    parser.add_argument("--ffiec002-file", default=None)
    parser.add_argument("--ncua-file", default=None)
    parser.add_argument("--bank-supplement-file", default=None)
    parser.add_argument("--series-catalog", default="configs/z1_series_catalog_full.yaml")
    parser.add_argument("--sector-defs", default="configs/sector_definitions_full.yaml")
    parser.add_argument("--model-config", default="configs/model_defaults.yaml")
    parser.add_argument("--series-config", default="configs/h15_series.yaml")
    parser.add_argument("--bank-constraints-config", default="configs/bank_constraints.yaml")
    parser.add_argument("--release-config", default="configs/full_coverage_release.yaml")
    parser.add_argument(
        "--supplement-missing-z1-levels-from-fred",
        action="store_true",
        help="Use configured level fred_ids to fill required-sector level series that are missing from the supplied Z.1 source.",
    )
    args = parser.parse_args()

    artifacts = build_full_coverage_release(
        out_dir=args.out_dir,
        source_provider=args.source_provider,
        coverage_scope=args.coverage_scope,
        end_date=args.end_date,
        quarters=args.quarters,
        summary_json_out=args.summary_json_out,
        command=shlex.join(sys.argv),
        z1_file=args.z1_file,
        h15_file=args.h15_file,
        curve_file=args.curve_file,
        soma_file=args.soma_file,
        foreign_shl_file=args.foreign_shl_file,
        foreign_slt_file=args.foreign_slt_file,
        bank_constraint_file=args.bank_constraint_file,
        ffiec_file=args.ffiec_file,
        ffiec002_file=args.ffiec002_file,
        ncua_file=args.ncua_file,
        bank_supplement_file=args.bank_supplement_file,
        series_catalog=args.series_catalog,
        sector_defs=args.sector_defs,
        model_config=args.model_config,
        series_config=args.series_config,
        bank_constraints_config=args.bank_constraints_config,
        release_config=args.release_config,
        supplement_missing_z1_levels_from_fred=args.supplement_missing_z1_levels_from_fred,
    )

    print(f"Wrote {artifacts.canonical_sector_maturity_path}")
    print(f"Wrote {artifacts.latest_sector_snapshot_path}")
    print(f"Wrote {artifacts.high_confidence_sector_maturity_path}")
    print(f"Wrote {artifacts.reconciliation_nodes_path}")
    print(f"Wrote {artifacts.fed_exact_overlay_path}")
    print(f"Wrote {artifacts.required_sector_inventory_path}")
    print(f"Wrote {artifacts.report_path}")
    print(f"Wrote {artifacts.manifest_path}")
    print(f"Wrote {artifacts.summary_json_path}")


if __name__ == "__main__":
    raise SystemExit(main())
