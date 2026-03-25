#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import argparse

from treasury_sector_maturity.public_release import build_public_release_report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the canonical public research-preview report and machine-readable outputs."
    )
    parser.add_argument("--end-date", default=None, help="Optional quarter-end cutoff. Defaults to the latest common quarter.")
    parser.add_argument("--quarters", type=int, default=None, help="Optional trailing quarter count to retain.")
    parser.add_argument("--out-dir", default="outputs/public_preview")
    parser.add_argument(
        "--summary-json-out",
        default=None,
        help="Optional machine-readable JSON companion path. When omitted, the canonical public-preview build still writes only the default three top-level artifacts.",
    )
    parser.add_argument("--source-provider", default="fed", choices=["auto", "fed", "fred"])
    parser.add_argument(
        "--include-optional-bank-paths",
        action="store_true",
        help="Include optional FFIEC 002 / supplement-backed bank perimeters when local inputs are provided.",
    )
    parser.add_argument("--z1-file", default=None)
    parser.add_argument("--h15-file", default=None)
    parser.add_argument("--soma-file", default=None)
    parser.add_argument("--foreign-shl-file", default=None)
    parser.add_argument("--foreign-slt-file", default=None)
    parser.add_argument("--bank-constraint-file", default=None)
    parser.add_argument("--ffiec-file", default=None)
    parser.add_argument("--ffiec002-file", default=None)
    parser.add_argument("--ncua-file", default=None)
    parser.add_argument("--bank-supplement-file", default=None)
    parser.add_argument("--series-catalog", default="configs/z1_series_catalog.yaml")
    parser.add_argument("--sector-defs", default="configs/sector_definitions.yaml")
    parser.add_argument("--model-config", default="configs/model_public_preview.yaml")
    parser.add_argument("--series-config", default="configs/h15_series.yaml")
    parser.add_argument("--bank-constraints-config", default="configs/bank_constraints.yaml")
    args = parser.parse_args()

    artifacts = build_public_release_report(
        out_dir=args.out_dir,
        source_provider=args.source_provider,
        end_date=args.end_date,
        quarters=args.quarters,
        include_optional_bank_paths=args.include_optional_bank_paths,
        summary_json_out=args.summary_json_out,
        z1_file=args.z1_file,
        h15_file=args.h15_file,
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
    )

    print(f"Wrote {artifacts.report_path}")
    print(f"Wrote {artifacts.sector_output_path}")
    print(f"Wrote {artifacts.manifest_path}")
    if artifacts.summary_json_path is not None:
        print(f"Wrote {artifacts.summary_json_path}")


if __name__ == "__main__":
    main()
