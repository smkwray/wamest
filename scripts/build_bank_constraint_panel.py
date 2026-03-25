#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import argparse

import pandas as pd

from treasury_sector_maturity.ffiec import build_bank_constraint_panel
from treasury_sector_maturity.utils import read_table, write_table


def _parse_paths(raw: str) -> list[Path]:
    return [Path(value.strip()) for value in raw.split(",") if value.strip()]


def _load_institution_files(paths: list[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in paths:
        if not path.exists():
            raise FileNotFoundError(f"Missing normalized institution file: {path}")
        frame = read_table(path)
        if "date" in frame.columns:
            frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        frames.append(frame)

    if not frames:
        raise ValueError("No normalized institution files were provided.")
    return pd.concat(frames, ignore_index=True, sort=False)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a processed bank-constraint panel from normalized FFIEC / NCUA institution files plus optional uncovered-perimeter supplements."
    )
    parser.add_argument("--ffiec-file", default="data/external/normalized/ffiec_call_reports_ffiec.csv")
    parser.add_argument("--ffiec002-file", default="data/external/normalized/ffiec002_call_reports_ffiec.csv")
    parser.add_argument("--ncua-file", default="data/external/normalized/ncua_call_reports_ncua.csv")
    parser.add_argument(
        "--institution-files",
        default=None,
        help="Optional comma-separated override list of normalized institution files to combine.",
    )
    parser.add_argument(
        "--supplement-file",
        default="data/processed/bank_constraint_supplement.csv",
        help="Optional processed supplement rows for uncovered or composite bank perimeters such as U.S.-affiliated-area banks, reserve-access core, or broad private depositories.",
    )
    parser.add_argument("--constraints-config", default="configs/bank_constraints.yaml")
    parser.add_argument("--out", default="data/processed/bank_constraint_panel.csv")
    args = parser.parse_args()

    if args.institution_files:
        paths = _parse_paths(args.institution_files)
    else:
        paths = []
        ffiec_path = Path(args.ffiec_file)
        if ffiec_path.exists():
            paths.append(ffiec_path)
        ffiec002_path = Path(args.ffiec002_file)
        if ffiec002_path.exists():
            paths.append(ffiec002_path)
        ncua_path = Path(args.ncua_file)
        if ncua_path.exists():
            paths.append(ncua_path)

    supplement = None
    supplement_path = Path(args.supplement_file)
    if supplement_path.exists():
        supplement = read_table(supplement_path)
    elif args.supplement_file != parser.get_default("supplement_file"):
        raise FileNotFoundError(f"Missing bank-constraint supplement file: {supplement_path}")

    if paths:
        institutions = _load_institution_files(paths)
    elif supplement is not None:
        institutions = pd.DataFrame()
    else:
        raise FileNotFoundError("No normalized institution files or bank-constraint supplement file were found.")

    panel = build_bank_constraint_panel(
        institutions,
        constraints_config_path=args.constraints_config,
        supplement_df=supplement,
    )
    write_table(panel, args.out)

    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
