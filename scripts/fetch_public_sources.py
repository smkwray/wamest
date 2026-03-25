#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import argparse

import pandas as pd

from treasury_sector_maturity.providers import (
    FetchArtifacts,
    fetch_ffiec_call_reports,
    fetch_ffiec002_call_reports,
    fetch_h15_curves,
    fetch_ncua_call_reports,
    fetch_soma_holdings,
    fetch_z1_series,
)


def _parse_datasets(raw: str) -> list[str]:
    values = [value.strip().lower() for value in raw.split(",") if value.strip()]
    if not values:
        raise ValueError("At least one dataset must be requested.")
    return values


def _parse_dates(raw: str) -> list[pd.Timestamp]:
    return [pd.Timestamp(value).normalize() for value in raw.split(",") if value.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch real public source datasets and normalize them for the pipeline.")
    parser.add_argument(
        "--datasets",
        default="z1,h15",
        help="Comma-separated list of datasets: z1,h15,soma,ffiec,ffiec002,ncua",
    )
    parser.add_argument("--source-provider", default="auto", choices=["auto", "fed", "fred", "ffiec", "ncua"])
    parser.add_argument("--raw-dir", default=None, help="Optional raw artifact directory override.")
    parser.add_argument("--normalized-dir", default="data/external/normalized")
    parser.add_argument("--catalog", default="configs/z1_series_catalog.yaml")
    parser.add_argument("--series-config", default="configs/h15_series.yaml")
    parser.add_argument("--ffiec-config", default="configs/ffiec_call_report.yaml")
    parser.add_argument("--ffiec002-config", default="configs/ffiec002_call_report.yaml")
    parser.add_argument("--ncua-config", default="configs/ncua_call_report.yaml")
    parser.add_argument(
        "--ffiec-report-date",
        default=None,
        help="Optional FFIEC report date such as 2025-12-31. Defaults to the latest available single-period bulk file.",
    )
    parser.add_argument(
        "--ncua-report-date",
        default=None,
        help="Optional NCUA report date such as 2025-12-31. Defaults to the latest available quarterly ZIP.",
    )
    parser.add_argument(
        "--ffiec002-report-date",
        default=None,
        help="Optional FFIEC 002 report date such as 2025-12-31. Defaults to the provided date or current quarter-end.",
    )
    parser.add_argument(
        "--soma-dates",
        default=None,
        help="Comma-separated SOMA as-of dates. Required when requesting soma directly from this script.",
    )
    args = parser.parse_args()

    datasets = _parse_datasets(args.datasets)
    normalized_dir = Path(args.normalized_dir)
    artifacts: list[FetchArtifacts] = []

    if "z1" in datasets:
        artifacts.append(
            fetch_z1_series(
                provider=args.source_provider,
                series_catalog_path=args.catalog,
                raw_dir=args.raw_dir,
                normalized_out=normalized_dir / f"z1_series_{args.source_provider}.csv",
            )
        )

    if "h15" in datasets:
        artifacts.append(
            fetch_h15_curves(
                provider=args.source_provider,
                series_config_path=args.series_config,
                raw_dir=args.raw_dir,
                normalized_out=normalized_dir / f"h15_curves_{args.source_provider}.csv",
            )
        )

    if "soma" in datasets:
        if not args.soma_dates:
            raise SystemExit("--soma-dates is required when fetching soma with this script.")
        artifacts.append(
            fetch_soma_holdings(
                as_of_dates=_parse_dates(args.soma_dates),
                raw_dir=args.raw_dir,
                normalized_out=normalized_dir / "soma_holdings_fed.csv",
            )
        )

    if "ffiec" in datasets:
        artifacts.append(
            fetch_ffiec_call_reports(
                report_date=args.ffiec_report_date,
                provider=args.source_provider,
                config_path=args.ffiec_config,
                raw_dir=args.raw_dir,
                normalized_out=normalized_dir / "ffiec_call_reports_ffiec.csv",
            )
        )

    if "ncua" in datasets:
        artifacts.append(
            fetch_ncua_call_reports(
                report_date=args.ncua_report_date,
                provider=args.source_provider,
                config_path=args.ncua_config,
                raw_dir=args.raw_dir,
                normalized_out=normalized_dir / "ncua_call_reports_ncua.csv",
            )
        )

    if "ffiec002" in datasets:
        artifacts.append(
            fetch_ffiec002_call_reports(
                report_date=args.ffiec002_report_date,
                provider=args.source_provider,
                config_path=args.ffiec002_config,
                raw_dir=args.raw_dir,
                normalized_out=normalized_dir / "ffiec002_call_reports_ffiec.csv",
            )
        )

    for artifact in artifacts:
        raw_text = str(artifact.raw_path) if artifact.raw_path is not None else "(multiple raw files)"
        print(f"[{artifact.dataset}] provider={artifact.provider} raw={raw_text} normalized={artifact.normalized_path}")


if __name__ == "__main__":
    main()
