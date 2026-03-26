from pathlib import Path

import json
import pandas as pd

from treasury_sector_maturity.public_release import (
    DEFAULT_OPTIONAL_BANK_SECTORS,
    DEFAULT_PUBLIC_PREVIEW_SECTORS,
    PUBLIC_PREVIEW_SCHEMA_VERSION,
    REQUIRED_PUBLIC_PREVIEW_COLUMNS,
    build_public_release_report,
)


ROOT = Path(__file__).resolve().parents[1]

EXPECTED_REPORT_HEADINGS = [
    "# Public Release Preview Report",
    "## Release Summary",
    "## Sector Coverage",
    "## Sector Interpretation",
    "## Evidence Tiers",
    "## Uncertainty and Identified Sets",
    "## Validation",
    "## Provenance",
]


def test_preview_release_contract_remains_unchanged(tmp_path):
    out_dir = tmp_path / "public_preview"
    artifacts = build_public_release_report(
        out_dir=out_dir,
        summary_json_out=out_dir / "public_release_summary.json",
        z1_file=ROOT / "data" / "examples" / "toy_z1_selected_series.csv",
        h15_file=ROOT / "data" / "examples" / "toy_h15_curves.csv",
        soma_file=ROOT / "data" / "examples" / "toy_soma_holdings.csv",
        foreign_shl_file=ROOT / "data" / "examples" / "toy_shl_issue_mix.csv",
        foreign_slt_file=ROOT / "data" / "examples" / "toy_slt_short_long.csv",
        bank_constraint_file=ROOT / "data" / "examples" / "toy_bank_constraint_panel.csv",
        end_date="2025-12-31",
        quarters=4,
    )

    report = artifacts.report_path.read_text(encoding="utf-8")
    for heading in EXPECTED_REPORT_HEADINGS:
        assert heading in report

    sector = pd.read_csv(artifacts.sector_output_path, parse_dates=["date"])
    assert set(REQUIRED_PUBLIC_PREVIEW_COLUMNS).issubset(set(sector.columns))
    assert set(DEFAULT_PUBLIC_PREVIEW_SECTORS).issubset(set(sector["sector_key"]))
    assert not set(DEFAULT_OPTIONAL_BANK_SECTORS) & set(sector["sector_key"])

    manifest = json.loads(artifacts.manifest_path.read_text(encoding="utf-8"))
    assert manifest["schema_version"] == PUBLIC_PREVIEW_SCHEMA_VERSION
    assert manifest["output_paths"]["public_release_report"] == str(artifacts.report_path)
    assert manifest["output_paths"]["sector_effective_maturity"] == str(artifacts.sector_output_path)
    assert manifest["output_paths"]["run_manifest"] == str(artifacts.manifest_path)

