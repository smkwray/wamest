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

EXPECTED_MANIFEST_KEYS = {
    "schema_version",
    "run_timestamp_utc",
    "command",
    "source_provider_requested",
    "source_provider_used",
    "model_config_path",
    "benchmark_contract",
    "end_date",
    "resolved_common_quarter_date",
    "quarter_count",
    "sector_keys_included",
    "optional_bank_paths_included",
    "optional_bank_sectors_skipped",
    "source_artifact_paths",
    "output_paths",
}

EXPECTED_SUMMARY_KEYS = {
    "schema_version",
    "release_summary",
    "sector_coverage",
    "sector_interpretation",
    "evidence_tiers",
    "uncertainty_identified_sets",
    "validation",
    "provenance",
    "excluded_optional_sectors",
    "foreign_support_snapshot",
    "fed_calibration_snapshot",
    "machine_readable_outputs",
    "source_artifact_paths",
}

EXPECTED_RELEASE_SUMMARY_KEYS = {
    "report_end_date",
    "quarter_count",
    "source_provider_requested",
    "source_provider_used",
    "model_config_path",
    "benchmark_contract",
    "optional_bank_paths_included",
    "command",
    "resolved_common_quarter_date",
}

ALLOWED_LEVEL_TIERS = {"A", "B", "C", "D"}
ALLOWED_MATURITY_TIERS = {"A", "B", "C", "D"}
ALLOWED_CONCEPT_MATCHES = {
    "aggregate",
    "anchor_consistent",
    "direct",
    "partial",
    "proxy",
    "residual",
    "residual_style",
}
EXPECTED_VALIDATION_CHECKS = {
    "required_public_preview_sectors_present",
    "required_public_columns_present",
    "resolved_common_quarter_present_for_all_selected_sectors",
    "optional_bank_sector_policy",
    "published_interval_bounds_are_ordered",
}


def _build_contract_artifacts(tmp_path: Path):
    out_dir = tmp_path / "public_preview"
    return build_public_release_report(
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


def test_public_output_schema_contract(tmp_path):
    artifacts = _build_contract_artifacts(tmp_path)

    report = artifacts.report_path.read_text(encoding="utf-8")
    for heading in EXPECTED_REPORT_HEADINGS:
        assert heading in report

    sector = pd.read_csv(artifacts.sector_output_path, parse_dates=["date"])
    assert set(REQUIRED_PUBLIC_PREVIEW_COLUMNS).issubset(sector.columns)
    assert sector["date"].notna().all()
    assert sector["sector_key"].notna().all()
    assert set(DEFAULT_PUBLIC_PREVIEW_SECTORS).issubset(set(sector["sector_key"]))
    assert not (set(DEFAULT_OPTIONAL_BANK_SECTORS) & set(sector["sector_key"]))

    assert set(sector["level_evidence_tier"].dropna()).issubset(ALLOWED_LEVEL_TIERS)
    assert set(sector["maturity_evidence_tier"].dropna()).issubset(ALLOWED_MATURITY_TIERS)
    assert set(sector["concept_match"].dropna()).issubset(ALLOWED_CONCEPT_MATCHES)

    _assert_bounds_valid(sector, "bill_share", "bill_share_lower", "bill_share_upper")
    _assert_bounds_valid(sector, "short_share_le_1y", "short_share_le_1y_lower", "short_share_le_1y_upper")
    _assert_bounds_valid(
        sector,
        "effective_duration_years",
        "effective_duration_years_lower",
        "effective_duration_years_upper",
    )
    _assert_bounds_valid(
        sector,
        "zero_coupon_equivalent_years",
        "zero_coupon_equivalent_years_lower",
        "zero_coupon_equivalent_years_upper",
    )

    manifest = json.loads(artifacts.manifest_path.read_text(encoding="utf-8"))
    assert manifest["schema_version"] == PUBLIC_PREVIEW_SCHEMA_VERSION
    assert EXPECTED_MANIFEST_KEYS.issubset(manifest)
    assert {
        "public_release_report",
        "sector_effective_maturity",
        "run_manifest",
        "public_release_summary",
    }.issubset(manifest["output_paths"])

    summary = json.loads(artifacts.summary_json_path.read_text(encoding="utf-8"))
    assert summary["schema_version"] == PUBLIC_PREVIEW_SCHEMA_VERSION
    assert EXPECTED_SUMMARY_KEYS.issubset(summary)
    assert EXPECTED_RELEASE_SUMMARY_KEYS.issubset(summary["release_summary"])

    validation_checks = {row["check"] for row in summary["validation"]["checks"]}
    assert summary["validation"]["overall_status"] == "pass"
    assert validation_checks == EXPECTED_VALIDATION_CHECKS

    interpretation_keys = {row["sector_key"] for row in summary["sector_interpretation"]}
    assert set(DEFAULT_PUBLIC_PREVIEW_SECTORS).issubset(interpretation_keys)
    assert set(DEFAULT_OPTIONAL_BANK_SECTORS).issubset(interpretation_keys)

    provenance_keys = {row["source_key"] for row in summary["provenance"]["sources"]}
    assert {"z1", "h15_nominal", "soma", "foreign_shl", "foreign_slt", "bank_constraints"}.issubset(
        provenance_keys
    )


def _assert_bounds_valid(frame: pd.DataFrame, point: str, lower: str, upper: str) -> None:
    subset = frame[[point, lower, upper]].apply(pd.to_numeric, errors="coerce").dropna()
    if subset.empty:
        return
    assert (subset[lower] <= subset[upper]).all()
    assert (subset[point] >= subset[lower]).all()
    assert (subset[point] <= subset[upper]).all()
