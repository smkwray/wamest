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


def test_public_release_report_builds_from_toy_inputs(tmp_path):
    out_dir = tmp_path / "public_preview"
    artifacts = build_public_release_report(
        out_dir=out_dir,
        z1_file=ROOT / "data" / "examples" / "toy_z1_selected_series.csv",
        h15_file=ROOT / "data" / "examples" / "toy_h15_curves.csv",
        soma_file=ROOT / "data" / "examples" / "toy_soma_holdings.csv",
        foreign_shl_file=ROOT / "data" / "examples" / "toy_shl_issue_mix.csv",
        foreign_slt_file=ROOT / "data" / "examples" / "toy_slt_short_long.csv",
        bank_constraint_file=ROOT / "data" / "examples" / "toy_bank_constraint_panel.csv",
        end_date="2025-12-31",
        quarters=4,
    )

    assert artifacts.report_path.exists()
    assert artifacts.sector_output_path.exists()
    assert artifacts.manifest_path.exists()
    assert artifacts.summary_json_path is None

    report = artifacts.report_path.read_text(encoding="utf-8")
    assert "# Public Release Preview Report" in report
    assert "## Sector Coverage" in report
    assert "## Sector Interpretation" in report
    assert "## Excluded Optional Sectors" in report
    assert "## Validation" in report
    assert "## Provenance" in report
    assert "`bank_us_chartered`" in report
    assert "`bank_us_affiliated_areas`" in report
    assert "`configs/model_public_preview.yaml`" in report

    sector = pd.read_csv(artifacts.sector_output_path, parse_dates=["date"])
    assert set(REQUIRED_PUBLIC_PREVIEW_COLUMNS).issubset(set(sector.columns))
    assert set(DEFAULT_PUBLIC_PREVIEW_SECTORS).issubset(set(sector["sector_key"]))
    assert not set(DEFAULT_OPTIONAL_BANK_SECTORS) & set(sector["sector_key"])
    assert sector["date"].max() == pd.Timestamp("2025-12-31")
    assert sector["date"].nunique() == 4

    manifest = json.loads(artifacts.manifest_path.read_text(encoding="utf-8"))
    assert manifest["schema_version"] == PUBLIC_PREVIEW_SCHEMA_VERSION
    assert manifest["source_provider_requested"] == "fed"
    assert manifest["model_config_path"] == "configs/model_public_preview.yaml"
    assert manifest["benchmark_contract"]["holdings_benchmark_families"] == ["nominal_treasury_constant_maturity"]
    assert manifest["benchmark_contract"]["factor_benchmark_families"] == []
    assert manifest["end_date"] == "2025-12-31"
    assert manifest["resolved_common_quarter_date"] == "2025-12-31"
    assert manifest["quarter_count"] == 4
    assert manifest["optional_bank_paths_included"] is False
    assert manifest["optional_bank_sectors_skipped"] == DEFAULT_OPTIONAL_BANK_SECTORS
    assert manifest["output_paths"]["public_release_summary"] is None


def test_public_release_report_can_write_optional_summary_json(tmp_path):
    out_dir = tmp_path / "public_preview"
    summary_path = out_dir / "public_release_summary.json"
    artifacts = build_public_release_report(
        out_dir=out_dir,
        summary_json_out=summary_path,
        z1_file=ROOT / "data" / "examples" / "toy_z1_selected_series.csv",
        h15_file=ROOT / "data" / "examples" / "toy_h15_curves.csv",
        soma_file=ROOT / "data" / "examples" / "toy_soma_holdings.csv",
        foreign_shl_file=ROOT / "data" / "examples" / "toy_shl_issue_mix.csv",
        foreign_slt_file=ROOT / "data" / "examples" / "toy_slt_short_long.csv",
        bank_constraint_file=ROOT / "data" / "examples" / "toy_bank_constraint_panel.csv",
        end_date="2025-12-31",
        quarters=4,
    )

    assert artifacts.summary_json_path == summary_path
    assert summary_path.exists()

    report = artifacts.report_path.read_text(encoding="utf-8")
    assert "`" + str(summary_path) + "`" in report

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["schema_version"] == PUBLIC_PREVIEW_SCHEMA_VERSION
    assert summary["release_summary"]["model_config_path"] == "configs/model_public_preview.yaml"
    assert summary["release_summary"]["benchmark_contract"]["holdings_benchmark_families"] == [
        "nominal_treasury_constant_maturity"
    ]
    assert summary["release_summary"]["resolved_common_quarter_date"] == "2025-12-31"
    assert summary["machine_readable_outputs"]["public_release_summary"] == str(summary_path)
    assert summary["excluded_optional_sectors"] == DEFAULT_OPTIONAL_BANK_SECTORS
    assert any(row["sector_key"] == "fed" and row["included"] for row in summary["sector_coverage"])
    assert any(row["sector_key"] == "bank_us_affiliated_areas" and not row["included"] for row in summary["sector_coverage"])
    assert any(row["sector_key"] == "foreigners_total" and row["interpretation_class"] == "survey_anchored" for row in summary["sector_interpretation"])
    assert summary["validation"]["overall_status"] == "pass"
    assert any(check["check"] == "required_public_columns_present" and check["status"] == "pass" for check in summary["validation"]["checks"])
    assert summary["provenance"]["resolved_common_quarter_date"] == "2025-12-31"
    assert any(row["source_key"] == "z1" for row in summary["provenance"]["sources"])
    assert summary["foreign_support_snapshot"]["total_rows"] > 0
    assert "direct_support" in summary["foreign_support_snapshot"]["support_kind_counts"]
