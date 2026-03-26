from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from treasury_sector_maturity.public_release import build_public_release_report


ROOT = Path(__file__).resolve().parents[1]
TOY_Z1 = ROOT / "data" / "examples" / "toy_z1_selected_series.csv"
TOY_H15 = ROOT / "data" / "examples" / "toy_h15_curves.csv"
TOY_TIPS = ROOT / "data" / "examples" / "toy_tips_real_yields.csv"
TOY_SOMA = ROOT / "data" / "examples" / "toy_soma_holdings.csv"
TOY_SHL = ROOT / "data" / "examples" / "toy_shl_issue_mix.csv"
TOY_SLT = ROOT / "data" / "examples" / "toy_slt_short_long.csv"
TOY_BANK_CONSTRAINTS = ROOT / "data" / "examples" / "toy_bank_constraint_panel.csv"
FULL_CATALOG = ROOT / "configs" / "z1_series_catalog_full.yaml"
FULL_SECTOR_DEFS = ROOT / "configs" / "sector_definitions_full.yaml"
FULL_SCRIPT = ROOT / "scripts" / "build_full_coverage_release.py"


def test_full_coverage_release_cli_builds_expected_artifacts(tmp_path):
    out_dir = tmp_path / "full_coverage_release_cli"
    summary_path = out_dir / "full_coverage_summary.json"
    cmd = [
        sys.executable,
        "-B",
        str(FULL_SCRIPT),
        "--out-dir",
        str(out_dir),
        "--coverage-scope",
        "full",
        "--source-provider",
        "fed",
        "--end-date",
        "2025-12-31",
        "--summary-json-out",
        str(summary_path),
        "--z1-file",
        str(TOY_Z1),
        "--h15-file",
        str(TOY_H15),
        "--curve-file",
        f"tips_real_yield_constant_maturity={TOY_TIPS}",
        "--soma-file",
        str(TOY_SOMA),
        "--foreign-shl-file",
        str(TOY_SHL),
        "--foreign-slt-file",
        str(TOY_SLT),
        "--bank-constraint-file",
        str(TOY_BANK_CONSTRAINTS),
        "--series-catalog",
        str(FULL_CATALOG),
        "--sector-defs",
        str(FULL_SECTOR_DEFS),
        "--model-config",
        str(ROOT / "configs" / "model_defaults.yaml"),
        "--series-config",
        str(ROOT / "configs" / "h15_series.yaml"),
        "--bank-constraints-config",
        str(ROOT / "configs" / "bank_constraints.yaml"),
        "--release-config",
        str(ROOT / "configs" / "full_coverage_release.yaml"),
    ]

    completed = subprocess.run(cmd, check=True, capture_output=True, text=True)
    assert "full_coverage_report.md" in completed.stdout
    assert "run_manifest.json" in completed.stdout
    assert "full_coverage_summary.json" in completed.stdout
    assert "required_sector_inventory.csv" in completed.stdout

    for name in [
        "canonical_atomic_sector_maturity.csv",
        "latest_atomic_sector_snapshot.csv",
        "high_confidence_sector_maturity.csv",
        "reconciliation_nodes.csv",
        "required_sector_inventory.csv",
        "full_coverage_report.md",
        "run_manifest.json",
        "full_coverage_summary.json",
    ]:
        assert (out_dir / name).exists()


def test_full_coverage_workflow_does_not_change_preview_contract(tmp_path):
    out_dir = tmp_path / "public_preview"
    artifacts = build_public_release_report(
        out_dir=out_dir,
        z1_file=TOY_Z1,
        h15_file=TOY_H15,
        soma_file=TOY_SOMA,
        foreign_shl_file=TOY_SHL,
        foreign_slt_file=TOY_SLT,
        bank_constraint_file=TOY_BANK_CONSTRAINTS,
        end_date="2025-12-31",
        quarters=4,
    )

    assert artifacts.report_path.name == "public_release_report.md"
    assert artifacts.sector_output_path.name == "sector_effective_maturity.csv"
    assert artifacts.manifest_path.name == "run_manifest.json"
    assert artifacts.summary_json_path is None
    report = artifacts.report_path.read_text(encoding="utf-8")
    assert "# Public Release Preview Report" in report
    assert "## Sector Coverage" in report
