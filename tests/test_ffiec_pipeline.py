from __future__ import annotations

import zipfile
from pathlib import Path

import pandas as pd
import pytest

from treasury_sector_maturity.ffiec import build_bank_constraint_panel, normalize_ffiec_call_report_zip


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"


def _fixture_zip(tmp_path: Path) -> Path:
    zip_path = tmp_path / "ffiec_subset_12312025.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.write(
            FIXTURES / "ffiec_call_bulk_por_12312025_subset.txt",
            arcname="FFIEC CDR Call Bulk POR 12312025.txt",
        )
        zf.write(
            FIXTURES / "ffiec_call_schedule_rcb_12312025_subset.txt",
            arcname="FFIEC CDR Call Schedule RCB 12312025(1 of 2).txt",
        )
    return zip_path


def test_build_bank_constraint_panel_from_ffiec_fixture(tmp_path: Path):
    normalized = normalize_ffiec_call_report_zip(
        _fixture_zip(tmp_path),
        config_path=ROOT / "configs" / "ffiec_call_report.yaml",
    )
    normalized["provider"] = "ffiec"
    normalized["dataset"] = "ffiec_call_reports"
    normalized["vintage"] = "2026-03-23"
    normalized["raw_file"] = "ffiec_subset_12312025.zip"

    panel = build_bank_constraint_panel(
        normalized,
        constraints_config_path=ROOT / "configs" / "bank_constraints.yaml",
    )

    assert len(panel) == 1
    row = panel.iloc[0]
    assert row["sector_key"] == "bank_us_chartered"
    assert row["constraint_level"] == 30290284
    assert row["constraint_bucket_basis_total"] == 71786698
    assert round(float(row["constraint_bill_share"]), 6) == round(15205386 / 71786698, 6)
    assert round(float(row["constraint_short_share_le_1y"]), 6) == round(26598757 / 71786698, 6)
    assert row["n_reporters"] == 3


def test_build_bank_constraint_panel_appends_uncovered_perimeter_supplement(tmp_path: Path):
    normalized = normalize_ffiec_call_report_zip(
        _fixture_zip(tmp_path),
        config_path=ROOT / "configs" / "ffiec_call_report.yaml",
    )
    normalized["provider"] = "ffiec"
    normalized["dataset"] = "ffiec_call_reports"
    normalized["vintage"] = "2026-03-23"
    normalized["raw_file"] = "ffiec_subset_12312025.zip"

    supplement = pd.DataFrame(
        [
            {
                "date": "2025-12-31",
                "sector_key": "bank_us_affiliated_areas",
                "constraint_level": 28.625,
                "constraint_bill_share": 0.074,
                "constraint_short_share_le_1y": 0.165,
                "share_constraints_available": True,
                "constraint_bucket_basis_total": 28.625,
                "provider": "toy_supplement",
                "dataset": "bank_perimeter_supplement",
                "raw_file": "toy_bank_constraint_supplement.csv",
            },
            {
                "date": "2025-12-31",
                "sector_key": "bank_reserve_access_core",
                "constraint_level": 1795.859,
                "constraint_bill_share": 0.128,
                "constraint_short_share_le_1y": 0.233,
                "share_constraints_available": True,
                "constraint_bucket_basis_total": 1795.859,
                "provider": "toy_supplement",
                "dataset": "bank_perimeter_supplement",
                "raw_file": "toy_bank_constraint_supplement.csv",
            },
            {
                "date": "2025-12-31",
                "sector_key": "bank_broad_private_depositories_marketable_proxy",
                "constraint_level": 1898.321,
                "constraint_bill_share": 0.120,
                "constraint_short_share_le_1y": 0.224,
                "share_constraints_available": True,
                "constraint_bucket_basis_total": 1898.321,
                "provider": "toy_supplement",
                "dataset": "bank_perimeter_supplement",
                "raw_file": "toy_bank_constraint_supplement.csv",
            },
        ]
    )

    panel = build_bank_constraint_panel(
        normalized,
        constraints_config_path=ROOT / "configs" / "bank_constraints.yaml",
        supplement_df=supplement,
    )

    assert set(panel["sector_key"]) == {
        "bank_broad_private_depositories_marketable_proxy",
        "bank_reserve_access_core",
        "bank_us_affiliated_areas",
        "bank_us_chartered",
    }
    affiliated = panel[panel["sector_key"] == "bank_us_affiliated_areas"].iloc[0]
    reserve_access = panel[panel["sector_key"] == "bank_reserve_access_core"].iloc[0]
    broad_private = panel[panel["sector_key"] == "bank_broad_private_depositories_marketable_proxy"].iloc[0]
    assert affiliated["constraint_level"] == 28.625
    assert affiliated["constraint_bill_share"] == 0.074
    assert affiliated["share_constraints_available"]
    assert affiliated["provider"] == "toy_supplement"
    assert affiliated["dataset"] == "bank_perimeter_supplement"
    assert affiliated["n_reporters"] == 0
    assert reserve_access["constraint_level"] == 1795.859
    assert reserve_access["constraint_short_share_le_1y"] == 0.233
    assert reserve_access["provider"] == "toy_supplement"
    assert broad_private["constraint_level"] == 1898.321
    assert broad_private["constraint_bill_share"] == 0.120
    assert broad_private["provider"] == "toy_supplement"


def test_build_bank_constraint_panel_rejects_overlapping_supplement_rows(tmp_path: Path):
    normalized = normalize_ffiec_call_report_zip(
        _fixture_zip(tmp_path),
        config_path=ROOT / "configs" / "ffiec_call_report.yaml",
    )
    normalized["provider"] = "ffiec"
    normalized["dataset"] = "ffiec_call_reports"
    normalized["vintage"] = "2026-03-23"
    normalized["raw_file"] = "ffiec_subset_12312025.zip"

    overlap = pd.DataFrame(
        [
            {
                "date": "2025-12-31",
                "sector_key": "bank_us_chartered",
                "constraint_level": 1.0,
            }
        ]
    )

    with pytest.raises(ValueError, match="overlaps observed panel rows"):
        build_bank_constraint_panel(
            normalized,
            constraints_config_path=ROOT / "configs" / "bank_constraints.yaml",
            supplement_df=overlap,
        )


def test_build_bank_constraint_panel_allows_supplement_only_input():
    supplement = pd.DataFrame(
        [
            {
                "date": "2025-12-31",
                "sector_key": "bank_reserve_access_core",
                "constraint_level": 1795.859,
                "constraint_bill_share": 0.128,
                "share_constraints_available": True,
            }
        ]
    )

    panel = build_bank_constraint_panel(
        pd.DataFrame(),
        constraints_config_path=ROOT / "configs" / "bank_constraints.yaml",
        supplement_df=supplement,
    )

    assert len(panel) == 1
    assert panel.iloc[0]["sector_key"] == "bank_reserve_access_core"
    assert panel.iloc[0]["provider"] == "supplement"
