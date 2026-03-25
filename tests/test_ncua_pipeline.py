from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pandas as pd
import pytest

from treasury_sector_maturity.ffiec import build_bank_constraint_panel, normalize_ffiec_call_report_zip

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"


def _build_ncua_fixture_zip_bytes() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.write(
            FIXTURES / "ncua_call_bulk_por_12312025_subset.txt",
            arcname="FOICU.txt",
        )
        zf.write(
            FIXTURES / "ncua_call_schedule_natural_person_12312025_subset.txt",
            arcname="FS220Q.txt",
        )
    return buffer.getvalue()


def _build_ffiec_fixture_zip_path(tmp_path: Path) -> Path:
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


def _load_ncua_call_report_normalizer():
    try:
        from treasury_sector_maturity.ncua import normalize_ncua_call_report_zip
    except ImportError as exc:
        pytest.skip(f"NCUA normalizer not available in this workspace yet: {exc}")

    return normalize_ncua_call_report_zip


def test_normalize_ncua_call_report_zip_preserves_core_bank_constraint_shape(tmp_path: Path):
    ncua_path = tmp_path / "ncua_natural_person_12312025.zip"
    ncua_path.write_bytes(_build_ncua_fixture_zip_bytes())

    normalize_ncua_call_report_zip = _load_ncua_call_report_normalizer()
    ncua_normalized = normalize_ncua_call_report_zip(ncua_path)

    assert list(ncua_normalized["date"].dt.strftime("%Y-%m-%d").unique()) == ["2025-12-31"]
    assert {"reporter_id", "bank_name", "total_treasuries_amortized_cost", "treasury_ladder_total"}.issubset(
        ncua_normalized.columns
    )
    assert list(ncua_normalized["reporter_id"]) == ["12", "2744", "60"]
    assert pd.api.types.is_numeric_dtype(ncua_normalized["treasury_ladder_total"])
    assert ncua_normalized["treasury_ladder_total"].isna().all()

    row_12 = ncua_normalized[ncua_normalized["reporter_id"] == "12"].iloc[0]
    assert row_12["total_treasuries_amortized_cost"] == 2468004

    row_2744 = ncua_normalized[ncua_normalized["reporter_id"] == "2744"].iloc[0]
    assert row_2744["total_treasuries_level_proxy"] == 1337374


def test_build_bank_constraint_panel_combines_ffiec_and_ncua(tmp_path: Path):
    ncua_path = tmp_path / "ncua_natural_person_12312025.zip"
    ncua_path.write_bytes(_build_ncua_fixture_zip_bytes())
    ffiec_path = _build_ffiec_fixture_zip_path(tmp_path)

    normalize_ncua_call_report_zip = _load_ncua_call_report_normalizer()
    ncua_normalized = normalize_ncua_call_report_zip(ncua_path)
    ncua_normalized["provider"] = "ncua"
    ncua_normalized["dataset"] = "ncua_call_reports"
    ncua_normalized["vintage"] = "2026-03-23"
    ncua_normalized["raw_file"] = "ncua_natural_person_12312025.zip"

    ffiec_normalized = normalize_ffiec_call_report_zip(
        ffiec_path,
        config_path=ROOT / "configs" / "ffiec_call_report.yaml",
    )
    ffiec_normalized["provider"] = "ffiec"
    ffiec_normalized["dataset"] = "ffiec_call_reports"
    ffiec_normalized["vintage"] = "2026-03-23"
    ffiec_normalized["raw_file"] = "ffiec_subset_12312025.zip"

    panel = build_bank_constraint_panel(
        pd.concat([ffiec_normalized, ncua_normalized], ignore_index=True, sort=False),
        constraints_config_path=ROOT / "configs" / "bank_constraints.yaml",
    )

    assert set(panel["sector_key"]) == {"bank_us_chartered", "credit_unions_marketable_proxy"}
    credit_union = panel[panel["sector_key"] == "credit_unions_marketable_proxy"].iloc[0]
    assert credit_union["constraint_level"] == 14321151
    assert pd.isna(credit_union["constraint_bill_share"])
    assert pd.isna(credit_union["constraint_short_share_le_1y"])
    assert not bool(credit_union["share_constraints_available"])
    assert pd.isna(credit_union["constraint_bucket_basis_total"])
