from __future__ import annotations

import json
import io
import zipfile
from pathlib import Path

import pandas as pd
import pytest
import requests

import treasury_sector_maturity.providers as providers
from treasury_sector_maturity.ffiec import normalize_ffiec_call_report_zip
from treasury_sector_maturity.ffiec002 import normalize_ffiec002_browser_bundle
from treasury_sector_maturity.h15 import build_benchmark_returns, load_h15_curve_file
from treasury_sector_maturity.providers import (
    SourceFetchError,
    _fetch_soma_snapshot,
    _fetch_z1_from_fred,
    extract_z1_release_zip_series,
    fetch_h15_curves,
    fetch_ffiec_call_reports,
    fetch_ffiec002_call_reports,
    normalize_fred_observations,
    parse_h15_fed_package_csv,
)
from treasury_sector_maturity.soma import read_soma_holdings, summarize_soma_quarterly
from treasury_sector_maturity.utils import load_yaml
from treasury_sector_maturity.z1 import parse_z1_ddp_csv


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"


def _load_fred_bundle() -> dict:
    with (FIXTURES / "fred_observations_bundle.json").open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _build_ffiec_fixture_zip_bytes() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.write(
            FIXTURES / "ffiec_call_bulk_por_12312025_subset.txt",
            arcname="FFIEC CDR Call Bulk POR 12312025.txt",
        )
        zf.write(
            FIXTURES / "ffiec_call_schedule_rcb_12312025_subset.txt",
            arcname="FFIEC CDR Call Schedule RCB 12312025(1 of 2).txt",
        )
    return buffer.getvalue()


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


def _build_ffiec002_fixture_zip_bytes() -> bytes:
    manifest = {
        "report_date": "2025-12-31",
        "institutions": [
            {
                "IdRssd": 908508,
                "Name": "BANK OF CHINA NY BR [BANK OF CHINA LIMITED]",
                "City": "NEW YORK",
                "State": "NY",
                "Country": "UNITED STATES",
                "EntityType": "Insured Federal Branch of an FBO",
                "HeadOfficeName": "BANK OF CHINA LIMITED",
            },
            {
                "IdRssd": 1218361,
                "Name": "BANK OF CHINA LA BR [BANK OF CHINA LIMITED]",
                "City": "LOS ANGELES",
                "State": "CA",
                "Country": "UNITED STATES",
                "EntityType": "Uninsured Federal Branch of an FBO",
                "HeadOfficeName": "BANK OF CHINA LIMITED",
            },
        ],
    }

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.writestr("manifest.json", json.dumps(manifest, indent=2, sort_keys=True))
        zf.write(FIXTURES / "ffiec002_908508_20251231.csv", arcname="reports/908508.csv")
        zf.write(FIXTURES / "ffiec002_1218361_20251231.csv", arcname="reports/1218361.csv")
    return buffer.getvalue()


def _load_fetch_ncua_provider():
    if not hasattr(providers, "fetch_ncua_call_reports"):
        pytest.skip("NCUA provider fetch is not available in this workspace yet.")
    return providers.fetch_ncua_call_reports


def _fetch_ncua_call_reports(**kwargs):
    return _load_fetch_ncua_provider()(**kwargs)


def test_parse_h15_fed_package_fixture():
    fed_codes = (
        load_yaml(ROOT / "configs" / "h15_series.yaml")["h15"]["nominal_treasury_constant_maturity"]["fed_codes"]
    )
    curves = parse_h15_fed_package_csv(FIXTURES / "fed_h15_treasury_constant_maturity.csv", fed_code_map=fed_codes)
    assert {"date", "1m", "10y", "30y"}.issubset(curves.columns)
    assert len(curves) == 6
    assert curves.iloc[-1]["10y"] == 4.25


def test_fred_h15_normalization_matches_fed_fixture():
    bundle = _load_fred_bundle()
    mapping = load_yaml(ROOT / "configs" / "h15_series.yaml")["h15"]["nominal_treasury_constant_maturity"]["fred_ids"]
    fed_curves = load_h15_curve_file(FIXTURES / "fed_h15_treasury_constant_maturity.csv", ROOT / "configs" / "h15_series.yaml")

    frames = []
    for label, series_id in mapping.items():
        frame = normalize_fred_observations(bundle[series_id], value_name=label)
        frames.append(frame)

    fred_curves = frames[0]
    for frame in frames[1:]:
        fred_curves = fred_curves.merge(frame, on="date", how="outer")

    fred_curves = fred_curves.sort_values("date").reset_index(drop=True)
    pd.testing.assert_frame_equal(
        fed_curves[["date", *mapping.keys()]].reset_index(drop=True),
        fred_curves[["date", *mapping.keys()]].reset_index(drop=True),
    )


def test_fetch_h15_tips_from_fred(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    dates = ["2024-03-31", "2024-06-30", "2024-09-30", "2024-12-31", "2025-03-31", "2025-06-30"]
    series_values = {
        "DFII5": [1.78, 1.92, 1.61, 1.48, 1.67, 1.54],
        "DFII7": [1.88, 2.03, 1.73, 1.61, 1.79, 1.66],
        "DFII10": [2.02, 2.17, 1.89, 1.79, 1.95, 1.84],
        "DFII20": [2.21, 2.34, 2.08, 2.01, 2.16, 2.05],
        "DFII30": [2.19, 2.32, 2.07, 2.00, 2.15, 2.04],
    }

    def fake_fetch(series_id: str, api_key: str | None = None, session=None):
        return {
            "seriess": [{"id": series_id}],
            "observations": [
                {"date": date, "value": f"{value:.2f}"}
                for date, value in zip(dates, series_values[series_id])
            ],
        }

    monkeypatch.setattr("treasury_sector_maturity.providers.fetch_fred_series_observations", fake_fetch)
    artifact = fetch_h15_curves(
        provider="fred",
        series_config_path=ROOT / "configs" / "h15_series.yaml",
        curve_key="tips_real_yield_constant_maturity",
        raw_dir=tmp_path,
        normalized_out=tmp_path / "tips_curves.csv",
        api_key="dummy",
    )

    curves = pd.read_csv(artifact.normalized_path)
    assert artifact.provider == "fred"
    assert artifact.dataset == "h15"
    assert {"date", "tips_5y", "tips_10y", "tips_30y"}.issubset(curves.columns)
    assert curves["tips_10y"].iloc[-1] == pytest.approx(1.84, abs=1e-9)
    assert curves["tips_30y"].iloc[-1] == pytest.approx(2.04, abs=1e-9)


def test_parse_date_indexed_z1_release_fixture():
    long_df = parse_z1_ddp_csv(FIXTURES / "fed_z1_l133_subset.csv")
    assert not long_df.empty
    assert set(long_df["series_code"].unique()) == {"FL263061105.Q", "FL263061705.Q"}
    assert long_df["date"].dt.quarter.tolist()[:2] == [1, 2]


def test_extract_z1_release_zip_series_reads_selected_members(tmp_path: Path):
    zip_path = tmp_path / "z1_subset.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.write(FIXTURES / "fed_z1_l133_subset.csv", arcname="csv/l133.csv")
        zf.write(FIXTURES / "fed_z1_fu133_subset.csv", arcname="csv/fu133.csv")

    long_df = extract_z1_release_zip_series(zip_path, {"FL263061105.Q", "FU263061105.Q"})
    assert set(long_df["series_code"].unique()) == {"FL263061105.Q", "FU263061105.Q"}
    assert long_df["raw_file"].nunique() == 2


def test_fred_z1_normalization_matches_fed_fixture():
    bundle = _load_fred_bundle()
    fed_long = parse_z1_ddp_csv(FIXTURES / "fed_z1_l133_subset.csv")
    fed_total = fed_long[fed_long["series_code"] == "FL263061105.Q"].reset_index(drop=True)

    fred_total = normalize_fred_observations(
        bundle["TEST_FL263061105Q"],
        value_name="value",
        frequency_suffix="Q",
    )
    fred_total["series_code"] = "FL263061105.Q"

    pd.testing.assert_series_equal(fed_total["date"], fred_total["date"], check_names=False)
    pd.testing.assert_series_equal(fed_total["value"], fred_total["value"], check_names=False)


def test_z1_fred_fetch_requires_explicit_mappings(tmp_path: Path):
    with pytest.raises(SourceFetchError, match="No explicit FRED mappings"):
        _fetch_z1_from_fred(FIXTURES / "z1_catalog_without_fred_ids.yaml", raw_dir=tmp_path, api_key="dummy")


def test_z1_fred_fetch_uses_explicit_mappings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    bundle = _load_fred_bundle()

    def fake_fetch(series_id: str, api_key: str | None = None, session=None):
        return bundle[series_id]

    monkeypatch.setattr("treasury_sector_maturity.providers.fetch_fred_series_observations", fake_fetch)
    raw_path, long_df = _fetch_z1_from_fred(
        FIXTURES / "z1_catalog_with_fred_ids.yaml",
        raw_dir=tmp_path,
        api_key="dummy",
    )

    assert raw_path.name == "fred_z1_series.csv"
    assert set(long_df["series_code"].unique()) == {"FL263061105.Q", "FU263061105.Q"}
    assert {"provider", "dataset", "vintage", "raw_file"}.issubset(long_df.columns)


def test_real_shape_h15_and_soma_flow():
    curves = load_h15_curve_file(FIXTURES / "fed_h15_treasury_constant_maturity.csv", ROOT / "configs" / "h15_series.yaml")
    benchmark = build_benchmark_returns(curves)
    soma = read_soma_holdings(FIXTURES / "nyfed_soma_tsy_2026-03-18.csv")
    summary = summarize_soma_quarterly(soma, curve_df=curves)

    assert not benchmark.empty
    assert {"date", "10y", "30y"}.issubset(benchmark.columns)
    assert not summary.empty
    assert summary["bill_share"].iloc[0] == 1.0


def test_fetch_soma_snapshot_retries_transient_disconnect(tmp_path: Path):
    fixture_text = (FIXTURES / "nyfed_soma_tsy_2026-03-18.csv").read_text(encoding="utf-8")

    class FakeResponse:
        def __init__(self, text: str) -> None:
            self.text = text

        def raise_for_status(self) -> None:
            return None

    class FlakySession:
        def __init__(self) -> None:
            self.calls = 0

        def get(self, url: str, timeout: int | None = None):
            self.calls += 1
            if self.calls == 1:
                raise requests.exceptions.ConnectionError("transient disconnect")
            return FakeResponse(fixture_text)

    session = FlakySession()
    raw_path, frame = _fetch_soma_snapshot(pd.Timestamp("2026-03-18"), raw_dir=tmp_path, session=session)

    assert session.calls == 2
    assert raw_path.exists()
    assert frame["provider"].iloc[0] == "fed"
    assert frame["dataset"].iloc[0] == "soma"
    assert frame["raw_file"].iloc[0] == raw_path.name


def test_normalize_ffiec_call_report_zip_fixture(tmp_path: Path):
    zip_path = tmp_path / "ffiec_subset_12312025.zip"
    zip_path.write_bytes(_build_ffiec_fixture_zip_bytes())

    normalized = normalize_ffiec_call_report_zip(zip_path, config_path=ROOT / "configs" / "ffiec_call_report.yaml")

    assert list(normalized["reporter_id"]) == ["12311", "242", "35301"]
    assert normalized["date"].dt.strftime("%Y-%m-%d").unique().tolist() == ["2025-12-31"]

    row_242 = normalized[normalized["reporter_id"] == "242"].iloc[0]
    assert row_242["total_treasuries_amortized_cost"] == 3266
    assert row_242["treasury_bucket_3m_or_less"] == 195
    assert row_242["treasury_bucket_3_12m"] == 2189

    row_12311 = normalized[normalized["reporter_id"] == "12311"].iloc[0]
    assert row_12311["total_treasuries_amortized_cost"] == 6939018
    assert row_12311["treasury_ladder_total"] == 11862185


def test_fetch_ffiec_call_reports_provider(tmp_path: Path):
    initial_html = """
    <form>
      <input type="hidden" name="__VIEWSTATE" value="initial" />
      <input type="hidden" name="__VIEWSTATEGENERATOR" value="abc" />
      <select id="ListBox1" name="ctl00$MainContentHolder$ListBox1"></select>
    </form>
    """
    selected_html = """
    <form>
      <input type="hidden" name="__VIEWSTATE" value="selected" />
      <input type="hidden" name="__VIEWSTATEGENERATOR" value="abc" />
      <select id="DatesDropDownList" name="ctl00$MainContentHolder$DatesDropDownList">
        <option value="150">12/31/2025</option>
        <option value="149">09/30/2025</option>
      </select>
    </form>
    """

    class FakeResponse:
        def __init__(self, *, text: str = "", content: bytes = b"", headers: dict[str, str] | None = None) -> None:
            self.text = text
            self.content = content
            self.headers = headers or {}

        def raise_for_status(self) -> None:
            return None

    class FakeSession:
        def __init__(self) -> None:
            self.posts: list[dict[str, str]] = []

        def get(self, url: str, timeout: int | None = None):
            return FakeResponse(text=initial_html)

        def post(self, url: str, data: dict[str, str], timeout: int | None = None):
            self.posts.append(dict(data))
            if len(self.posts) == 1:
                return FakeResponse(text=selected_html)
            return FakeResponse(
                content=_build_ffiec_fixture_zip_bytes(),
                headers={"Content-Type": "application/zip"},
            )

    session = FakeSession()
    artifacts = fetch_ffiec_call_reports(
        report_date="2025-12-31",
        raw_dir=tmp_path,
        normalized_out=tmp_path / "ffiec_call_reports_ffiec.csv",
        session=session,
    )

    assert artifacts.provider == "ffiec"
    assert artifacts.dataset == "ffiec_call_reports"
    assert artifacts.raw_path is not None and artifacts.raw_path.exists()
    assert artifacts.normalized_path.exists()
    output = pd.read_csv(artifacts.normalized_path)
    assert {"provider", "dataset", "vintage", "raw_file"}.issubset(output.columns)
    assert session.posts[1]["ctl00$MainContentHolder$DatesDropDownList"] == "150"


def test_normalize_ffiec002_bundle_fixture(tmp_path: Path):
    zip_path = tmp_path / "ffiec002_subset_20251231.zip"
    zip_path.write_bytes(_build_ffiec002_fixture_zip_bytes())

    normalized = normalize_ffiec002_browser_bundle(
        zip_path,
        config_path=ROOT / "configs" / "ffiec002_call_report.yaml",
    )

    assert set(normalized["reporter_id"]) == {"908508", "1218361"}
    assert normalized["us_treasury_securities"].sum() == 36872387.0
    assert normalized["institution_type"].nunique() == 2


def test_fetch_ffiec002_call_reports_provider(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    fixture_zip = tmp_path / "ffiec002_subset_20251231.zip"
    fixture_zip.write_bytes(_build_ffiec002_fixture_zip_bytes())
    normalized = normalize_ffiec002_browser_bundle(
        fixture_zip,
        config_path=ROOT / "configs" / "ffiec002_call_report.yaml",
    )
    normalized["provider"] = "ffiec"
    normalized["dataset"] = "ffiec002_call_reports"
    normalized["vintage"] = "2026-03-23"
    normalized["raw_file"] = fixture_zip.name

    def fake_fetch_ffiec002_from_npw(report_date, config_path, raw_dir):
        return fixture_zip, normalized.copy()

    monkeypatch.setattr(providers, "_fetch_ffiec002_from_npw", fake_fetch_ffiec002_from_npw)
    artifacts = fetch_ffiec002_call_reports(
        report_date="2025-12-31",
        raw_dir=tmp_path,
        normalized_out=tmp_path / "ffiec002_call_reports_ffiec.csv",
    )

    assert artifacts.provider == "ffiec"
    assert artifacts.dataset == "ffiec002_call_reports"
    assert artifacts.raw_path == fixture_zip
    output = pd.read_csv(artifacts.normalized_path)
    assert {"provider", "dataset", "vintage", "raw_file"}.issubset(output.columns)
    assert output["us_treasury_securities"].sum() == 36872387.0


def test_fetch_ncua_call_reports_provider(tmp_path: Path):
    quarterly_page = """
    <html>
      <body>
        <a href="/files/publications/analysis/call-report-data-2025-12.zip">December 2025</a>
        <a href="/files/publications/analysis/call-report-data-2025-09.zip">September 2025</a>
      </body>
    </html>
    """

    class FakeResponse:
        def __init__(self, *, text: str = "", content: bytes = b"", headers: dict[str, str] | None = None) -> None:
            self.text = text
            self.content = content
            self.headers = headers or {}

        def raise_for_status(self) -> None:
            return None

    class FakeSession:
        def __init__(self) -> None:
            self.gets: list[str] = []
            self.headers: dict[str, str] = {}

        def get(self, url: str, timeout: int | None = None, **kwargs):
            self.gets.append(url)
            if len(self.gets) == 1:
                return FakeResponse(text=quarterly_page)
            return FakeResponse(
                content=_build_ncua_fixture_zip_bytes(),
                headers={"Content-Type": "application/zip"},
            )

    session = FakeSession()
    artifacts = _fetch_ncua_call_reports(
        report_date="2025-12-31",
        config_path=ROOT / "configs" / "ncua_call_report.yaml",
        raw_dir=tmp_path,
        normalized_out=tmp_path / "ncua_call_reports_ncua.csv",
        session=session,
    )

    assert artifacts.provider == "ncua"
    assert artifacts.dataset == "ncua_call_reports"
    assert artifacts.raw_path is not None and artifacts.raw_path.exists()
    assert artifacts.normalized_path.exists()
    output = pd.read_csv(artifacts.normalized_path)
    assert {"provider", "dataset", "vintage", "raw_file"}.issubset(output.columns)
    assert session.gets[1].endswith("/files/publications/analysis/call-report-data-2025-12.zip")
    assert len(output) == 3
