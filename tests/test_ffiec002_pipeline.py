from pathlib import Path

from treasury_sector_maturity.ffiec002 import (
    SEARCH_EXPORT_COLUMNS,
    load_ffiec002_search_export,
    normalize_ffiec002_browser_bundle,
    summarize_ffiec002_foreign_banking_offices,
)


ROOT = Path(__file__).resolve().parents[1]


def test_load_ffiec002_search_export_fixture():
    manifest = load_ffiec002_search_export(ROOT / "tests" / "fixtures" / "ffiec002_search_export.csv")
    assert set(SEARCH_EXPORT_COLUMNS).issubset(manifest.columns)
    assert list(manifest["RssdID"]) == ["908508", "317810"]
    assert manifest["InstitutionName"].str.contains("BANK OF CHINA").any()


def test_normalize_ffiec002_browser_bundle_fixture():
    manifest = load_ffiec002_search_export(ROOT / "tests" / "fixtures" / "ffiec002_search_export.csv")
    normalized = normalize_ffiec002_browser_bundle(
        manifest_df=manifest,
        reports_dir=ROOT / "tests" / "fixtures",
        report_date="2025-12-31",
        missing_records=[],
    )
    assert len(normalized) == 2
    assert normalized.loc[normalized["rssd_id"] == "908508", "us_treasury_securities"].iloc[0] == 2872387.0
    assert normalized.loc[normalized["rssd_id"] == "317810", "ffiec002_download_status"].iloc[0] == "downloaded_no_rcfd0260"

    aggregate = summarize_ffiec002_foreign_banking_offices(normalized)
    assert aggregate["constraint_level"].iloc[0] == 2872387.0
    assert aggregate["downloaded_no_rcfd0260_count"].iloc[0] == 1
    assert aggregate["coverage_ratio"].iloc[0] == 0.5
    assert aggregate["missing_rcfd0260_ratio"].iloc[0] == 0.5
    assert "non-exact" in aggregate["constraint_level_exactness"].iloc[0]
    assert aggregate["constraint_level_concept_match"].iloc[0] == "partial"
