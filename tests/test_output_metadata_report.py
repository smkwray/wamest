from pathlib import Path

from treasury_sector_maturity.reporting import build_output_metadata_report


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"


def test_output_metadata_report_builds_from_toy_fallback(tmp_path):
    out = tmp_path / "output_metadata_report.md"
    path = build_output_metadata_report(
        sector_file=FIXTURES / "report_sector_effective_maturity.csv",
        foreign_nowcast_file=FIXTURES / "report_foreign_nowcast_panel.csv",
        fed_summary_file=FIXTURES / "report_fed_calibration_summary.json",
        out=out,
    )

    assert path == out
    text = out.read_text(encoding="utf-8")
    assert "# Output Metadata Report" in text
    assert "## Sector Metadata Summary" in text
    assert "## Identified-Set and Constraint Summary" in text
    assert "## Hybrid Estimation Summary" in text
    assert "## Foreign Nowcast Support Summary" in text
    assert "## Fed Calibration Context" in text
    assert "foreigners_total" in text
    assert "bank_us_chartered" in text
    assert "two_sided_between_supports" in text
    assert "rolling_benchmark_weights_plus_factors" in text
    assert "Factor exposure columns" in text
    assert "n/a" in text


def test_output_metadata_report_handles_missing_fed_summary(tmp_path):
    out = tmp_path / "output_metadata_report_no_summary.md"
    path = build_output_metadata_report(
        sector_file=FIXTURES / "report_sector_effective_maturity.csv",
        foreign_nowcast_file=FIXTURES / "report_foreign_nowcast_panel.csv",
        fed_summary_file=ROOT / "outputs" / "missing_summary.json",
        out=out,
    )

    assert path == out
    text = out.read_text(encoding="utf-8")
    assert "Calibration context unavailable." in text
    assert "The report was still built because the sector and foreign artifacts were present." in text
