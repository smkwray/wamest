from pathlib import Path

from treasury_sector_maturity.tic import (
    build_foreign_anchor_panel_from_public_sources,
    build_slt_foreign_holder_panel,
    extract_shl_total_foreign_benchmark,
    load_shl_historical_treasury_benchmark,
    load_slt_table3,
)


ROOT = Path(__file__).resolve().parents[1]


def test_load_slt_table3_subset():
    df = load_slt_table3(ROOT / "tests" / "fixtures" / "slt_table3_subset.txt")
    assert {"country", "country_code", "date", "total_treasury_holdings"}.issubset(df.columns)
    assert len(df) == 4

    holder_panel = build_slt_foreign_holder_panel(df)
    assert set(holder_panel["holder_group"]) == {"total", "official", "private"}
    total_dec = holder_panel[(holder_panel["holder_group"] == "total") & (holder_panel["date"].dt.strftime("%Y-%m") == "2025-12")]
    assert float(total_dec["short_term_share_slt"].iloc[0]) > 0


def test_load_shl_historical_subset():
    df = load_shl_historical_treasury_benchmark(ROOT / "tests" / "fixtures" / "shlhist_subset.csv")
    benchmark = extract_shl_total_foreign_benchmark(df)
    assert len(benchmark) == 2
    latest = benchmark[benchmark["date"].dt.strftime("%Y-%m") == "2024-06"]
    assert float(latest["shl_total_treasury_holdings"].iloc[0]) == 8549265.0


def test_build_foreign_anchor_panel_from_public_source_fixtures():
    shl = load_shl_historical_treasury_benchmark(ROOT / "tests" / "fixtures" / "shlhist_subset.csv")
    slt = load_slt_table3(ROOT / "tests" / "fixtures" / "slt_table3_subset.txt")
    panel = build_foreign_anchor_panel_from_public_sources(
        shl_benchmark_df=extract_shl_total_foreign_benchmark(shl),
        slt_holder_df=build_slt_foreign_holder_panel(slt),
    )
    total_dec = panel[(panel["holder_group"] == "total") & (panel["date"].dt.strftime("%Y-%m") == "2025-12")]
    assert float(total_dec["total_treasury_holdings"].iloc[0]) == 9270975.0
    total_jun = panel[(panel["holder_group"] == "total") & (panel["date"].dt.strftime("%Y-%m") == "2024-06")]
    assert total_jun.empty
