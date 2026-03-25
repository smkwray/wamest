from pathlib import Path

from treasury_sector_maturity.tic import (
    build_foreign_monthly_nowcast,
    build_slt_foreign_holder_panel,
    extract_shl_total_foreign_benchmark,
    load_extracted_shl_issue_mix,
    load_shl_historical_treasury_benchmark,
    load_slt_short_long,
    load_slt_table3,
)


ROOT = Path(__file__).resolve().parents[1]


def test_build_foreign_monthly_nowcast_from_manual_toy_inputs():
    shl = load_extracted_shl_issue_mix(ROOT / "data" / "examples" / "toy_shl_issue_mix.csv")
    slt = load_slt_short_long(ROOT / "data" / "examples" / "toy_slt_short_long.csv")
    panel = build_foreign_monthly_nowcast(shl, slt)

    assert set(panel["holder_group"]) == {"official", "private", "total"}
    assert len(panel) == 57
    assert (panel["uncertainty_band_type"] == "assumption_band").all()
    assert (panel["uncertainty_band_method"] == "linear_point_with_forward_backward_envelope").all()

    total_anchor = panel[(panel["holder_group"] == "total") & (panel["date"] == "2024-06-30")].iloc[0]
    assert total_anchor["has_shl_anchor"]
    assert not total_anchor["uncertainty_band_active"]
    assert total_anchor["uncertainty_support_kind"] == "direct_support"
    assert round(float(total_anchor["short_term_nominal_share_nowcast"]), 6) == 0.134
    assert round(float(total_anchor["short_term_nominal_share_nowcast_lower"]), 6) == 0.134
    assert round(float(total_anchor["short_term_nominal_share_nowcast_upper"]), 6) == 0.134
    assert round(float(total_anchor["long_term_nominal_share_nowcast"]), 6) == 0.762
    assert round(float(total_anchor["frn_share_nowcast"]), 6) == 0.013
    assert round(float(total_anchor["tips_share_nowcast"]), 6) == 0.091
    assert round(float(total_anchor["wam_years_nowcast"]), 6) == 6.3

    total_mid = panel[(panel["holder_group"] == "total") & (panel["date"] == "2025-06-30")].iloc[0]
    assert total_mid["uncertainty_band_active"]
    assert total_mid["uncertainty_support_kind"] == "two_sided_between_supports"
    assert total_mid["short_term_share_nowcast_lower"] < total_mid["short_term_share_nowcast"]
    assert total_mid["short_term_share_nowcast"] < total_mid["short_term_share_nowcast_upper"]
    assert round(float(total_mid["short_term_share_nowcast_lower"]), 6) == 0.134
    assert round(float(total_mid["short_term_share_nowcast_upper"]), 6) == round(1420.0 / 9280.0, 6)
    assert round(float(total_mid["long_term_share_nowcast_lower"]), 6) == round(1.0 - float(total_mid["short_term_share_nowcast_upper"]), 6)
    assert round(float(total_mid["long_term_share_nowcast_upper"]), 6) == round(1.0 - float(total_mid["short_term_share_nowcast_lower"]), 6)

    total_dec = panel[(panel["holder_group"] == "total") & (panel["date"] == "2025-12-31")].iloc[0]
    assert total_dec["has_slt_observation"]
    assert not total_dec["uncertainty_band_active"]
    assert total_dec["uncertainty_support_kind"] == "direct_support"
    assert round(float(total_dec["total_treasury_holdings_nowcast"]), 6) == 9280.0
    assert round(float(total_dec["total_treasury_holdings_nowcast_lower"]), 6) == 9280.0
    assert round(float(total_dec["total_treasury_holdings_nowcast_upper"]), 6) == 9280.0
    assert round(float(total_dec["short_term_value_nowcast"]), 6) == 1420.0
    assert round(float(total_dec["long_term_value_nowcast"]), 6) == 7860.0
    assert round(float(total_dec["short_term_share_nowcast"]), 6) == round(1420.0 / 9280.0, 6)
    assert round(float(total_dec["short_term_value_nowcast_lower"]), 6) == 1420.0
    assert round(float(total_dec["short_term_value_nowcast_upper"]), 6) == 1420.0


def test_build_foreign_monthly_nowcast_from_public_source_fixtures():
    shl = extract_shl_total_foreign_benchmark(
        load_shl_historical_treasury_benchmark(ROOT / "tests" / "fixtures" / "shlhist_subset.csv")
    )
    slt = build_slt_foreign_holder_panel(load_slt_table3(ROOT / "tests" / "fixtures" / "slt_table3_subset.txt"))
    panel = build_foreign_monthly_nowcast(shl, slt)

    total_jun = panel[(panel["holder_group"] == "total") & (panel["date"] == "2024-06-30")].iloc[0]
    assert total_jun["has_shl_anchor"]
    assert not total_jun["uncertainty_band_active"]
    assert total_jun["uncertainty_support_kind"] == "direct_support"
    assert round(float(total_jun["total_treasury_holdings_nowcast"]), 6) == 8549265.0

    total_dec = panel[(panel["holder_group"] == "total") & (panel["date"] == "2025-12-31")].iloc[0]
    assert total_dec["has_slt_observation"]
    assert round(float(total_dec["total_treasury_holdings_nowcast"]), 6) == 9270975.0
    assert round(float(total_dec["short_term_share_nowcast"]), 6) == round(1120975.0 / 9270975.0, 6)

    official = panel[panel["holder_group"] == "official"]
    assert official["date"].dt.strftime("%Y-%m").tolist() == ["2025-11", "2025-12"]
    assert official["has_slt_observation"].tolist() == [False, True]
    assert official["within_slt_window"].all()
    assert official["uncertainty_support_kind"].tolist() == ["one_sided_flat_fill", "direct_support"]
    assert not official["uncertainty_band_active"].any()
    assert official["wam_years_nowcast"].isna().all()
    assert official["long_term_nominal_share_nowcast"].isna().all()
    assert official["long_term_nominal_share_nowcast_lower"].isna().all()
    assert official["long_term_nominal_share_nowcast_upper"].isna().all()
