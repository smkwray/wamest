from pathlib import Path

import numpy as np
import pandas as pd

from treasury_sector_maturity.benchmark_sets import build_estimation_benchmark_blocks
from treasury_sector_maturity.calibration import build_fed_interval_calibration, summarize_interval_calibration
from treasury_sector_maturity.estimation import (
    EstimationSettings,
    estimate_effective_maturity_panel,
    fit_static_weights,
    rolling_weight_estimates,
    weights_to_summary_metrics,
)
from treasury_sector_maturity.h15 import build_benchmark_panel, build_benchmark_returns, curve_block_config, load_h15_curve_file
from treasury_sector_maturity.soma import read_soma_holdings, summarize_soma_quarterly
from treasury_sector_maturity.tic import build_foreign_monthly_nowcast, load_extracted_shl_issue_mix, load_slt_short_long
from treasury_sector_maturity.utils import read_table


ROOT = Path(__file__).resolve().parents[1]


def _toy_multifamily_blocks() -> tuple[pd.DataFrame, pd.DataFrame]:
    holdings, factors = build_estimation_benchmark_blocks(
        series_config_path=ROOT / "configs" / "h15_series.yaml",
        provider="fed",
        holdings_families=[
            "nominal_treasury_constant_maturity",
            "tips_real_yield_constant_maturity",
            "frn_proxy_from_nominal",
        ],
        factor_families=["key_rate_buckets_from_nominal"],
        curve_file_overrides={
            "nominal_treasury_constant_maturity": ROOT / "data" / "examples" / "toy_h15_curves.csv",
            "tips_real_yield_constant_maturity": ROOT / "data" / "examples" / "toy_tips_real_yields.csv",
        },
    )
    assert factors is not None
    return holdings, factors


def _load_toy_foreign_nowcast() -> pd.DataFrame:
    shl = load_extracted_shl_issue_mix(ROOT / "data" / "examples" / "toy_shl_issue_mix.csv")
    slt = load_slt_short_long(ROOT / "data" / "examples" / "toy_slt_short_long.csv")
    return build_foreign_monthly_nowcast(shl, slt)


def _load_toy_bank_constraints() -> pd.DataFrame:
    return read_table(ROOT / "data" / "examples" / "toy_bank_constraint_panel.csv")


def _toy_bank_constraint_value(sector_key: str, quarter: pd.Timestamp, column: str) -> float:
    panel = _load_toy_bank_constraints().assign(date=lambda df: pd.to_datetime(df["date"]))
    row = panel[(panel["sector_key"] == sector_key) & (panel["date"] == quarter)].iloc[0]
    return float(row[column])


def test_toy_h15_benchmark_returns():
    curves = load_h15_curve_file(ROOT / "data" / "examples" / "toy_h15_curves.csv")
    bench = build_benchmark_returns(curves)
    assert not bench.empty
    assert {"date", "1m", "10y", "30y"}.issubset(bench.columns)


def test_coupon_benchmark_returns_have_term_variation():
    curves = load_h15_curve_file(ROOT / "data" / "examples" / "toy_h15_curves.csv")
    bench = build_benchmark_returns(curves)
    stdev = bench.drop(columns=["date"]).std()

    assert stdev["2y"] > 1e-4
    assert stdev["5y"] > stdev["2y"]
    assert stdev["10y"] > stdev["5y"]
    assert stdev["30y"] > stdev["10y"]


def test_toy_tips_benchmark_returns_have_prefixed_columns():
    curves = load_h15_curve_file(
        ROOT / "data" / "examples" / "toy_tips_real_yields.csv",
        ROOT / "configs" / "h15_series.yaml",
        curve_key="tips_real_yield_constant_maturity",
    )
    bench = build_benchmark_returns(curves)
    stdev = bench.drop(columns=["date"]).std()

    assert {"date", "tips_5y", "tips_10y", "tips_30y"}.issubset(bench.columns)
    assert stdev["tips_5y"] > 1e-4
    assert stdev["tips_10y"] > stdev["tips_5y"]
    assert stdev["tips_30y"] > stdev["tips_10y"]


def test_toy_frn_proxy_benchmark_returns_are_low_duration_and_nonzero():
    curves = load_h15_curve_file(
        ROOT / "data" / "examples" / "toy_h15_curves.csv",
        ROOT / "configs" / "h15_series.yaml",
        curve_key="nominal_treasury_constant_maturity",
    )
    curve_block = curve_block_config(ROOT / "configs" / "h15_series.yaml", "frn_proxy_from_nominal")
    bench = build_benchmark_panel(curves, curve_block=curve_block)
    nominal = build_benchmark_returns(curves)

    assert {"date", "frn_3m"}.issubset(bench.columns)
    assert bench["frn_3m"].std() > 1e-4
    assert bench["frn_3m"].std() < nominal["2y"].std()


def test_toy_key_rate_benchmark_returns_have_prefixed_term_structure():
    curves = load_h15_curve_file(
        ROOT / "data" / "examples" / "toy_h15_curves.csv",
        ROOT / "configs" / "h15_series.yaml",
        curve_key="nominal_treasury_constant_maturity",
    )
    curve_block = curve_block_config(ROOT / "configs" / "h15_series.yaml", "key_rate_buckets_from_nominal")
    bench = build_benchmark_panel(curves, curve_block=curve_block)

    stdev = bench.drop(columns=["date"]).std()
    assert {"date", "kr_2y", "kr_10y", "kr_30y"}.issubset(bench.columns)
    assert stdev["kr_2y"] > 1e-4
    assert stdev["kr_10y"] > stdev["kr_2y"]
    assert stdev["kr_30y"] > stdev["kr_10y"]


def test_toy_soma_summary():
    curves = load_h15_curve_file(ROOT / "data" / "examples" / "toy_h15_curves.csv")
    soma = read_soma_holdings(ROOT / "data" / "examples" / "toy_soma_holdings.csv")
    summary = summarize_soma_quarterly(soma, curve_df=curves)
    assert not summary.empty
    assert (summary["exact_wam_years"] > 0).all()


def test_fit_static_weights_sums_to_one():
    y = np.array([0.01, 0.02, 0.015, 0.017])
    X = np.array([
        [0.008, 0.012, 0.018],
        [0.015, 0.019, 0.024],
        [0.011, 0.014, 0.020],
        [0.013, 0.016, 0.021],
    ])
    w = fit_static_weights(y, X)
    assert np.isclose(w.sum(), 1.0, atol=1e-6)
    assert (w >= -1e-8).all()


def test_rolling_weight_estimates_skips_all_missing_target_windows():
    dates = pd.to_datetime(["2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31"])
    target = pd.Series([np.nan, np.nan, np.nan, np.nan], index=dates)
    benchmark = pd.DataFrame(
        {
            "date": dates,
            "3m": [0.001, 0.002, 0.003, 0.004],
            "5y": [0.01, 0.011, 0.012, 0.013],
        }
    ).set_index("date")

    out = rolling_weight_estimates(target, benchmark, window=4)
    assert out.empty


def test_weights_to_summary_metrics_distinguishes_bills_tips_and_frn():
    weights = pd.Series(
        {
            "3m": 0.25,
            "5y": 0.35,
            "tips_10y": 0.2,
            "frn_3m": 0.2,
        }
    )
    metrics = weights_to_summary_metrics(
        weights,
        maturity_years={
            "3m": 0.25,
            "5y": 5.0,
            "tips_10y": 10.0,
            "frn_3m": 0.25,
        },
        strict_duration=True,
    )

    assert np.isclose(metrics["bill_share"], 0.25)
    assert np.isclose(metrics["short_share_le_1y"], 0.45)
    assert np.isclose(metrics["coupon_share"], 0.75)
    assert np.isclose(metrics["tips_share"], 0.2)
    assert np.isclose(metrics["frn_share"], 0.2)
    assert pd.isna(metrics["effective_duration_years"])
    assert metrics["effective_duration_status"] == "not_separately_estimated"


def test_weights_to_summary_metrics_uses_distinct_duration_map_when_provided():
    weights = pd.Series({"2y": 0.4, "10y": 0.6})
    metrics = weights_to_summary_metrics(
        weights,
        maturity_years={"2y": 2.0, "10y": 10.0},
        duration_years={"2y": 1.8, "10y": 7.5},
    )

    assert np.isclose(metrics["zero_coupon_equivalent_years"], 6.8)
    assert np.isclose(metrics["effective_duration_years"], 5.22)
    assert metrics["effective_duration_status"] == "estimated_from_duration_map"


def test_build_estimation_benchmark_blocks_separates_holdings_and_factors():
    holdings, factors = _toy_multifamily_blocks()

    assert not holdings.empty
    assert not factors.empty
    assert {"date", "10y", "tips_10y", "frn_3m"}.issubset(holdings.columns)
    assert {"date", "kr_2y", "kr_10y", "kr_30y"}.issubset(factors.columns)
    assert set(holdings.columns).intersection(factors.columns) == {"date"}


def test_effective_maturity_panel_accepts_multi_family_holdings_and_key_rate_factors():
    sector_panel = read_table(ROOT / "data" / "examples" / "toy_sector_panel_ready.csv")
    holdings, factors = _toy_multifamily_blocks()

    result = estimate_effective_maturity_panel(
        sector_panel,
        holdings,
        factor_returns=factors,
        settings=EstimationSettings(rolling_window_quarters=4),
        foreign_nowcast=_load_toy_foreign_nowcast(),
        bank_constraints=_load_toy_bank_constraints(),
        annotation_mode="full_coverage",
    )
    assert not result.empty
    assert "frn_share" in result.columns
    assert "factor_exposure_kr_10y" in result.columns
    assert (result["tips_share"] >= 0.0).all()
    assert (result["frn_share"] >= 0.0).all()
    assert (result["tips_share"] > 0.0).any()
    assert (result["method"] == "rolling_benchmark_weights_plus_factors").all()


def test_effective_maturity_panel_from_toy_pipeline_inputs():
    sector_panel = read_table(ROOT / "data" / "examples" / "toy_sector_panel_ready.csv")
    bench, factor_bench = _toy_multifamily_blocks()
    foreign_nowcast = _load_toy_foreign_nowcast()
    bank_constraints = _load_toy_bank_constraints()
    result = estimate_effective_maturity_panel(
        sector_panel,
        bench,
        factor_returns=factor_bench,
        settings=EstimationSettings(rolling_window_quarters=4),
        foreign_nowcast=foreign_nowcast,
        bank_constraints=bank_constraints,
        annotation_mode="full_coverage",
    )
    assert not result.empty
    assert {
        "sector_key",
        "effective_duration_years",
        "effective_duration_status",
        "bill_share",
        "short_share_le_1y",
        "level_evidence_tier",
        "maturity_evidence_tier",
        "anchor_type",
        "effective_duration_years_lower",
        "effective_duration_years_upper",
        "bill_share_lower",
        "bill_share_upper",
        "short_share_le_1y_lower",
        "short_share_le_1y_upper",
        "uncertainty_support_source",
        "uncertainty_support_lower",
        "uncertainty_support_upper",
        "uncertainty_short_share_support_source",
        "uncertainty_short_share_support_lower",
        "uncertainty_short_share_support_upper",
        "identified_set_active",
        "identified_set_source",
        "identified_set_bill_share_lower",
        "identified_set_bill_share_upper",
        "identified_set_short_share_le_1y_active",
        "identified_set_short_share_le_1y_source",
        "identified_set_short_share_le_1y_lower",
        "identified_set_short_share_le_1y_upper",
        "bank_constraint_provider",
        "bank_constraint_dataset",
        "bank_constraint_raw_file",
        "factor_exposure_kr_10y",
        "point_estimate_origin",
        "interval_origin",
    }.issubset(result.columns)
    assert result["effective_duration_years"].isna().all()
    assert result["effective_duration_status"].eq("not_separately_estimated").all()
    assert result["bill_share"].nunique() > 1
    assert result["effective_duration_years_lower"].isna().all()
    assert result["effective_duration_years_upper"].isna().all()
    assert (result["bill_share_lower"] <= result["bill_share"]).all()
    assert (result["bill_share"] <= result["bill_share_upper"]).all()
    assert (result["short_share_le_1y_lower"] <= result["short_share_le_1y"]).all()
    assert (result["short_share_le_1y"] <= result["short_share_le_1y_upper"]).all()

    bank_quarter = pd.to_datetime(
        result[
            (result["sector_key"] == "bank_us_chartered")
            & (result["identified_set_source"] == "bank_constraint_panel_direct")
        ]["date"]
    ).min()
    foreign_quarter = pd.to_datetime(
        result[
            (result["sector_key"] == "foreigners_total")
            & (result["uncertainty_short_share_support_source"] == "z1_bills_lower_cap_and_foreign_short_share")
            & (pd.to_numeric(result["uncertainty_short_share_support_upper"], errors="coerce") < 1.0)
        ]["date"]
    ).min()
    expected_bank_support = _toy_bank_constraint_value("bank_us_chartered", bank_quarter, "constraint_bill_share")
    expected_bank_short_support = _toy_bank_constraint_value("bank_us_chartered", bank_quarter, "constraint_short_share_le_1y")
    expected_affiliated_support = _toy_bank_constraint_value(
        "bank_us_affiliated_areas",
        bank_quarter,
        "constraint_bill_share",
    )
    expected_affiliated_short_support = _toy_bank_constraint_value(
        "bank_us_affiliated_areas",
        bank_quarter,
        "constraint_short_share_le_1y",
    )
    expected_bank_core_support = _toy_bank_constraint_value(
        "bank_reserve_access_core",
        bank_quarter,
        "constraint_bill_share",
    )
    expected_bank_core_short_support = _toy_bank_constraint_value(
        "bank_reserve_access_core",
        bank_quarter,
        "constraint_short_share_le_1y",
    )
    expected_bank_broad_support = _toy_bank_constraint_value(
        "bank_broad_private_depositories_marketable_proxy",
        bank_quarter,
        "constraint_bill_share",
    )
    expected_bank_broad_short_support = _toy_bank_constraint_value(
        "bank_broad_private_depositories_marketable_proxy",
        bank_quarter,
        "constraint_short_share_le_1y",
    )
    fed = result[(result["sector_key"] == "fed") & (result["date"] == bank_quarter)].iloc[0]
    affiliated = result[(result["sector_key"] == "bank_us_affiliated_areas") & (result["date"] == bank_quarter)].iloc[0]
    bank = result[(result["sector_key"] == "bank_us_chartered") & (result["date"] == bank_quarter)].iloc[0]
    bank_core = result[(result["sector_key"] == "bank_reserve_access_core") & (result["date"] == bank_quarter)].iloc[0]
    bank_broad = result[
        (result["sector_key"] == "bank_broad_private_depositories_marketable_proxy") & (result["date"] == bank_quarter)
    ].iloc[0]
    foreign = result[(result["sector_key"] == "foreigners_total") & (result["date"] == foreign_quarter)].iloc[0]
    residual = result[(result["sector_key"] == "domestic_nonbank_residual_broad") & (result["date"] == bank_quarter)].iloc[0]
    assert fed["maturity_evidence_tier"] == "A"
    assert fed["anchor_type"] == "soma_calibration_context"
    assert fed["point_estimate_origin"] == "rolling_benchmark_weights_plus_factors"
    assert foreign["anchor_type"] == "shl_slt_anchor"
    assert bank["maturity_evidence_tier"] == "D"
    assert fed["uncertainty_support_source"] == "z1_bills_observed"
    assert foreign["uncertainty_support_source"] == "z1_bills_and_foreign_short_share_cap"
    assert foreign["uncertainty_short_share_support_source"] == "z1_bills_lower_cap_and_foreign_short_share"
    assert bank["identified_set_active"]
    assert bank["identified_set_source"] == "bank_constraint_panel_direct"
    assert bank["uncertainty_support_source"] == "bank_constraint_panel_direct"
    assert abs(float(bank["identified_set_bill_share_lower"]) - float(expected_bank_support)) < 1e-9
    assert abs(float(bank["identified_set_bill_share_upper"]) - float(expected_bank_support)) < 1e-9
    assert bank["identified_set_short_share_le_1y_source"] == "bank_constraint_short_share_direct"
    assert abs(float(bank["identified_set_short_share_le_1y_lower"]) - float(expected_bank_short_support)) < 1e-9
    assert abs(float(bank["identified_set_short_share_le_1y_upper"]) - float(expected_bank_short_support)) < 1e-9
    assert affiliated["identified_set_source"] == "bank_constraint_panel_direct"
    assert affiliated["bank_constraint_provider"] == "toy_supplement"
    assert affiliated["bank_constraint_dataset"] == "toy_bank_perimeter_supplement"
    assert affiliated["bank_constraint_raw_file"] == "toy_bank_constraint_supplement.csv"
    assert abs(float(affiliated["identified_set_bill_share_lower"]) - float(expected_affiliated_support)) < 1e-9
    assert abs(float(affiliated["identified_set_bill_share_upper"]) - float(expected_affiliated_support)) < 1e-9
    assert affiliated["identified_set_short_share_le_1y_source"] == "bank_constraint_short_share_direct"
    assert abs(float(affiliated["identified_set_short_share_le_1y_lower"]) - float(expected_affiliated_short_support)) < 1e-9
    assert abs(float(affiliated["identified_set_short_share_le_1y_upper"]) - float(expected_affiliated_short_support)) < 1e-9
    assert bank_core["identified_set_source"] == "bank_constraint_panel_direct"
    assert bank_core["bank_constraint_provider"] == "toy_supplement"
    assert bank_core["bank_constraint_dataset"] == "toy_bank_perimeter_supplement"
    assert abs(float(bank_core["identified_set_bill_share_lower"]) - float(expected_bank_core_support)) < 1e-9
    assert abs(float(bank_core["identified_set_bill_share_upper"]) - float(expected_bank_core_support)) < 1e-9
    assert bank_core["identified_set_short_share_le_1y_source"] == "bank_constraint_short_share_direct"
    assert abs(float(bank_core["identified_set_short_share_le_1y_lower"]) - float(expected_bank_core_short_support)) < 1e-9
    assert abs(float(bank_core["identified_set_short_share_le_1y_upper"]) - float(expected_bank_core_short_support)) < 1e-9
    assert bank_broad["identified_set_source"] == "bank_constraint_panel_direct"
    assert bank_broad["bank_constraint_provider"] == "toy_supplement"
    assert bank_broad["bank_constraint_dataset"] == "toy_bank_perimeter_supplement"
    assert abs(float(bank_broad["identified_set_bill_share_lower"]) - float(expected_bank_broad_support)) < 1e-9
    assert abs(float(bank_broad["identified_set_bill_share_upper"]) - float(expected_bank_broad_support)) < 1e-9
    assert bank_broad["identified_set_short_share_le_1y_source"] == "bank_constraint_short_share_direct"
    assert abs(float(bank_broad["identified_set_short_share_le_1y_lower"]) - float(expected_bank_broad_short_support)) < 1e-9
    assert abs(float(bank_broad["identified_set_short_share_le_1y_upper"]) - float(expected_bank_broad_short_support)) < 1e-9
    assert residual["identified_set_source"] == "residual_formula_component_set"
    assert 0.0 <= float(residual["identified_set_bill_share_lower"]) <= float(residual["identified_set_bill_share_upper"]) <= 1.0
    assert residual["identified_set_short_share_le_1y_source"] == "residual_formula_component_set"
    assert abs(float(fed["bill_share"]) - float(fed["bill_share_observed"])) < 0.05
    assert abs(float(foreign["bill_share"]) - float(foreign["bill_share_observed"])) < 0.05
    assert fed["method"] == "rolling_benchmark_weights_plus_factors"


def test_fed_interval_calibration_replaces_heuristic_band_metadata():
    sector_panel = read_table(ROOT / "data" / "examples" / "toy_sector_panel_ready.csv")
    curves = load_h15_curve_file(ROOT / "data" / "examples" / "toy_h15_curves.csv")
    soma = read_soma_holdings(ROOT / "data" / "examples" / "toy_soma_holdings.csv")
    exact_metrics = summarize_soma_quarterly(soma, curve_df=curves)
    foreign_nowcast = _load_toy_foreign_nowcast()
    bank_constraints = _load_toy_bank_constraints()
    bench, factor_bench = _toy_multifamily_blocks()
    settings = EstimationSettings(rolling_window_quarters=4)

    fed_panel = sector_panel[sector_panel["sector_key"] == "fed"].copy()
    calibration = build_fed_interval_calibration(
        fed_panel,
        exact_metrics,
        bench,
        factor_returns=factor_bench,
        settings=settings,
        strict_duration=True,
    )
    summary = summarize_interval_calibration(calibration)

    assert not calibration.empty
    assert {
        "effective_duration_years_abs_error",
        "zero_coupon_equivalent_years_abs_error",
        "bill_share_abs_error",
        "raw_estimated_zero_coupon_equivalent_years",
        "estimated_tips_share",
        "estimated_frn_share",
        "fed_wam_correction_status",
    }.issubset(calibration.columns)
    assert summary["status"] == "ok"
    assert "effective_duration_years" not in summary["metrics"]
    assert summary["metrics"]["zero_coupon_equivalent_years"]["half_width"] is not None
    assert calibration["fed_wam_correction_status"].eq("ok").all()
    raw_mae = (
        pd.to_numeric(calibration["raw_estimated_zero_coupon_equivalent_years"], errors="coerce")
        - pd.to_numeric(calibration["exact_wam_years"], errors="coerce")
    ).abs().mean()
    corrected_mae = (
        pd.to_numeric(calibration["estimated_zero_coupon_equivalent_years"], errors="coerce")
        - pd.to_numeric(calibration["exact_wam_years"], errors="coerce")
    ).abs().mean()
    assert corrected_mae <= raw_mae

    result = estimate_effective_maturity_panel(
        sector_panel,
        bench,
        factor_returns=factor_bench,
        settings=settings,
        interval_calibration=calibration,
        foreign_nowcast=foreign_nowcast,
        bank_constraints=bank_constraints,
        annotation_mode="full_coverage",
    )
    assert (result["uncertainty_band_method"] == "fed_interval_calibration_with_sector_support").all()
    assert (result["uncertainty_calibration_source"] == "fed_soma").all()

    bank_quarter = pd.to_datetime(
        result[
            (result["sector_key"] == "bank_us_chartered")
            & (result["uncertainty_support_source"] == "bank_constraint_panel_direct")
        ]["date"]
    ).min()
    foreign_quarter = pd.to_datetime(
        result[
            (result["sector_key"] == "foreigners_total")
            & (result["uncertainty_short_share_support_source"] == "z1_bills_lower_cap_and_foreign_short_share")
            & (pd.to_numeric(result["uncertainty_short_share_support_upper"], errors="coerce") < 1.0)
        ]["date"]
    ).min()
    fed = result[(result["sector_key"] == "fed") & (result["date"] == bank_quarter)].iloc[0]
    bank = result[(result["sector_key"] == "bank_us_chartered") & (result["date"] == bank_quarter)].iloc[0]
    foreign = result[(result["sector_key"] == "foreigners_total") & (result["date"] == foreign_quarter)].iloc[0]
    residual = result[(result["sector_key"] == "domestic_nonbank_residual_broad") & (result["date"] == bank_quarter)].iloc[0]
    assert fed["uncertainty_band_type"] == "support_anchored_fed_band"
    assert fed["interval_origin"] == "fed_soma_calibrated_uncertainty_band"
    assert fed["uncertainty_scale_multiplier"] == 1.0
    assert bank["uncertainty_scale_multiplier"] > fed["uncertainty_scale_multiplier"]
    assert bank["uncertainty_support_source"] == "bank_constraint_panel_direct"
    assert foreign["uncertainty_support_source"] == "z1_bills_and_foreign_short_share_cap"
    assert foreign["uncertainty_short_share_support_source"] == "z1_bills_lower_cap_and_foreign_short_share"
    assert foreign["uncertainty_support_upper"] >= foreign["bill_share_upper"]
    assert foreign["short_share_le_1y_upper"] <= foreign["uncertainty_short_share_support_upper"]
    assert residual["uncertainty_support_source"] == "residual_formula_component_set"
