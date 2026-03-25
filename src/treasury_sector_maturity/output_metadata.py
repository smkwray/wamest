from __future__ import annotations

import ast
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .utils import load_yaml


def annotate_estimated_output(
    result: pd.DataFrame,
    sector_panel: pd.DataFrame,
    interval_calibration: pd.DataFrame | None = None,
    interval_settings: dict[str, Any] | None = None,
    foreign_nowcast: pd.DataFrame | None = None,
    bank_constraints: pd.DataFrame | None = None,
    sector_config_path: str | Path = "configs/sector_definitions.yaml",
) -> pd.DataFrame:
    out = result.copy()
    out["date"] = pd.to_datetime(out.get("date"), errors="coerce")

    if sector_panel.empty:
        return _apply_annotation_and_bands(out, interval_calibration=interval_calibration, interval_settings=interval_settings)

    support_panel = sector_panel.copy()
    support_panel["date"] = pd.to_datetime(support_panel.get("date"), errors="coerce")
    panel = support_panel.copy()
    merge_cols = [col for col in ["date", "sector_key", "label", "method_priority", "warnings", "bill_share_observed"] if col in panel.columns]
    if {"date", "sector_key"}.issubset(merge_cols):
        panel = panel[merge_cols].drop_duplicates(["date", "sector_key"])
        out = out.merge(panel, on=["date", "sector_key"], how="left")
    else:
        panel = panel[[col for col in ["sector_key", "label", "method_priority", "warnings"] if col in panel.columns]]
        panel = panel.drop_duplicates(["sector_key"])
        out = out.merge(panel, on=["sector_key"], how="left")

    foreign_support = _prepare_foreign_support_panel(foreign_nowcast)
    if not foreign_support.empty:
        out = out.merge(foreign_support, on=["date", "sector_key"], how="left")

    bank_support = _prepare_bank_constraint_support_panel(bank_constraints)
    if not bank_support.empty:
        out = out.merge(bank_support, on=["date", "sector_key"], how="left")

    out = _attach_identified_set_support(
        out,
        sector_panel=support_panel,
        interval_settings=interval_settings,
        sector_config_path=sector_config_path,
    )

    return _apply_annotation_and_bands(out, interval_calibration=interval_calibration, interval_settings=interval_settings)


def _prepare_foreign_support_panel(foreign_nowcast: pd.DataFrame | None) -> pd.DataFrame:
    if foreign_nowcast is None or foreign_nowcast.empty:
        return pd.DataFrame(columns=["date", "sector_key"])

    holder_map = {
        "total": "foreigners_total",
        "official": "foreigners_official",
        "private": "foreigners_private",
    }
    support = foreign_nowcast.copy()
    support["date"] = pd.to_datetime(support.get("date"), errors="coerce")
    support["sector_key"] = support.get("holder_group").map(holder_map)
    support = support[support["sector_key"].notna()].copy()
    keep_cols = [
        "date",
        "sector_key",
        "short_term_share_nowcast",
        "short_term_share_nowcast_lower",
        "short_term_share_nowcast_upper",
        "uncertainty_support_kind",
        "uncertainty_band_active",
        "has_shl_anchor",
        "has_slt_observation",
        "within_slt_window",
    ]
    keep_cols = [col for col in keep_cols if col in support.columns]
    if not {"date", "sector_key"}.issubset(keep_cols):
        return pd.DataFrame(columns=["date", "sector_key"])
    support = support[keep_cols].drop_duplicates(["date", "sector_key"])
    return support.rename(
        columns={
            "short_term_share_nowcast": "foreign_short_term_share_support_point",
            "short_term_share_nowcast_lower": "foreign_short_term_share_support_lower",
            "short_term_share_nowcast_upper": "foreign_short_term_share_support_upper",
            "uncertainty_support_kind": "foreign_support_kind",
            "uncertainty_band_active": "foreign_support_band_active",
            "has_shl_anchor": "foreign_has_shl_anchor",
            "has_slt_observation": "foreign_has_slt_observation",
            "within_slt_window": "foreign_within_slt_window",
        }
    )


def _prepare_bank_constraint_support_panel(bank_constraints: pd.DataFrame | None) -> pd.DataFrame:
    if bank_constraints is None or bank_constraints.empty:
        return pd.DataFrame(columns=["date", "sector_key"])

    support = bank_constraints.copy()
    support["date"] = pd.to_datetime(support.get("date"), errors="coerce")
    keep_cols = [
        "date",
        "sector_key",
        "constraint_bill_share",
        "constraint_short_share_le_1y",
        "share_constraints_available",
        "constraint_bucket_basis_total",
        "constraint_level",
        "n_reporters",
        "provider",
        "dataset",
        "vintage",
        "raw_file",
    ]
    keep_cols = [col for col in keep_cols if col in support.columns]
    if not {"date", "sector_key"}.issubset(keep_cols):
        return pd.DataFrame(columns=["date", "sector_key"])
    support = support[keep_cols].drop_duplicates(["date", "sector_key"])
    return support.rename(
        columns={
            "constraint_bill_share": "bank_constraint_bill_share",
            "constraint_short_share_le_1y": "bank_constraint_short_share_le_1y",
            "share_constraints_available": "bank_share_constraints_available",
            "constraint_bucket_basis_total": "bank_constraint_bucket_basis_total",
            "constraint_level": "bank_constraint_level",
            "n_reporters": "bank_constraint_n_reporters",
            "provider": "bank_constraint_provider",
            "dataset": "bank_constraint_dataset",
            "vintage": "bank_constraint_vintage",
            "raw_file": "bank_constraint_raw_file",
        }
    )


def _attach_identified_set_support(
    df: pd.DataFrame,
    sector_panel: pd.DataFrame,
    interval_settings: dict[str, Any] | None,
    sector_config_path: str | Path,
) -> pd.DataFrame:
    out = df.copy()
    out["identified_set_active"] = False
    out["identified_set_source"] = pd.NA
    out["identified_set_notes"] = pd.NA
    out["identified_set_bill_share_lower"] = pd.NA
    out["identified_set_bill_share_upper"] = pd.NA
    out["identified_set_point_clipped"] = False
    out["identified_set_bill_share_gap"] = pd.NA
    out["identified_set_short_share_le_1y_active"] = False
    out["identified_set_short_share_le_1y_source"] = pd.NA
    out["identified_set_short_share_le_1y_notes"] = pd.NA
    out["identified_set_short_share_le_1y_lower"] = pd.NA
    out["identified_set_short_share_le_1y_upper"] = pd.NA
    out["identified_set_short_share_le_1y_point_clipped"] = False
    out["identified_set_short_share_le_1y_gap"] = pd.NA

    if out.empty:
        return out

    cfg = _resolved_interval_settings(interval_settings)
    direct_intervals: dict[tuple[pd.Timestamp, str], tuple[float, float, str]] = {}
    direct_short_intervals: dict[tuple[pd.Timestamp, str], tuple[float, float, str]] = {}

    for idx, row in out.iterrows():
        lower, upper, source = _base_bill_share_support_bounds(row, cfg)
        if lower is None or upper is None:
            pass
        else:
            key = (pd.Timestamp(row["date"]).normalize(), str(row["sector_key"]))
            direct_intervals[key] = (lower, upper, source)
            out.at[idx, "identified_set_active"] = True
            out.at[idx, "identified_set_source"] = source
            out.at[idx, "identified_set_notes"] = _identified_set_notes(source)
            out.at[idx, "identified_set_bill_share_lower"] = float(lower)
            out.at[idx, "identified_set_bill_share_upper"] = float(upper)

        short_lower, short_upper, short_source = _base_short_share_support_bounds(row, cfg)
        if short_lower is None or short_upper is None:
            continue
        key = (pd.Timestamp(row["date"]).normalize(), str(row["sector_key"]))
        direct_short_intervals[key] = (short_lower, short_upper, short_source)
        out.at[idx, "identified_set_short_share_le_1y_active"] = True
        out.at[idx, "identified_set_short_share_le_1y_source"] = short_source
        out.at[idx, "identified_set_short_share_le_1y_notes"] = _identified_set_notes(short_source)
        out.at[idx, "identified_set_short_share_le_1y_lower"] = float(short_lower)
        out.at[idx, "identified_set_short_share_le_1y_upper"] = float(short_upper)

    sector_defs = (load_yaml(sector_config_path).get("sectors") or {}) if sector_config_path else {}
    if not sector_defs:
        return _apply_identified_set_projection(out)

    panel = sector_panel.copy()
    panel["date"] = pd.to_datetime(panel.get("date"), errors="coerce")
    level_lookup = {
        (pd.Timestamp(row["date"]).normalize(), str(row["sector_key"])): _as_float(row["level"])
        for _, row in panel[["date", "sector_key", "level"]].dropna(subset=["date", "sector_key"]).iterrows()
    }
    levels_by_date: dict[pd.Timestamp, dict[str, float]] = {}
    for (date, sector_key), level in level_lookup.items():
        levels_by_date.setdefault(date, {})[sector_key] = level

    interval_state_by_date: dict[pd.Timestamp, dict[str, tuple[float, float, str]]] = {}
    short_interval_state_by_date: dict[pd.Timestamp, dict[str, tuple[float, float, str]]] = {}
    for (date, sector_key), interval in direct_intervals.items():
        interval_state_by_date.setdefault(date, {})[sector_key] = interval
    for (date, sector_key), interval in direct_short_intervals.items():
        short_interval_state_by_date.setdefault(date, {})[sector_key] = interval

    for date, level_env in levels_by_date.items():
        interval_state = interval_state_by_date.setdefault(date, {})
        short_interval_state = short_interval_state_by_date.setdefault(date, {})
        for sector_key, spec in sector_defs.items():
            expression = spec.get("formula_level")
            if not expression:
                continue
            total_level = level_env.get(sector_key, np.nan)
            if math.isnan(total_level) or total_level <= 0:
                continue

            mask = (out["date"] == date) & (out["sector_key"] == sector_key)
            if sector_key not in interval_state:
                bill_level_bounds: dict[str, tuple[float, float]] = {}
                for symbol, symbol_level in level_env.items():
                    if math.isnan(symbol_level) or symbol_level < 0:
                        bill_level_bounds[symbol] = (np.nan, np.nan)
                        continue
                    if symbol in interval_state:
                        share_lower, share_upper, _ = interval_state[symbol]
                        bill_level_bounds[symbol] = (share_lower * symbol_level, share_upper * symbol_level)
                    else:
                        bill_level_bounds[symbol] = (0.0, symbol_level)

                bill_lower, bill_upper = _evaluate_interval_expression(str(expression), bill_level_bounds)
                if not math.isnan(bill_lower) and not math.isnan(bill_upper):
                    bill_lower = max(0.0, min(float(total_level), bill_lower))
                    bill_upper = max(0.0, min(float(total_level), bill_upper))
                    if bill_lower > bill_upper:
                        bill_lower, bill_upper = bill_upper, bill_lower

                    share_lower = min(1.0, max(0.0, bill_lower / float(total_level)))
                    share_upper = min(1.0, max(0.0, bill_upper / float(total_level)))
                    source = _formula_identified_set_source(sector_key)
                    interval_state[sector_key] = (share_lower, share_upper, source)

                    if mask.any():
                        out.loc[mask, "identified_set_active"] = True
                        out.loc[mask, "identified_set_source"] = source
                        out.loc[mask, "identified_set_notes"] = _identified_set_notes(source)
                        out.loc[mask, "identified_set_bill_share_lower"] = float(share_lower)
                        out.loc[mask, "identified_set_bill_share_upper"] = float(share_upper)

            if sector_key not in short_interval_state:
                short_level_bounds: dict[str, tuple[float, float]] = {}
                for symbol, symbol_level in level_env.items():
                    if math.isnan(symbol_level) or symbol_level < 0:
                        short_level_bounds[symbol] = (np.nan, np.nan)
                        continue
                    if symbol in short_interval_state:
                        short_share_lower, short_share_upper, _ = short_interval_state[symbol]
                        short_level_bounds[symbol] = (
                            short_share_lower * symbol_level,
                            short_share_upper * symbol_level,
                        )
                    else:
                        short_level_bounds[symbol] = (0.0, symbol_level)

                short_lower, short_upper = _evaluate_interval_expression(str(expression), short_level_bounds)
                if not math.isnan(short_lower) and not math.isnan(short_upper):
                    short_lower = max(0.0, min(float(total_level), short_lower))
                    short_upper = max(0.0, min(float(total_level), short_upper))
                    if short_lower > short_upper:
                        short_lower, short_upper = short_upper, short_lower

                    short_share_lower = min(1.0, max(0.0, short_lower / float(total_level)))
                    short_share_upper = min(1.0, max(0.0, short_upper / float(total_level)))
                    short_source = _formula_identified_set_source(sector_key)
                    short_interval_state[sector_key] = (short_share_lower, short_share_upper, short_source)

                    if mask.any():
                        out.loc[mask, "identified_set_short_share_le_1y_active"] = True
                        out.loc[mask, "identified_set_short_share_le_1y_source"] = short_source
                        out.loc[mask, "identified_set_short_share_le_1y_notes"] = _identified_set_notes(short_source)
                        out.loc[mask, "identified_set_short_share_le_1y_lower"] = float(short_share_lower)
                        out.loc[mask, "identified_set_short_share_le_1y_upper"] = float(short_share_upper)

    return _apply_identified_set_projection(out)


def _apply_identified_set_projection(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    mask = out["identified_set_active"].fillna(False).astype(bool)
    if not mask.any() or "bill_share" not in out.columns:
        return out

    lower = pd.to_numeric(out.loc[mask, "identified_set_bill_share_lower"], errors="coerce")
    upper = pd.to_numeric(out.loc[mask, "identified_set_bill_share_upper"], errors="coerce")
    point = pd.to_numeric(out.loc[mask, "bill_share"], errors="coerce")
    clipped = point.clip(lower=lower, upper=upper)
    point_gap = (point - clipped).abs()
    clipped_flag = point_gap > 1e-12

    out.loc[mask, "identified_set_bill_share_gap"] = point_gap
    out.loc[mask, "identified_set_point_clipped"] = clipped_flag
    out.loc[mask, "bill_share"] = clipped
    if "coupon_share" in out.columns:
        out.loc[mask, "coupon_share"] = 1.0 - pd.to_numeric(out.loc[mask, "bill_share"], errors="coerce")

    short_mask = _coerce_bool_series(out.get("identified_set_short_share_le_1y_active"))
    if short_mask.any() and "short_share_le_1y" in out.columns:
        short_lower = pd.to_numeric(out.loc[short_mask, "identified_set_short_share_le_1y_lower"], errors="coerce")
        short_upper = pd.to_numeric(out.loc[short_mask, "identified_set_short_share_le_1y_upper"], errors="coerce")
        short_point = pd.to_numeric(out.loc[short_mask, "short_share_le_1y"], errors="coerce")
        short_clipped = short_point.clip(lower=short_lower, upper=short_upper)
        short_gap = (short_point - short_clipped).abs()
        out.loc[short_mask, "identified_set_short_share_le_1y_gap"] = short_gap
        out.loc[short_mask, "identified_set_short_share_le_1y_point_clipped"] = short_gap > 1e-12
        out.loc[short_mask, "short_share_le_1y"] = short_clipped

    return out


def _apply_annotation_and_bands(
    df: pd.DataFrame,
    interval_calibration: pd.DataFrame | None = None,
    interval_settings: dict[str, Any] | None = None,
) -> pd.DataFrame:
    if df.empty:
        return df

    annotation_rows = df.apply(
        lambda row: pd.Series(_annotation_fields(row.get("sector_key"), row.get("method_priority"))),
        axis=1,
    )
    out = pd.concat([df, annotation_rows], axis=1)
    out["observed_bill_share_available"] = out.get("bill_share_observed").notna()
    out["quality_tier"] = out["maturity_evidence_tier"]

    calibration_profile = _build_interval_profile(interval_calibration, interval_settings)
    if calibration_profile is None:
        band_rows = out.apply(_heuristic_band_fields, axis=1)
    else:
        band_rows = out.apply(
            lambda row: _calibrated_band_fields(row, calibration_profile=calibration_profile),
            axis=1,
        )
    return pd.concat([out, band_rows], axis=1)


def _annotation_fields(sector_key: Any, method_priority: Any) -> dict[str, Any]:
    primary = _primary_method(method_priority)
    sector_class = _sector_class(str(sector_key or ""))
    level_basis, level_tier = _level_measurement(primary)

    maturity_basis_map = {
        "fed": "calibrated_model_inference",
        "foreign": "survey_anchored_model_inference",
        "bank": "revaluation_inference",
        "bank_proxy": "proxy_and_revaluation_inference",
        "residual": "residual_revaluation_inference",
        "narrow_proxy": "proxy_revaluation_inference",
        "aggregate": "aggregate_revaluation_inference",
        "domestic_direct": "direct_revaluation_inference",
    }
    maturity_tier_map = {
        "fed": "B",
        "foreign": "B",
        "bank": "D",
        "bank_proxy": "D",
        "residual": "D",
        "narrow_proxy": "D",
        "aggregate": "C",
        "domestic_direct": "D" if primary == "direct_z1_residual_style" else "C",
    }
    anchor_type_map = {
        "fed": "exact_soma_overlay",
        "foreign": "shl_slt_anchor",
        "bank": "revaluation_inference",
        "bank_proxy": "proxy_constraint",
        "residual": "identity_residual",
        "narrow_proxy": "proxy_identity",
        "aggregate": "aggregate_identity",
        "domestic_direct": "direct_z1_revaluation",
    }
    concept_match_map = {
        "fed": "direct",
        "foreign": "anchor_consistent",
        "bank": "partial",
        "bank_proxy": "proxy",
        "residual": "residual",
        "narrow_proxy": "proxy",
        "aggregate": "aggregate",
        "domestic_direct": "residual_style" if primary == "direct_z1_residual_style" else "direct",
    }
    coverage_ratio_map = {
        "fed": 1.0,
        "foreign": 1.0,
        "bank": 1.0,
        "bank_proxy": pd.NA,
        "residual": 1.0,
        "narrow_proxy": pd.NA,
        "aggregate": 1.0,
        "domestic_direct": 1.0,
    }
    return {
        "level_measurement_basis": level_basis,
        "maturity_measurement_basis": maturity_basis_map[sector_class],
        "level_evidence_tier": level_tier,
        "maturity_evidence_tier": maturity_tier_map[sector_class],
        "anchor_type": anchor_type_map[sector_class],
        "concept_match": concept_match_map[sector_class],
        "coverage_ratio": coverage_ratio_map[sector_class],
    }


def _heuristic_band_fields(row: pd.Series) -> pd.Series:
    sector_class = _sector_class(str(row.get("sector_key") or ""))
    duration_half_width_map = {
        "fed": 0.50,
        "foreign": 1.25,
        "bank": 2.00,
        "bank_proxy": 2.50,
        "residual": 3.00,
        "narrow_proxy": 2.50,
        "aggregate": 1.50,
        "domestic_direct": 1.75,
    }
    bill_half_width_map = {
        "fed": 0.03,
        "foreign": 0.06,
        "bank": 0.12,
        "bank_proxy": 0.15,
        "residual": 0.18,
        "narrow_proxy": 0.15,
        "aggregate": 0.08,
        "domestic_direct": 0.10,
    }

    fit_rmse = _as_float(row.get("fit_rmse_window"), default=0.0)
    window_obs = _as_float(row.get("window_obs"), default=np.nan)
    obs_multiplier = 1.25 if not math.isnan(window_obs) and window_obs < 8 else 1.0

    duration_half_width = (
        duration_half_width_map[sector_class] + min(2.5, fit_rmse * 100.0)
    ) * obs_multiplier
    bill_half_width = min(
        0.49,
        (bill_half_width_map[sector_class] + min(0.15, fit_rmse * 8.0)) * obs_multiplier,
    )

    duration = _as_float(row.get("effective_duration_years"))
    zero_coupon = _as_float(row.get("zero_coupon_equivalent_years"))
    bill_share = _as_float(row.get("bill_share"))
    support_cfg = _default_interval_settings()
    support_lower, support_upper, support_source = _bill_share_support_bounds(row, support_cfg)
    support_gap = _support_gap(bill_share, support_lower, support_upper)
    support_multiplier = _support_multiplier(
        support_gap,
        support_cfg["support_gap_weight"],
        support_cfg["support_multiplier_cap"],
    )
    duration_half_width *= support_multiplier
    bill_half_width = min(0.49, bill_half_width * support_multiplier)
    bill_lower = _bounded_lower(bill_share, bill_half_width, lower=0.0)
    bill_upper = _bounded_upper(bill_share, bill_half_width, upper=1.0)
    bill_lower, bill_upper = _apply_support_interval(bill_lower, bill_upper, support_lower, support_upper)
    short_share = _as_float(row.get("short_share_le_1y"))
    short_support_lower, short_support_upper, short_support_source = _short_share_support_bounds(row, support_cfg)
    short_support_gap = _support_gap(short_share, short_support_lower, short_support_upper)
    short_support_multiplier = _support_multiplier(
        short_support_gap,
        support_cfg["support_gap_weight"],
        support_cfg["support_multiplier_cap"],
    )
    short_half_width = min(0.49, bill_half_width * short_support_multiplier)
    short_lower = _bounded_lower(short_share, short_half_width, lower=0.0)
    short_upper = _bounded_upper(short_share, short_half_width, upper=1.0)
    short_lower, short_upper = _apply_support_interval(
        short_lower,
        short_upper,
        short_support_lower,
        short_support_upper,
    )

    return pd.Series(
        {
            "uncertainty_band_type": _heuristic_band_type(sector_class),
            "uncertainty_band_method": "sector_class_plus_fit_rmse_heuristic",
            "uncertainty_notes": _heuristic_band_notes(sector_class),
            "uncertainty_calibration_source": pd.NA,
            "uncertainty_calibration_n_obs": pd.NA,
            "uncertainty_interval_quantile": pd.NA,
            "uncertainty_scale_multiplier": 1.0,
            "uncertainty_scale_source": "heuristic_default",
            "uncertainty_fit_multiplier": pd.NA,
            "uncertainty_window_obs_multiplier": obs_multiplier,
            "uncertainty_support_source": support_source,
            "uncertainty_support_lower": _maybe_nullable_float(support_lower),
            "uncertainty_support_upper": _maybe_nullable_float(support_upper),
            "uncertainty_support_gap_bill_share": _maybe_nullable_float(support_gap),
            "uncertainty_support_multiplier": support_multiplier,
            "uncertainty_short_share_support_source": short_support_source,
            "uncertainty_short_share_support_lower": _maybe_nullable_float(short_support_lower),
            "uncertainty_short_share_support_upper": _maybe_nullable_float(short_support_upper),
            "uncertainty_short_share_support_gap": _maybe_nullable_float(short_support_gap),
            "uncertainty_short_share_support_multiplier": short_support_multiplier,
            "effective_duration_years_lower": _bounded_lower(duration, duration_half_width, lower=0.0),
            "effective_duration_years_upper": _bounded_upper(duration, duration_half_width),
            "zero_coupon_equivalent_years_lower": _bounded_lower(zero_coupon, duration_half_width, lower=0.0),
            "zero_coupon_equivalent_years_upper": _bounded_upper(zero_coupon, duration_half_width),
            "bill_share_lower": bill_lower,
            "bill_share_upper": bill_upper,
            "short_share_le_1y_lower": short_lower,
            "short_share_le_1y_upper": short_upper,
        }
    )


def _calibrated_band_fields(row: pd.Series, calibration_profile: dict[str, Any]) -> pd.Series:
    sector_class = _sector_class(str(row.get("sector_key") or ""))
    scale_multiplier, scale_source = _sector_scale_multiplier(row, calibration_profile, sector_class)
    fit_multiplier = _fit_multiplier(
        _as_float(row.get("fit_rmse_window")),
        calibration_profile.get("fit_rmse_reference"),
        calibration_profile["fit_rmse_weight"],
        calibration_profile["fit_multiplier_cap"],
    )
    obs_multiplier = _obs_multiplier(
        _as_float(row.get("window_obs")),
        calibration_profile["low_window_obs_threshold"],
        calibration_profile["low_window_obs_multiplier"],
    )
    bill_share = _as_float(row.get("bill_share"))
    support_lower, support_upper, support_source = _bill_share_support_bounds(row, calibration_profile)
    support_gap = _support_gap(bill_share, support_lower, support_upper)
    support_multiplier = _support_multiplier(
        support_gap,
        calibration_profile["support_gap_weight"],
        calibration_profile["support_multiplier_cap"],
    )
    total_multiplier = scale_multiplier * fit_multiplier * obs_multiplier * support_multiplier

    duration_half_width = calibration_profile["metrics"]["effective_duration_years"]["half_width"] * total_multiplier
    zero_coupon_half_width = calibration_profile["metrics"]["zero_coupon_equivalent_years"]["half_width"] * total_multiplier
    bill_half_width = min(
        0.49,
        calibration_profile["metrics"]["bill_share"]["half_width"] * total_multiplier,
    )
    short_share = _as_float(row.get("short_share_le_1y"))
    short_support_lower, short_support_upper, short_support_source = _short_share_support_bounds(row, calibration_profile)
    short_support_gap = _support_gap(short_share, short_support_lower, short_support_upper)
    short_support_multiplier = _support_multiplier(
        short_support_gap,
        calibration_profile["support_gap_weight"],
        calibration_profile["support_multiplier_cap"],
    )
    short_half_width = min(
        0.49,
        calibration_profile["metrics"]["bill_share"]["half_width"]
        * scale_multiplier
        * fit_multiplier
        * obs_multiplier
        * short_support_multiplier,
    )

    duration = _as_float(row.get("effective_duration_years"))
    zero_coupon = _as_float(row.get("zero_coupon_equivalent_years"))
    bill_lower = _bounded_lower(bill_share, bill_half_width, lower=0.0)
    bill_upper = _bounded_upper(bill_share, bill_half_width, upper=1.0)
    bill_lower, bill_upper = _apply_support_interval(bill_lower, bill_upper, support_lower, support_upper)
    short_lower = _bounded_lower(short_share, short_half_width, lower=0.0)
    short_upper = _bounded_upper(short_share, short_half_width, upper=1.0)
    short_lower, short_upper = _apply_support_interval(
        short_lower,
        short_upper,
        short_support_lower,
        short_support_upper,
    )

    return pd.Series(
        {
            "uncertainty_band_type": _support_aware_band_type(sector_class, support_source),
            "uncertainty_band_method": "fed_interval_calibration_with_sector_support",
            "uncertainty_notes": _support_aware_band_notes(sector_class, support_source),
            "uncertainty_calibration_source": "fed_soma",
            "uncertainty_calibration_n_obs": calibration_profile["n_obs"],
            "uncertainty_interval_quantile": calibration_profile["abs_error_quantile"],
            "uncertainty_scale_multiplier": scale_multiplier,
            "uncertainty_scale_source": scale_source,
            "uncertainty_fit_multiplier": fit_multiplier,
            "uncertainty_window_obs_multiplier": obs_multiplier,
            "uncertainty_support_source": support_source,
            "uncertainty_support_lower": _maybe_nullable_float(support_lower),
            "uncertainty_support_upper": _maybe_nullable_float(support_upper),
            "uncertainty_support_gap_bill_share": _maybe_nullable_float(support_gap),
            "uncertainty_support_multiplier": support_multiplier,
            "uncertainty_short_share_support_source": short_support_source,
            "uncertainty_short_share_support_lower": _maybe_nullable_float(short_support_lower),
            "uncertainty_short_share_support_upper": _maybe_nullable_float(short_support_upper),
            "uncertainty_short_share_support_gap": _maybe_nullable_float(short_support_gap),
            "uncertainty_short_share_support_multiplier": short_support_multiplier,
            "effective_duration_years_lower": _bounded_lower(duration, duration_half_width, lower=0.0),
            "effective_duration_years_upper": _bounded_upper(duration, duration_half_width),
            "zero_coupon_equivalent_years_lower": _bounded_lower(zero_coupon, zero_coupon_half_width, lower=0.0),
            "zero_coupon_equivalent_years_upper": _bounded_upper(zero_coupon, zero_coupon_half_width),
            "bill_share_lower": bill_lower,
            "bill_share_upper": bill_upper,
            "short_share_le_1y_lower": short_lower,
            "short_share_le_1y_upper": short_upper,
        }
    )


def _primary_method(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "unknown"
    return text.split("|", 1)[0]


def _sector_class(sector_key: str) -> str:
    if sector_key == "fed":
        return "fed"
    if sector_key.startswith("foreigners_"):
        return "foreign"
    if sector_key in {"bank_us_chartered", "bank_foreign_banking_offices_us", "bank_us_affiliated_areas"}:
        return "bank"
    if sector_key in {"credit_unions_marketable_proxy", "bank_reserve_access_core", "bank_broad_private_depositories_marketable_proxy"}:
        return "bank_proxy"
    if sector_key == "domestic_nonbank_residual_broad":
        return "residual"
    if sector_key == "deposit_user_narrow_proxy":
        return "narrow_proxy"
    if sector_key == "all_holders_total":
        return "aggregate"
    return "domestic_direct"


def _level_measurement(primary_method: str) -> tuple[str, str]:
    mapping = {
        "direct_z1": ("observed_direct", "A"),
        "direct_z1_residual_style": ("observed_residual_style", "C"),
        "computed_identity": ("computed_identity", "B"),
        "computed_from_total_minus_official": ("computed_identity", "B"),
        "computed_series_proxy": ("computed_proxy", "B"),
        "exact_soma_overlay": ("observed_security_level", "A"),
    }
    return mapping.get(primary_method, ("mixed_or_unknown", "C"))


def _build_interval_profile(
    interval_calibration: pd.DataFrame | None,
    interval_settings: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if interval_calibration is None or interval_calibration.empty:
        return None

    cfg = _default_interval_settings()
    if interval_settings:
        sector_scale = interval_settings.get("sector_scale_multipliers", {})
        sector_overrides = interval_settings.get("sector_scale_overrides", {})
        cfg.update({k: v for k, v in interval_settings.items() if k not in {"sector_scale_multipliers", "sector_scale_overrides"}})
        cfg["sector_scale_multipliers"].update(sector_scale)
        cfg["sector_scale_overrides"].update(sector_overrides)

    fit_rmse = pd.to_numeric(interval_calibration.get("fit_rmse_window"), errors="coerce")
    profile = {
        "n_obs": int(len(interval_calibration)),
        "abs_error_quantile": float(cfg["abs_error_quantile"]),
        "fit_rmse_reference": _safe_quantile(fit_rmse, float(cfg["fit_rmse_reference_quantile"])),
        "fit_rmse_weight": float(cfg["fit_rmse_weight"]),
        "fit_multiplier_cap": float(cfg["fit_multiplier_cap"]),
        "low_window_obs_threshold": int(cfg["low_window_obs_threshold"]),
        "low_window_obs_multiplier": float(cfg["low_window_obs_multiplier"]),
        "sector_scale_multipliers": dict(cfg["sector_scale_multipliers"]),
        "sector_scale_overrides": dict(cfg["sector_scale_overrides"]),
        "support_gap_weight": float(cfg["support_gap_weight"]),
        "support_multiplier_cap": float(cfg["support_multiplier_cap"]),
        "direct_bill_share_half_width": float(cfg["direct_bill_share_half_width"]),
        "metrics": {},
    }

    metric_specs = {
        "effective_duration_years": "effective_duration_years_abs_error",
        "zero_coupon_equivalent_years": "zero_coupon_equivalent_years_abs_error",
        "bill_share": "bill_share_abs_error",
    }
    for metric_name, column in metric_specs.items():
        abs_error = pd.to_numeric(interval_calibration.get(column), errors="coerce")
        half_width = _safe_quantile(abs_error, float(cfg["abs_error_quantile"]))
        if half_width is None:
            return None
        profile["metrics"][metric_name] = {
            "half_width": float(half_width),
            "n_obs": int(abs_error.notna().sum()),
        }

    return profile


def _default_interval_settings() -> dict[str, Any]:
    return {
        "abs_error_quantile": 0.9,
        "fit_rmse_reference_quantile": 0.5,
        "fit_rmse_weight": 0.5,
        "fit_multiplier_cap": 2.5,
        "low_window_obs_threshold": 8,
        "low_window_obs_multiplier": 1.25,
        "support_gap_weight": 3.0,
        "support_multiplier_cap": 2.5,
        "direct_bill_share_half_width": 0.01,
        "sector_scale_multipliers": {
            "fed": 1.0,
            "foreign": 1.5,
            "bank": 2.0,
            "bank_proxy": 2.25,
            "residual": 2.75,
            "narrow_proxy": 2.25,
            "aggregate": 1.5,
            "domestic_direct": 1.75,
        },
        "sector_scale_overrides": {},
    }


def _heuristic_band_type(sector_class: str) -> str:
    mapping = {
        "fed": "calibrated_model_error",
        "foreign": "anchor_consistent_assumption_band",
        "bank": "wide_inference_band",
        "bank_proxy": "wide_proxy_band",
        "residual": "residual_closure_band",
        "narrow_proxy": "wide_proxy_band",
        "aggregate": "aggregate_band",
        "domestic_direct": "direct_revaluation_band",
    }
    return mapping[sector_class]


def _heuristic_band_notes(sector_class: str) -> str:
    mapping = {
        "fed": "Bands are narrowest because SOMA provides the public calibration truth set.",
        "foreign": "Bands reflect survey-anchor consistency and interpolation assumptions between TIC dates.",
        "bank": "Bands remain wide because public data do not expose Treasury-only maturity ladders.",
        "bank_proxy": "Bands remain wide because both the perimeter and maturity mix are partly proxied.",
        "residual": "Bands are widest because the sector is identified as a residual after upstream blocks are specified.",
        "narrow_proxy": "Bands are wide because the sector is a proxy composition, not a direct Treasury holder census.",
        "aggregate": "Bands reflect aggregate benchmark fit rather than a directly observed maturity distribution.",
        "domestic_direct": "Bands reflect revaluation-based maturity inference rather than observed maturity ladders.",
    }
    return mapping[sector_class]


def _support_aware_band_type(sector_class: str, support_source: str) -> str:
    mapping = {
        "fed": "calibrated_model_error_band",
        "foreign": "support_anchored_foreign_band",
        "bank": "support_aware_inference_band",
        "bank_proxy": "support_aware_proxy_band",
        "residual": "support_aware_residual_band",
        "narrow_proxy": "support_aware_proxy_band",
        "aggregate": "support_aware_aggregate_band",
        "domestic_direct": "support_aware_direct_band",
    }
    if sector_class == "fed" and support_source != "none":
        return "support_anchored_fed_band"
    return mapping[sector_class]


def _support_aware_band_notes(sector_class: str, support_source: str) -> str:
    mapping = {
        "fed": "Bands use empirical SOMA recovery error quantiles from the Fed calibration harness.",
        "foreign": "Bands start from empirical SOMA recovery error quantiles, then tighten or widen against foreign short-end support from SHL/SLT and Z.1 bills shares.",
        "bank": "Bands start from empirical SOMA recovery error quantiles and widen for weaker public bank maturity observability.",
        "bank_proxy": "Bands start from empirical SOMA recovery error quantiles and widen further because both perimeter and mix are proxied.",
        "residual": "Bands start from empirical SOMA recovery error quantiles and widen most for residual-closure uncertainty.",
        "narrow_proxy": "Bands start from empirical SOMA recovery error quantiles and widen because the sector is a proxy composition.",
        "aggregate": "Bands start from empirical SOMA recovery error quantiles and widen moderately for aggregate benchmark-fit uncertainty.",
        "domestic_direct": "Bands start from empirical SOMA recovery error quantiles and widen for direct revaluation-based inference without observed ladders.",
    }
    note = mapping[sector_class]
    if support_source == "z1_bills_observed":
        return f"{note} Short-end support is anchored by observed Z.1 bills shares."
    if support_source == "foreign_nowcast_short_share":
        return f"{note} Short-end support is anchored by the TIC foreign-holder monthly short-share envelope."
    if support_source == "foreign_short_share_cap":
        return f"{note} Bill-share support is capped above by the TIC foreign-holder short-share envelope."
    if support_source == "z1_bills_and_foreign_short_share_cap":
        return f"{note} Bill-share support combines observed Z.1 bills shares with an upper cap from the TIC foreign-holder short-share envelope."
    if support_source == "z1_bills_lower_cap_and_foreign_short_share":
        return f"{note} Short-share support combines a lower bound from observed Z.1 bills shares with the TIC foreign-holder short-share envelope."
    if support_source == "bank_constraint_panel_direct":
        return f"{note} Short-end support is anchored by directly observed FFIEC Treasury bill-share buckets."
    if support_source == "bank_constraint_short_share_direct":
        return f"{note} Short-end support is anchored by directly observed FFIEC Treasury short-share buckets."
    if support_source == "bank_constraint_bill_share_lower_cap":
        return f"{note} Short-end support is bounded below by the directly observed FFIEC Treasury bill-share bucket."
    if support_source == "bank_formula_component_set":
        return f"{note} Short-end support is bounded by component bank constraints plus unknown-share extremes for uncovered banking slices."
    if support_source == "residual_formula_component_set":
        return f"{note} Short-end support is bounded by closure against total holders, Fed, foreigners, and bank component identified sets."
    return note


def _sector_scale_multiplier(
    row: pd.Series,
    calibration_profile: dict[str, Any],
    sector_class: str,
) -> tuple[float, str]:
    sector_key = str(row.get("sector_key") or "")
    overrides = calibration_profile.get("sector_scale_overrides", {})
    if sector_key in overrides:
        return float(overrides[sector_key]), "sector_override"
    return float(calibration_profile["sector_scale_multipliers"][sector_class]), "sector_class_default"


def _bill_share_support_bounds(
    row: pd.Series,
    calibration_profile: dict[str, Any],
) -> tuple[float | None, float | None, str]:
    if bool(row.get("identified_set_active", False)):
        identified_lower = _as_float(row.get("identified_set_bill_share_lower"))
        identified_upper = _as_float(row.get("identified_set_bill_share_upper"))
        if not math.isnan(identified_lower) and not math.isnan(identified_upper):
            return identified_lower, identified_upper, str(row.get("identified_set_source") or "identified_set")

    return _base_bill_share_support_bounds(row, calibration_profile)


def _short_share_support_bounds(
    row: pd.Series,
    calibration_profile: dict[str, Any],
) -> tuple[float | None, float | None, str]:
    if bool(row.get("identified_set_short_share_le_1y_active", False)):
        identified_lower = _as_float(row.get("identified_set_short_share_le_1y_lower"))
        identified_upper = _as_float(row.get("identified_set_short_share_le_1y_upper"))
        if not math.isnan(identified_lower) and not math.isnan(identified_upper):
            return (
                identified_lower,
                identified_upper,
                str(row.get("identified_set_short_share_le_1y_source") or "identified_set_short_share_le_1y"),
            )

    return _base_short_share_support_bounds(row, calibration_profile)


def _base_bill_share_support_bounds(
    row: pd.Series,
    calibration_profile: dict[str, Any],
) -> tuple[float | None, float | None, str]:
    direct_value = _as_float(row.get("bill_share_observed"))
    direct_half_width = float(calibration_profile["direct_bill_share_half_width"])
    intervals: list[tuple[float, float, str]] = []
    if not math.isnan(direct_value):
        intervals.append(
            (
                max(0.0, direct_value - direct_half_width),
                min(1.0, direct_value + direct_half_width),
                "z1_bills_observed",
            )
        )

    bank_constraint_share = _as_float(row.get("bank_constraint_bill_share"))
    if bool(row.get("bank_share_constraints_available", False)) and not math.isnan(bank_constraint_share):
        intervals.append((bank_constraint_share, bank_constraint_share, "bank_constraint_panel_direct"))

    bank_constraint_short_share = _as_float(row.get("bank_constraint_short_share_le_1y"))
    if bool(row.get("bank_share_constraints_available", False)) and math.isnan(bank_constraint_share) and not math.isnan(bank_constraint_short_share):
        intervals.append((0.0, min(1.0, bank_constraint_short_share), "bank_constraint_short_share_cap"))

    foreign_upper = _as_float(row.get("foreign_short_term_share_support_upper"))
    if not math.isnan(foreign_upper):
        intervals.append((0.0, min(1.0, foreign_upper), "foreign_short_share_cap"))

    if not intervals:
        return None, None, "none"

    lower = max(value[0] for value in intervals)
    upper = min(value[1] for value in intervals)
    if lower > upper:
        lower = min(value[0] for value in intervals)
        upper = max(value[1] for value in intervals)
    source = _combine_support_sources([value[2] for value in intervals])
    return lower, upper, source


def _base_short_share_support_bounds(
    row: pd.Series,
    calibration_profile: dict[str, Any],
) -> tuple[float | None, float | None, str]:
    direct_half_width = float(calibration_profile["direct_bill_share_half_width"])
    intervals: list[tuple[float, float, str]] = []
    has_exact_short_support = False

    bank_constraint_short_share = _as_float(row.get("bank_constraint_short_share_le_1y"))
    if bool(row.get("bank_share_constraints_available", False)) and not math.isnan(bank_constraint_short_share):
        intervals.append(
            (
                bank_constraint_short_share,
                bank_constraint_short_share,
                "bank_constraint_short_share_direct",
            )
        )
        has_exact_short_support = True

    foreign_lower = _as_float(row.get("foreign_short_term_share_support_lower"))
    foreign_upper = _as_float(row.get("foreign_short_term_share_support_upper"))
    if not math.isnan(foreign_lower) and not math.isnan(foreign_upper):
        intervals.append((foreign_lower, foreign_upper, "foreign_nowcast_short_share"))

    direct_value = _as_float(row.get("bill_share_observed"))
    if not math.isnan(direct_value) and not has_exact_short_support:
        intervals.append(
            (
                max(0.0, direct_value - direct_half_width),
                1.0,
                "z1_bills_lower_cap",
            )
        )

    bank_constraint_bill_share = _as_float(row.get("bank_constraint_bill_share"))
    if bool(row.get("bank_share_constraints_available", False)) and not math.isnan(bank_constraint_bill_share) and not has_exact_short_support:
        intervals.append((bank_constraint_bill_share, 1.0, "bank_constraint_bill_share_lower_cap"))

    if not intervals:
        return None, None, "none"

    lower = max(value[0] for value in intervals)
    upper = min(value[1] for value in intervals)
    if lower > upper:
        lower = min(value[0] for value in intervals)
        upper = max(value[1] for value in intervals)
    source = _combine_support_sources([value[2] for value in intervals])
    return lower, upper, source


def _resolved_interval_settings(interval_settings: dict[str, Any] | None) -> dict[str, Any]:
    cfg = _default_interval_settings()
    if interval_settings:
        sector_scale = interval_settings.get("sector_scale_multipliers", {})
        sector_overrides = interval_settings.get("sector_scale_overrides", {})
        cfg.update({k: v for k, v in interval_settings.items() if k not in {"sector_scale_multipliers", "sector_scale_overrides"}})
        cfg["sector_scale_multipliers"].update(sector_scale)
        cfg["sector_scale_overrides"].update(sector_overrides)
    return cfg


def _combine_support_sources(sources: list[str]) -> str:
    normalized = sorted({str(source) for source in sources if source and source != "none"})
    if not normalized:
        return "none"
    if normalized == ["foreign_short_share_cap", "z1_bills_observed"]:
        return "z1_bills_and_foreign_short_share_cap"
    if normalized == ["foreign_nowcast_short_share", "z1_bills_lower_cap"]:
        return "z1_bills_lower_cap_and_foreign_short_share"
    if len(normalized) == 1:
        return normalized[0]
    return "_and_".join(normalized)


def _formula_identified_set_source(sector_key: str) -> str:
    sector_class = _sector_class(str(sector_key or ""))
    if sector_class in {"bank", "bank_proxy"}:
        return "bank_formula_component_set"
    if sector_class == "residual":
        return "residual_formula_component_set"
    if sector_class == "narrow_proxy":
        return "proxy_formula_component_set"
    return "formula_component_set"


def _identified_set_notes(source: str) -> str:
    mapping = {
        "z1_bills_observed": "Bill-share identified set comes from the observed Z.1 bills share with a small measurement tolerance.",
        "foreign_short_share_cap": "Bill-share identified set is capped above by the TIC foreign-holder short-term share envelope because bills are only a subset of foreign holdings under one year.",
        "z1_bills_and_foreign_short_share_cap": "Bill-share identified set combines observed Z.1 bills shares with an upper cap from the TIC foreign-holder short-term share envelope.",
        "bank_constraint_panel_direct": "Bill-share identified set comes directly from observed FFIEC Treasury maturity buckets.",
        "bank_constraint_short_share_cap": "Bill-share identified set is only capped above by the observed short-share constraint because no bill-specific bucket is available.",
        "bank_constraint_short_share_direct": "Short-share identified set comes directly from observed FFIEC Treasury short-maturity buckets.",
        "foreign_nowcast_short_share": "Short-share identified set comes directly from the TIC foreign-holder short-term share envelope.",
        "z1_bills_lower_cap": "Short-share identified set is bounded below by the observed Z.1 bills share with a small measurement tolerance.",
        "z1_bills_lower_cap_and_foreign_short_share": "Short-share identified set combines a lower bound from observed Z.1 bills shares with the TIC foreign-holder short-term share envelope.",
        "bank_constraint_bill_share_lower_cap": "Short-share identified set is bounded below by the directly observed FFIEC bill-share bucket.",
        "bank_formula_component_set": "Bill-share identified set is derived by identity from component bank sectors with observed constraints where available and [0,1] extremes where not.",
        "residual_formula_component_set": "Bill-share identified set is derived by residual closure against total holders and upstream sector support sets.",
        "proxy_formula_component_set": "Bill-share identified set is derived by identity from component proxy sectors.",
        "formula_component_set": "Bill-share identified set is derived by identity from component sector support sets.",
    }
    return mapping.get(source, "Bill-share identified set is derived from the available support inputs for this sector.")


class _ExpressionIntervalEvaluator(ast.NodeVisitor):
    def __init__(self, env: dict[str, tuple[float, float]]) -> None:
        self.env = env

    def visit_Expression(self, node: ast.Expression) -> tuple[float, float]:
        return self.visit(node.body)

    def visit_Name(self, node: ast.Name) -> tuple[float, float]:
        if node.id not in self.env:
            raise KeyError(f"Unknown symbol in expression: {node.id}")
        return self.env[node.id]

    def visit_Constant(self, node: ast.Constant) -> tuple[float, float]:
        value = float(node.value)
        return value, value

    def visit_BinOp(self, node: ast.BinOp) -> tuple[float, float]:
        left = self.visit(node.left)
        right = self.visit(node.right)

        if isinstance(node.op, ast.Add):
            return left[0] + right[0], left[1] + right[1]
        if isinstance(node.op, ast.Sub):
            return left[0] - right[1], left[1] - right[0]
        if isinstance(node.op, ast.Mult):
            values = [left[0] * right[0], left[0] * right[1], left[1] * right[0], left[1] * right[1]]
            return min(values), max(values)
        if isinstance(node.op, ast.Div):
            if right[0] <= 0 <= right[1]:
                raise ZeroDivisionError("Interval division crosses zero.")
            values = [left[0] / right[0], left[0] / right[1], left[1] / right[0], left[1] / right[1]]
            return min(values), max(values)

        raise TypeError(f"Unsupported operator: {ast.dump(node.op)}")

    def visit_UnaryOp(self, node: ast.UnaryOp) -> tuple[float, float]:
        lower, upper = self.visit(node.operand)
        if isinstance(node.op, ast.USub):
            return -upper, -lower
        if isinstance(node.op, ast.UAdd):
            return lower, upper
        raise TypeError(f"Unsupported unary operator: {ast.dump(node.op)}")

    def generic_visit(self, node: ast.AST) -> tuple[float, float]:
        raise TypeError(f"Unsupported expression element: {ast.dump(node)}")


def _evaluate_interval_expression(expression: str, env: dict[str, tuple[float, float]]) -> tuple[float, float]:
    tree = ast.parse(expression, mode="eval")
    return _ExpressionIntervalEvaluator(env).visit(tree)
    return None, None, "none"


def _support_gap(value: float, support_lower: float | None, support_upper: float | None) -> float:
    if math.isnan(value) or support_lower is None or support_upper is None:
        return 0.0
    if value < support_lower:
        return float(support_lower - value)
    if value > support_upper:
        return float(value - support_upper)
    return 0.0


def _support_multiplier(gap: float, weight: float, cap: float) -> float:
    if gap <= 0:
        return 1.0
    return min(cap, 1.0 + weight * gap)


def _apply_support_interval(
    interval_lower: float | pd.NA,
    interval_upper: float | pd.NA,
    support_lower: float | None,
    support_upper: float | None,
) -> tuple[float | pd.NA, float | pd.NA]:
    if support_lower is None or support_upper is None:
        return interval_lower, interval_upper
    if pd.isna(interval_lower) or pd.isna(interval_upper):
        return support_lower, support_upper
    if interval_lower <= support_upper and support_lower <= interval_upper:
        return max(float(interval_lower), support_lower), min(float(interval_upper), support_upper)
    return min(float(interval_lower), support_lower), max(float(interval_upper), support_upper)


def _fit_multiplier(
    fit_rmse: float,
    fit_rmse_reference: float | None,
    fit_rmse_weight: float,
    fit_multiplier_cap: float,
) -> float:
    if math.isnan(fit_rmse) or fit_rmse_reference is None or fit_rmse_reference <= 0:
        return 1.0
    ratio = max(1.0, fit_rmse / fit_rmse_reference)
    adjusted = 1.0 + fit_rmse_weight * (ratio - 1.0)
    return min(fit_multiplier_cap, adjusted)


def _obs_multiplier(window_obs: float, threshold: int, low_obs_multiplier: float) -> float:
    if math.isnan(window_obs):
        return 1.0
    return low_obs_multiplier if window_obs < threshold else 1.0


def _safe_quantile(series: pd.Series, quantile: float) -> float | None:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return None
    return float(clean.quantile(quantile))


def _coerce_bool_series(series: pd.Series | None) -> pd.Series:
    if series is None:
        return pd.Series(dtype=bool)
    if pd.api.types.is_bool_dtype(series):
        return pd.Series(series).fillna(False).astype(bool)
    mapped = (
        pd.Series(series)
        .astype("string")
        .str.strip()
        .str.lower()
        .map({"true": True, "false": False, "1": True, "0": False, "yes": True, "no": False})
    )
    return mapped.fillna(False).astype(bool)


def _as_float(value: Any, default: float = np.nan) -> float:
    try:
        parsed = float(value)
    except Exception:
        return default
    return parsed if math.isfinite(parsed) else default


def _maybe_nullable_float(value: float | None) -> float | pd.NA:
    if value is None or math.isnan(value):
        return pd.NA
    return float(value)


def _bounded_lower(value: float, half_width: float, lower: float = 0.0) -> float | pd.NA:
    if math.isnan(value):
        return pd.NA
    return max(lower, value - half_width)


def _bounded_upper(value: float, half_width: float, upper: float | None = None) -> float | pd.NA:
    if math.isnan(value):
        return pd.NA
    candidate = value + half_width
    if upper is None:
        return candidate
    return min(upper, candidate)
