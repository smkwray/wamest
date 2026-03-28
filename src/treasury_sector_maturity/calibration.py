from __future__ import annotations

import numpy as np
import pandas as pd

from .estimation import (
    EstimationSettings,
    attach_revaluation_returns,
    fit_static_weights_with_factors,
    rolling_weight_estimates,
    weights_to_summary_metrics,
)
from .h15 import maturity_label_to_years


INTERVAL_METRIC_SPECS = {
    "effective_duration_years": {
        "exact_column": "approx_modified_duration_years",
        "estimated_column": "estimated_effective_duration_years",
        "error_column": "effective_duration_years_error",
        "abs_error_column": "effective_duration_years_abs_error",
    },
    "zero_coupon_equivalent_years": {
        "exact_column": "exact_wam_years",
        "estimated_column": "estimated_zero_coupon_equivalent_years",
        "error_column": "zero_coupon_equivalent_years_error",
        "abs_error_column": "zero_coupon_equivalent_years_abs_error",
    },
    "bill_share": {
        "exact_column": "exact_bill_share",
        "estimated_column": "estimated_bill_share",
        "error_column": "bill_share_error",
        "abs_error_column": "bill_share_abs_error",
    },
}

FED_WAM_CORRECTION_MIN_OBS = 8
FED_WAM_CORRECTION_RIDGE = 1e-6


def calibrate_fed_revaluation_mapping(
    fed_sector_panel: pd.DataFrame,
    fed_exact_metrics: pd.DataFrame,
    benchmark_returns: pd.DataFrame,
    factor_returns: pd.DataFrame | None = None,
    smoothness_penalty: float = 10.0,
    ridge_penalty: float = 0.01,
    factor_ridge_penalty: float = 0.1,
) -> dict:
    z1 = fed_sector_panel.copy()
    z1["date"] = pd.to_datetime(z1["date"])

    exact = fed_exact_metrics.copy()
    exact["date"] = pd.to_datetime(exact["date"])

    bench = benchmark_returns.copy()
    bench["date"] = pd.to_datetime(bench["date"])
    factor = None
    if factor_returns is not None and not factor_returns.empty:
        factor = factor_returns.copy()
        factor["date"] = pd.to_datetime(factor["date"])

    merged = z1.merge(exact, on="date", how="inner", suffixes=("_z1", "_exact"))
    merged = merged.merge(bench, on="date", how="inner")
    if factor is not None:
        merged = merged.merge(factor, on="date", how="inner")

    if merged.empty:
        return {"status": "empty", "n_obs": 0}

    y = merged["revaluation_return"].to_numpy(dtype=float)
    asset_cols = [c for c in bench.columns if c != "date"]
    X = merged[asset_cols].to_numpy(dtype=float)
    factor_cols = [c for c in factor.columns if c != "date"] if factor is not None else []
    F = merged[factor_cols].to_numpy(dtype=float) if factor_cols else None

    weights, factor_coefficients = fit_static_weights_with_factors(
        y,
        X,
        smoothness_penalty=smoothness_penalty,
        ridge_penalty=ridge_penalty,
        factor_returns=F,
        factor_ridge_penalty=factor_ridge_penalty,
    )
    fitted = X @ weights
    if F is not None and np.isfinite(factor_coefficients).all():
        fitted = fitted + (F @ factor_coefficients)

    summary = {
        "status": "ok",
        "n_obs": int(len(merged)),
        "asset_cols": asset_cols,
        "weights": {c: float(w) for c, w in zip(asset_cols, weights)},
        "revaluation_fit_rmse": _safe_rmse(pd.Series(y - fitted)),
        "corr_revaluation_fitted": _safe_corr(y, fitted),
    }
    if factor_cols:
        summary["factor_cols"] = factor_cols
        summary["factor_coefficients"] = {c: float(v) for c, v in zip(factor_cols, factor_coefficients)}

    if "approx_modified_duration_years" in merged.columns:
        summary["corr_revaluation_duration"] = _safe_corr(
            merged["revaluation_return"].to_numpy(dtype=float),
            merged["approx_modified_duration_years"].to_numpy(dtype=float),
        )

    if "exact_wam_years" in merged.columns:
        summary["corr_revaluation_exact_wam"] = _safe_corr(
            merged["revaluation_return"].to_numpy(dtype=float),
            merged["exact_wam_years"].to_numpy(dtype=float),
        )

    return summary


def build_fed_interval_calibration(
    fed_sector_panel: pd.DataFrame,
    fed_exact_metrics: pd.DataFrame,
    benchmark_returns: pd.DataFrame,
    factor_returns: pd.DataFrame | None = None,
    settings: EstimationSettings | None = None,
    strict_duration: bool = False,
) -> pd.DataFrame:
    settings = settings or EstimationSettings()

    z1 = fed_sector_panel.copy()
    z1["date"] = pd.to_datetime(z1["date"])
    if "revaluation_return" not in z1.columns:
        z1 = attach_revaluation_returns(z1, group_col="sector_key")

    exact = fed_exact_metrics.copy()
    exact["date"] = pd.to_datetime(exact["date"])
    exact = exact.rename(
        columns={
            "bill_share": "exact_bill_share",
            "coupon_share": "exact_coupon_share",
            "tips_share": "exact_tips_share",
        }
    )

    bench = benchmark_returns.copy()
    bench["date"] = pd.to_datetime(bench["date"])
    bench = bench.set_index("date").sort_index()
    asset_cols = list(bench.columns)
    maturity_years = {col: _parse_maturity_from_label(col) for col in asset_cols}
    factor = None
    if factor_returns is not None and not factor_returns.empty:
        factor = factor_returns.copy()
        factor["date"] = pd.to_datetime(factor["date"])
        factor = factor.set_index("date").sort_index()

    sub_series = z1.sort_values("date").set_index("date")["revaluation_return"]
    weights_df = rolling_weight_estimates(
        sub_series,
        bench,
        window=settings.rolling_window_quarters,
        smoothness_penalty=settings.smoothness_penalty,
        turnover_penalty=settings.turnover_penalty,
        ridge_penalty=settings.ridge_penalty,
        bill_share_observed=z1.set_index("date").get("bill_share_observed"),
        bill_share_penalty=settings.bill_share_penalty,
        factor_returns=factor,
        factor_ridge_penalty=settings.factor_ridge_penalty,
        factor_turnover_penalty=settings.factor_turnover_penalty,
    )
    if weights_df.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "window_obs",
                "fit_rmse_window",
                "estimated_effective_duration_years",
                "estimated_zero_coupon_equivalent_years",
                "estimated_bill_share",
                "approx_modified_duration_years",
                "exact_wam_years",
                "exact_bill_share",
                "effective_duration_years_error",
                "effective_duration_years_abs_error",
                "zero_coupon_equivalent_years_error",
                "zero_coupon_equivalent_years_abs_error",
                "bill_share_error",
                "bill_share_abs_error",
            ]
        )

    rows = []
    for _, wrow in weights_df.iterrows():
        weight_series = wrow[asset_cols]
        metrics = weights_to_summary_metrics(
            weight_series,
            maturity_years=maturity_years,
            strict_duration=strict_duration,
        )
        rows.append(
            {
                "date": pd.Timestamp(wrow["date"]),
                "window_obs": int(wrow["window_obs"]),
                "fit_rmse_window": float(wrow["fit_rmse_window"]),
                "estimated_effective_duration_years": float(metrics["effective_duration_years"]),
                "estimated_zero_coupon_equivalent_years": float(metrics["zero_coupon_equivalent_years"]),
                "estimated_bill_share": float(metrics["bill_share"]),
                "estimated_coupon_share": float(metrics["coupon_share"]),
                "estimated_tips_share": float(metrics["tips_share"]),
                "estimated_frn_share": float(metrics["frn_share"]),
                "estimated_coupon_only_maturity_years": float(metrics["coupon_only_maturity_years"]),
            }
        )

    calibration = pd.DataFrame(rows).merge(
        exact[
            [
                "date",
                "approx_modified_duration_years",
                "exact_wam_years",
                "exact_bill_share",
            ]
        ],
        on="date",
        how="left",
    )
    calibration["raw_estimated_zero_coupon_equivalent_years"] = pd.to_numeric(
        calibration["estimated_zero_coupon_equivalent_years"],
        errors="coerce",
    )
    calibration["raw_estimated_coupon_only_maturity_years"] = pd.to_numeric(
        calibration["estimated_coupon_only_maturity_years"],
        errors="coerce",
    )
    wam_correction = fit_fed_wam_correction(calibration)
    calibration = apply_fed_wam_correction(
        calibration,
        wam_correction,
        estimated_wam_col="raw_estimated_zero_coupon_equivalent_years",
        tips_share_col="estimated_tips_share",
        frn_share_col="estimated_frn_share",
        out_wam_col="estimated_zero_coupon_equivalent_years",
        coupon_share_col="estimated_coupon_share",
        bill_share_col="estimated_bill_share",
        coupon_only_out_col="estimated_coupon_only_maturity_years",
    )
    calibration["fed_wam_correction_status"] = wam_correction.get("status")
    calibration["fed_wam_correction_loocv_mae"] = wam_correction.get("loocv_mae")

    for metric_name, spec in INTERVAL_METRIC_SPECS.items():
        calibration[spec["error_column"]] = (
            pd.to_numeric(calibration[spec["estimated_column"]], errors="coerce")
            - pd.to_numeric(calibration[spec["exact_column"]], errors="coerce")
        )
        calibration[spec["abs_error_column"]] = calibration[spec["error_column"]].abs()

    return calibration.sort_values("date").reset_index(drop=True)


def fit_fed_wam_correction(
    calibration: pd.DataFrame,
    *,
    min_obs: int = FED_WAM_CORRECTION_MIN_OBS,
    ridge_penalty: float = FED_WAM_CORRECTION_RIDGE,
) -> dict:
    if calibration.empty:
        return {"status": "empty", "n_obs": 0}

    est_col = (
        "raw_estimated_zero_coupon_equivalent_years"
        if "raw_estimated_zero_coupon_equivalent_years" in calibration.columns
        else "estimated_zero_coupon_equivalent_years"
    )
    required = [est_col, "estimated_tips_share", "estimated_frn_share", "exact_wam_years"]
    if any(column not in calibration.columns for column in required):
        return {
            "status": "missing_columns",
            "n_obs": 0,
            "required_columns": required,
        }

    valid = calibration[required].apply(pd.to_numeric, errors="coerce").dropna().copy()
    if len(valid) < int(min_obs):
        return {"status": "insufficient_obs", "n_obs": int(len(valid))}

    X = np.column_stack(
        [
            np.ones(len(valid), dtype=float),
            valid[est_col].to_numpy(dtype=float),
            valid["estimated_tips_share"].to_numpy(dtype=float),
            valid["estimated_frn_share"].to_numpy(dtype=float),
        ]
    )
    y = valid["exact_wam_years"].to_numpy(dtype=float)
    coefficients = _fit_ridge_linear_model(y, X, ridge_penalty=ridge_penalty)
    fitted = X @ coefficients

    loocv_errors: list[float] = []
    for idx in range(len(valid)):
        mask = np.ones(len(valid), dtype=bool)
        mask[idx] = False
        loo_coefficients = _fit_ridge_linear_model(
            y[mask],
            X[mask],
            ridge_penalty=ridge_penalty,
        )
        loocv_errors.append(float(abs((X[idx] @ loo_coefficients) - y[idx])))

    return {
        "status": "ok",
        "n_obs": int(len(valid)),
        "ridge_penalty": float(ridge_penalty),
        "input_columns": {
            "estimated_wam": est_col,
            "tips_share": "estimated_tips_share",
            "frn_share": "estimated_frn_share",
        },
        "coefficients": {
            "intercept": float(coefficients[0]),
            "estimated_wam": float(coefficients[1]),
            "tips_share": float(coefficients[2]),
            "frn_share": float(coefficients[3]),
        },
        "train_mae": float(np.mean(np.abs(fitted - y))),
        "train_max_abs_error": float(np.max(np.abs(fitted - y))),
        "loocv_mae": float(np.mean(loocv_errors)),
        "loocv_max_abs_error": float(np.max(loocv_errors)),
    }


def apply_fed_wam_correction(
    frame: pd.DataFrame,
    correction: dict | None,
    *,
    estimated_wam_col: str,
    tips_share_col: str,
    frn_share_col: str,
    out_wam_col: str,
    coupon_share_col: str | None = None,
    bill_share_col: str | None = None,
    coupon_only_out_col: str | None = None,
    bill_wam_years: float = 0.25,
) -> pd.DataFrame:
    out = frame.copy()
    if not correction or correction.get("status") != "ok":
        return out

    coefficients = correction.get("coefficients") or {}
    required = [estimated_wam_col, tips_share_col, frn_share_col]
    if any(column not in out.columns for column in required):
        return out

    estimated_wam = pd.to_numeric(out[estimated_wam_col], errors="coerce")
    tips_share = pd.to_numeric(out[tips_share_col], errors="coerce")
    frn_share = pd.to_numeric(out[frn_share_col], errors="coerce")
    mask = estimated_wam.notna() & tips_share.notna() & frn_share.notna()
    if not bool(mask.any()):
        return out

    corrected = (
        float(coefficients.get("intercept", 0.0))
        + float(coefficients.get("estimated_wam", 1.0)) * estimated_wam
        + float(coefficients.get("tips_share", 0.0)) * tips_share
        + float(coefficients.get("frn_share", 0.0)) * frn_share
    ).clip(lower=0.0)
    out.loc[mask, out_wam_col] = corrected.loc[mask]

    if (
        coupon_only_out_col
        and coupon_share_col
        and bill_share_col
        and coupon_only_out_col in out.columns
        and coupon_share_col in out.columns
        and bill_share_col in out.columns
    ):
        coupon_share = pd.to_numeric(out[coupon_share_col], errors="coerce")
        bill_share = pd.to_numeric(out[bill_share_col], errors="coerce")
        coupon_mask = mask & coupon_share.gt(1e-8) & bill_share.notna()
        if bool(coupon_mask.any()):
            corrected_coupon = (
                corrected - (bill_share * float(bill_wam_years))
            ) / coupon_share
            out.loc[coupon_mask, coupon_only_out_col] = corrected_coupon.loc[coupon_mask]

    return out


def recenter_estimate_interval(
    frame: pd.DataFrame,
    *,
    raw_point_col: str,
    corrected_point_col: str,
    lower_col: str,
    upper_col: str,
    floor: float = 0.0,
) -> pd.DataFrame:
    out = frame.copy()
    required = [raw_point_col, corrected_point_col, lower_col, upper_col]
    if any(column not in out.columns for column in required):
        return out

    raw_point = pd.to_numeric(out[raw_point_col], errors="coerce")
    corrected_point = pd.to_numeric(out[corrected_point_col], errors="coerce")
    lower = pd.to_numeric(out[lower_col], errors="coerce")
    upper = pd.to_numeric(out[upper_col], errors="coerce")
    mask = raw_point.notna() & corrected_point.notna() & lower.notna() & upper.notna()
    if not bool(mask.any()):
        return out

    lower_gap = (raw_point - lower).clip(lower=0.0)
    upper_gap = (upper - raw_point).clip(lower=0.0)
    out.loc[mask, lower_col] = (corrected_point - lower_gap).clip(lower=floor).loc[mask]
    out.loc[mask, upper_col] = (corrected_point + upper_gap).clip(lower=floor).loc[mask]
    return out


def _fit_ridge_linear_model(
    y: np.ndarray,
    X: np.ndarray,
    *,
    ridge_penalty: float,
) -> np.ndarray:
    X = np.asarray(X, dtype=float)
    y = np.asarray(y, dtype=float)
    penalty = np.eye(X.shape[1], dtype=float) * float(ridge_penalty)
    penalty[0, 0] = 0.0
    return np.linalg.solve(X.T @ X + penalty, X.T @ y)


def summarize_interval_calibration(
    interval_calibration: pd.DataFrame,
    settings: dict | None = None,
) -> dict:
    calibration = interval_calibration.copy()
    if calibration.empty:
        return {"status": "empty", "n_obs": 0}

    cfg = {
        "abs_error_quantile": 0.9,
        "fit_rmse_reference_quantile": 0.5,
    }
    if settings:
        cfg.update(settings)

    fit_rmse = pd.to_numeric(calibration.get("fit_rmse_window"), errors="coerce")
    summary = {
        "status": "ok",
        "n_obs": int(len(calibration)),
        "abs_error_quantile": float(cfg["abs_error_quantile"]),
        "fit_rmse_reference_quantile": float(cfg["fit_rmse_reference_quantile"]),
        "fit_rmse_reference": _safe_quantile(fit_rmse, float(cfg["fit_rmse_reference_quantile"])),
        "metrics": {},
    }

    for metric_name, spec in INTERVAL_METRIC_SPECS.items():
        abs_error = pd.to_numeric(calibration.get(spec["abs_error_column"]), errors="coerce")
        signed_error = pd.to_numeric(calibration.get(spec["error_column"]), errors="coerce")
        if int(abs_error.notna().sum()) == 0:
            continue
        summary["metrics"][metric_name] = {
            "exact_column": spec["exact_column"],
            "estimated_column": spec["estimated_column"],
            "n_obs": int(abs_error.notna().sum()),
            "half_width": _safe_quantile(abs_error, float(cfg["abs_error_quantile"])),
            "median_abs_error": _safe_quantile(abs_error, 0.5),
            "mean_abs_error": _safe_mean(abs_error),
            "rmse": _safe_rmse(signed_error),
            "max_abs_error": _safe_max(abs_error),
        }

    return summary


def _safe_corr(a: np.ndarray, b: np.ndarray) -> float | None:
    mask = np.isfinite(a) & np.isfinite(b)
    if mask.sum() < 2:
        return None
    return float(np.corrcoef(a[mask], b[mask])[0, 1])


def _parse_maturity_from_label(label: str) -> float:
    return maturity_label_to_years(label)


def _safe_quantile(series: pd.Series, quantile: float) -> float | None:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return None
    return float(clean.quantile(quantile))


def _safe_mean(series: pd.Series) -> float | None:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return None
    return float(clean.mean())


def _safe_rmse(series: pd.Series) -> float | None:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return None
    return float(np.sqrt(np.mean(clean**2)))


def _safe_max(series: pd.Series) -> float | None:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return None
    return float(clean.max())
