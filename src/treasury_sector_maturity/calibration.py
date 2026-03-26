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

    for metric_name, spec in INTERVAL_METRIC_SPECS.items():
        calibration[spec["error_column"]] = (
            pd.to_numeric(calibration[spec["estimated_column"]], errors="coerce")
            - pd.to_numeric(calibration[spec["exact_column"]], errors="coerce")
        )
        calibration[spec["abs_error_column"]] = calibration[spec["error_column"]].abs()

    return calibration.sort_values("date").reset_index(drop=True)


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
