from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd
from scipy.optimize import minimize

from .output_metadata import annotate_estimated_output


def compute_exposure_base(
    lag_level: pd.Series,
    transactions: pd.Series | None = None,
    other_volume: pd.Series | None = None,
    method: str = "midpoint",
    floor: float = 1e-6,
) -> pd.Series:
    lag_level = lag_level.astype(float)
    tx = transactions.astype(float) if transactions is not None else pd.Series(0.0, index=lag_level.index)
    ov = other_volume.astype(float) if other_volume is not None else pd.Series(0.0, index=lag_level.index)

    if method == "midpoint":
        base = lag_level + 0.5 * (tx.fillna(0.0) + ov.fillna(0.0))
    elif method == "lag":
        base = lag_level
    else:
        raise ValueError(f"Unsupported exposure base method: {method}")

    return base.abs().clip(lower=floor)


def attach_revaluation_returns(
    panel: pd.DataFrame,
    group_col: str = "sector_key",
    level_col: str = "level",
    transactions_col: str = "transactions",
    revaluation_col: str = "revaluation",
    other_volume_col: str = "other_volume",
    method: str = "midpoint",
    floor: float = 1e-6,
) -> pd.DataFrame:
    panel = panel.copy()
    panel["date"] = pd.to_datetime(panel["date"])
    panel = panel.sort_values([group_col, "date"]).copy()
    panel["lag_level"] = panel.groupby(group_col)[level_col].shift(1)
    panel["exposure_base"] = compute_exposure_base(
        panel["lag_level"],
        panel[transactions_col],
        panel[other_volume_col],
        method=method,
        floor=floor,
    )
    panel["revaluation_return"] = panel[revaluation_col] / panel["exposure_base"]
    return panel


def merge_benchmark_return_panels(*panels: pd.DataFrame) -> pd.DataFrame:
    merged: pd.DataFrame | None = None

    for panel in panels:
        if panel is None or panel.empty:
            continue

        current = panel.copy()
        if "date" not in current.columns:
            raise ValueError("Benchmark return panels must include a date column.")
        current["date"] = pd.to_datetime(current["date"], errors="coerce")
        if current["date"].isna().any():
            raise ValueError("Benchmark return panels contain invalid dates.")

        overlap = set() if merged is None else (set(merged.columns) & set(current.columns)) - {"date"}
        if overlap:
            raise ValueError(f"Benchmark return panels contain overlapping columns: {', '.join(sorted(overlap))}")

        current = current.sort_values("date").drop_duplicates(["date"])
        merged = current if merged is None else merged.merge(current, on="date", how="outer")

    if merged is None:
        return pd.DataFrame(columns=["date"])
    return merged.sort_values("date").reset_index(drop=True)


def _benchmark_family_and_core_label(label: str) -> tuple[str, str]:
    text = str(label).strip().lower()
    for prefix, family in [("tips_", "tips"), ("frn_", "frn"), ("kr_", "key_rate")]:
        if text.startswith(prefix):
            return family, text[len(prefix) :]
    return "nominal", text


def _difference_penalty_matrix(labels: list[str]) -> np.ndarray:
    n = len(labels)
    if n <= 1:
        return np.zeros((0, n))

    rows: list[np.ndarray] = []
    families = [_benchmark_family_and_core_label(label)[0] for label in labels]
    for i in range(n - 1):
        if families[i] != families[i + 1]:
            continue
        row = np.zeros(n)
        row[i] = 1.0
        row[i + 1] = -1.0
        rows.append(row)

    if not rows:
        return np.zeros((0, n))
    return np.vstack(rows)


def fit_static_weights(
    target_returns: np.ndarray,
    benchmark_returns: np.ndarray,
    smoothness_penalty: float = 10.0,
    ridge_penalty: float = 0.01,
    previous_weights: np.ndarray | None = None,
    turnover_penalty: float = 0.0,
    bill_asset_mask: np.ndarray | None = None,
    bill_share_target: float | None = None,
    bill_share_penalty: float = 0.0,
    difference_matrix: np.ndarray | None = None,
) -> np.ndarray:
    weights, _ = fit_static_weights_with_factors(
        target_returns,
        benchmark_returns,
        smoothness_penalty=smoothness_penalty,
        ridge_penalty=ridge_penalty,
        previous_weights=previous_weights,
        turnover_penalty=turnover_penalty,
        bill_asset_mask=bill_asset_mask,
        bill_share_target=bill_share_target,
        bill_share_penalty=bill_share_penalty,
        difference_matrix=difference_matrix,
    )
    return weights


def fit_static_weights_with_factors(
    target_returns: np.ndarray,
    benchmark_returns: np.ndarray,
    smoothness_penalty: float = 10.0,
    ridge_penalty: float = 0.01,
    previous_weights: np.ndarray | None = None,
    turnover_penalty: float = 0.0,
    bill_asset_mask: np.ndarray | None = None,
    bill_share_target: float | None = None,
    bill_share_penalty: float = 0.0,
    difference_matrix: np.ndarray | None = None,
    factor_returns: np.ndarray | None = None,
    previous_factor_coefficients: np.ndarray | None = None,
    factor_ridge_penalty: float = 0.1,
    factor_turnover_penalty: float = 0.0,
) -> tuple[np.ndarray, np.ndarray]:
    y = np.asarray(target_returns, dtype=float)
    X = np.asarray(benchmark_returns, dtype=float)
    F = (
        np.asarray(factor_returns, dtype=float)
        if factor_returns is not None
        else np.empty((len(y), 0), dtype=float)
    )

    mask = np.isfinite(y) & np.isfinite(X).all(axis=1)
    if F.size:
        mask &= np.isfinite(F).all(axis=1)
    y = y[mask]
    X = X[mask]
    if F.size:
        F = F[mask]
    else:
        F = np.empty((len(y), 0), dtype=float)

    n_assets = X.shape[1]
    n_factors = F.shape[1]
    if y.size == 0 or X.size == 0:
        return np.full(n_assets, np.nan), np.full(n_factors, np.nan)
    if y.size < 2:
        return np.repeat(1.0 / n_assets, n_assets), np.zeros(n_factors, dtype=float)

    D = _difference_penalty_matrix([str(i) for i in range(n_assets)]) if difference_matrix is None else np.asarray(difference_matrix, dtype=float)
    prev = previous_weights if previous_weights is not None else np.repeat(1.0 / n_assets, n_assets)
    prev_factor = (
        np.asarray(previous_factor_coefficients, dtype=float)
        if previous_factor_coefficients is not None
        else np.zeros(n_factors, dtype=float)
    )
    if bill_asset_mask is None:
        bill_mask = np.zeros(n_assets, dtype=bool)
    else:
        bill_mask = np.asarray(bill_asset_mask, dtype=bool)
        if bill_mask.shape != (n_assets,):
            raise ValueError("bill_asset_mask must have the same length as the number of benchmark assets.")
    bill_target = float(bill_share_target) if bill_share_target is not None else np.nan

    def objective(theta: np.ndarray) -> float:
        w = theta[:n_assets]
        factor_coefficients = theta[n_assets:]
        fitted = X @ w
        if n_factors:
            fitted = fitted + (F @ factor_coefficients)
        residual = y - fitted
        value = float(residual @ residual)

        if smoothness_penalty > 0.0 and D.size:
            value += float(smoothness_penalty * ((D @ w) @ (D @ w)))

        if ridge_penalty > 0.0:
            value += float(ridge_penalty * (w @ w))

        if turnover_penalty > 0.0 and previous_weights is not None:
            diff = w - prev
            value += float(turnover_penalty * (diff @ diff))

        if factor_ridge_penalty > 0.0 and n_factors:
            value += float(factor_ridge_penalty * (factor_coefficients @ factor_coefficients))

        if factor_turnover_penalty > 0.0 and n_factors and previous_factor_coefficients is not None:
            diff = factor_coefficients - prev_factor
            value += float(factor_turnover_penalty * (diff @ diff))

        if bill_share_penalty > 0.0 and bill_mask.any() and np.isfinite(bill_target):
            bill_gap = float(np.sum(w[bill_mask]) - bill_target)
            value += float(bill_share_penalty * (bill_gap * bill_gap))

        return value

    x0 = np.concatenate([np.repeat(1.0 / n_assets, n_assets), np.zeros(n_factors, dtype=float)])
    bounds = [(0.0, 1.0) for _ in range(n_assets)] + [(None, None) for _ in range(n_factors)]
    constraints = [{"type": "eq", "fun": lambda theta: float(np.sum(theta[:n_assets]) - 1.0)}]

    result = minimize(objective, x0=x0, bounds=bounds, constraints=constraints, method="SLSQP")
    if not result.success:
        return x0[:n_assets], x0[n_assets:]
    return result.x[:n_assets], result.x[n_assets:]


def rolling_weight_estimates(
    target_returns: pd.Series,
    benchmark_returns: pd.DataFrame,
    window: int = 12,
    smoothness_penalty: float = 10.0,
    turnover_penalty: float = 2.0,
    ridge_penalty: float = 0.01,
    bill_share_observed: pd.Series | None = None,
    bill_share_penalty: float = 0.0,
    factor_returns: pd.DataFrame | None = None,
    factor_ridge_penalty: float = 0.1,
    factor_turnover_penalty: float = 0.0,
) -> pd.DataFrame:
    target_returns = target_returns.sort_index()
    benchmark_returns = benchmark_returns.sort_index()

    dates = target_returns.index.intersection(benchmark_returns.index)
    factor_cols: list[str] = []
    if factor_returns is not None and not factor_returns.empty:
        factor_returns = factor_returns.sort_index()
        dates = dates.intersection(factor_returns.index)
        factor_cols = list(factor_returns.columns)
    target_returns = target_returns.loc[dates]
    benchmark_returns = benchmark_returns.loc[dates]
    assets = list(benchmark_returns.columns)
    support_series = None
    if bill_share_observed is not None:
        support_series = pd.to_numeric(bill_share_observed, errors="coerce").sort_index().reindex(dates)

    rows = []
    prev: pd.Series | None = None
    prev_factor: pd.Series | None = None

    for idx in range(len(dates)):
        if idx + 1 < window:
            continue

        window_dates = dates[idx + 1 - window : idx + 1]
        y = target_returns.loc[window_dates].values
        window_bench = benchmark_returns.loc[window_dates, assets]
        available_assets = [asset for asset in assets if window_bench[asset].notna().all()]
        if not available_assets:
            continue

        difference_matrix = _difference_penalty_matrix(available_assets)
        bill_mask = np.array([_is_bill_asset(asset) for asset in available_assets], dtype=bool)
        X = window_bench.loc[:, available_assets].values
        F = factor_returns.loc[window_dates, factor_cols].values if factor_cols else None
        previous_weights = None
        if prev is not None:
            previous_weights = prev.reindex(available_assets).fillna(0.0).to_numpy()
        previous_factor_coefficients = None
        if prev_factor is not None and factor_cols:
            previous_factor_coefficients = prev_factor.reindex(factor_cols).fillna(0.0).to_numpy()

        weights, factor_coefficients = fit_static_weights_with_factors(
            y,
            X,
            smoothness_penalty=smoothness_penalty,
            ridge_penalty=ridge_penalty,
            previous_weights=previous_weights,
            turnover_penalty=turnover_penalty,
            bill_asset_mask=bill_mask,
            bill_share_target=(
                None
                if support_series is None or pd.isna(support_series.loc[dates[idx]])
                else float(support_series.loc[dates[idx]])
            ),
            bill_share_penalty=bill_share_penalty,
            difference_matrix=difference_matrix,
            factor_returns=F,
            previous_factor_coefficients=previous_factor_coefficients,
            factor_ridge_penalty=factor_ridge_penalty,
            factor_turnover_penalty=factor_turnover_penalty,
        )

        valid_mask = np.isfinite(y) & np.isfinite(X).all(axis=1)
        if F is not None:
            valid_mask &= np.isfinite(F).all(axis=1)
        y_valid = y[valid_mask]
        X_valid = X[valid_mask]
        F_valid = F[valid_mask] if F is not None else None
        fit_rmse = np.nan
        if y_valid.size and np.isfinite(weights).all():
            fitted = X_valid @ weights
            if F_valid is not None and np.isfinite(factor_coefficients).all():
                fitted = fitted + (F_valid @ factor_coefficients)
            fit_rmse = float(np.sqrt(np.mean((y_valid - fitted) ** 2)))

        if np.isfinite(weights).all():
            prev = pd.Series(0.0, index=assets, dtype=float)
            prev.loc[available_assets] = weights
        if factor_cols and np.isfinite(factor_coefficients).all():
            prev_factor = pd.Series(factor_coefficients, index=factor_cols, dtype=float)
        row = {
            "date": dates[idx],
            "window_obs": int(y_valid.size),
            "fit_rmse_window": fit_rmse,
        }
        row.update({asset: 0.0 for asset in assets})
        row.update({asset: weight for asset, weight in zip(available_assets, weights)})
        row.update({factor: coefficient for factor, coefficient in zip(factor_cols, factor_coefficients)})
        rows.append(row)

    return pd.DataFrame(rows)


def weights_to_summary_metrics(
    weights: pd.Series,
    maturity_years: dict[str, float] | pd.Series,
    duration_years: dict[str, float] | pd.Series | None = None,
) -> dict[str, float]:
    weights = weights.astype(float)
    maturities = pd.Series(maturity_years, dtype=float).reindex(weights.index)

    if duration_years is None:
        durations = maturities.copy()
    else:
        durations = pd.Series(duration_years, dtype=float).reindex(weights.index)

    labels = pd.Index(weights.index.astype(str))
    tips_mask = labels.str.contains("tips", case=False, regex=False)
    frn_mask = labels.str.contains("frn", case=False, regex=False)
    key_rate_mask = labels.str.contains("kr_", case=False, regex=False)
    bill_mask = (maturities <= 1.0) & ~tips_mask & ~frn_mask & ~key_rate_mask
    asset_like_mask = ~key_rate_mask
    short_share_mask = (maturities <= 1.0) & asset_like_mask

    coupon_share = float(weights[asset_like_mask & ~bill_mask].sum()) if asset_like_mask.any() else np.nan
    bill_share = float(weights[bill_mask].sum()) if bill_mask.any() else 0.0
    short_share_le_1y = float(weights[short_share_mask].sum()) if short_share_mask.any() else 0.0

    nonbill_equiv = np.nan
    if coupon_share > 0:
        nonbill_equiv = float((weights[asset_like_mask & ~bill_mask] * maturities[asset_like_mask & ~bill_mask]).sum() / coupon_share)

    out = {
        "bill_share": bill_share,
        "short_share_le_1y": short_share_le_1y,
        "coupon_share": coupon_share,
        "tips_share": float(weights[tips_mask].sum()) if tips_mask.any() else 0.0,
        "frn_share": float(weights[frn_mask].sum()) if frn_mask.any() else 0.0,
        "effective_duration_years": float((weights * durations).sum()),
        "zero_coupon_equivalent_years": float((weights * maturities).sum()),
        "coupon_only_maturity_years": nonbill_equiv,
    }
    return out


@dataclass
class EstimationSettings:
    rolling_window_quarters: int = 12
    smoothness_penalty: float = 10.0
    turnover_penalty: float = 2.0
    ridge_penalty: float = 0.01
    bill_share_penalty: float = 100.0
    factor_ridge_penalty: float = 0.1
    factor_turnover_penalty: float = 0.0


def estimate_effective_maturity_panel(
    sector_panel: pd.DataFrame,
    benchmark_returns: pd.DataFrame,
    factor_returns: pd.DataFrame | None = None,
    settings: EstimationSettings | None = None,
    sectors: Iterable[str] | None = None,
    interval_calibration: pd.DataFrame | None = None,
    interval_settings: dict | None = None,
    foreign_nowcast: pd.DataFrame | None = None,
    bank_constraints: pd.DataFrame | None = None,
    sector_config_path: str = "configs/sector_definitions.yaml",
) -> pd.DataFrame:
    settings = settings or EstimationSettings()
    sector_panel = attach_revaluation_returns(sector_panel, group_col="sector_key")
    bank_constraints_panel = None
    if bank_constraints is not None and not bank_constraints.empty:
        bank_constraints_panel = bank_constraints.copy()
        bank_constraints_panel["date"] = pd.to_datetime(bank_constraints_panel.get("date"), errors="coerce")

    benchmark_returns = benchmark_returns.copy()
    benchmark_returns["date"] = pd.to_datetime(benchmark_returns["date"])
    benchmark_returns = benchmark_returns.set_index("date").sort_index()
    maturity_years = {col: _parse_maturity_from_label(col) for col in benchmark_returns.columns}
    factor_block = None
    factor_cols: list[str] = []
    if factor_returns is not None and not factor_returns.empty:
        factor_block = factor_returns.copy()
        factor_block["date"] = pd.to_datetime(factor_block["date"])
        factor_block = factor_block.set_index("date").sort_index()
        factor_cols = list(factor_block.columns)

    rows = []
    sector_keys = list(sector_panel["sector_key"].dropna().unique())
    if sectors is not None:
        requested = set(sectors)
        sector_keys = [s for s in sector_keys if s in requested]

    for sector in sector_keys:
        sub = sector_panel[sector_panel["sector_key"] == sector].copy()
        sub = sub.sort_values("date")
        sub_series = sub.set_index("date")["revaluation_return"]
        bill_share_observed = None
        if "bill_share_observed" in sub.columns:
            bill_share_observed = sub.set_index("date")["bill_share_observed"]
        if bank_constraints_panel is not None and {"sector_key", "constraint_bill_share"}.issubset(bank_constraints_panel.columns):
            bank_sub = bank_constraints_panel[bank_constraints_panel["sector_key"] == sector].copy()
            if not bank_sub.empty:
                if "share_constraints_available" in bank_sub.columns:
                    bank_sub = bank_sub[bank_sub["share_constraints_available"].fillna(False)]
                bank_support = (
                    bank_sub.dropna(subset=["date"])
                    .drop_duplicates(["date"])
                    .set_index("date")["constraint_bill_share"]
                    .pipe(pd.to_numeric, errors="coerce")
                )
                if not bank_support.empty:
                    if bill_share_observed is None:
                        bill_share_observed = bank_support
                    else:
                        bill_share_observed = bill_share_observed.combine_first(bank_support)
        asset_cols = list(benchmark_returns.columns)

        weights_df = rolling_weight_estimates(
            sub_series,
            benchmark_returns,
            window=settings.rolling_window_quarters,
            smoothness_penalty=settings.smoothness_penalty,
            turnover_penalty=settings.turnover_penalty,
            ridge_penalty=settings.ridge_penalty,
            bill_share_observed=bill_share_observed,
            bill_share_penalty=settings.bill_share_penalty,
            factor_returns=factor_block,
            factor_ridge_penalty=settings.factor_ridge_penalty,
            factor_turnover_penalty=settings.factor_turnover_penalty,
        )

        if weights_df.empty:
            continue

        for _, wrow in weights_df.iterrows():
            weight_series = wrow[asset_cols]
            metrics = weights_to_summary_metrics(weight_series, maturity_years=maturity_years)
            rows.append(
                {
                    "date": pd.Timestamp(wrow["date"]),
                    "sector_key": sector,
                    **metrics,
                    "method": "rolling_benchmark_weights_plus_factors" if factor_cols else "rolling_benchmark_weights",
                    "window_obs": int(wrow["window_obs"]),
                    "fit_rmse_window": float(wrow["fit_rmse_window"]),
                    **{f"factor_exposure_{col}": float(wrow[col]) for col in factor_cols},
                }
            )

    if not rows:
        return pd.DataFrame(
            columns=[
                "date",
                "sector_key",
                "bill_share",
                "coupon_share",
                "tips_share",
                "frn_share",
                "effective_duration_years",
                "zero_coupon_equivalent_years",
                "coupon_only_maturity_years",
                "method",
            ]
        )

    result = pd.DataFrame(rows).sort_values(["sector_key", "date"]).reset_index(drop=True)
    return annotate_estimated_output(
        result,
        sector_panel,
        interval_calibration=interval_calibration,
        interval_settings=interval_settings,
        foreign_nowcast=foreign_nowcast,
        bank_constraints=bank_constraints,
        sector_config_path=sector_config_path,
    )


def _parse_maturity_from_label(label: str) -> float:
    _, label = _benchmark_family_and_core_label(label)
    if label.endswith("m"):
        return float(label[:-1]) / 12.0
    if label.endswith("y"):
        return float(label[:-1])

    try:
        return float(label)
    except Exception as exc:
        raise ValueError(f"Cannot parse maturity label: {label}") from exc


def _is_bill_asset(label: str) -> bool:
    family, _ = _benchmark_family_and_core_label(label)
    if family in {"tips", "frn", "key_rate"}:
        return False
    return _parse_maturity_from_label(label) <= 1.0
