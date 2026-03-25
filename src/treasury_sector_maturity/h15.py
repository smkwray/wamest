from __future__ import annotations

import re
from pathlib import Path
import csv
from typing import Any

import numpy as np
import pandas as pd

from .utils import load_yaml


def prefixed_curve_label(label: str, value_prefix: str | None = None) -> str:
    label = str(label).strip()
    prefix = str(value_prefix or "").strip()
    if not prefix:
        return label
    if label.startswith(f"{prefix}_"):
        return label
    return f"{prefix}_{label}"


def maturity_label_to_years(label: str) -> float:
    text = str(label).strip().lower()
    if "_" in text:
        text = text.rsplit("_", 1)[-1]
    if text.endswith("m"):
        return float(text[:-1]) / 12.0
    if text.endswith("y"):
        return float(text[:-1])
    raise ValueError(f"Unsupported maturity label: {label}")


def years_to_maturity_label(years: float) -> str:
    if years < 1:
        months = int(round(years * 12))
        return f"{months}m"
    if float(years).is_integer():
        return f"{int(years)}y"
    return f"{years:g}y"


def normalize_col(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(name).strip().lower()).strip("_")


def _find_date_col(df: pd.DataFrame) -> str | None:
    for col in df.columns:
        if normalize_col(col) in {"date", "observation_date", "time_period", "period"}:
            return col
    return None


def curve_value_columns(df: pd.DataFrame) -> list[str]:
    cols: list[str] = []
    for col in df.columns:
        if col == "date":
            continue
        try:
            maturity_label_to_years(str(col))
            cols.append(str(col))
        except Exception:
            continue
    return cols


def _parse_wide_curve_csv(df: pd.DataFrame) -> pd.DataFrame | None:
    date_col = _find_date_col(df)
    if date_col is None:
        return None

    maturity_cols = [col for col in df.columns if col != date_col and str(col) in curve_value_columns(df)]

    if not maturity_cols:
        return None

    out = df[[date_col] + maturity_cols].copy()
    out[date_col] = pd.to_datetime(out[date_col])
    out = out.rename(columns={date_col: "date"})

    for col in maturity_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.sort_values("date").reset_index(drop=True)
    return out


def _parse_long_ddp_csv(df: pd.DataFrame, series_config: dict[str, Any] | None = None) -> pd.DataFrame | None:
    cols = {normalize_col(c): c for c in df.columns}

    code_col = None
    for candidate in ["series_code", "series_id", "seriesname", "series_name", "series"]:
        if candidate in cols:
            code_col = cols[candidate]
            break

    date_col = None
    for candidate in ["time_period", "date", "observation_date", "period", "obs_date"]:
        if candidate in cols:
            date_col = cols[candidate]
            break

    value_col = None
    for candidate in ["obs_value", "value", "observation_value", "val"]:
        if candidate in cols:
            value_col = cols[candidate]
            break

    if not (code_col and date_col and value_col):
        return None

    mapping: dict[str, str] = {}
    if series_config:
        for block in series_config.values():
            if not isinstance(block, dict):
                continue
            value_prefix = block.get("value_prefix")
            for code_key in ("fed_codes", "fred_ids"):
                for label, code in (block.get(code_key, {}) or {}).items():
                    if isinstance(code, str):
                        mapping[code] = prefixed_curve_label(str(label), value_prefix)

    long_df = df[[code_col, date_col, value_col]].copy()
    long_df.columns = ["series_code", "date", "value"]
    long_df["series_code"] = long_df["series_code"].astype(str).str.strip()
    long_df["date"] = pd.to_datetime(long_df["date"], errors="coerce")
    long_df["value"] = pd.to_numeric(long_df["value"], errors="coerce")
    long_df["maturity"] = long_df["series_code"].map(mapping)

    if long_df["maturity"].isna().all():
        return None

    out = long_df.dropna(subset=["date", "maturity"]).pivot_table(
        index="date", columns="maturity", values="value", aggfunc="last"
    )
    out = out.sort_index().reset_index()
    return out


def _parse_fed_package_csv(path: str | Path, series_config: dict[str, Any] | None = None) -> pd.DataFrame | None:
    fed_codes = {}
    if series_config:
        for block in series_config.values():
            if isinstance(block, dict):
                value_prefix = block.get("value_prefix")
                fed_codes.update(
                    {
                        prefixed_curve_label(label, value_prefix): code
                        for label, code in (block.get("fed_codes", {}) or {}).items()
                    }
                )

    if not fed_codes:
        return None

    with Path(path).open("r", encoding="utf-8-sig", newline="") as fh:
        rows = list(csv.reader(fh))

    header_idx = next((i for i, row in enumerate(rows) if row and row[0].strip() == "Time Period"), None)
    if header_idx is None:
        return None

    header = rows[header_idx]
    reverse_map = {code: label for label, code in fed_codes.items()}
    selected_codes = [code for code in reverse_map if code in header]
    if not selected_codes:
        return None

    data = pd.DataFrame(rows[header_idx + 1 :], columns=header)
    data = data[["Time Period", *selected_codes]].copy()
    data = data.rename(columns={"Time Period": "date", **reverse_map})
    data["date"] = pd.to_datetime(data["date"], errors="coerce")

    for label in reverse_map.values():
        if label in data.columns:
            data[label] = pd.to_numeric(data[label].replace({"ND": pd.NA, "": pd.NA}), errors="coerce")

    return data.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)


def load_h15_curve_file(
    path: str | Path,
    series_config_path: str | Path | None = None,
    curve_key: str | None = None,
) -> pd.DataFrame:
    path = Path(path)
    df = pd.read_csv(path)

    series_config = None
    curve_block = None
    if series_config_path is not None:
        cfg = load_yaml(series_config_path)
        series_config = cfg.get("h15", {})
        if curve_key is not None:
            curve_block = curve_block_config(series_config_path, curve_key)

    parsed = _parse_wide_curve_csv(df)
    if parsed is not None:
        return apply_curve_block_format(parsed, curve_block)

    parsed = _parse_fed_package_csv(path, series_config=series_config)
    if parsed is not None:
        return apply_curve_block_format(parsed, curve_block)

    parsed = _parse_long_ddp_csv(df, series_config=series_config)
    if parsed is not None:
        return apply_curve_block_format(parsed, curve_block)

    raise ValueError(
        "Could not infer H.15 CSV layout. Expect either a wide curve CSV or a long DDP extract with known series codes."
    )


def quarter_end_curves(curves: pd.DataFrame) -> pd.DataFrame:
    out = curves.copy()
    out["date"] = pd.to_datetime(out["date"])
    out = out.set_index("date").sort_index()
    curve_cols = curve_value_columns(out.reset_index())
    meta_cols = [c for c in out.columns if c not in curve_cols]

    curve_part = out[curve_cols].resample("QE").last().dropna(how="all")
    if meta_cols:
        meta_part = out[meta_cols].resample("QE").last().reindex(curve_part.index)
        out = pd.concat([curve_part, meta_part], axis=1)
    else:
        out = curve_part

    out.index = out.index.to_period("Q").end_time.normalize()
    out.index.name = "date"
    return out.reset_index()


def to_decimal_yields(curves: pd.DataFrame) -> pd.DataFrame:
    out = curves.copy()
    cols = curve_value_columns(out)
    sample = out[cols].stack().dropna()
    if not sample.empty and sample.abs().median() > 1.0:
        out[cols] = out[cols] / 100.0
    return out


def zero_coupon_price(yield_rate: float, maturity_years: float, freq: int = 2) -> float:
    y = float(yield_rate)
    if maturity_years <= 0:
        return 1.0
    if maturity_years < 1.0:
        return 1.0 / (1.0 + y * maturity_years)
    return (1.0 + y / freq) ** (-freq * maturity_years)


def _coupon_cashflow_times(
    maturity_years: float,
    freq: int = 2,
    elapsed_years: float = 0.0,
) -> list[tuple[int, float]]:
    periods = int(round(float(maturity_years) * freq))
    return [
        (period, (period / freq) - elapsed_years)
        for period in range(1, periods + 1)
        if ((period / freq) - elapsed_years) > 1e-12
    ]


def bond_price(
    coupon_rate: float,
    yield_rate: float,
    maturity_years: float,
    freq: int = 2,
    elapsed_years: float = 0.0,
) -> float:
    maturity_years = float(maturity_years)
    elapsed_years = float(elapsed_years)
    y = float(yield_rate)
    c = float(coupon_rate)

    remaining_maturity = max(maturity_years - elapsed_years, 0.0)
    if remaining_maturity <= 0:
        return 1.0
    if maturity_years < 1.0:
        return zero_coupon_price(y, remaining_maturity, freq=freq)

    cashflow_times = _coupon_cashflow_times(maturity_years, freq=freq, elapsed_years=elapsed_years)
    if not cashflow_times:
        return 1.0

    coupon = c / freq
    discount = 1.0 + y / freq
    price = 0.0

    for period, pay_time in cashflow_times:
        cash = coupon
        if period == int(round(maturity_years * freq)):
            cash += 1.0
        price += cash / (discount ** (freq * pay_time))

    return price


def bond_price_par_coupon(yield_rate: float, maturity_years: float, freq: int = 2) -> float:
    y = float(yield_rate)
    if maturity_years <= 0:
        return 1.0
    return bond_price(y, y, maturity_years, freq=freq, elapsed_years=0.0)


def modified_duration_par_bond(yield_rate: float, maturity_years: float, freq: int = 2) -> float:
    y = float(yield_rate)
    if maturity_years <= 0:
        return 0.0
    if maturity_years < 1.0:
        return maturity_years / max(1.0 + y, 1e-9)

    cashflow_times = _coupon_cashflow_times(maturity_years, freq=freq, elapsed_years=0.0)
    c = y / freq
    discount = 1.0 + y / freq
    price = bond_price_par_coupon(y, maturity_years, freq=freq)
    macaulay = 0.0

    for period, pay_time in cashflow_times:
        cash = c
        if period == int(round(maturity_years * freq)):
            cash += 1.0
        pv = cash / (discount ** (freq * pay_time))
        macaulay += pay_time * pv

    macaulay /= max(price, 1e-12)
    return macaulay / discount


def price_return_from_yields(
    prev_yield: float,
    curr_yield: float,
    maturity_years: float,
    dt_years: float = 0.25,
    coupon_frequency: int = 2,
    zero_coupon: bool = False,
) -> float:
    prev_yield = float(prev_yield)
    curr_yield = float(curr_yield)
    maturity_years = float(maturity_years)
    dt_years = float(dt_years)

    if zero_coupon or maturity_years < 1.0:
        p0 = zero_coupon_price(prev_yield, maturity_years, freq=coupon_frequency)
        p1 = zero_coupon_price(curr_yield, max(maturity_years - dt_years, 0.0), freq=coupon_frequency)
        return (p1 / max(p0, 1e-12)) - 1.0

    coupon_rate = prev_yield
    p0 = bond_price(coupon_rate, prev_yield, maturity_years, freq=coupon_frequency, elapsed_years=0.0)
    p1 = bond_price(coupon_rate, curr_yield, maturity_years, freq=coupon_frequency, elapsed_years=dt_years)
    return (p1 / max(p0, 1e-12)) - 1.0


def build_benchmark_returns(
    curves: pd.DataFrame,
    maturities: list[float] | None = None,
    zero_coupon: bool = False,
    coupon_frequency: int = 2,
    dt_years: float = 0.25,
) -> pd.DataFrame:
    curves = to_decimal_yields(curves)
    curves = quarter_end_curves(curves)

    maturity_cols = curve_value_columns(curves)
    label_to_years = {col: maturity_label_to_years(col) for col in maturity_cols}

    if maturities is not None:
        keep = {years_to_maturity_label(m) for m in maturities}
        maturity_cols = [c for c in maturity_cols if c in keep]
        label_to_years = {col: label_to_years[col] for col in maturity_cols}

    curves = curves.sort_values("date").reset_index(drop=True)
    rows = []

    for i in range(1, len(curves)):
        prev = curves.iloc[i - 1]
        curr = curves.iloc[i]
        row = {"date": curr["date"]}

        for col in maturity_cols:
            py = prev[col]
            cy = curr[col]
            if pd.isna(py) or pd.isna(cy):
                row[col] = np.nan
            else:
                row[col] = price_return_from_yields(
                    py,
                    cy,
                    maturity_years=label_to_years[col],
                    dt_years=dt_years,
                    coupon_frequency=coupon_frequency,
                    zero_coupon=zero_coupon,
                )
        rows.append(row)

    return pd.DataFrame(rows)


def frn_proxy_return_from_yields(
    prev_yield: float,
    curr_yield: float,
    carry_years: float = 0.25,
    duration_years: float = 0.25,
) -> float:
    prev_yield = float(prev_yield)
    curr_yield = float(curr_yield)
    carry_years = float(carry_years)
    duration_years = float(duration_years)
    return (prev_yield * carry_years) - (duration_years * (curr_yield - prev_yield))


def build_frn_proxy_returns(
    curves: pd.DataFrame,
    base_label: str = "3m",
    output_label: str = "frn_3m",
    carry_years: float = 0.25,
    duration_years: float = 0.25,
) -> pd.DataFrame:
    curves = to_decimal_yields(curves)
    curves = quarter_end_curves(curves)
    if base_label not in curves.columns:
        raise KeyError(f"Base curve label {base_label!r} is not present in the input curve data.")

    curves = curves.sort_values("date").reset_index(drop=True)
    rows = []
    for i in range(1, len(curves)):
        prev = curves.iloc[i - 1]
        curr = curves.iloc[i]
        py = prev[base_label]
        cy = curr[base_label]
        row = {"date": curr["date"]}
        if pd.isna(py) or pd.isna(cy):
            row[output_label] = np.nan
        else:
            row[output_label] = frn_proxy_return_from_yields(
                py,
                cy,
                carry_years=carry_years,
                duration_years=duration_years,
            )
        rows.append(row)

    return pd.DataFrame(rows)


def key_rate_return_from_yields(
    prev_yield: float,
    curr_yield: float,
    maturity_years: float,
    coupon_frequency: int = 2,
    zero_coupon: bool = False,
) -> float:
    prev_yield = float(prev_yield)
    curr_yield = float(curr_yield)
    maturity_years = float(maturity_years)

    if zero_coupon or maturity_years < 1.0:
        p0 = zero_coupon_price(prev_yield, maturity_years, freq=coupon_frequency)
        p1 = zero_coupon_price(curr_yield, maturity_years, freq=coupon_frequency)
        return (p1 / max(p0, 1e-12)) - 1.0

    coupon_rate = prev_yield
    p0 = bond_price(coupon_rate, prev_yield, maturity_years, freq=coupon_frequency, elapsed_years=0.0)
    p1 = bond_price(coupon_rate, curr_yield, maturity_years, freq=coupon_frequency, elapsed_years=0.0)
    return (p1 / max(p0, 1e-12)) - 1.0


def build_key_rate_returns(
    curves: pd.DataFrame,
    bucket_labels: list[str] | None = None,
    output_prefix: str = "kr",
    coupon_frequency: int = 2,
    zero_coupon: bool = False,
) -> pd.DataFrame:
    curves = to_decimal_yields(curves)
    curves = quarter_end_curves(curves)

    maturity_cols = curve_value_columns(curves)
    if bucket_labels is not None:
        requested = {str(label) for label in bucket_labels}
        missing = sorted(requested.difference(maturity_cols))
        if missing:
            raise KeyError(f"Requested key-rate bucket labels are not present in the input curve data: {missing}")
        maturity_cols = [col for col in maturity_cols if col in requested]

    label_to_years = {col: maturity_label_to_years(col) for col in maturity_cols}
    curves = curves.sort_values("date").reset_index(drop=True)

    rows = []
    for i in range(1, len(curves)):
        prev = curves.iloc[i - 1]
        curr = curves.iloc[i]
        row = {"date": curr["date"]}
        for col in maturity_cols:
            py = prev[col]
            cy = curr[col]
            output_label = prefixed_curve_label(col, output_prefix)
            if pd.isna(py) or pd.isna(cy):
                row[output_label] = np.nan
            else:
                row[output_label] = key_rate_return_from_yields(
                    py,
                    cy,
                    maturity_years=label_to_years[col],
                    coupon_frequency=coupon_frequency,
                    zero_coupon=zero_coupon,
                )
        rows.append(row)

    return pd.DataFrame(rows)


def curve_block_config(
    series_config_path: str | Path,
    curve_key: str,
) -> dict[str, Any]:
    h15_cfg = load_yaml(series_config_path).get("h15", {})
    block = h15_cfg.get(curve_key)
    if not isinstance(block, dict):
        raise KeyError(f"Unknown H.15 curve key: {curve_key}")
    return block


def curve_block_label_map(curve_block: dict[str, Any], code_key: str) -> dict[str, str]:
    value_prefix = curve_block.get("value_prefix")
    mapping = curve_block.get(code_key, {}) or {}
    return {prefixed_curve_label(str(label), value_prefix): str(code) for label, code in mapping.items()}


def apply_curve_block_format(
    curves: pd.DataFrame,
    curve_block: dict[str, Any] | None = None,
) -> pd.DataFrame:
    if curve_block is None:
        return curves

    out = curves.copy()
    value_prefix = curve_block.get("value_prefix")
    if not value_prefix:
        return out

    rename_map = {
        col: prefixed_curve_label(col, value_prefix)
        for col in curve_value_columns(out)
        if not str(col).startswith(f"{value_prefix}_")
    }
    return out.rename(columns=rename_map)


def build_benchmark_panel(
    curves: pd.DataFrame,
    curve_block: dict[str, Any] | None = None,
    zero_coupon: bool = False,
    coupon_frequency: int = 2,
    dt_years: float = 0.25,
) -> pd.DataFrame:
    if curve_block and curve_block.get("proxy_type") == "frn":
        return build_frn_proxy_returns(
            curves,
            base_label=str(curve_block.get("base_label", "3m")),
            output_label=str(curve_block.get("output_label", prefixed_curve_label("3m", curve_block.get("value_prefix")))),
            carry_years=float(curve_block.get("carry_years", dt_years)),
            duration_years=float(curve_block.get("duration_years", dt_years)),
        )
    if curve_block and curve_block.get("proxy_type") == "key_rate":
        return build_key_rate_returns(
            curves,
            bucket_labels=[str(label) for label in (curve_block.get("bucket_labels") or [])] or None,
            output_prefix=str(curve_block.get("value_prefix", "kr")),
            coupon_frequency=coupon_frequency,
            zero_coupon=bool(curve_block.get("zero_coupon", zero_coupon)),
        )

    return build_benchmark_returns(
        curves,
        zero_coupon=zero_coupon,
        coupon_frequency=coupon_frequency,
        dt_years=dt_years,
    )


def benchmark_duration_grid(curves: pd.DataFrame) -> pd.DataFrame:
    curves = to_decimal_yields(curves)
    curves = quarter_end_curves(curves)
    maturity_cols = curve_value_columns(curves)

    rows = []
    for _, row in curves.iterrows():
        out = {"date": row["date"]}
        for col in maturity_cols:
            years = maturity_label_to_years(col)
            y = row[col]
            if pd.isna(y):
                out[col] = np.nan
            else:
                out[col] = modified_duration_par_bond(float(y), years)
        rows.append(out)

    return pd.DataFrame(rows)
