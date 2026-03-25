from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pandas as pd

from .h15 import maturity_label_to_years, modified_duration_par_bond, quarter_end_curves, to_decimal_yields


def normalize_col(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(name).strip().lower()).strip("_")


def read_soma_holdings(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = df.rename(columns={c: normalize_col(c) for c in df.columns})
    return df


def _first_existing(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for name in candidates:
        if name in df.columns:
            return name
    return None


def prepare_soma_treasury_holdings(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    date_col = _first_existing(df, ["as_of_date", "date", "record_date", "report_date"])
    maturity_col = _first_existing(df, ["maturity_date", "maturity", "maturitydate"])
    par_col = _first_existing(
        df,
        ["par_value", "current_par_amount", "paramount", "par", "current_face_amount", "current_face_value"],
    )
    type_col = _first_existing(df, ["security_type", "security_type_description", "security", "asset_type"])
    desc_col = _first_existing(df, ["description", "security_description", "security_desc", "security_description"])

    if date_col is None or maturity_col is None or par_col is None:
        raise ValueError("SOMA holdings file needs date, maturity, and par-value style columns.")

    df["as_of_date"] = pd.to_datetime(df[date_col], errors="coerce")
    df["maturity_date"] = pd.to_datetime(df[maturity_col], errors="coerce")
    df["par_value"] = pd.to_numeric(df[par_col], errors="coerce")
    df = df.dropna(subset=["as_of_date", "maturity_date", "par_value"]).copy()

    df["remaining_maturity_years"] = (
        (df["maturity_date"] - df["as_of_date"]).dt.days.astype(float) / 365.25
    ).clip(lower=0.0)

    df["security_text"] = ""
    if type_col is not None:
        df["security_text"] = df[type_col].astype(str)
    if desc_col is not None:
        df["security_text"] = (df["security_text"].astype(str) + " " + df[desc_col].astype(str)).str.strip()

    if "cusip" in df.columns:
        df["cusip"] = df["cusip"].astype(str).str.replace("'", "", regex=False).str.strip()

    df["instrument_type"] = df.apply(classify_instrument, axis=1)
    df["is_treasury"] = df["instrument_type"].isin(["bill", "coupon", "tips", "frn"])
    df = df[df["is_treasury"]].copy()

    return df


def classify_instrument(row: pd.Series) -> str:
    text = str(row.get("security_text", "")).lower()

    if "agency" in text or "mbs" in text or "mortgage" in text:
        return "other"
    if "inflation" in text or "tips" in text:
        return "tips"
    if "floating" in text or "frn" in text:
        return "frn"
    if "bill" in text:
        return "bill"
    if any(token in text for token in ["note", "bond", "treasury"]):
        return "coupon"

    rem = row.get("remaining_maturity_years")
    if pd.notna(rem) and float(rem) <= 1.0:
        return "bill"
    return "coupon"


def _nearest_curve_yield(curve_row: pd.Series, years: float) -> float | None:
    if pd.isna(years):
        return None

    maturity_map = {}
    for col in curve_row.index:
        if col == "date":
            continue
        try:
            maturity_map[col] = maturity_label_to_years(col)
        except Exception:
            continue

    if not maturity_map:
        return None

    nearest_col = min(maturity_map, key=lambda c: abs(maturity_map[c] - years))
    value = curve_row.get(nearest_col)
    return float(value) if pd.notna(value) else None


def summarize_soma_quarterly(df: pd.DataFrame, curve_df: pd.DataFrame | None = None) -> pd.DataFrame:
    df = prepare_soma_treasury_holdings(df)
    df["quarter"] = df["as_of_date"].dt.to_period("Q")

    quarter_end_dates = df.groupby("quarter")["as_of_date"].max()
    df = df.merge(quarter_end_dates.rename("quarter_snapshot_date"), left_on="quarter", right_index=True)
    df = df[df["as_of_date"] == df["quarter_snapshot_date"]].copy()

    curves = None
    if curve_df is not None:
        curves = to_decimal_yields(quarter_end_curves(curve_df))

    rows = []
    for quarter, sub in df.groupby("quarter"):
        as_of = sub["as_of_date"].iloc[0]
        total = sub["par_value"].sum()
        if total == 0:
            continue

        bill_mask = sub["instrument_type"] == "bill"
        tips_mask = sub["instrument_type"] == "tips"
        frn_mask = sub["instrument_type"] == "frn"
        coupon_mask = sub["instrument_type"].isin(["coupon", "tips", "frn"])
        exact_wam = float((sub["par_value"] * sub["remaining_maturity_years"]).sum() / total)

        approx_duration = np.nan
        if curves is not None and not curves.empty:
            curve_row = curves[curves["date"] == as_of]
            if curve_row.empty:
                curve_row = curves[curves["date"] <= as_of].tail(1)

            if not curve_row.empty:
                curve_row = curve_row.iloc[0]
                durations = []

                for _, r in sub.iterrows():
                    y = _nearest_curve_yield(curve_row, float(r["remaining_maturity_years"]))
                    if y is None:
                        durations.append(np.nan)
                        continue

                    rem = float(r["remaining_maturity_years"])
                    itype = r["instrument_type"]

                    if itype == "bill":
                        durations.append(rem / max(1.0 + y, 1e-9))
                    elif itype == "frn":
                        durations.append(min(0.25, rem))
                    else:
                        durations.append(modified_duration_par_bond(y, rem))

                sub = sub.copy()
                sub["duration_proxy"] = durations
                approx_duration = float((sub["par_value"] * sub["duration_proxy"]).sum() / total)

        rows.append(
            {
                "date": as_of,
                "exact_wam_years": exact_wam,
                "bill_share": float(sub.loc[bill_mask, "par_value"].sum() / total),
                "coupon_share": float(sub.loc[coupon_mask, "par_value"].sum() / total),
                "tips_share": float(sub.loc[tips_mask, "par_value"].sum() / total),
                "frn_share": float(sub.loc[frn_mask, "par_value"].sum() / total),
                "level": float(total),
                "approx_modified_duration_years": approx_duration,
            }
        )

    return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
