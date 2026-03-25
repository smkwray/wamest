from __future__ import annotations

import csv
import io
import re
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen

import numpy as np
import pandas as pd

DEFAULT_SHL_HISTORICAL_URL = "https://ticdata.treasury.gov/Publish/shlhistdat.csv"
DEFAULT_SLT_TABLE3_URL = "https://ticdata.treasury.gov/Publish/slt_table3.txt"


def _is_url(value: str | Path) -> bool:
    parsed = urlparse(str(value))
    return parsed.scheme in {"http", "https"}


def _read_text(value: str | Path) -> str:
    if _is_url(value):
        with urlopen(str(value)) as response:
            return response.read().decode("utf-8", errors="replace")
    return Path(value).read_text(encoding="utf-8")


def _to_number(value: str | float | int | None) -> float | pd.NA:
    if value is None:
        return pd.NA
    if isinstance(value, (float, int)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return pd.NA
    try:
        return float(text)
    except ValueError:
        return pd.NA


def _normalize_col(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def _load_delimited_matrix(source: str | Path, delimiter: str) -> list[list[str]]:
    text = _read_text(source)
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    return [list(row) for row in reader]


def load_shl_historical_treasury_benchmark(source: str | Path) -> pd.DataFrame:
    """Load SHL historical TIC benchmark data in a normalized long format."""
    rows = _load_delimited_matrix(source, delimiter=",")
    if not rows:
        return pd.DataFrame(
            columns=[
                "country_code",
                "country",
                "date",
                "long_term_treasury_holdings",
                "short_term_treasury_holdings",
            ]
        )

    width = max(len(r) for r in rows)
    padded = [r + [""] * (width - len(r)) for r in rows]

    header_idx = None
    for idx, row in enumerate(padded):
        if _normalize_col(row[0]) == "country_code":
            header_idx = idx
            break
    if header_idx is None or header_idx < 2:
        raise ValueError("Could not locate SHL header rows")

    date_row = padded[header_idx - 2]
    maturity_row = padded[header_idx - 1]
    metric_row = padded[header_idx]

    records: dict[tuple[str, str, pd.Timestamp], dict[str, object]] = {}
    for row in padded[header_idx + 1 :]:
        country_code = row[0].strip()
        country = row[1].strip()
        if not country_code or not country:
            continue

        for col_idx in range(2, width):
            metric = _normalize_col(metric_row[col_idx])
            if "treasury_debt" not in metric:
                continue
            parsed_date = pd.to_datetime(date_row[col_idx], errors="coerce")
            if pd.isna(parsed_date):
                continue

            maturity = _normalize_col(maturity_row[col_idx])
            key = (country_code, country, parsed_date.to_period("M").to_timestamp())
            rec = records.setdefault(
                key,
                {
                    "country_code": country_code,
                    "country": country,
                    "date": parsed_date.to_period("M").to_timestamp(),
                    "long_term_treasury_holdings": pd.NA,
                    "short_term_treasury_holdings": pd.NA,
                },
            )
            value = _to_number(row[col_idx])
            if "long_term" in maturity:
                rec["long_term_treasury_holdings"] = value
            elif "short_term" in maturity:
                rec["short_term_treasury_holdings"] = value

    out = pd.DataFrame(list(records.values()))
    if out.empty:
        return out

    out["date"] = pd.to_datetime(out["date"])
    out["long_term_treasury_holdings"] = pd.to_numeric(out["long_term_treasury_holdings"], errors="coerce")
    out["short_term_treasury_holdings"] = pd.to_numeric(out["short_term_treasury_holdings"], errors="coerce")
    return out.sort_values(["country_code", "date"]).reset_index(drop=True)


def load_slt_table3(source: str | Path) -> pd.DataFrame:
    """Load TIC SLT Table 3 text data."""
    rows = _load_delimited_matrix(source, delimiter="\t")
    if not rows:
        return pd.DataFrame()

    width = max(len(r) for r in rows)
    padded = [r + [""] * (width - len(r)) for r in rows]

    header_idx = None
    for idx, row in enumerate(padded):
        norm = [_normalize_col(v) for v in row]
        if "country" in norm and "country_code" in norm and "date" in norm:
            header_idx = idx
    if header_idx is None:
        raise ValueError("Could not locate SLT Table 3 header row")

    raw_cols = padded[header_idx]
    cols = [_normalize_col(c) or f"col_{i}" for i, c in enumerate(raw_cols)]
    data_rows = [r for r in padded[header_idx + 1 :] if any(cell.strip() for cell in r)]
    df = pd.DataFrame(data_rows, columns=cols)
    if df.empty:
        return df

    for col in ["country", "country_code", "date"]:
        if col not in df.columns:
            raise ValueError(f"SLT data missing required column: {col}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.to_period("M").dt.to_timestamp()
    num_cols = [
        "for_treas_pos",
        "for_treas_neg",
        "long_for_treas_pos",
        "long_for_treas_neg",
        "long_for_treas_val",
        "short_for_treas_pos",
        "short_for_treas_neg",
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].map(_to_number), errors="coerce")
    if "for_treas_pos" in df.columns and "total_treasury_holdings" not in df.columns:
        df["total_treasury_holdings"] = df["for_treas_pos"]

    df = df[df["date"].notna()].copy()
    return df.reset_index(drop=True)


def extract_shl_total_foreign_benchmark(shl_df: pd.DataFrame) -> pd.DataFrame:
    """Extract the SHL benchmark series for total foreign Treasury holdings."""
    df = shl_df.copy()
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    total_mask = pd.Series(False, index=df.index)
    if "country_code" in df.columns:
        total_mask = total_mask | (df["country_code"].astype(str).str.strip() == "99996")
    if "country" in df.columns:
        total_mask = total_mask | df["country"].astype(str).str.contains(
            "All Countries and International and Regional Organizations", case=False, na=False
        )

    total = df[total_mask].copy()
    total["shl_total_treasury_holdings"] = (
        pd.to_numeric(total.get("long_term_treasury_holdings"), errors="coerce").fillna(0.0)
        + pd.to_numeric(total.get("short_term_treasury_holdings"), errors="coerce").fillna(0.0)
    )
    total["holder_group"] = "total"
    keep = ["date", "holder_group", "shl_total_treasury_holdings", "long_term_treasury_holdings", "short_term_treasury_holdings"]
    keep = [c for c in keep if c in total.columns]
    return total[keep].sort_values(["date"]).reset_index(drop=True)


def build_slt_foreign_holder_panel(slt_df: pd.DataFrame) -> pd.DataFrame:
    """Build total/official/private holder panel from SLT Table 3 country rows."""
    df = slt_df.copy()
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    def _holder_group(row: pd.Series) -> str | None:
        code = str(row.get("country_code", "")).strip()
        country = str(row.get("country", "")).lower()
        if code == "99996" or "grand total" in country:
            return "total"
        if code == "99991" or "non-official" in country:
            return "private"
        if code == "99990" or "official" in country:
            return "official"
        return None

    df["holder_group"] = df.apply(_holder_group, axis=1)
    df = df[df["holder_group"].notna()].copy()
    if df.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "holder_group",
                "total_treasury_holdings",
                "long_term_value",
                "short_term_value",
                "short_term_share_slt",
                "long_term_share_slt",
            ]
        )

    df["total_treasury_holdings"] = pd.to_numeric(df.get("for_treas_pos"), errors="coerce")
    df["long_term_value"] = pd.to_numeric(df.get("long_for_treas_pos"), errors="coerce")
    df["short_term_value"] = pd.to_numeric(df.get("short_for_treas_pos"), errors="coerce")

    out = (
        df.groupby(["date", "holder_group"], as_index=False)[
            ["total_treasury_holdings", "long_term_value", "short_term_value"]
        ]
        .sum(min_count=1)
        .sort_values(["date", "holder_group"])
        .reset_index(drop=True)
    )
    denom = out["short_term_value"].fillna(0.0) + out["long_term_value"].fillna(0.0)
    out["short_term_share_slt"] = out["short_term_value"] / denom.replace(0, pd.NA)
    out["long_term_share_slt"] = out["long_term_value"] / denom.replace(0, pd.NA)
    return out


def build_foreign_anchor_panel_from_public_sources(
    shl_benchmark_df: pd.DataFrame, slt_holder_df: pd.DataFrame
) -> pd.DataFrame:
    """Combine monthly SLT holder panel with available SHL benchmark observations."""
    shl = shl_benchmark_df.copy()
    slt = slt_holder_df.copy()
    for frame in (shl, slt):
        if "date" in frame.columns:
            frame["date"] = pd.to_datetime(frame["date"], errors="coerce")

    if slt.empty:
        return shl.sort_values(["date", "holder_group"]).reset_index(drop=True)
    if shl.empty:
        return slt.sort_values(["date", "holder_group"]).reset_index(drop=True)

    merge_cols = [c for c in ["date", "holder_group"] if c in shl.columns and c in slt.columns]
    if not merge_cols:
        return slt.sort_values(["date", "holder_group"]).reset_index(drop=True)

    out = slt.merge(shl, on=merge_cols, how="left")
    return out.sort_values(["date", "holder_group"]).reset_index(drop=True)


def load_extracted_shl_issue_mix(path: str | Path) -> pd.DataFrame:
    """Load a manually extracted SHL Treasury issue-mix CSV.

    Expected columns are flexible, but recommended:
    - date
    - holder_group  (e.g. total / official / private)
    - long_term_nominal_share
    - short_term_nominal_share
    - frn_share
    - tips_share
    - wam_years
    """
    df = pd.read_csv(path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df


def load_slt_short_long(path: str | Path) -> pd.DataFrame:
    """Load an extracted SLT holdings file.

    Recommended columns:
    - date
    - holder_group
    - short_term_value
    - long_term_value
    """
    df = pd.read_csv(path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df


def build_foreign_anchor_panel(
    shl_df: pd.DataFrame,
    slt_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    shl = shl_df.copy()

    if slt_df is None or slt_df.empty:
        sort_cols = [c for c in ["date", "holder_group"] if c in shl.columns]
        return shl.sort_values(sort_cols).reset_index(drop=True)

    slt = slt_df.copy()
    merge_cols = [c for c in ["date", "holder_group"] if c in shl.columns and c in slt.columns]
    if not merge_cols:
        return shl

    out = shl.merge(slt, on=merge_cols, how="outer", suffixes=("_shl", "_slt"))

    if {"short_term_value", "long_term_value"}.issubset(out.columns):
        total = out["short_term_value"].fillna(0.0) + out["long_term_value"].fillna(0.0)
        out["short_term_share_slt"] = out["short_term_value"] / total.replace(0, pd.NA)
        out["long_term_share_slt"] = out["long_term_value"] / total.replace(0, pd.NA)

    return out.sort_values(merge_cols).reset_index(drop=True)


def build_foreign_monthly_nowcast(
    shl_df: pd.DataFrame,
    slt_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build a monthly foreign-holder nowcast that respects SHL anchors and SLT monthly short/long observations."""
    shl = shl_df.copy()
    slt = slt_df.copy() if slt_df is not None else pd.DataFrame()

    if shl.empty and slt.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "holder_group",
                "total_treasury_holdings_nowcast",
                "short_term_share_nowcast",
                "long_term_share_nowcast",
                "short_term_value_nowcast",
                "long_term_value_nowcast",
                "short_term_nominal_share_nowcast",
                "long_term_nominal_share_nowcast",
                "frn_share_nowcast",
                "tips_share_nowcast",
                "wam_years_nowcast",
                "uncertainty_band_type",
                "uncertainty_band_method",
                "uncertainty_band_active",
                "uncertainty_support_kind",
                "has_shl_anchor",
                "has_slt_observation",
                "within_slt_window",
                "method",
            ]
        )

    if "date" in shl.columns:
        shl["date"] = pd.to_datetime(shl["date"], errors="coerce")
        shl["date"] = shl["date"].dt.to_period("M").dt.to_timestamp(how="end").dt.normalize()
    if "date" in slt.columns:
        slt["date"] = pd.to_datetime(slt["date"], errors="coerce")
        slt["date"] = slt["date"].dt.to_period("M").dt.to_timestamp(how="end").dt.normalize()

    holder_groups = sorted(
        {
            *[str(value) for value in shl.get("holder_group", pd.Series(dtype=str)).dropna().unique()],
            *[str(value) for value in slt.get("holder_group", pd.Series(dtype=str)).dropna().unique()],
        }
    )
    if not holder_groups:
        holder_groups = ["total"]

    anchor = _prepare_shl_anchor_inputs(shl)
    slt_monthly = _prepare_slt_inputs(slt)
    out = _build_holder_group_calendar(holder_groups, anchor, slt_monthly).merge(
        anchor, on=["date", "holder_group"], how="left"
    ).merge(slt_monthly, on=["date", "holder_group"], how="left")

    scenario_methods = {
        "linear": "linear",
        "carry_previous": "carry_previous",
        "carry_next": "carry_next",
    }
    rows: list[pd.DataFrame] = []
    for holder_group, sub in out.groupby("holder_group", sort=True):
        sub = sub.sort_values("date").reset_index(drop=True)

        observed_total = sub["observed_total_treasury_holdings"]
        if "shl_total_treasury_holdings" in sub.columns:
            observed_total = observed_total.where(observed_total.notna(), sub["shl_total_treasury_holdings"])

        short_share_observed = sub["observed_short_term_share"].where(
            sub["observed_short_term_share"].notna(), sub["anchor_short_term_share"]
        )
        scenario_frames = {
            scenario_name: _build_nowcast_scenario_frame(
                sub,
                observed_total=observed_total,
                short_share_observed=short_share_observed,
                interpolation_method=interpolation_method,
            )
            for scenario_name, interpolation_method in scenario_methods.items()
        }
        base = scenario_frames["linear"]
        for col in base.columns:
            sub[col] = base[col]

        sub["has_shl_anchor"] = sub["anchor_observation_date"].notna()
        sub["has_slt_observation"] = (
            sub["observed_total_treasury_holdings"].notna()
            | sub["observed_short_term_share"].notna()
            | sub["observed_long_term_share"].notna()
        )
        if sub["has_slt_observation"].any():
            if sub["has_shl_anchor"].any():
                slt_window_start = sub.loc[sub["has_slt_observation"], "date"].min()
            else:
                slt_window_start = sub["date"].min()
            slt_window_end = sub.loc[sub["has_slt_observation"], "date"].max()
            sub["within_slt_window"] = sub["date"].between(slt_window_start, slt_window_end)
        else:
            sub["within_slt_window"] = False
        sub = _add_nowcast_band_fields(sub, scenario_frames)
        sub["method"] = "shl_slt_monthly_nowcast"
        rows.append(sub)

    result = pd.concat(rows, ignore_index=True)
    preferred_columns = [
        "date",
        "holder_group",
        "total_treasury_holdings_nowcast",
        "short_term_share_nowcast",
        "long_term_share_nowcast",
        "short_term_value_nowcast",
        "long_term_value_nowcast",
        "short_term_nominal_share_nowcast",
        "long_term_nominal_share_nowcast",
        "frn_share_nowcast",
        "tips_share_nowcast",
        "wam_years_nowcast",
        "uncertainty_band_type",
        "uncertainty_band_method",
        "uncertainty_band_active",
        "uncertainty_support_kind",
        "has_shl_anchor",
        "has_slt_observation",
        "within_slt_window",
        "anchor_observation_date",
        "shl_total_treasury_holdings",
        "observed_total_treasury_holdings",
        "observed_short_term_share",
        "observed_long_term_share",
        "observed_short_term_value",
        "observed_long_term_value",
        "method",
    ]
    preferred_columns.extend(
        [
            f"{col}_{bound}"
            for col in [
                "total_treasury_holdings_nowcast",
                "short_term_share_nowcast",
                "long_term_share_nowcast",
                "short_term_value_nowcast",
                "long_term_value_nowcast",
                "short_term_nominal_share_nowcast",
                "long_term_nominal_share_nowcast",
                "frn_share_nowcast",
                "tips_share_nowcast",
                "wam_years_nowcast",
            ]
            for bound in ["lower", "upper"]
        ]
    )
    keep = [col for col in preferred_columns if col in result.columns]
    return result[keep].sort_values(["date", "holder_group"]).reset_index(drop=True)


def _optional_numeric_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column in df.columns:
        return pd.to_numeric(df[column], errors="coerce")
    return pd.Series(np.nan, index=df.index, dtype=float)


def _prepare_shl_anchor_inputs(shl_df: pd.DataFrame) -> pd.DataFrame:
    shl = shl_df.copy()
    if shl.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "holder_group",
                "anchor_observation_date",
                "shl_total_treasury_holdings",
                "anchor_short_term_share",
                "anchor_long_nominal_ratio",
                "anchor_frn_ratio",
                "anchor_tips_ratio",
                "anchor_wam_years",
            ]
        )

    shl["date"] = pd.to_datetime(shl["date"], errors="coerce")
    shl["anchor_observation_date"] = shl["date"]
    shl["date"] = shl["date"].dt.to_period("M").dt.to_timestamp(how="end").dt.normalize()
    if "holder_group" not in shl.columns:
        shl["holder_group"] = "total"

    short_nominal_share = _optional_numeric_series(shl, "short_term_nominal_share")
    long_nominal = _optional_numeric_series(shl, "long_term_nominal_share")
    frn_share = _optional_numeric_series(shl, "frn_share")
    tips_share = _optional_numeric_series(shl, "tips_share")
    shl_long_holdings = _optional_numeric_series(shl, "long_term_treasury_holdings")
    shl_short_holdings = _optional_numeric_series(shl, "short_term_treasury_holdings")

    derived_total = shl_long_holdings.fillna(0.0) + shl_short_holdings.fillna(0.0)
    derived_total = derived_total.where(shl_long_holdings.notna() | shl_short_holdings.notna(), np.nan)
    shl["shl_total_treasury_holdings"] = _optional_numeric_series(shl, "shl_total_treasury_holdings").where(
        lambda series: series.notna(),
        derived_total,
    )

    derived_short_share = shl_short_holdings / shl["shl_total_treasury_holdings"]
    shl["anchor_short_term_share"] = short_nominal_share.where(short_nominal_share.notna(), derived_short_share)
    long_bucket = long_nominal.fillna(0.0) + frn_share.fillna(0.0) + tips_share.fillna(0.0)
    long_bucket = long_bucket.where(long_bucket > 0, np.nan)
    shl["anchor_long_nominal_ratio"] = long_nominal / long_bucket
    shl["anchor_frn_ratio"] = frn_share / long_bucket
    shl["anchor_tips_ratio"] = tips_share / long_bucket
    shl["anchor_wam_years"] = _optional_numeric_series(shl, "wam_years")

    keep = [
        "date",
        "holder_group",
        "anchor_observation_date",
        "shl_total_treasury_holdings",
        "anchor_short_term_share",
        "anchor_long_nominal_ratio",
        "anchor_frn_ratio",
        "anchor_tips_ratio",
        "anchor_wam_years",
    ]
    return shl[keep].drop_duplicates(["date", "holder_group"]).sort_values(["date", "holder_group"]).reset_index(drop=True)


def _build_holder_group_calendar(
    holder_groups: list[str],
    anchor_df: pd.DataFrame,
    slt_df: pd.DataFrame,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    slt_start = slt_df["date"].min() if not slt_df.empty else pd.NaT
    slt_end = slt_df["date"].max() if not slt_df.empty else pd.NaT
    for holder_group in holder_groups:
        group_anchor_dates = (
            anchor_df.loc[anchor_df["holder_group"] == holder_group, "date"].dropna().tolist()
            if not anchor_df.empty
            else []
        )
        group_slt_dates = (
            slt_df.loc[slt_df["holder_group"] == holder_group, "date"].dropna().tolist()
            if not slt_df.empty
            else []
        )
        if not group_anchor_dates and not group_slt_dates:
            continue
        if group_anchor_dates:
            start = min([*group_anchor_dates, *group_slt_dates])
            end = max([*group_anchor_dates, *group_slt_dates])
        else:
            start = slt_start if pd.notna(slt_start) else min(group_slt_dates)
            end = slt_end if pd.notna(slt_end) else max(group_slt_dates)
        monthly_dates = pd.date_range(start, end, freq="ME")
        frames.append(pd.DataFrame({"date": monthly_dates, "holder_group": holder_group}))

    if not frames:
        return pd.DataFrame(columns=["date", "holder_group"])
    return pd.concat(frames, ignore_index=True)


def _prepare_slt_inputs(slt_df: pd.DataFrame) -> pd.DataFrame:
    slt = slt_df.copy()
    if slt.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "holder_group",
                "observed_total_treasury_holdings",
                "observed_short_term_share",
                "observed_long_term_share",
                "observed_short_term_value",
                "observed_long_term_value",
            ]
        )

    if "holder_group" not in slt.columns:
        slt["holder_group"] = "total"

    short_value = _optional_numeric_series(slt, "short_term_value")
    long_value = _optional_numeric_series(slt, "long_term_value")
    total_from_values = short_value.fillna(0.0) + long_value.fillna(0.0)
    total_from_values = total_from_values.where(short_value.notna() | long_value.notna(), np.nan)

    total_holdings = _optional_numeric_series(slt, "total_treasury_holdings")
    slt["observed_total_treasury_holdings"] = total_holdings.where(total_holdings.notna(), total_from_values)
    slt["observed_short_term_value"] = short_value
    slt["observed_long_term_value"] = long_value
    if "short_term_share_slt" in slt.columns:
        slt["observed_short_term_share"] = _optional_numeric_series(slt, "short_term_share_slt")
    else:
        slt["observed_short_term_share"] = short_value / slt["observed_total_treasury_holdings"]
    if "long_term_share_slt" in slt.columns:
        slt["observed_long_term_share"] = _optional_numeric_series(slt, "long_term_share_slt")
    else:
        slt["observed_long_term_share"] = long_value / slt["observed_total_treasury_holdings"]

    keep = [
        "date",
        "holder_group",
        "observed_total_treasury_holdings",
        "observed_short_term_share",
        "observed_long_term_share",
        "observed_short_term_value",
        "observed_long_term_value",
    ]
    return slt[keep].drop_duplicates(["date", "holder_group"]).sort_values(["date", "holder_group"]).reset_index(drop=True)


def _build_nowcast_scenario_frame(
    sub: pd.DataFrame,
    observed_total: pd.Series,
    short_share_observed: pd.Series,
    interpolation_method: str,
) -> pd.DataFrame:
    total_nowcast = _interpolate_monthly_series(observed_total, method=interpolation_method)
    short_share_nowcast = _interpolate_monthly_series(short_share_observed, method=interpolation_method).clip(
        lower=0.0, upper=1.0
    )
    long_share_nowcast = (1.0 - short_share_nowcast).clip(lower=0.0, upper=1.0)

    long_nominal_ratio = _interpolate_monthly_series(sub["anchor_long_nominal_ratio"], method=interpolation_method)
    frn_ratio = _interpolate_monthly_series(sub["anchor_frn_ratio"], method=interpolation_method)
    tips_ratio = _interpolate_monthly_series(sub["anchor_tips_ratio"], method=interpolation_method)
    ratio_sum = long_nominal_ratio.fillna(0.0) + frn_ratio.fillna(0.0) + tips_ratio.fillna(0.0)
    ratio_sum = ratio_sum.where(ratio_sum > 0, np.nan)

    long_nominal_ratio = long_nominal_ratio / ratio_sum
    frn_ratio = frn_ratio / ratio_sum
    tips_ratio = tips_ratio / ratio_sum
    wam_years = _interpolate_monthly_series(sub["anchor_wam_years"], method=interpolation_method)

    return pd.DataFrame(
        {
            "total_treasury_holdings_nowcast": total_nowcast,
            "short_term_share_nowcast": short_share_nowcast,
            "long_term_share_nowcast": long_share_nowcast,
            "short_term_value_nowcast": total_nowcast * short_share_nowcast,
            "long_term_value_nowcast": total_nowcast * long_share_nowcast,
            "short_term_nominal_share_nowcast": short_share_nowcast,
            "long_term_nominal_share_nowcast": long_share_nowcast * long_nominal_ratio,
            "frn_share_nowcast": long_share_nowcast * frn_ratio,
            "tips_share_nowcast": long_share_nowcast * tips_ratio,
            "wam_years_nowcast": wam_years,
        },
        index=sub.index,
    )


def _add_nowcast_band_fields(sub: pd.DataFrame, scenario_frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    metric_cols = list(next(iter(scenario_frames.values())).columns)
    for col in metric_cols:
        stacked = pd.concat([frame[col] for frame in scenario_frames.values()], axis=1)
        sub[f"{col}_lower"] = stacked.min(axis=1, skipna=True)
        sub[f"{col}_upper"] = stacked.max(axis=1, skipna=True)

    active_cols = [
        "total_treasury_holdings_nowcast",
        "short_term_share_nowcast",
        "long_term_nominal_share_nowcast",
        "frn_share_nowcast",
        "tips_share_nowcast",
        "wam_years_nowcast",
    ]
    band_active = pd.Series(False, index=sub.index, dtype=bool)
    for col in active_cols:
        lower = pd.to_numeric(sub.get(f"{col}_lower"), errors="coerce")
        upper = pd.to_numeric(sub.get(f"{col}_upper"), errors="coerce")
        band_active = band_active | ((upper - lower).abs() > 1e-12)

    sub["uncertainty_band_type"] = "assumption_band"
    sub["uncertainty_band_method"] = "linear_point_with_forward_backward_envelope"
    sub["uncertainty_band_active"] = band_active
    sub["uncertainty_support_kind"] = _classify_nowcast_support_kind(sub, band_active)
    return sub


def _classify_nowcast_support_kind(sub: pd.DataFrame, band_active: pd.Series) -> pd.Series:
    if sub.empty:
        return pd.Series(dtype="object")

    support = pd.Series("no_support", index=sub.index, dtype="object")
    direct = sub["has_shl_anchor"] | sub["has_slt_observation"]
    support.loc[direct] = "direct_support"
    support.loc[~direct & band_active] = "two_sided_between_supports"
    support.loc[~direct & ~band_active & sub["within_slt_window"]] = "one_sided_flat_fill"
    return support


def _interpolate_monthly_series(series: pd.Series, method: str = "linear") -> pd.Series:
    values = pd.to_numeric(series, errors="coerce").astype(float)
    if values.notna().sum() == 0:
        return values
    if method == "linear":
        return values.interpolate(method="linear", limit_direction="both")
    if method == "carry_previous":
        return values.ffill().bfill()
    if method == "carry_next":
        return values.bfill().ffill()
    raise ValueError(f"Unsupported TIC interpolation method: {method}")


__all__ = [
    "DEFAULT_SHL_HISTORICAL_URL",
    "DEFAULT_SLT_TABLE3_URL",
    "load_shl_historical_treasury_benchmark",
    "load_slt_table3",
    "extract_shl_total_foreign_benchmark",
    "build_slt_foreign_holder_panel",
    "build_foreign_anchor_panel_from_public_sources",
    "build_foreign_monthly_nowcast",
    "load_extracted_shl_issue_mix",
    "load_slt_short_long",
    "build_foreign_anchor_panel",
]
