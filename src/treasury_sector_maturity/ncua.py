from __future__ import annotations

import io
import zipfile
from pathlib import Path
from typing import Any

import pandas as pd

from .utils import load_yaml


def _read_ncua_csv_member(fh: Any) -> pd.DataFrame:
    return pd.read_csv(fh, dtype=str)


def _match_zip_member(names: list[str], member_name: str) -> str:
    requested = member_name.lower()
    for name in names:
        if Path(name).name.lower() == requested:
            return name
    raise ValueError(f"Could not locate NCUA zip member '{member_name}'.")


def _coalesce_numeric(df: pd.DataFrame, candidates: list[str]) -> pd.Series:
    values = pd.Series(pd.NA, index=df.index, dtype="object")
    for col in candidates:
        if col not in df.columns:
            continue
        series = df[col].replace({"": pd.NA})
        values = values.where(values.notna(), series)
    return pd.to_numeric(values, errors="coerce")


def _na_float_series(index: pd.Index) -> pd.Series:
    return pd.Series(pd.NA, index=index, dtype="Float64")


def normalize_ncua_call_report_zip(
    path: str | Path,
    config_path: str | Path = "configs/ncua_call_report.yaml",
) -> pd.DataFrame:
    config = load_yaml(config_path).get("ncua", {})
    if not config:
        raise ValueError("NCUA config is missing.")

    member_names = config.get("member_names", {})
    institution_cols = config.get("institution_columns", {})
    field_map = config.get("fields", {})
    cycle_date_cols = [str(value) for value in config.get("cycle_date_columns", [])]
    reporter_id_col = str(institution_cols.get("reporter_id") or "CU_NUMBER")
    default_bank_class = str(config.get("default_bank_class") or "credit_unions")
    filing_type_value = str(config.get("filing_type_value") or "5300")

    with zipfile.ZipFile(path) as zf:
        names = zf.namelist()
        profile_member = _match_zip_member(names, str(member_names.get("profile", "FOICU.txt")))
        securities_member = _match_zip_member(names, str(member_names.get("securities", "FS220Q.txt")))

        with zf.open(profile_member) as fh:
            profile = _read_ncua_csv_member(io.TextIOWrapper(fh, encoding="utf-8-sig"))
        with zf.open(securities_member) as fh:
            securities = _read_ncua_csv_member(io.TextIOWrapper(fh, encoding="utf-8-sig"))

    profile.columns = [str(col).strip() for col in profile.columns]
    securities.columns = [str(col).strip() for col in securities.columns]

    profile[reporter_id_col] = profile[reporter_id_col].astype(str).str.strip()
    securities[reporter_id_col] = securities[reporter_id_col].astype(str).str.strip()

    profile = profile[profile[reporter_id_col].str.fullmatch(r"\d+")].drop_duplicates(subset=[reporter_id_col]).copy()
    securities = securities[securities[reporter_id_col].str.fullmatch(r"\d+")].drop_duplicates(subset=[reporter_id_col]).copy()

    out = profile[
        [
            reporter_id_col,
            institution_cols.get("bank_name", "CU_NAME"),
            institution_cols.get("city", "CITY"),
            institution_cols.get("state", "STATE"),
        ]
    ].copy()
    out.columns = ["reporter_id", "bank_name", "city", "state"]
    out = out.merge(securities, left_on="reporter_id", right_on=reporter_id_col, how="inner")

    cycle_date = pd.Series(pd.NaT, index=out.index, dtype="datetime64[ns]")
    for col in cycle_date_cols:
        if col not in out.columns:
            continue
        parsed = pd.to_datetime(out[col], errors="coerce")
        cycle_date = cycle_date.where(cycle_date.notna(), parsed)

    normalized = pd.DataFrame(
        {
            "date": cycle_date.dt.normalize(),
            "reporter_id": out["reporter_id"].astype(str),
            "bank_name": out["bank_name"].astype(str),
            "city": out["city"].astype(str),
            "state": out["state"].astype(str),
            "filing_type": filing_type_value,
            "bank_class": default_bank_class,
        }
    )

    for field_name, candidates in field_map.items():
        normalized[field_name] = _coalesce_numeric(out, list(candidates or []))

    normalized["total_treasuries_amortized_cost"] = normalized[
        ["treasury_htm_amortized_cost", "treasury_afs_amortized_cost"]
    ].fillna(0.0).sum(axis=1)
    normalized["total_treasuries_fair_value"] = normalized[
        ["treasury_htm_fair_value", "treasury_afs_fair_value", "treasury_trading_fair_value"]
    ].fillna(0.0).sum(axis=1)
    normalized["total_treasuries_level_proxy"] = normalized["total_treasuries_amortized_cost"].fillna(0.0) + normalized[
        "treasury_trading_fair_value"
    ].fillna(0.0)

    bucket_cols = [
        "treasury_bucket_3m_or_less",
        "treasury_bucket_3_12m",
        "treasury_bucket_1_3y",
        "treasury_bucket_3_5y",
        "treasury_bucket_5_15y",
        "treasury_bucket_over_15y",
    ]
    for col in bucket_cols:
        normalized[col] = _na_float_series(normalized.index)

    normalized["treasury_ladder_total"] = _na_float_series(normalized.index)
    normalized["treasury_short_share_le_1y"] = _na_float_series(normalized.index)
    normalized["treasury_bill_share_proxy_3m_or_less"] = _na_float_series(normalized.index)

    keep_cols = [
        "date",
        "reporter_id",
        "bank_name",
        "city",
        "state",
        "filing_type",
        "bank_class",
        "treasury_htm_amortized_cost",
        "treasury_htm_fair_value",
        "treasury_afs_amortized_cost",
        "treasury_afs_fair_value",
        "treasury_trading_fair_value",
        "total_treasuries_amortized_cost",
        "total_treasuries_fair_value",
        "total_treasuries_level_proxy",
        *bucket_cols,
        "treasury_ladder_total",
        "treasury_short_share_le_1y",
        "treasury_bill_share_proxy_3m_or_less",
    ]
    return normalized[keep_cols].dropna(subset=["date"]).sort_values(["date", "reporter_id"]).reset_index(drop=True)
