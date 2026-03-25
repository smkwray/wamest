from __future__ import annotations

import io
import re
import zipfile
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import pandas as pd

from .utils import load_yaml

DATE_TOKEN_RE = re.compile(r"(\d{8})")
BANK_CONSTRAINT_PANEL_COLUMNS = [
    "date",
    "sector_key",
    "constraint_level",
    "constraint_bill_share",
    "constraint_short_share_le_1y",
    "share_constraints_available",
    "constraint_bucket_basis_total",
    "n_reporters",
    "provider",
    "dataset",
    "vintage",
    "raw_file",
]


class _BulkDownloadPageParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hidden_inputs: dict[str, str] = {}
        self.select_options: dict[str, list[tuple[str, str]]] = {}
        self._active_select: str | None = None
        self._active_option_value: str | None = None
        self._option_text_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = dict(attrs)

        if tag == "input" and attr_map.get("type") == "hidden" and attr_map.get("name"):
            self.hidden_inputs[str(attr_map["name"])] = str(attr_map.get("value") or "")
            return

        if tag == "select" and attr_map.get("id"):
            self._active_select = str(attr_map["id"])
            self.select_options.setdefault(self._active_select, [])
            return

        if tag == "option" and self._active_select is not None:
            self._active_option_value = str(attr_map.get("value") or "")
            self._option_text_parts = []

    def handle_data(self, data: str) -> None:
        if self._active_option_value is not None:
            self._option_text_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "option" and self._active_select is not None and self._active_option_value is not None:
            text = "".join(self._option_text_parts).strip()
            self.select_options.setdefault(self._active_select, []).append((text, self._active_option_value))
            self._active_option_value = None
            self._option_text_parts = []
            return

        if tag == "select":
            self._active_select = None


def parse_ffiec_bulk_download_page(html: str) -> dict[str, Any]:
    parser = _BulkDownloadPageParser()
    parser.feed(html)
    return {
        "hidden_inputs": parser.hidden_inputs,
        "select_options": parser.select_options,
    }


def load_ffiec_call_report_file(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


def _read_ffiec_tab_delimited(fh: Any) -> pd.DataFrame:
    return pd.read_csv(fh, sep="\t", dtype=str)


def _coalesce_numeric(df: pd.DataFrame, candidates: list[str]) -> pd.Series:
    values = pd.Series(pd.NA, index=df.index, dtype="object")
    for col in candidates:
        if col not in df.columns:
            continue
        series = df[col].replace({"": pd.NA})
        values = values.where(values.notna(), series)
    return pd.to_numeric(values, errors="coerce")


def _clean_ffiec_frame(df: pd.DataFrame, reporter_id_col: str) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(col).strip() for col in out.columns]
    out[reporter_id_col] = out[reporter_id_col].astype(str).str.strip()
    out = out[out[reporter_id_col].str.fullmatch(r"\d+")].copy()
    return out.drop_duplicates(subset=[reporter_id_col]).reset_index(drop=True)


def _match_zip_members(names: list[str], token: str) -> list[str]:
    token_lower = token.lower()
    matched = [name for name in names if token_lower in name.lower()]
    if not matched:
        raise ValueError(f"Could not locate FFIEC zip member containing '{token}'.")
    return sorted(matched)


def _merge_ffiec_members(
    zf: zipfile.ZipFile,
    members: list[str],
    reporter_id_col: str,
) -> pd.DataFrame:
    combined: pd.DataFrame | None = None

    for member in members:
        with zf.open(member) as fh:
            current = _read_ffiec_tab_delimited(io.TextIOWrapper(fh, encoding="utf-8-sig"))
        current = _clean_ffiec_frame(current, reporter_id_col=reporter_id_col)

        if combined is None:
            combined = current
            continue

        extra_cols = [col for col in current.columns if col == reporter_id_col or col not in combined.columns]
        combined = combined.merge(current[extra_cols], on=reporter_id_col, how="outer")

    if combined is None:
        raise ValueError("No FFIEC members were merged.")
    return combined


def _extract_report_date(text: str) -> pd.Timestamp:
    tokens = DATE_TOKEN_RE.findall(text)
    for token in tokens:
        for fmt in ("%m%d%Y", "%Y%m%d"):
            try:
                return pd.to_datetime(token, format=fmt)
            except ValueError:
                continue
    raise ValueError(f"Could not infer FFIEC report date from '{text}'.")


def _empty_bank_constraint_panel() -> pd.DataFrame:
    return pd.DataFrame(columns=BANK_CONSTRAINT_PANEL_COLUMNS)


def _coerce_boolean(series: pd.Series, default: bool | None = None) -> pd.Series:
    if series.empty:
        return series.astype("boolean")

    if pd.api.types.is_bool_dtype(series):
        out = series.astype("boolean")
    else:
        mapped = (
            series.astype("string")
            .str.strip()
            .str.lower()
            .map(
                {
                    "true": True,
                    "false": False,
                    "1": True,
                    "0": False,
                    "yes": True,
                    "no": False,
                }
            )
        )
        out = mapped.astype("boolean")

    if default is not None:
        return out.fillna(default)
    return out


def _standardize_bank_constraint_panel(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return _empty_bank_constraint_panel()

    out = df.copy()
    out["date"] = pd.to_datetime(out.get("date"), errors="coerce")
    out["sector_key"] = pd.Series(out.get("sector_key"), index=out.index, dtype="string").str.strip()
    if out["date"].isna().any():
        raise ValueError("Bank constraint panel contains rows with invalid dates.")
    if out["sector_key"].isna().any() or (out["sector_key"] == "").any():
        raise ValueError("Bank constraint panel contains rows with missing sector_key values.")

    numeric_cols = [
        "constraint_level",
        "constraint_bill_share",
        "constraint_short_share_le_1y",
        "constraint_bucket_basis_total",
        "n_reporters",
    ]
    for col in numeric_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    for col in ["constraint_bill_share", "constraint_short_share_le_1y"]:
        if col in out.columns:
            invalid = out[col].notna() & ((out[col] < 0.0) | (out[col] > 1.0))
            if invalid.any():
                raise ValueError(f"Bank constraint panel column '{col}' must stay within [0, 1].")

    support_available = out.get("share_constraints_available")
    if support_available is None:
        support_available = pd.Series(pd.NA, index=out.index, dtype="boolean")
    else:
        support_available = _coerce_boolean(pd.Series(support_available, index=out.index))
    inferred_support = (
        pd.Series(out.get("constraint_bill_share"), index=out.index).notna()
        | pd.Series(out.get("constraint_short_share_le_1y"), index=out.index).notna()
    )
    out["share_constraints_available"] = support_available.fillna(inferred_support).astype(bool)

    text_defaults = {
        "provider": pd.NA,
        "dataset": pd.NA,
        "vintage": pd.NA,
        "raw_file": pd.NA,
    }
    for col, default in text_defaults.items():
        if col not in out.columns:
            out[col] = default

    if "n_reporters" not in out.columns:
        out["n_reporters"] = pd.NA

    for col in BANK_CONSTRAINT_PANEL_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA
    return out[BANK_CONSTRAINT_PANEL_COLUMNS].sort_values(["date", "sector_key"]).reset_index(drop=True)


def _prepare_bank_constraint_supplement_panel(
    supplement_df: pd.DataFrame | None,
    constraints: dict[str, Any],
    supplement_sector_keys: set[str] | None = None,
) -> pd.DataFrame:
    if supplement_df is None or supplement_df.empty:
        return _empty_bank_constraint_panel()

    if not {"date", "sector_key"}.issubset(supplement_df.columns):
        raise ValueError("Bank constraint supplement must include at least date and sector_key columns.")

    supplement = _standardize_bank_constraint_panel(supplement_df)
    valid_sector_keys = {str(spec.get("sector_key")) for spec in constraints.values() if spec.get("sector_key")}
    valid_sector_keys.update(str(value) for value in (supplement_sector_keys or set()) if str(value).strip())
    unknown_sector_keys = sorted(set(supplement["sector_key"]) - valid_sector_keys)
    if unknown_sector_keys:
        raise ValueError(f"Bank constraint supplement has unknown sector_key values: {', '.join(unknown_sector_keys)}")

    if supplement["provider"].isna().all():
        supplement["provider"] = "supplement"
    if supplement["dataset"].isna().all():
        supplement["dataset"] = "bank_constraint_supplement"
    if supplement["n_reporters"].isna().all():
        supplement["n_reporters"] = 0
    return supplement


def normalize_ffiec_call_report_zip(
    path: str | Path,
    config_path: str | Path = "configs/ffiec_call_report.yaml",
) -> pd.DataFrame:
    config = load_yaml(config_path).get("ffiec", {})
    if not config:
        raise ValueError("FFIEC config is missing.")

    institution_cols = config.get("institution_columns", {})
    reporter_id_col = institution_cols.get("reporter_id")
    if not reporter_id_col:
        raise ValueError("FFIEC config is missing institution reporter_id mapping.")

    member_tokens = config.get("member_tokens", {})
    field_map = config.get("fields", {})
    maturity_buckets = config.get("maturity_buckets", {})
    default_bank_class = config.get("default_bank_class", "all_commercial_banks")

    with zipfile.ZipFile(path) as zf:
        names = zf.namelist()
        por_members = _match_zip_members(names, member_tokens.get("por", "Call Bulk POR"))
        rcb_members = _match_zip_members(names, member_tokens.get("rcb", "Call Schedule RCB"))
        por = _merge_ffiec_members(zf, por_members, reporter_id_col=reporter_id_col)
        rcb = _merge_ffiec_members(zf, rcb_members, reporter_id_col=reporter_id_col)

    report_date = _extract_report_date(" ".join([Path(path).name, *rcb_members]))
    out = por[
        [
            reporter_id_col,
            institution_cols.get("bank_name", "Financial Institution Name"),
            institution_cols.get("city", "Financial Institution City"),
            institution_cols.get("state", "Financial Institution State"),
            institution_cols.get("filing_type", "Financial Institution Filing Type"),
        ]
    ].copy()
    out.columns = ["reporter_id", "bank_name", "city", "state", "filing_type"]
    out = out.merge(rcb, left_on="reporter_id", right_on=reporter_id_col, how="inner")

    normalized = pd.DataFrame(
        {
            "date": report_date.normalize(),
            "reporter_id": out["reporter_id"].astype(str),
            "bank_name": out["bank_name"].astype(str),
            "city": out["city"].astype(str),
            "state": out["state"].astype(str),
            "filing_type": out["filing_type"].astype(str),
            "bank_class": default_bank_class,
        }
    )

    for field_name, candidates in field_map.items():
        normalized[field_name] = _coalesce_numeric(out, list(candidates or []))

    for field_name, candidates in maturity_buckets.items():
        normalized[field_name] = _coalesce_numeric(out, list(candidates or []))

    normalized["total_treasuries_amortized_cost"] = normalized[
        ["treasury_htm_amortized_cost", "treasury_afs_amortized_cost"]
    ].fillna(0.0).sum(axis=1)
    normalized["total_treasuries_fair_value"] = normalized[
        ["treasury_htm_fair_value", "treasury_afs_fair_value"]
    ].fillna(0.0).sum(axis=1)

    bucket_cols = list(maturity_buckets.keys())
    normalized["treasury_ladder_total"] = normalized[bucket_cols].fillna(0.0).sum(axis=1)
    short_le_1y = normalized[["treasury_bucket_3m_or_less", "treasury_bucket_3_12m"]].fillna(0.0).sum(axis=1)
    normalized["treasury_short_share_le_1y"] = short_le_1y.div(normalized["treasury_ladder_total"]).where(
        normalized["treasury_ladder_total"] > 0
    )
    normalized["treasury_bill_share_proxy_3m_or_less"] = normalized["treasury_bucket_3m_or_less"].div(
        normalized["treasury_ladder_total"]
    ).where(normalized["treasury_ladder_total"] > 0)

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
        "total_treasuries_amortized_cost",
        "total_treasuries_fair_value",
        *bucket_cols,
        "treasury_ladder_total",
        "treasury_short_share_le_1y",
        "treasury_bill_share_proxy_3m_or_less",
    ]
    return normalized[keep_cols].sort_values(["date", "reporter_id"]).reset_index(drop=True)


def build_bank_constraint_panel(
    ffiec_df: pd.DataFrame,
    constraints_config_path: str | Path = "configs/bank_constraints.yaml",
    supplement_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    config = load_yaml(constraints_config_path)
    constraints = config.get("bank_constraints", {})
    if not constraints:
        raise ValueError("Bank constraints config is missing.")
    supplement_sector_keys = {str(value) for value in config.get("supplement_sector_keys", []) if str(value).strip()}

    supplement_panel = _prepare_bank_constraint_supplement_panel(
        supplement_df,
        constraints,
        supplement_sector_keys=supplement_sector_keys,
    )

    df = ffiec_df.copy()
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if df.empty or "bank_class" not in df.columns:
        return supplement_panel

    sector_map = {bank_class: spec.get("sector_key") for bank_class, spec in constraints.items()}
    level_column_map = {
        bank_class: str(spec.get("level_column") or "total_treasuries_amortized_cost")
        for bank_class, spec in constraints.items()
    }
    df["sector_key"] = df["bank_class"].map(sector_map)
    df = df[df["sector_key"].notna()].copy()
    if df.empty:
        return supplement_panel

    df["constraint_level_input"] = 0.0
    for bank_class, level_column in level_column_map.items():
        mask = df["bank_class"] == bank_class
        if not mask.any():
            continue
        if level_column in df.columns:
            df.loc[mask, "constraint_level_input"] = pd.to_numeric(df.loc[mask, level_column], errors="coerce").fillna(0.0)

    grouped_rows: list[dict[str, Any]] = []
    for (date, sector_key), sub in df.groupby(["date", "sector_key"], dropna=False):
        ladder_total = pd.to_numeric(sub["treasury_ladder_total"], errors="coerce")
        share_constraints_available = bool(ladder_total.notna().any())
        if share_constraints_available:
            bucket_basis_total = float(ladder_total.fillna(0.0).sum())
            bill_numerator = float(sub["treasury_bucket_3m_or_less"].fillna(0.0).sum())
            short_numerator = float(
                sub[["treasury_bucket_3m_or_less", "treasury_bucket_3_12m"]].fillna(0.0).sum(axis=1).sum()
            )
        else:
            bucket_basis_total = pd.NA
            bill_numerator = 0.0
            short_numerator = 0.0

        grouped_rows.append(
            {
                "date": date,
                "sector_key": sector_key,
                "constraint_level": float(sub["constraint_level_input"].fillna(0.0).sum()),
                "constraint_bill_share": (
                    bill_numerator / bucket_basis_total
                    if share_constraints_available and bucket_basis_total > 0
                    else pd.NA
                ),
                "constraint_short_share_le_1y": (
                    short_numerator / bucket_basis_total
                    if share_constraints_available and bucket_basis_total > 0
                    else pd.NA
                ),
                "share_constraints_available": share_constraints_available,
                "constraint_bucket_basis_total": bucket_basis_total,
                "n_reporters": int(sub["reporter_id"].nunique()),
                "provider": sub["provider"].dropna().iloc[0] if "provider" in sub.columns else "ffiec",
                "dataset": sub["dataset"].dropna().iloc[0] if "dataset" in sub.columns else "ffiec_call_reports",
                "vintage": sub["vintage"].dropna().iloc[0] if "vintage" in sub.columns else pd.NA,
                "raw_file": ";".join(sorted({str(value) for value in sub.get("raw_file", pd.Series(dtype=str)).dropna()})),
            }
        )

    observed_panel = _standardize_bank_constraint_panel(pd.DataFrame(grouped_rows))
    if supplement_panel.empty:
        return observed_panel

    observed_keys = set(observed_panel[["date", "sector_key"]].itertuples(index=False, name=None))
    supplement_keys = set(supplement_panel[["date", "sector_key"]].itertuples(index=False, name=None))
    overlapping = sorted(observed_keys & supplement_keys)
    if overlapping:
        overlap_preview = ", ".join(f"{date.date()}:{sector}" for date, sector in overlapping[:5])
        raise ValueError(
            "Bank constraint supplement overlaps observed panel rows; "
            f"remove duplicate sector/date pairs before merging ({overlap_preview})."
        )

    return _standardize_bank_constraint_panel(pd.concat([observed_panel, supplement_panel], ignore_index=True, sort=False))
