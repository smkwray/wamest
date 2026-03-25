from __future__ import annotations

import io
import json
import re
import zipfile
from pathlib import Path
from typing import Any

import pandas as pd

from .utils import load_yaml

EMPTY_MATURITY_COLUMNS = (
    "treasury_bucket_3m_or_less",
    "treasury_bucket_3_12m",
    "treasury_bucket_1_3y",
    "treasury_bucket_3_5y",
    "treasury_bucket_5_15y",
    "treasury_bucket_over_15y",
)
SEARCH_EXPORT_COLUMNS = [
    "InstitutionName",
    "RssdID",
    "City",
    "StateOrCountry",
    "EntityType",
    "Status",
    "StartDate",
    "EndDate",
]
SEARCH_EXPORT_COLUMN_ALIASES = {
    "reporter_id": ("RssdID", "RSSD ID", "RSSD", "ID_RSSD", "IdRssd"),
    "bank_name": ("InstitutionName", "Name", "Institution Name"),
    "city": ("City",),
    "state_country": ("StateOrCountry", "State/ Country", "State / Country", "State/Country"),
    "institution_type": ("EntityType", "Institution Type"),
    "status": ("Status",),
    "start_date": ("StartDate",),
    "end_date": ("EndDate",),
}
REPORT_FILE_RE = re.compile(r"ffiec002_(\d+)_(\d{8})\.csv$", re.IGNORECASE)


def load_ffiec002_call_report_file(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


def load_ffiec002_search_export(path: str | Path) -> pd.DataFrame:
    raw = pd.read_csv(path, dtype=str).fillna("")
    column_map: dict[str, str] = {}
    for canonical, aliases in SEARCH_EXPORT_COLUMN_ALIASES.items():
        for alias in aliases:
            if alias in raw.columns:
                column_map[canonical] = alias
                break

    required = {"reporter_id", "bank_name", "city", "state_country", "institution_type", "status"}
    missing = sorted(required - set(column_map))
    if missing:
        raise ValueError(f"FFIEC 002 search export is missing required columns: {', '.join(missing)}")

    base = pd.DataFrame(
        {
            "reporter_id": raw[column_map["reporter_id"]].astype(str).str.strip(),
            "bank_name": raw[column_map["bank_name"]].astype(str).str.strip(),
            "city": raw[column_map["city"]].astype(str).str.strip(),
            "institution_type": raw[column_map["institution_type"]].astype(str).str.strip(),
            "status": raw[column_map["status"]].astype(str).str.strip(),
        }
    )
    state_country = raw[column_map["state_country"]].astype(str).str.strip()
    base["state_country"] = state_country
    base["state"] = state_country.where(state_country.str.fullmatch(r"[A-Z]{2}"), pd.NA)
    base["country"] = state_country.where(~state_country.str.fullmatch(r"[A-Z]{2}"), "UNITED STATES")
    base["profile_url"] = base["reporter_id"].map(lambda value: f"https://www.ffiec.gov/npw/Institution/Profile/{value}")
    base["start_date"] = (
        raw[column_map["start_date"]].astype(str).str.strip() if "start_date" in column_map else ""
    )
    base["end_date"] = raw[column_map["end_date"]].astype(str).str.strip() if "end_date" in column_map else ""
    base = base[base["reporter_id"].str.fullmatch(r"\d+")].copy().reset_index(drop=True)

    # Preserve script compatibility with NIC table parsing + existing canonical names.
    base["InstitutionName"] = base["bank_name"]
    base["RssdID"] = base["reporter_id"]
    base["City"] = base["city"]
    base["StateOrCountry"] = base["state_country"]
    base["EntityType"] = base["institution_type"]
    base["Status"] = base["status"]
    base["StartDate"] = base["start_date"]
    base["EndDate"] = base["end_date"]
    return base[[*SEARCH_EXPORT_COLUMNS, *[col for col in base.columns if col not in SEARCH_EXPORT_COLUMNS]]]


def write_ffiec002_browser_bundle(
    reports: dict[str, str],
    institutions: pd.DataFrame,
    report_date: pd.Timestamp | str,
    path: str | Path,
    missing_reports: list[dict[str, Any]] | None = None,
) -> Path:
    report_ts = pd.Timestamp(report_date).normalize()
    manifest = {
        "report_date": report_ts.date().isoformat(),
        "institutions": [
            {
                "IdRssd": int(row["reporter_id"]),
                "Name": row.get("bank_name") or "",
                "City": row.get("city") or "",
                "State": row.get("state") or "",
                "Country": row.get("country") or "",
                "EntityType": row.get("institution_type") or "",
                "Status": row.get("status") or "",
                "ProfileUrl": row.get("profile_url") or "",
            }
            for row in institutions.to_dict(orient="records")
        ],
        "missing_reports": missing_reports or [],
    }

    bundle_path = Path(path)
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", json.dumps(manifest, indent=2, sort_keys=True))
        for rssd, text in sorted(reports.items()):
            zf.writestr(f"reports/{rssd}.csv", text)
    return bundle_path


def normalize_ffiec002_browser_bundle(
    path: str | Path | None = None,
    config_path: str | Path = "configs/ffiec002_call_report.yaml",
    *,
    manifest_df: pd.DataFrame | None = None,
    reports_dir: str | Path | None = None,
    report_date: pd.Timestamp | str | None = None,
    missing_records: list[dict[str, Any]] | None = None,
) -> pd.DataFrame:
    if path is not None:
        return _normalize_ffiec002_bundle_zip(path, config_path=config_path)
    if manifest_df is None or reports_dir is None or report_date is None:
        raise ValueError(
            "normalize_ffiec002_browser_bundle requires either `path` for bundle mode or "
            "`manifest_df`, `reports_dir`, and `report_date` for report-directory mode."
        )
    return _normalize_ffiec002_report_directory(
        manifest_df=manifest_df,
        reports_dir=reports_dir,
        report_date=report_date,
        config_path=config_path,
        missing_records=missing_records,
    )


def summarize_ffiec002_foreign_banking_offices(normalized_df: pd.DataFrame) -> pd.DataFrame:
    if normalized_df.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "sector_key",
                "bank_class",
                "constraint_level",
                "n_reporters",
                "downloaded_count",
                "downloaded_with_rcfd0260_count",
                "downloaded_no_rcfd0260_count",
                "missing_report_count",
                "coverage_ratio",
                "missing_rcfd0260_ratio",
                "constraint_level_concept",
                "constraint_level_exactness",
                "constraint_level_concept_match",
                "trading_account_semantics",
                "maturity_ladder_available",
                "provider",
                "dataset",
            ]
        )

    df = normalized_df.copy()
    df["date"] = pd.to_datetime(df.get("date"), errors="coerce")
    id_col = "rssd_id" if "rssd_id" in df.columns else "reporter_id"
    if id_col not in df.columns:
        raise ValueError("FFIEC 002 normalized frame must include `rssd_id` or `reporter_id`.")
    if "ffiec002_download_status" not in df.columns:
        df["ffiec002_download_status"] = "downloaded_with_rcfd0260"
    if "us_treasury_securities" not in df.columns:
        df["us_treasury_securities"] = pd.NA
    df["us_treasury_securities"] = pd.to_numeric(df["us_treasury_securities"], errors="coerce")

    rows: list[dict[str, Any]] = []
    for date, sub in df.groupby("date", dropna=False):
        n_reporters = int(sub[id_col].astype(str).str.strip().replace("", pd.NA).dropna().nunique())
        status = sub["ffiec002_download_status"].astype(str)
        downloaded_mask = status.str.startswith("downloaded")
        with_rcfd0260_mask = status == "downloaded_with_rcfd0260"
        no_rcfd0260_mask = status == "downloaded_no_rcfd0260"
        missing_mask = status.str.startswith("missing")

        downloaded_count = int(downloaded_mask.sum())
        downloaded_with_count = int(with_rcfd0260_mask.sum())
        downloaded_no_count = int(no_rcfd0260_mask.sum())
        missing_count = int(missing_mask.sum())

        rows.append(
            {
                "date": date,
                "sector_key": "bank_foreign_banking_offices_us",
                "bank_class": "foreign_banking_offices_us",
                "constraint_level": float(sub["us_treasury_securities"].fillna(0.0).sum()),
                "n_reporters": n_reporters,
                "downloaded_count": downloaded_count,
                "downloaded_with_rcfd0260_count": downloaded_with_count,
                "downloaded_no_rcfd0260_count": downloaded_no_count,
                "missing_report_count": missing_count,
                "coverage_ratio": (downloaded_with_count / n_reporters) if n_reporters > 0 else pd.NA,
                "missing_rcfd0260_ratio": (downloaded_no_count / downloaded_count) if downloaded_count > 0 else pd.NA,
                "constraint_level_concept": "Schedule RAL 1.b.(1) RCFD0260 coverage-based proxy",
                "constraint_level_exactness": "non-exact aggregate on observed filing coverage",
                "constraint_level_concept_match": "partial",
                "trading_account_semantics": (
                    "RCFDK479 mixes U.S. Treasury and Agency securities; keep separate from pure Treasury level"
                ),
                "maturity_ladder_available": False,
                "provider": sub["provider"].dropna().iloc[0] if "provider" in sub.columns and sub["provider"].notna().any() else "ffiec",
                "dataset": (
                    sub["dataset"].dropna().iloc[0]
                    if "dataset" in sub.columns and sub["dataset"].notna().any()
                    else "ffiec002_call_reports"
                ),
            }
        )

    return pd.DataFrame(rows).sort_values(["date"]).reset_index(drop=True)


def _normalize_ffiec002_bundle_zip(
    path: str | Path,
    config_path: str | Path = "configs/ffiec002_call_report.yaml",
) -> pd.DataFrame:
    config = load_yaml(config_path).get("ffiec002", {})
    if not config:
        raise ValueError("FFIEC 002 config is missing.")

    field_map = config.get("fields", {})
    default_bank_class = str(config.get("default_bank_class") or "foreign_banking_offices_us")

    with zipfile.ZipFile(path) as zf:
        if "manifest.json" not in zf.namelist():
            raise ValueError("FFIEC 002 raw bundle is missing manifest.json.")
        manifest = json.loads(zf.read("manifest.json").decode("utf-8"))

        report_date = pd.Timestamp(manifest.get("report_date")).normalize()
        institutions = {
            str(row.get("IdRssd")): row
            for row in manifest.get("institutions", [])
            if row.get("IdRssd") is not None
        }

        report_members = sorted(
            member
            for member in zf.namelist()
            if member.startswith("reports/") and member.lower().endswith(".csv")
        )
        if not report_members:
            raise ValueError("FFIEC 002 raw bundle did not include any report CSV members.")

        rows: list[dict[str, Any]] = []
        for member in report_members:
            rssd = Path(member).stem
            institution = institutions.get(rssd, {})
            item_frame = _read_ffiec002_item_csv(zf.read(member).decode("utf-8-sig"))

            row = {
                "date": report_date,
                "reporter_id": _metadata_value(item_frame, ["ID_RSSD"]) or rssd,
                "bank_name": _metadata_value(item_frame, ["Institution Name"])
                or _search_name_without_head_office(str(institution.get("Name") or "")),
                "city": _metadata_value(item_frame, ["City"]) or institution.get("City") or pd.NA,
                "state": _metadata_value(item_frame, ["State"]) or institution.get("State") or pd.NA,
                "head_office_name": _metadata_value(item_frame, ["Head Office Name"])
                or institution.get("HeadOfficeName")
                or pd.NA,
                "institution_type": institution.get("EntityType") or pd.NA,
                "country": institution.get("Country") or pd.NA,
                "bank_class": default_bank_class,
            }

            for field_name, codes in field_map.items():
                row[field_name] = _coalesce_numeric_item_value(item_frame, list(codes or []))

            for field_name in EMPTY_MATURITY_COLUMNS:
                row[field_name] = pd.NA

            row["treasury_ladder_total"] = pd.NA
            row["treasury_short_share_le_1y"] = pd.NA
            row["treasury_bill_share_proxy_3m_or_less"] = pd.NA
            row["rssd_id"] = str(row["reporter_id"])
            row["ffiec002_download_status"] = (
                "downloaded_with_rcfd0260" if pd.notna(row.get("us_treasury_securities")) else "downloaded_no_rcfd0260"
            )
            row["ffiec002_missing_reason"] = pd.NA
            row["raw_report_member"] = member
            rows.append(row)

    normalized = pd.DataFrame(rows)
    if normalized.empty:
        raise ValueError("FFIEC 002 normalization produced no rows.")

    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    keep_columns = [
        "date",
        "reporter_id",
        "rssd_id",
        "bank_name",
        "city",
        "state",
        "head_office_name",
        "institution_type",
        "country",
        "bank_class",
        *field_map.keys(),
        *EMPTY_MATURITY_COLUMNS,
        "treasury_ladder_total",
        "treasury_short_share_le_1y",
        "treasury_bill_share_proxy_3m_or_less",
        "ffiec002_download_status",
        "ffiec002_missing_reason",
        "raw_report_member",
    ]
    return normalized[keep_columns].sort_values(["date", "reporter_id"]).reset_index(drop=True)


def _normalize_ffiec002_report_directory(
    manifest_df: pd.DataFrame,
    reports_dir: str | Path,
    report_date: pd.Timestamp | str,
    config_path: str | Path = "configs/ffiec002_call_report.yaml",
    missing_records: list[dict[str, Any]] | None = None,
) -> pd.DataFrame:
    config = load_yaml(config_path).get("ffiec002", {})
    if not config:
        raise ValueError("FFIEC 002 config is missing.")
    field_map = config.get("fields", {})
    default_bank_class = str(config.get("default_bank_class") or "foreign_banking_offices_us")
    report_ts = pd.Timestamp(report_date).normalize()
    report_token = report_ts.strftime("%Y%m%d")

    manifest = manifest_df.copy()
    if "RssdID" not in manifest.columns:
        if "reporter_id" in manifest.columns:
            manifest["RssdID"] = manifest["reporter_id"].astype(str)
        else:
            raise ValueError("FFIEC 002 manifest must include `RssdID` or `reporter_id`.")

    report_lookup = _index_report_files(reports_dir)
    missing_map = _missing_record_map(missing_records or [])

    rows: list[dict[str, Any]] = []
    for _, item in manifest.iterrows():
        rssd = str(item.get("RssdID") or "").strip()
        if not rssd:
            continue

        match_path = report_lookup.get((rssd, report_token)) or report_lookup.get((rssd, ""))
        item_frame: pd.DataFrame | None = None
        download_status = "missing_report"
        missing_reason = missing_map.get(rssd)
        raw_member: str | pd.NA = pd.NA

        if match_path is not None and match_path.exists() and match_path.stat().st_size > 0:
            try:
                item_frame = _read_ffiec002_item_csv(match_path.read_text(encoding="utf-8-sig"))
            except Exception:
                item_frame = None
            raw_member = str(match_path.name)
            download_status = "downloaded_unparsed" if item_frame is None else "downloaded_with_rcfd0260"

        row = {
            "date": report_ts,
            "reporter_id": rssd,
            "rssd_id": rssd,
            "bank_name": _first_non_empty(
                _metadata_value(item_frame, ["Institution Name"]) if item_frame is not None else None,
                _search_name_without_head_office(str(item.get("InstitutionName") or item.get("bank_name") or "")),
            )
            or pd.NA,
            "city": _first_non_empty(
                _metadata_value(item_frame, ["City"]) if item_frame is not None else None,
                str(item.get("City") or item.get("city") or "").strip(),
            )
            or pd.NA,
            "state": _first_non_empty(
                _metadata_value(item_frame, ["State"]) if item_frame is not None else None,
                _derive_state(str(item.get("StateOrCountry") or item.get("state_country") or "")),
                str(item.get("state") or "").strip(),
            )
            or pd.NA,
            "head_office_name": _extract_head_office_name(str(item.get("InstitutionName") or item.get("bank_name") or "")),
            "institution_type": _first_non_empty(
                str(item.get("EntityType") or "").strip(),
                str(item.get("institution_type") or "").strip(),
            )
            or pd.NA,
            "country": _first_non_empty(
                str(item.get("country") or "").strip(),
                _derive_country(str(item.get("StateOrCountry") or item.get("state_country") or "")),
            )
            or pd.NA,
            "bank_class": default_bank_class,
            "ffiec002_download_status": download_status,
            "ffiec002_missing_reason": missing_reason or pd.NA,
            "raw_report_member": raw_member,
        }

        if item_frame is not None:
            for field_name, codes in field_map.items():
                row[field_name] = _coalesce_numeric_item_value(item_frame, list(codes or []))
            if pd.isna(row.get("us_treasury_securities")):
                row["ffiec002_download_status"] = "downloaded_no_rcfd0260"
            else:
                row["ffiec002_download_status"] = "downloaded_with_rcfd0260"
        else:
            for field_name in field_map.keys():
                row[field_name] = pd.NA

        for field_name in EMPTY_MATURITY_COLUMNS:
            row[field_name] = pd.NA
        row["treasury_ladder_total"] = pd.NA
        row["treasury_short_share_le_1y"] = pd.NA
        row["treasury_bill_share_proxy_3m_or_less"] = pd.NA
        rows.append(row)

    normalized = pd.DataFrame(rows)
    if normalized.empty:
        raise ValueError("FFIEC 002 normalization produced no rows.")

    keep_columns = [
        "date",
        "reporter_id",
        "rssd_id",
        "bank_name",
        "city",
        "state",
        "head_office_name",
        "institution_type",
        "country",
        "bank_class",
        *field_map.keys(),
        *EMPTY_MATURITY_COLUMNS,
        "treasury_ladder_total",
        "treasury_short_share_le_1y",
        "treasury_bill_share_proxy_3m_or_less",
        "ffiec002_download_status",
        "ffiec002_missing_reason",
        "raw_report_member",
    ]
    return normalized[keep_columns].sort_values(["date", "reporter_id"]).reset_index(drop=True)


def _read_ffiec002_item_csv(raw_text: str) -> pd.DataFrame:
    df = pd.read_csv(io.StringIO(raw_text), dtype=str)
    df.columns = [str(col).strip() for col in df.columns]
    if not {"ItemName", "Value"}.issubset(df.columns):
        raise ValueError("FFIEC 002 item CSV is missing ItemName/Value columns.")

    out = df.copy()
    out["ItemName"] = out["ItemName"].astype(str).str.strip()
    out["Value"] = out["Value"].fillna("").astype(str).str.strip()
    if "Description" not in out.columns:
        out["Description"] = ""
    out["Description"] = out["Description"].fillna("").astype(str).str.strip()
    return out


def _metadata_value(item_frame: pd.DataFrame, names: list[str]) -> str | None:
    if item_frame is None:
        return None
    for name in names:
        matches = item_frame.loc[item_frame["ItemName"] == name, "Value"]
        for value in matches:
            cleaned = str(value).strip()
            if cleaned:
                return cleaned
    return None


def _coalesce_numeric_item_value(item_frame: pd.DataFrame, codes: list[str]) -> float | pd.NA:
    numeric: list[float] = []
    normalized_names = item_frame["ItemName"].map(_normalize_item_name)

    for code in codes:
        target = _normalize_item_name(code)
        matches = item_frame.loc[normalized_names.str.endswith(target), "Value"]
        for value in matches:
            parsed = pd.to_numeric(pd.Series([value]).replace({"": pd.NA}), errors="coerce").iloc[0]
            if pd.notna(parsed):
                numeric.append(float(parsed))
                break
        if numeric:
            break

    if not numeric:
        return pd.NA
    return numeric[0]


def _normalize_item_name(value: Any) -> str:
    raw = str(value or "").upper()
    return "".join(ch for ch in raw if ch.isalnum())


def _search_name_without_head_office(value: str) -> str | pd.NA:
    cleaned = value.strip()
    if not cleaned:
        return pd.NA
    return cleaned.split(" [", 1)[0].strip()


def _extract_head_office_name(value: str) -> str | pd.NA:
    cleaned = value.strip()
    if "[" not in cleaned or "]" not in cleaned:
        return pd.NA
    return cleaned.split("[", 1)[1].rsplit("]", 1)[0].strip() or pd.NA


def _first_non_empty(*values: Any) -> str | None:
    for value in values:
        cleaned = str(value).strip() if value is not None and value is not pd.NA else ""
        if cleaned and cleaned.lower() != "nan":
            return cleaned
    return None


def _derive_state(state_or_country: str) -> str | None:
    cleaned = state_or_country.strip().upper()
    return cleaned if re.fullmatch(r"[A-Z]{2}", cleaned) else None


def _derive_country(state_or_country: str) -> str | None:
    cleaned = state_or_country.strip()
    if not cleaned:
        return None
    return "UNITED STATES" if re.fullmatch(r"[A-Za-z]{2}", cleaned) else cleaned


def _index_report_files(reports_dir: str | Path) -> dict[tuple[str, str], Path]:
    index: dict[tuple[str, str], Path] = {}
    for member in Path(reports_dir).glob("*.csv"):
        match = REPORT_FILE_RE.match(member.name)
        if not match:
            continue
        rssd, token = match.group(1), match.group(2)
        index[(rssd, token)] = member
        index.setdefault((rssd, ""), member)
    return index


def _missing_record_map(missing_records: list[dict[str, Any]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for row in missing_records:
        rssd = str(row.get("rssd") or row.get("reporter_id") or row.get("RssdID") or "").strip()
        reason = str(row.get("reason") or "").strip()
        if rssd:
            out[rssd] = reason
    return out
