from __future__ import annotations

import csv
import io
import json
import os
import re
import zipfile
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlencode, urljoin

import pandas as pd
import requests

from .ffiec import normalize_ffiec_call_report_zip, parse_ffiec_bulk_download_page
from .ffiec002 import normalize_ffiec002_browser_bundle
from .h15 import curve_block_label_map
from .ncua import normalize_ncua_call_report_zip
from .utils import ensure_parent, load_yaml, read_table, write_table
from .z1 import extract_series_code, load_series_catalog, maybe_parse_quarter

FED_DDP_OUTPUT_URL = "https://www.federalreserve.gov/datadownload/Output.aspx"
FED_Z1_RELEASE_PAGE = "https://www.federalreserve.gov/releases/z1/current/default.htm"
FRED_SERIES_OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"
NYFED_SOMA_TSY_ASOF_CSV_URL = "https://markets.newyorkfed.org/api/soma/tsy/get/asof/{date}.csv"
FFIEC_BULK_DOWNLOAD_URL = "https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx"
NCUA_QUARTERLY_DATA_URL = "https://ncua.gov/analysis/credit-union-corporate-call-report-data/quarterly-data"
URL_TIMEOUT_SECONDS = 60
HTTP_RETRY_ATTEMPTS = 3


def _today_utc() -> pd.Timestamp:
    return pd.Timestamp.now("UTC")


class SourceFetchError(RuntimeError):
    pass


@dataclass(frozen=True)
class FetchArtifacts:
    provider: str
    dataset: str
    raw_path: Path | None
    normalized_path: Path


def default_external_dir() -> Path:
    return Path("data/external")


def default_raw_dir() -> Path:
    shared_root = os.environ.get("PROJ_SHARED_DATA_ROOT")
    if shared_root:
        return Path(shared_root) / "sources" / "wamest"
    return default_external_dir() / "raw"


def default_normalized_dir() -> Path:
    return default_external_dir() / "normalized"


def build_fed_ddp_url(query: str | dict[str, Any]) -> str:
    if isinstance(query, dict):
        query_string = urlencode({k: v for k, v in query.items() if v is not None})
    else:
        query_string = str(query).lstrip("?")
    return f"{FED_DDP_OUTPUT_URL}?{query_string}"


def fred_api_key(explicit_api_key: str | None = None) -> str:
    api_key = explicit_api_key or os.environ.get("FRED_API_KEY")
    if not api_key:
        raise SourceFetchError("FRED_API_KEY is required for FRED-backed ingestion.")
    return api_key


def fetch_h15_curves(
    provider: str = "auto",
    series_config_path: str | Path = "configs/h15_series.yaml",
    curve_key: str = "nominal_treasury_constant_maturity",
    raw_dir: str | Path | None = None,
    normalized_out: str | Path | None = None,
    api_key: str | None = None,
    session: requests.Session | None = None,
) -> FetchArtifacts:
    raw_dir = Path(raw_dir) if raw_dir is not None else default_raw_dir()
    normalized_path = (
        Path(normalized_out)
        if normalized_out is not None
        else default_normalized_dir() / f"h15_curves_{provider}.csv"
    )

    providers = _provider_chain(provider, allowed=("fed", "fred"))
    errors: list[str] = []

    for candidate in providers:
        try:
            if candidate == "fed":
                raw_path, curves = _fetch_h15_from_fed(
                    series_config_path=series_config_path,
                    curve_key=curve_key,
                    raw_dir=raw_dir,
                    session=session,
                )
            else:
                raw_path, curves = _fetch_h15_from_fred(
                    series_config_path=series_config_path,
                    curve_key=curve_key,
                    raw_dir=raw_dir,
                    api_key=api_key,
                    session=session,
                )

            write_table(curves, normalized_path)
            return FetchArtifacts(provider=candidate, dataset="h15", raw_path=raw_path, normalized_path=normalized_path)
        except Exception as exc:
            errors.append(f"{candidate}: {exc}")

    raise SourceFetchError(f"Unable to fetch H.15 curves. Tried {' -> '.join(providers)}. Errors: {'; '.join(errors)}")


def fetch_z1_series(
    provider: str = "auto",
    series_catalog_path: str | Path = "configs/z1_series_catalog.yaml",
    raw_dir: str | Path | None = None,
    normalized_out: str | Path | None = None,
    api_key: str | None = None,
    session: requests.Session | None = None,
) -> FetchArtifacts:
    raw_dir = Path(raw_dir) if raw_dir is not None else default_raw_dir()
    normalized_path = (
        Path(normalized_out)
        if normalized_out is not None
        else default_normalized_dir() / f"z1_series_{provider}.csv"
    )

    providers = _provider_chain(provider, allowed=("fed", "fred"))
    errors: list[str] = []

    for candidate in providers:
        try:
            if candidate == "fed":
                raw_path, long_df = _fetch_z1_from_fed(
                    series_catalog_path=series_catalog_path,
                    raw_dir=raw_dir,
                    session=session,
                )
            else:
                raw_path, long_df = _fetch_z1_from_fred(
                    series_catalog_path=series_catalog_path,
                    raw_dir=raw_dir,
                    api_key=api_key,
                    session=session,
                )

            write_table(long_df, normalized_path)
            return FetchArtifacts(provider=candidate, dataset="z1", raw_path=raw_path, normalized_path=normalized_path)
        except Exception as exc:
            errors.append(f"{candidate}: {exc}")

    raise SourceFetchError(f"Unable to fetch Z.1 data. Tried {' -> '.join(providers)}. Errors: {'; '.join(errors)}")


def fetch_soma_holdings(
    as_of_dates: Iterable[pd.Timestamp | str],
    provider: str = "fed",
    raw_dir: str | Path | None = None,
    normalized_out: str | Path | None = None,
    session: requests.Session | None = None,
) -> FetchArtifacts:
    if provider != "fed":
        raise SourceFetchError("SOMA holdings are only supported from the NY Fed source.")

    raw_dir = Path(raw_dir) if raw_dir is not None else default_raw_dir()
    normalized_path = (
        Path(normalized_out)
        if normalized_out is not None
        else default_normalized_dir() / "soma_holdings_fed.csv"
    )

    raw_dir.mkdir(parents=True, exist_ok=True)

    frames: list[pd.DataFrame] = []
    raw_files: list[Path] = []

    for requested in sorted({pd.Timestamp(value).normalize() for value in as_of_dates}):
        raw_path, frame = _fetch_soma_snapshot(requested, raw_dir=raw_dir, session=session)
        raw_files.append(raw_path)
        frames.append(frame)

    if not frames:
        raise SourceFetchError("No SOMA holdings snapshots were fetched.")

    out = pd.concat(frames, ignore_index=True).drop_duplicates().reset_index(drop=True)
    write_table(out, normalized_path)
    return FetchArtifacts(provider="fed", dataset="soma", raw_path=raw_files[-1], normalized_path=normalized_path)


def fetch_ffiec_call_reports(
    report_date: pd.Timestamp | str | None = None,
    provider: str = "auto",
    config_path: str | Path = "configs/ffiec_call_report.yaml",
    raw_dir: str | Path | None = None,
    normalized_out: str | Path | None = None,
    session: requests.Session | None = None,
) -> FetchArtifacts:
    raw_dir = Path(raw_dir) if raw_dir is not None else default_raw_dir()
    normalized_path = (
        Path(normalized_out)
        if normalized_out is not None
        else default_normalized_dir() / "ffiec_call_reports_ffiec.csv"
    )

    providers = _provider_chain(provider, allowed=("ffiec",))
    errors: list[str] = []

    for candidate in providers:
        try:
            raw_path, normalized = _fetch_ffiec_from_bulk(
                report_date=report_date,
                config_path=config_path,
                raw_dir=raw_dir,
                session=session,
            )
            write_table(normalized, normalized_path)
            return FetchArtifacts(
                provider=candidate,
                dataset="ffiec_call_reports",
                raw_path=raw_path,
                normalized_path=normalized_path,
            )
        except Exception as exc:
            errors.append(f"{candidate}: {exc}")

    raise SourceFetchError(
        f"Unable to fetch FFIEC call reports. Tried {' -> '.join(providers)}. Errors: {'; '.join(errors)}"
    )


def fetch_ncua_call_reports(
    report_date: pd.Timestamp | str | None = None,
    provider: str = "auto",
    config_path: str | Path = "configs/ncua_call_report.yaml",
    raw_dir: str | Path | None = None,
    normalized_out: str | Path | None = None,
    session: requests.Session | None = None,
) -> FetchArtifacts:
    raw_dir = Path(raw_dir) if raw_dir is not None else default_raw_dir()
    normalized_path = (
        Path(normalized_out)
        if normalized_out is not None
        else default_normalized_dir() / "ncua_call_reports_ncua.csv"
    )

    providers = _provider_chain(provider, allowed=("ncua",))
    errors: list[str] = []

    for candidate in providers:
        try:
            raw_path, normalized = _fetch_ncua_from_quarterly_data(
                report_date=report_date,
                config_path=config_path,
                raw_dir=raw_dir,
                session=session,
            )
            write_table(normalized, normalized_path)
            return FetchArtifacts(
                provider=candidate,
                dataset="ncua_call_reports",
                raw_path=raw_path,
                normalized_path=normalized_path,
            )
        except Exception as exc:
            errors.append(f"{candidate}: {exc}")

    raise SourceFetchError(
        f"Unable to fetch NCUA call reports. Tried {' -> '.join(providers)}. Errors: {'; '.join(errors)}"
    )


def fetch_ffiec002_call_reports(
    report_date: pd.Timestamp | str | None = None,
    provider: str = "auto",
    config_path: str | Path = "configs/ffiec002_call_report.yaml",
    raw_dir: str | Path | None = None,
    normalized_out: str | Path | None = None,
) -> FetchArtifacts:
    raw_dir = Path(raw_dir) if raw_dir is not None else default_raw_dir()
    normalized_path = (
        Path(normalized_out)
        if normalized_out is not None
        else default_normalized_dir() / "ffiec002_call_reports_ffiec.csv"
    )

    providers = _provider_chain(provider, allowed=("ffiec",))
    errors: list[str] = []

    for candidate in providers:
        try:
            raw_path, normalized = _fetch_ffiec002_from_npw(
                report_date=report_date,
                config_path=config_path,
                raw_dir=raw_dir,
            )
            write_table(normalized, normalized_path)
            return FetchArtifacts(
                provider=candidate,
                dataset="ffiec002_call_reports",
                raw_path=raw_path,
                normalized_path=normalized_path,
            )
        except Exception as exc:
            errors.append(f"{candidate}: {exc}")

    raise SourceFetchError(
        f"Unable to fetch FFIEC 002 call reports. Tried {' -> '.join(providers)}. Errors: {'; '.join(errors)}"
    )


def _provider_chain(provider: str, allowed: tuple[str, ...]) -> list[str]:
    if provider == "auto":
        return list(allowed)
    if provider not in allowed:
        raise ValueError(f"Unsupported provider: {provider}")
    return [provider]


def _session(session: requests.Session | None = None) -> requests.Session:
    return session or requests.Session()


def _fetch_text(url: str, session: requests.Session | None = None) -> str:
    response = _session(session).get(url, timeout=URL_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.text


def _fetch_bytes(url: str, session: requests.Session | None = None) -> bytes:
    response = _session(session).get(url, timeout=URL_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.content


def _fetch_h15_from_fed(
    series_config_path: str | Path,
    curve_key: str,
    raw_dir: Path,
    session: requests.Session | None = None,
) -> tuple[Path, pd.DataFrame]:
    config = load_yaml(series_config_path).get("h15", {}).get(curve_key, {})
    query = config.get("fed_package_query")
    fed_codes = curve_block_label_map(config, "fed_codes")
    if not query or not fed_codes:
        raise SourceFetchError("H.15 config is missing Fed package query or Fed code mappings.")

    url = build_fed_ddp_url(query)
    raw_bytes = _fetch_bytes(url, session=session)
    raw_path = ensure_parent(raw_dir / "fed" / "h15" / f"FRB_H15_{curve_key}.csv")
    raw_path.write_bytes(raw_bytes)

    curves = parse_h15_fed_package_csv(raw_path, fed_code_map=fed_codes)
    curves["provider"] = "fed"
    curves["dataset"] = f"h15_{curve_key}"
    curves["vintage"] = _today_utc().normalize().date().isoformat()
    curves["raw_file"] = raw_path.name
    return raw_path, curves


def parse_h15_fed_package_csv(
    path: str | Path,
    fed_code_map: dict[str, str],
) -> pd.DataFrame:
    with Path(path).open("r", encoding="utf-8-sig", newline="") as fh:
        rows = list(csv.reader(fh))

    header_idx = next((i for i, row in enumerate(rows) if row and row[0].strip() == "Time Period"), None)
    if header_idx is None:
        raise ValueError("Could not find the H.15 package data header row.")

    header = rows[header_idx]
    reverse_map = {code: label for label, code in fed_code_map.items()}
    keep_columns = ["Time Period", *[code for code in fed_code_map.values() if code in header]]
    data = pd.DataFrame(rows[header_idx + 1 :], columns=header)
    data = data[[col for col in keep_columns if col in data.columns]].copy()
    data = data.rename(columns={"Time Period": "date", **reverse_map})
    data["date"] = pd.to_datetime(data["date"], errors="coerce")

    for label in reverse_map.values():
        if label in data.columns:
            data[label] = pd.to_numeric(data[label].replace({"ND": pd.NA, "": pd.NA}), errors="coerce")

    return data.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)


def _fetch_h15_from_fred(
    series_config_path: str | Path,
    curve_key: str,
    raw_dir: Path,
    api_key: str | None = None,
    session: requests.Session | None = None,
) -> tuple[Path, pd.DataFrame]:
    config = load_yaml(series_config_path).get("h15", {}).get(curve_key, {})
    fred_ids = curve_block_label_map(config, "fred_ids")
    if not fred_ids:
        raise SourceFetchError("H.15 config is missing FRED series mappings.")

    raw_dir = ensure_parent(raw_dir / "fred" / "h15" / f"fred_h15_{curve_key}.csv").parent
    frames: list[pd.DataFrame] = []

    for label, series_id in fred_ids.items():
        observations = fetch_fred_series_observations(series_id, api_key=api_key, session=session)
        frame = normalize_fred_observations(observations, value_name=label)
        frames.append(frame)

    curves = frames[0]
    for frame in frames[1:]:
        curves = curves.merge(frame, on="date", how="outer")

    curves = curves.sort_values("date").reset_index(drop=True)
    curves["provider"] = "fred"
    curves["dataset"] = f"h15_{curve_key}"
    curves["vintage"] = _today_utc().normalize().date().isoformat()
    curves["raw_file"] = f"fred_h15_{curve_key}.csv"

    raw_path = ensure_parent(raw_dir / f"fred_h15_{curve_key}.csv")
    write_table(curves, raw_path)
    return raw_path, curves


def fetch_fred_series_observations(
    series_id: str,
    api_key: str | None = None,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    params = {
        "series_id": series_id,
        "api_key": fred_api_key(api_key),
        "file_type": "json",
        "sort_order": "asc",
    }
    response = _session(session).get(FRED_SERIES_OBSERVATIONS_URL, params=params, timeout=URL_TIMEOUT_SECONDS)
    response.raise_for_status()
    payload = response.json()
    if payload.get("error_code"):
        raise SourceFetchError(payload.get("error_message", f"FRED error for {series_id}"))
    return payload


def normalize_fred_observations(
    payload: dict[str, Any],
    value_name: str = "value",
    frequency_suffix: str | None = None,
) -> pd.DataFrame:
    observations = pd.DataFrame(payload.get("observations", []))
    if observations.empty:
        return pd.DataFrame(columns=["date", value_name])

    observations = observations[["date", "value"]].copy()
    observations["value"] = pd.to_numeric(observations["value"].replace({".": pd.NA}), errors="coerce")
    observations["date"] = pd.to_datetime(observations["date"], errors="coerce")

    suffix = frequency_suffix
    if suffix is None:
        series_id = str(payload.get("seriess", [{}])[0].get("id", "")) if payload.get("seriess") else ""
        if series_id.startswith("BOGZ1") and series_id.endswith("Q"):
            suffix = "Q"

    if suffix == "Q":
        observations["date"] = observations["date"].dt.to_period("Q").dt.end_time.dt.normalize()

    return observations.rename(columns={"value": value_name}).dropna(subset=["date"]).reset_index(drop=True)


def _fetch_z1_from_fed(
    series_catalog_path: str | Path,
    raw_dir: Path,
    session: requests.Session | None = None,
) -> tuple[Path, pd.DataFrame]:
    release_zip_url = discover_current_z1_release_csv_zip_url(session=session)
    raw_bytes = _fetch_bytes(release_zip_url, session=session)
    release_date = _extract_release_date_from_url(release_zip_url) or _today_utc().strftime("%Y%m%d")
    raw_path = ensure_parent(raw_dir / "fed" / "z1" / f"z1_csv_files_{release_date}.zip")
    raw_path.write_bytes(raw_bytes)

    catalog = load_series_catalog(series_catalog_path)
    series_codes = _series_codes_from_catalog(catalog)
    long_df = extract_z1_release_zip_series(raw_path, series_codes=series_codes)
    long_df["provider"] = "fed"
    long_df["dataset"] = "z1"
    long_df["vintage"] = release_date
    long_df["raw_file"] = long_df["raw_file"].astype(str)
    return raw_path, long_df


def discover_current_z1_release_csv_zip_url(session: requests.Session | None = None) -> str:
    html = _fetch_text(FED_Z1_RELEASE_PAGE, session=session)
    match = re.search(r'href="([^"]*z1_csv_files\.zip)"', html, flags=re.IGNORECASE)
    if not match:
        raise SourceFetchError("Could not locate the current Z.1 CSV release zip URL.")
    return urljoin(FED_Z1_RELEASE_PAGE, match.group(1))


def _series_codes_from_catalog(catalog: dict[str, Any]) -> set[str]:
    codes: set[str] = set()
    for raw in catalog.values():
        for field in ("level", "transactions", "revaluation", "other_volume"):
            code = getattr(raw, field, None) if hasattr(raw, field) else raw.get(field)
            if code:
                codes.add(str(code))
    return codes


def _z1_fred_series_map(catalog: dict[str, Any]) -> tuple[dict[str, str], list[str]]:
    mapped: dict[str, str] = {}
    unsupported: list[str] = []
    required_fields = {"level", "transactions", "revaluation"}
    optional_fields = {"other_volume"}
    allowed_fields = required_fields | optional_fields

    for spec in catalog.values():
        spec_key = getattr(spec, "key", None)
        if getattr(spec, "computed", None):
            continue

        fred_ids = getattr(spec, "fred_ids", None) or {}
        invalid_fields = sorted(set(fred_ids) - allowed_fields)
        if invalid_fields:
            joined = ", ".join(invalid_fields)
            raise SourceFetchError(f"Invalid Z.1 FRED mapping fields for {spec_key}: {joined}")

        for field in sorted(allowed_fields):
            series_code = getattr(spec, field, None) if hasattr(spec, field) else spec.get(field)
            if not series_code:
                continue
            fred_id = fred_ids.get(field)
            if fred_id:
                mapped[str(series_code)] = str(fred_id)
            elif field in required_fields:
                label = f"{spec_key}.{field}" if spec_key else str(series_code)
                unsupported.append(label)

    return mapped, unsupported


def extract_z1_release_zip_series(path: str | Path, series_codes: Iterable[str]) -> pd.DataFrame:
    target_codes = {code for code in series_codes if code}
    if not target_codes:
        raise ValueError("No Z.1 series codes requested.")

    rows: list[pd.DataFrame] = []

    with zipfile.ZipFile(path) as zf:
        for member in zf.namelist():
            if not member.lower().endswith(".csv"):
                continue

            with zf.open(member) as fh:
                header_line = io.TextIOWrapper(fh, encoding="utf-8-sig").readline().strip()

            if not header_line:
                continue

            header = next(csv.reader([header_line]))
            member_codes = {extract_series_code(value) or str(value).strip() for value in header[1:]}
            matched = sorted(target_codes & member_codes)
            if not matched:
                continue

            with zf.open(member) as fh:
                df = pd.read_csv(fh, dtype=str)

            if "date" not in df.columns:
                continue

            available = [code for code in matched if code in df.columns]
            if not available:
                continue

            subset = df[["date", *available]].copy()
            long_df = subset.melt(id_vars=["date"], var_name="series_code", value_name="value")
            long_df["date"] = long_df["date"].map(maybe_parse_quarter)
            long_df["value"] = pd.to_numeric(long_df["value"].replace({"ND": pd.NA, "": pd.NA}), errors="coerce")
            long_df["raw_file"] = member
            rows.append(long_df.dropna(subset=["date"]))

    if not rows:
        raise SourceFetchError("The Z.1 release zip did not contain the requested series.")

    out = pd.concat(rows, ignore_index=True)
    out = out.sort_values(["series_code", "date"]).drop_duplicates(["series_code", "date"], keep="last")
    return out.reset_index(drop=True)


def _fetch_z1_from_fred(
    series_catalog_path: str | Path,
    raw_dir: Path,
    api_key: str | None = None,
    session: requests.Session | None = None,
) -> tuple[Path, pd.DataFrame]:
    catalog = load_series_catalog(series_catalog_path)
    series_code_map, unsupported = _z1_fred_series_map(catalog)
    if not series_code_map:
        raise SourceFetchError("No explicit FRED mappings are configured for the requested Z.1 catalog.")
    if unsupported:
        preview = ", ".join(unsupported[:12])
        suffix = "" if len(unsupported) <= 12 else f", ... (+{len(unsupported) - 12} more)"
        raise SourceFetchError(f"Incomplete Z.1 FRED mapping coverage: {preview}{suffix}")

    frames: list[pd.DataFrame] = []

    for series_code, series_id in sorted(series_code_map.items()):
        payload = fetch_fred_series_observations(series_id, api_key=api_key, session=session)
        frame = normalize_fred_observations(payload, value_name="value", frequency_suffix="Q")
        frame["series_code"] = series_code
        frames.append(frame[["series_code", "date", "value"]])

    long_df = pd.concat(frames, ignore_index=True).sort_values(["series_code", "date"]).reset_index(drop=True)
    long_df["provider"] = "fred"
    long_df["dataset"] = "z1"
    long_df["vintage"] = _today_utc().normalize().date().isoformat()
    long_df["raw_file"] = "fred_z1_series.csv"

    raw_path = ensure_parent(raw_dir / "fred" / "z1" / "fred_z1_series.csv")
    write_table(long_df, raw_path)
    return raw_path, long_df


def _fetch_soma_snapshot(
    requested_date: pd.Timestamp,
    raw_dir: Path,
    session: requests.Session | None = None,
) -> tuple[Path, pd.DataFrame]:
    for offset in range(0, 8):
        candidate = (requested_date - pd.Timedelta(days=offset)).date().isoformat()
        url = NYFED_SOMA_TSY_ASOF_CSV_URL.format(date=candidate)
        response = _get_with_retries(url, session=session)
        response.raise_for_status()
        text = response.text.strip()
        if not text or "As Of Date" not in text:
            continue

        raw_path = ensure_parent(raw_dir / "fed" / "soma" / f"soma_tsy_{candidate}.csv")
        raw_path.write_text(text + "\n", encoding="utf-8")

        frame = read_table(raw_path)
        frame["provider"] = "fed"
        frame["dataset"] = "soma"
        frame["vintage"] = candidate
        frame["raw_file"] = raw_path.name
        return raw_path, frame

    raise SourceFetchError(f"Unable to locate a SOMA Treasury snapshot on or before {requested_date.date().isoformat()}.")


def _get_with_retries(
    url: str,
    *,
    session: requests.Session | None = None,
    timeout: int = URL_TIMEOUT_SECONDS,
    attempts: int = HTTP_RETRY_ATTEMPTS,
) -> requests.Response:
    last_error: Exception | None = None
    for _attempt in range(max(1, attempts)):
        try:
            return _session(session).get(url, timeout=timeout)
        except requests.exceptions.RequestException as exc:
            last_error = exc
    assert last_error is not None
    raise last_error


def _extract_release_date_from_url(url: str) -> str | None:
    match = re.search(r"/(\d{8})/[^/]+$", url)
    if match:
        return match.group(1)
    return None


def _fetch_ncua_from_quarterly_data(
    report_date: pd.Timestamp | str | None,
    config_path: str | Path,
    raw_dir: Path,
    session: requests.Session | None = None,
) -> tuple[Path, pd.DataFrame]:
    config = load_yaml(config_path).get("ncua", {})
    quarterly_config = config.get("quarterly_data", {})
    if not quarterly_config:
        raise SourceFetchError("NCUA config is missing quarterly data settings.")

    index_url = str(quarterly_config.get("index_url") or NCUA_QUARTERLY_DATA_URL)
    zip_pattern = str(
        quarterly_config.get("zip_url_pattern")
        or r'href="([^"]*call-report-data-(\d{4})-(\d{2})\.zip)"'
    )
    page_html = _fetch_text(index_url, session=session)

    available: dict[str, str] = {}
    for match in re.finditer(zip_pattern, page_html, flags=re.IGNORECASE):
        raw_href, year, month = match.group(1), match.group(2), match.group(3)
        token = f"{year}-{month}"
        available[token] = urljoin(index_url, raw_href)

    if not available:
        raise SourceFetchError("NCUA quarterly data page did not expose any quarterly ZIP links.")

    if report_date is None:
        requested_token = sorted(available)[-1]
    else:
        requested_token = pd.Timestamp(report_date).normalize().strftime("%Y-%m")

    if requested_token not in available:
        available_text = ", ".join(sorted(available)[-8:])
        raise SourceFetchError(
            f"NCUA report date {requested_token} is unavailable. Latest available options: {available_text}"
        )

    raw_bytes = _fetch_bytes(available[requested_token], session=session)
    if not zipfile.is_zipfile(io.BytesIO(raw_bytes)):
        raise SourceFetchError("NCUA quarterly data download did not return a zip archive.")

    raw_path = ensure_parent(raw_dir / "ncua" / f"ncua_call_reports_{requested_token.replace('-', '')}.zip")
    raw_path.write_bytes(raw_bytes)

    normalized = normalize_ncua_call_report_zip(raw_path, config_path=config_path)
    normalized["provider"] = "ncua"
    normalized["dataset"] = "ncua_call_reports"
    normalized["vintage"] = _today_utc().normalize().date().isoformat()
    normalized["raw_file"] = raw_path.name
    return raw_path, normalized


def _fetch_ffiec_from_bulk(
    report_date: pd.Timestamp | str | None,
    config_path: str | Path,
    raw_dir: Path,
    session: requests.Session | None = None,
) -> tuple[Path, pd.DataFrame]:
    config = load_yaml(config_path).get("ffiec", {})
    bulk_config = config.get("bulk_download", {})
    if not bulk_config:
        raise SourceFetchError("FFIEC config is missing bulk download settings.")

    bulk_url = str(bulk_config.get("url") or FFIEC_BULK_DOWNLOAD_URL)
    product_field = str(bulk_config.get("product_field"))
    product_value = str(bulk_config.get("product_value"))
    product_event_target = str(bulk_config.get("product_event_target"))
    date_select_id = str(bulk_config.get("report_date_select_id"))
    date_field = str(bulk_config.get("report_date_field"))
    format_field = str(bulk_config.get("format_field"))
    format_value = str(bulk_config.get("format_value"))
    download_field = str(bulk_config.get("download_button_field"))
    download_value = str(bulk_config.get("download_button_value"))

    sess = _session(session)
    if hasattr(sess, "headers"):
        sess.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Referer": bulk_url,
            }
        )

    initial_response = sess.get(bulk_url, timeout=URL_TIMEOUT_SECONDS)
    initial_response.raise_for_status()
    initial_page = parse_ffiec_bulk_download_page(initial_response.text)
    selection_payload = dict(initial_page["hidden_inputs"])
    selection_payload.update(
        {
            "__EVENTTARGET": product_event_target,
            "__EVENTARGUMENT": "",
            product_field: product_value,
        }
    )
    selection_response = sess.post(bulk_url, data=selection_payload, timeout=URL_TIMEOUT_SECONDS)
    selection_response.raise_for_status()

    selection_page = parse_ffiec_bulk_download_page(selection_response.text)
    options = dict(selection_page["select_options"].get(date_select_id, []))
    if not options:
        raise SourceFetchError("FFIEC bulk page did not return any reporting-period options.")

    requested_text = _ffiec_requested_report_date(report_date, options)
    if requested_text not in options:
        available = ", ".join(list(options)[:8])
        raise SourceFetchError(
            f"FFIEC report date {requested_text} is unavailable. First available options: {available}"
        )

    download_payload = dict(selection_page["hidden_inputs"])
    download_payload.update(
        {
            product_field: product_value,
            date_field: options[requested_text],
            format_field: format_value,
            download_field: download_value,
        }
    )
    download_response = sess.post(bulk_url, data=download_payload, timeout=URL_TIMEOUT_SECONDS)
    download_response.raise_for_status()

    content_type = download_response.headers.get("Content-Type", "").lower()
    if "zip" not in content_type and not zipfile.is_zipfile(io.BytesIO(download_response.content)):
        raise SourceFetchError("FFIEC bulk download did not return a zip archive.")

    raw_path = ensure_parent(raw_dir / "ffiec" / f"ffiec_call_reports_single_period_{pd.Timestamp(requested_text).strftime('%Y%m%d')}.zip")
    raw_path.write_bytes(download_response.content)

    normalized = normalize_ffiec_call_report_zip(raw_path, config_path=config_path)
    normalized["provider"] = "ffiec"
    normalized["dataset"] = "ffiec_call_reports"
    normalized["vintage"] = _today_utc().normalize().date().isoformat()
    normalized["raw_file"] = raw_path.name
    return raw_path, normalized


def _ffiec_requested_report_date(report_date: pd.Timestamp | str | None, options: dict[str, str]) -> str:
    if report_date is None:
        return next(iter(options))

    ts = pd.Timestamp(report_date)
    text = ts.strftime("%m/%d/%Y")
    if text in options:
        return text

    normalized = {pd.Timestamp(unescape(label)): label for label in options}
    if ts.normalize() in normalized:
        return normalized[ts.normalize()]
    return text


def _fetch_ffiec002_from_npw(
    report_date: pd.Timestamp | str | None,
    config_path: str | Path,
    raw_dir: Path,
) -> tuple[Path, pd.DataFrame]:
    config = load_yaml(config_path).get("ffiec002", {})
    npw_config = config.get("npw", {})
    if not npw_config:
        raise SourceFetchError("FFIEC 002 config is missing NIC/NPW settings.")

    report_ts = pd.Timestamp(report_date or _today_utc().normalize()).normalize()
    report_token = report_ts.strftime("%Y%m%d")
    raw_path = ensure_parent(raw_dir / "ffiec002" / f"ffiec002_call_reports_{report_token}.zip")

    browser_entry_url = str(npw_config.get("browser_entry_url") or "https://www.ffiec.gov/npw/")
    search_endpoint = str(npw_config.get("search_endpoint") or "/npw/Institution/Search")
    report_csv_endpoint = str(npw_config.get("report_csv_endpoint") or "/npw/FinancialReport/ReturnFinancialReportCSV")
    entity_group = str(npw_config.get("entity_group") or "USBA")
    country_code = str(npw_config.get("country_code") or "1007")
    id_type = str(npw_config.get("id_type") or "fdic-cert")
    statuses = [str(value) for value in npw_config.get("statuses") or ["Active"]]
    report_code = str(npw_config.get("report_code") or "FFIEC002")
    batch_size = int(npw_config.get("batch_size") or 12)
    timeout_ms = int(npw_config.get("timeout_seconds") or 120) * 1000
    headless = bool(npw_config.get("headless", False))

    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise SourceFetchError(
            "FFIEC 002 fetch requires Playwright. Install the `playwright` Python package and run "
            "`python -m playwright install chromium`."
        ) from exc

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=headless)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                )
            )
            page = context.new_page()
            page.goto(browser_entry_url, wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_selector("form#form0", timeout=timeout_ms)

            state_values = page.evaluate(
                "() => Array.from(document.querySelectorAll('#States option')).map((option) => option.value).filter(Boolean)"
            )
            if not state_values:
                raise SourceFetchError("NIC search page did not expose any state filters for FFIEC 002 enumeration.")

            institutions = _fetch_ffiec002_search_population(
                page=page,
                search_endpoint=search_endpoint,
                state_values=list(state_values),
                entity_group=entity_group,
                country_code=country_code,
                id_type=id_type,
                statuses=statuses,
            )
            reports, missing = _fetch_ffiec002_reports(
                page=page,
                report_csv_endpoint=report_csv_endpoint,
                report_code=report_code,
                report_token=report_token,
                institutions=institutions,
                batch_size=batch_size,
            )
            browser.close()
    except PlaywrightTimeoutError as exc:
        raise SourceFetchError(
            "FFIEC 002 browser session timed out while waiting for the NIC search page. "
            "If Chromium is not installed yet, run `python -m playwright install chromium`."
        ) from exc
    except Exception as exc:
        if "Executable doesn't exist" in str(exc):
            raise SourceFetchError(
                "Playwright is installed but Chromium is unavailable. Run `python -m playwright install chromium`."
            ) from exc
        raise

    if not reports:
        raise SourceFetchError(
            f"FFIEC 002 enumeration succeeded but no report CSVs were retrieved for {report_ts.date().isoformat()}."
        )

    manifest = {
        "report_date": report_ts.date().isoformat(),
        "report_code": report_code,
        "search_endpoint": search_endpoint,
        "report_csv_endpoint": report_csv_endpoint,
        "entity_group": entity_group,
        "country_code": country_code,
        "statuses": statuses,
        "fetched_at": _today_utc().isoformat(),
        "institutions": institutions,
        "missing_reports": missing,
    }
    with zipfile.ZipFile(raw_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", json.dumps(manifest, indent=2, sort_keys=True))
        for rssd, csv_text in sorted(reports.items()):
            zf.writestr(f"reports/{rssd}.csv", csv_text)

    normalized = normalize_ffiec002_browser_bundle(raw_path, config_path=config_path)
    normalized["provider"] = "ffiec"
    normalized["dataset"] = "ffiec002_call_reports"
    normalized["vintage"] = _today_utc().normalize().date().isoformat()
    normalized["raw_file"] = raw_path.name
    return raw_path, normalized


def _fetch_ffiec002_search_population(
    page: Any,
    search_endpoint: str,
    state_values: list[str],
    entity_group: str,
    country_code: str,
    id_type: str,
    statuses: list[str],
) -> list[dict[str, Any]]:
    institutions: dict[str, dict[str, Any]] = {}

    for state in state_values:
        payload = _ffiec002_page_post_json(
            page,
            search_endpoint,
            {
                "Term": "",
                "City": "",
                "States": state,
                "Countries": country_code,
                "IdType": id_type,
                "Identifier": "",
                "EntityGroups": entity_group,
                "Statuses": statuses,
            },
        )

        for row in payload.get("InstitutionData") or []:
            rssd = row.get("IdRssd")
            if rssd is None:
                continue
            institutions[str(rssd)] = dict(row)

    return sorted(institutions.values(), key=lambda row: (str(row.get("State") or ""), str(row.get("Name") or "")))


def _fetch_ffiec002_reports(
    page: Any,
    report_csv_endpoint: str,
    report_code: str,
    report_token: str,
    institutions: list[dict[str, Any]],
    batch_size: int,
) -> tuple[dict[str, str], list[dict[str, Any]]]:
    reports: dict[str, str] = {}
    missing: list[dict[str, Any]] = []
    ids = [str(row.get("IdRssd")) for row in institutions if row.get("IdRssd") is not None]

    for start in range(0, len(ids), max(batch_size, 1)):
        batch = ids[start : start + max(batch_size, 1)]
        payload = page.evaluate(
            """
            async ({ endpoint, ids, reportCode, reportToken }) => {
              return await Promise.all(ids.map(async (rssd) => {
                const url = `${endpoint}?rpt=${encodeURIComponent(reportCode)}&id=${encodeURIComponent(rssd)}&dt=${encodeURIComponent(reportToken)}`;
                const response = await fetch(url, { credentials: 'include' });
                const text = await response.text();
                return {
                  id: String(rssd),
                  status: response.status,
                  contentType: response.headers.get('content-type') || '',
                  text,
                };
              }));
            }
            """,
            {
                "endpoint": report_csv_endpoint,
                "ids": batch,
                "reportCode": report_code,
                "reportToken": report_token,
            },
        )

        for row in payload:
            rssd = str(row["id"])
            content_type = str(row.get("contentType") or "").lower()
            text = str(row.get("text") or "")
            if int(row.get("status") or 0) == 200 and ("csv" in content_type or text.startswith("ItemName,")):
                reports[rssd] = text
            else:
                missing.append({"reporter_id": rssd, "status": int(row.get("status") or 0)})

    return reports, missing


def _ffiec002_page_post_json(page: Any, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
    body_parts: list[tuple[str, str]] = []
    for key, value in params.items():
        if isinstance(value, (list, tuple)):
            body_parts.extend((key, str(item)) for item in value)
        elif value is not None:
            body_parts.append((key, str(value)))

    response_payload = page.evaluate(
        """
        async ({ endpoint, bodyParts }) => {
          const payload = new URLSearchParams();
          for (const [key, value] of bodyParts) {
            payload.append(key, value);
          }
          const response = await fetch(endpoint, {
            method: 'POST',
            credentials: 'include',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' },
            body: payload.toString(),
          });
          return {
            status: response.status,
            contentType: response.headers.get('content-type') || '',
            text: await response.text(),
          };
        }
        """,
        {"endpoint": endpoint, "bodyParts": body_parts},
    )
    status = int(response_payload.get("status") or 0)
    content_type = str(response_payload.get("contentType") or "").lower()
    response_text = str(response_payload.get("text") or "")
    if status >= 400 or response_text.lstrip().startswith("<!DOCTYPE html"):
        raise SourceFetchError(
            "FFIEC NIC blocked the automated search request with a browser challenge. "
            "The FFIEC 002 path requires an interactive desktop Chromium session and may still need a manual challenge pass."
        )
    try:
        return json.loads(response_text)
    except json.JSONDecodeError as exc:
        raise SourceFetchError("FFIEC NIC search returned a non-JSON response after the browser challenge.") from exc
