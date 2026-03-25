from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_yaml(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def ensure_parent(path: str | Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    return target


def read_table(path: str | Path, **kwargs: Any) -> pd.DataFrame:
    path = Path(path)
    suffix = path.suffix.lower()

    if suffix == ".parquet":
        return pd.read_parquet(path, **kwargs)
    if suffix in {".csv", ".txt"}:
        return pd.read_csv(path, **kwargs)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path, **kwargs)

    raise ValueError(f"Unsupported table format: {path}")


def write_table(df: pd.DataFrame, path: str | Path, **kwargs: Any) -> Path:
    path = ensure_parent(path)
    suffix = path.suffix.lower()

    if suffix == ".parquet":
        df.to_parquet(path, index=False, **kwargs)
        return path
    if suffix == ".csv":
        df.to_csv(path, index=False, **kwargs)
        return path

    raise ValueError(f"Unsupported write format: {path}")


def dump_json(obj: dict[str, Any], path: str | Path) -> Path:
    path = ensure_parent(path)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=2, sort_keys=True)
    return path


def quarter_end(ts: pd.Timestamp) -> pd.Timestamp:
    return ts.to_period("Q").end_time.normalize()


def as_timestamp(value: Any) -> pd.Timestamp:
    if isinstance(value, pd.Timestamp):
        return value
    return pd.Timestamp(value)
