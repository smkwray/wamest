from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .utils import load_yaml

FLOW_PREFIXES = {
    "level": "FL",
    "transactions": "FU",
    "revaluation": "FR",
    "other_volume": "FV",
    "annual_rate": "FA",
}

FLOW_FIELDS = ("level", "transactions", "revaluation", "other_volume", "annual_rate")
CODE_RE = re.compile(r"[A-Z]{2}\d{6,12}\.[A-Z]+")
QUARTER_RE = re.compile(r"^\s*(\d{4})\s*[-:/ ]?Q([1-4])\s*$", re.IGNORECASE)


@dataclass
class SeriesSpec:
    key: str
    description: str | None = None
    base_code: str | None = None
    level: str | None = None
    transactions: str | None = None
    revaluation: str | None = None
    other_volume: str | None = None
    annual_rate: str | None = None
    fred_ids: dict[str, str] | None = None
    computed: str | None = None
    notes: list[str] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "description": self.description,
            "base_code": self.base_code,
            "level": self.level,
            "transactions": self.transactions,
            "revaluation": self.revaluation,
            "other_volume": self.other_volume,
            "annual_rate": self.annual_rate,
            "fred_ids": self.fred_ids or {},
            "computed": self.computed,
            "notes": self.notes or [],
        }


def expand_base_code(base_code: str, frequency: str = "Q") -> dict[str, str]:
    base = str(base_code).strip()
    base = re.sub(r"^[A-Z]{2}", "", base)
    base = re.sub(r"\.[A-Z]+$", "", base)
    return {field: f"{prefix}{base}.{frequency}" for field, prefix in FLOW_PREFIXES.items()}


def normalize_col(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(name).strip().lower()).strip("_")


def maybe_parse_quarter(value: Any) -> pd.Timestamp | None:
    if pd.isna(value):
        return None

    text = str(value).strip()
    match = QUARTER_RE.match(text)
    if match:
        year = int(match.group(1))
        quarter = int(match.group(2))
        return pd.Period(f"{year}Q{quarter}", freq="Q").end_time.normalize()

    try:
        ts = pd.Timestamp(text)
    except Exception:
        return None

    if pd.isna(ts):
        return None
    return ts.normalize()


def extract_series_code(text: Any) -> str | None:
    if pd.isna(text):
        return None

    text = str(text)
    match = CODE_RE.search(text)
    if match:
        return match.group(0)

    if re.fullmatch(r"[A-Z]{2}\d{6,12}(?:\.[A-Z]+)?", text.strip()):
        value = text.strip()
        if "." not in value:
            return f"{value}.Q"
        return value
    return None


def load_series_catalog(path: str | Path) -> dict[str, SeriesSpec]:
    data = load_yaml(path)
    series_specs: dict[str, SeriesSpec] = {}

    for key, raw in (data.get("series") or {}).items():
        raw = dict(raw or {})
        base_code = raw.get("base_code")

        if base_code:
            derived = expand_base_code(str(base_code), frequency=raw.get("frequency", "Q"))
            for field, code in derived.items():
                raw.setdefault(field, code)

        series_specs[key] = SeriesSpec(
            key=key,
            description=raw.get("description"),
            base_code=base_code,
            level=raw.get("level"),
            transactions=raw.get("transactions"),
            revaluation=raw.get("revaluation"),
            other_volume=raw.get("other_volume"),
            annual_rate=raw.get("annual_rate"),
            fred_ids=dict(raw.get("fred_ids", {}) or {}),
            computed=raw.get("computed"),
            notes=list(raw.get("notes", []) or []),
        )
    return series_specs


def _parse_long_layout(df: pd.DataFrame) -> pd.DataFrame | None:
    cols = {normalize_col(c): c for c in df.columns}

    code_col = None
    for candidate in [
        "series_code",
        "series_id",
        "seriesname",
        "series_name",
        "series",
        "series_description",
    ]:
        if candidate in cols:
            code_col = cols[candidate]
            break

    date_col = None
    for candidate in ["time_period", "date", "observation_date", "period", "obs_date"]:
        if candidate in cols:
            date_col = cols[candidate]
            break

    value_col = None
    for candidate in ["obs_value", "value", "observation_value", "val", "amount"]:
        if candidate in cols:
            value_col = cols[candidate]
            break

    if code_col and date_col and value_col:
        out = df[[code_col, date_col, value_col]].copy()
        out.columns = ["series_text", "date_raw", "value"]
        out["series_code"] = out["series_text"].map(extract_series_code).fillna(out["series_text"].astype(str))
        out["date"] = out["date_raw"].map(maybe_parse_quarter)
        out["value"] = pd.to_numeric(out["value"], errors="coerce")
        out = out.dropna(subset=["date"])
        return out[["series_code", "date", "value"]]

    return None


def _parse_normalized_long_layout(df: pd.DataFrame) -> pd.DataFrame | None:
    cols = {normalize_col(c): c for c in df.columns}
    if not {"series_code", "date", "value"}.issubset(cols):
        return None

    out = df[[cols["series_code"], cols["date"], cols["value"]]].copy()
    out.columns = ["series_code", "date_raw", "value"]
    out["series_code"] = out["series_code"].astype(str).str.strip()
    out["date"] = out["date_raw"].map(maybe_parse_quarter)
    out["value"] = pd.to_numeric(out["value"].replace({"ND": pd.NA, "": pd.NA}), errors="coerce")
    return out.dropna(subset=["date"])[["series_code", "date", "value"]]


def _parse_wide_layout(df: pd.DataFrame) -> pd.DataFrame | None:
    date_like_cols = [c for c in df.columns if maybe_parse_quarter(c) is not None]
    if not date_like_cols:
        return None

    id_candidates = [c for c in df.columns if c not in date_like_cols]
    id_col = id_candidates[0] if id_candidates else df.columns[0]

    out = df[[id_col] + date_like_cols].copy().melt(
        id_vars=[id_col], value_vars=date_like_cols, var_name="date_raw", value_name="value"
    )

    out["series_code"] = out[id_col].map(extract_series_code).fillna(out[id_col].astype(str))
    out["date"] = out["date_raw"].map(maybe_parse_quarter)
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    out = out.dropna(subset=["date"])
    return out[["series_code", "date", "value"]]


def _parse_date_indexed_wide_layout(df: pd.DataFrame) -> pd.DataFrame | None:
    cols = {normalize_col(c): c for c in df.columns}
    date_col = cols.get("date")
    if date_col is None:
        return None

    series_cols = [c for c in df.columns if c != date_col and extract_series_code(c)]
    if not series_cols:
        return None

    out = df[[date_col, *series_cols]].copy().melt(id_vars=[date_col], var_name="series_code", value_name="value")
    out["series_code"] = out["series_code"].astype(str).str.strip()
    out["date"] = out[date_col].map(maybe_parse_quarter)
    out["value"] = pd.to_numeric(out["value"].replace({"ND": pd.NA, "": pd.NA}), errors="coerce")
    out = out.dropna(subset=["date"])
    return out[["series_code", "date", "value"]]


def parse_z1_ddp_csv(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path)

    parsed = _parse_normalized_long_layout(df)
    if parsed is None:
        parsed = _parse_long_layout(df)
    if parsed is None:
        parsed = _parse_date_indexed_wide_layout(df)
    if parsed is None:
        parsed = _parse_wide_layout(df)

    if parsed is None:
        raise ValueError(
            "Could not infer Z.1 CSV layout. Expect a long DDP layout or a wide table with date-like columns."
        )

    parsed["series_code"] = parsed["series_code"].astype(str).str.strip()
    parsed = parsed.sort_values(["series_code", "date"]).reset_index(drop=True)
    return parsed


def long_to_wide(long_df: pd.DataFrame) -> pd.DataFrame:
    wide = long_df.pivot_table(index="date", columns="series_code", values="value", aggfunc="last")
    wide = wide.sort_index()
    wide.columns = wide.columns.astype(str)
    return wide


class _ExpressionEvaluator(ast.NodeVisitor):
    def __init__(self, env: dict[str, Any]) -> None:
        self.env = env

    def visit_Expression(self, node: ast.Expression) -> Any:
        return self.visit(node.body)

    def visit_Name(self, node: ast.Name) -> Any:
        if node.id not in self.env:
            raise KeyError(f"Unknown symbol in expression: {node.id}")
        return self.env[node.id]

    def visit_Constant(self, node: ast.Constant) -> Any:
        return node.value

    def visit_BinOp(self, node: ast.BinOp) -> Any:
        left = self.visit(node.left)
        right = self.visit(node.right)

        if isinstance(node.op, ast.Add):
            return left + right
        if isinstance(node.op, ast.Sub):
            return left - right
        if isinstance(node.op, ast.Mult):
            return left * right
        if isinstance(node.op, ast.Div):
            return left / right

        raise TypeError(f"Unsupported operator: {ast.dump(node.op)}")

    def visit_UnaryOp(self, node: ast.UnaryOp) -> Any:
        operand = self.visit(node.operand)

        if isinstance(node.op, ast.USub):
            return -operand
        if isinstance(node.op, ast.UAdd):
            return operand

        raise TypeError(f"Unsupported unary operator: {ast.dump(node.op)}")

    def generic_visit(self, node: ast.AST) -> Any:
        raise TypeError(f"Unsupported expression element: {ast.dump(node)}")


def evaluate_expression(expression: str, env: dict[str, Any]) -> Any:
    tree = ast.parse(expression, mode="eval")
    return _ExpressionEvaluator(env).visit(tree)


def materialize_series_panel(long_df: pd.DataFrame, catalog: dict[str, SeriesSpec]) -> pd.DataFrame:
    wide = long_to_wide(long_df)
    rows: list[pd.DataFrame] = []

    for key, spec in catalog.items():
        if spec.computed:
            continue

        data = pd.DataFrame({"date": wide.index}).reset_index(drop=True)
        data["series_key"] = key
        data["description"] = spec.description

        for field in FLOW_FIELDS:
            code = getattr(spec, field)
            if code is None or code not in wide.columns:
                data[field] = np.nan
            else:
                data[field] = wide[code].reindex(wide.index).values

        rows.append(data)

    panel = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    if panel.empty:
        return panel

    pivot = {
        field: panel.pivot_table(index="date", columns="series_key", values=field, aggfunc="last")
        for field in FLOW_FIELDS
    }

    computed_rows: list[pd.DataFrame] = []
    for key, spec in catalog.items():
        if not spec.computed:
            continue

        frame = pd.DataFrame({"date": wide.index}).reset_index(drop=True)
        frame["series_key"] = key
        frame["description"] = spec.description

        for field in FLOW_FIELDS:
            env = {series_key: pivot[field][series_key] for series_key in pivot[field].columns}
            try:
                result = evaluate_expression(spec.computed, env)
            except KeyError:
                result = pd.Series(np.nan, index=wide.index)

            if not isinstance(result, pd.Series):
                result = pd.Series(result, index=wide.index)

            frame[field] = result.reindex(wide.index).values

        computed_rows.append(frame)

    if computed_rows:
        panel = pd.concat([panel, *computed_rows], ignore_index=True)

    panel = panel.sort_values(["series_key", "date"]).reset_index(drop=True)
    return panel


def compute_identity_errors(panel: pd.DataFrame) -> pd.DataFrame:
    panel = panel.sort_values(["series_key", "date"]).copy()
    panel["lag_level"] = panel.groupby("series_key")["level"].shift(1)
    panel["identity_error"] = (
        panel["level"]
        - panel["lag_level"]
        - panel["transactions"]
        - panel["revaluation"]
        - panel["other_volume"]
    )
    return panel


def build_sector_panel(series_panel: pd.DataFrame, sector_config_path: str | Path) -> pd.DataFrame:
    config = load_yaml(sector_config_path)
    sector_defs = config.get("sectors") or {}

    if series_panel.empty:
        return pd.DataFrame()

    series_panel = series_panel.copy()
    series_panel = series_panel.sort_values(["series_key", "date"]).reset_index(drop=True)
    dates = pd.Index(sorted(series_panel["date"].unique()))

    metric_env: dict[str, dict[str, pd.Series]] = {field: {} for field in FLOW_FIELDS}

    for field in FLOW_FIELDS:
        pivot = series_panel.pivot_table(index="date", columns="series_key", values=field, aggfunc="last").reindex(dates)
        for col in pivot.columns:
            metric_env[field][col] = pivot[col]

    rows: list[pd.DataFrame] = []

    for sector_key, spec in sector_defs.items():
        frame = pd.DataFrame({"date": dates}).reset_index(drop=True)
        frame["sector_key"] = sector_key
        frame["label"] = spec.get("label", sector_key)
        frame["method_priority"] = "|".join(spec.get("method_priority", []))
        frame["warnings"] = " | ".join(spec.get("warnings", []))

        if "level_series" in spec:
            series_key = spec["level_series"]
            for field in FLOW_FIELDS:
                frame[field] = metric_env[field].get(series_key, pd.Series(np.nan, index=dates)).values
        elif "formula_level" in spec:
            expression = spec["formula_level"]
            for field in FLOW_FIELDS:
                env = dict(metric_env[field])
                try:
                    result = evaluate_expression(expression, env)
                except KeyError:
                    result = pd.Series(np.nan, index=dates)
                if not isinstance(result, pd.Series):
                    result = pd.Series(result, index=dates)
                frame[field] = result.reindex(dates).values
        else:
            for field in FLOW_FIELDS:
                frame[field] = np.nan

        bills_series = spec.get("bills_series")
        if bills_series:
            bill_series = metric_env["level"].get(bills_series, pd.Series(np.nan, index=dates))
            frame["bills_level"] = bill_series.reindex(dates).values
        else:
            frame["bills_level"] = np.nan

        frame["bill_share_observed"] = np.where(
            frame["level"].notna() & (frame["level"] != 0) & frame["bills_level"].notna(),
            frame["bills_level"] / frame["level"],
            np.nan,
        )

        for field in FLOW_FIELDS:
            metric_env[field][sector_key] = pd.Series(frame[field].values, index=dates)

        rows.append(frame)

    out = pd.concat(rows, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"])
    out = out.sort_values(["sector_key", "date"]).reset_index(drop=True)
    return out
