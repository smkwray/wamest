from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from .utils import ensure_parent, read_table


DEFAULT_SECTOR_FILE = Path("data/processed/sector_effective_maturity.csv")
TOY_SECTOR_FILE = Path("data/processed/toy_sector_effective_maturity.csv")
DEFAULT_FOREIGN_NOWCAST_FILE = Path("data/processed/foreign_nowcast_panel.csv")
TOY_FOREIGN_NOWCAST_FILE = Path("data/processed/toy_foreign_nowcast_panel.csv")
DEFAULT_FED_SUMMARY_FILE = Path("outputs/fed_calibration_summary.json")
TOY_FED_SUMMARY_FILE = Path("outputs/toy_fed_calibration_summary.json")

FOREIGN_SUPPORT_KINDS = [
    "direct_support",
    "two_sided_between_supports",
    "one_sided_flat_fill",
    "no_support",
]


def build_output_metadata_report(
    sector_file: str | Path = DEFAULT_SECTOR_FILE,
    foreign_nowcast_file: str | Path = DEFAULT_FOREIGN_NOWCAST_FILE,
    fed_summary_file: str | Path = DEFAULT_FED_SUMMARY_FILE,
    out: str | Path = "outputs/output_metadata_report.md",
) -> Path:
    sector_path, sector_is_fallback = _resolve_input_path(
        sector_file,
        default_path=DEFAULT_SECTOR_FILE,
        fallback_path=TOY_SECTOR_FILE,
        required=True,
    )
    foreign_path, foreign_is_fallback = _resolve_input_path(
        foreign_nowcast_file,
        default_path=DEFAULT_FOREIGN_NOWCAST_FILE,
        fallback_path=TOY_FOREIGN_NOWCAST_FILE,
        required=True,
    )
    summary_path, summary_is_fallback = _resolve_input_path(
        fed_summary_file,
        default_path=DEFAULT_FED_SUMMARY_FILE,
        fallback_path=TOY_FED_SUMMARY_FILE,
        required=False,
    )

    sector = read_table(sector_path)
    foreign = read_table(foreign_path)
    fed_summary = _load_optional_json(summary_path)

    markdown = render_output_metadata_report(
        sector,
        foreign,
        fed_summary=fed_summary,
        sector_path=sector_path,
        foreign_nowcast_path=foreign_path,
        fed_summary_path=summary_path,
        sector_is_fallback=sector_is_fallback,
        foreign_is_fallback=foreign_is_fallback,
        fed_summary_is_fallback=summary_is_fallback,
    )

    out_path = ensure_parent(out)
    out_path.write_text(markdown, encoding="utf-8")
    return out_path


def render_output_metadata_report(
    sector: pd.DataFrame,
    foreign: pd.DataFrame,
    fed_summary: dict[str, Any] | None,
    sector_path: str | Path,
    foreign_nowcast_path: str | Path,
    fed_summary_path: str | Path | None,
    sector_is_fallback: bool = False,
    foreign_is_fallback: bool = False,
    fed_summary_is_fallback: bool = False,
) -> str:
    lines = [
        "# Output Metadata Report",
        "",
        f"Sector input: `{sector_path}`{_fallback_suffix(sector_is_fallback)}",
        f"Foreign nowcast input: `{foreign_nowcast_path}`{_fallback_suffix(foreign_is_fallback)}",
    ]
    if fed_summary_path is None:
        lines.append("Fed summary input: unavailable")
    else:
        lines.append(f"Fed summary input: `{fed_summary_path}`{_fallback_suffix(fed_summary_is_fallback)}")

    lines.extend(
        [
            "",
            "## Sector Metadata Summary",
            "",
            _render_markdown_table(_build_sector_summary_rows(sector)),
            "",
            "## Identified-Set and Constraint Summary",
            "",
            _render_markdown_table(_build_identified_set_rows(sector)),
            "",
            "## Hybrid Estimation Summary",
            "",
            *_render_hybrid_estimation_section(sector, fed_summary=fed_summary),
            "",
            "## Foreign Nowcast Support Summary",
            "",
            _render_markdown_table(_build_foreign_support_rows(foreign)),
            "",
            "## Fed Calibration Context",
            "",
        ]
    )

    if fed_summary is None:
        lines.extend(
            [
                "Calibration context unavailable.",
                "",
                "The report was still built because the sector and foreign artifacts were present.",
            ]
        )
    else:
        lines.extend(_render_fed_calibration_section(fed_summary))

    return "\n".join(lines).rstrip() + "\n"


def _resolve_input_path(
    requested_path: str | Path,
    default_path: Path,
    fallback_path: Path,
    required: bool,
) -> tuple[Path | None, bool]:
    path = Path(requested_path)
    if path.exists():
        return path, False
    if path == default_path and fallback_path.exists():
        return fallback_path, True
    if required:
        raise FileNotFoundError(f"Required input file does not exist: {path}")
    return None, False


def _load_optional_json(path: str | Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    with Path(path).open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _build_sector_summary_rows(sector: pd.DataFrame) -> list[dict[str, str]]:
    if sector.empty:
        return [{"sector_key": "none", "note": "No sector rows found."}]

    frame = sector.copy()
    frame["date"] = pd.to_datetime(frame.get("date"), errors="coerce")
    rows: list[dict[str, str]] = []

    for sector_key, sub in frame.groupby("sector_key", sort=True):
        total_multiplier = (
            _numeric_series(sub, "uncertainty_scale_multiplier", default=1.0)
            * _numeric_series(sub, "uncertainty_fit_multiplier", default=1.0)
            * _numeric_series(sub, "uncertainty_window_obs_multiplier", default=1.0)
            * _numeric_series(sub, "uncertainty_support_multiplier", default=1.0)
        )
        rows.append(
            {
                "sector_key": str(sector_key),
                "dates": _format_date_span(sub["date"]),
                "rows": str(len(sub)),
                "level": _join_unique(sub.get("level_evidence_tier")),
                "maturity": _join_unique(sub.get("maturity_evidence_tier")),
                "anchor": _join_unique(sub.get("anchor_type")),
                "concept": _join_unique(sub.get("concept_match")),
                "coverage": _format_ratio_range(sub.get("coverage_ratio")),
                "support": _join_unique(sub.get("uncertainty_support_source")),
                "short_support": _join_unique(sub.get("uncertainty_short_share_support_source")),
                "band_type": _join_unique(sub.get("uncertainty_band_type")),
                "band_method": _join_unique(sub.get("uncertainty_band_method")),
                "calibration": _join_unique(sub.get("uncertainty_calibration_source")),
                "interval_mult": _format_range(total_multiplier),
            }
        )

    return rows


def _build_foreign_support_rows(foreign: pd.DataFrame) -> list[dict[str, str]]:
    if foreign.empty:
        return [{"holder_group": "none", "note": "No foreign nowcast rows found."}]

    frame = foreign.copy()
    frame["date"] = pd.to_datetime(frame.get("date"), errors="coerce")
    rows: list[dict[str, str]] = []

    for holder_group, sub in frame.groupby("holder_group", sort=True):
        active = _bool_series(sub, "uncertainty_band_active")
        row = {
            "holder_group": str(holder_group),
            "dates": _format_date_span(sub["date"]),
            "rows": str(len(sub)),
            "band_active": f"{int(active.sum())}/{len(sub)} ({(active.mean() if len(sub) else 0.0):.1%})",
        }
        support_kind = sub.get("uncertainty_support_kind")
        for kind in FOREIGN_SUPPORT_KINDS:
            count = int((support_kind == kind).sum()) if support_kind is not None else 0
            row[kind] = str(count)
        rows.append(row)

    return rows


def _build_identified_set_rows(sector: pd.DataFrame) -> list[dict[str, str]]:
    if sector.empty:
        return [{"sector_key": "none", "note": "No sector rows found."}]

    frame = sector.copy()
    frame["date"] = pd.to_datetime(frame.get("date"), errors="coerce")
    rows: list[dict[str, str]] = []

    for sector_key, sub in frame.groupby("sector_key", sort=True):
        identified_active = _bool_series(sub, "identified_set_active")
        bank_provider = _join_unique(sub.get("bank_constraint_provider"))
        bank_dataset = _join_unique(sub.get("bank_constraint_dataset"))
        if not identified_active.any() and bank_provider == "n/a" and bank_dataset == "n/a":
            continue

        clipped = _bool_series(sub, "identified_set_point_clipped")
        clip_gap = _numeric_series(sub, "identified_set_bill_share_gap", default=float("nan"))
        short_identified = _bool_series(sub, "identified_set_short_share_le_1y_active")
        short_clipped = _bool_series(sub, "identified_set_short_share_le_1y_point_clipped")
        short_clip_gap = _numeric_series(sub, "identified_set_short_share_le_1y_gap", default=float("nan"))
        rows.append(
            {
                "sector_key": str(sector_key),
                "dates": _format_date_span(sub["date"]),
                "identified_rows": _format_count_ratio(int(identified_active.sum()), len(sub)),
                "identified_source": _join_unique(sub.get("identified_set_source")),
                "point_clipped_rows": _format_count_ratio(int(clipped.sum()), len(sub)),
                "max_clip_gap": _format_scalar(clip_gap.max() if not clip_gap.dropna().empty else None),
                "short_identified_rows": _format_count_ratio(int(short_identified.sum()), len(sub)),
                "short_identified_source": _join_unique(sub.get("identified_set_short_share_le_1y_source")),
                "short_point_clipped_rows": _format_count_ratio(int(short_clipped.sum()), len(sub)),
                "max_short_clip_gap": _format_scalar(short_clip_gap.max() if not short_clip_gap.dropna().empty else None),
                "bank_provider": bank_provider,
                "bank_dataset": bank_dataset,
                "bank_raw_file": _join_unique(sub.get("bank_constraint_raw_file")),
            }
        )

    if not rows:
        return [{"sector_key": "none", "note": "No identified-set or bank-constraint rows found."}]
    return rows


def _render_hybrid_estimation_section(
    sector: pd.DataFrame,
    fed_summary: dict[str, Any] | None,
) -> list[str]:
    if sector.empty:
        return ["No sector rows found."]

    methods = _join_unique(sector.get("method"))
    factor_cols = sorted([str(col) for col in sector.columns if str(col).startswith("factor_exposure_")])
    factor_method_mask = pd.Series(False, index=sector.index, dtype=bool)
    if "method" in sector.columns:
        factor_method_mask = sector["method"].astype(str).str.contains("plus_factors", regex=False)

    lines = [
        f"- Methods: `{methods}`",
        f"- Rows using factor block: `{_format_count_ratio(int(factor_method_mask.sum()), len(sector))}`",
        f"- Factor exposure columns: `{_summarize_labels(factor_cols)}`",
    ]

    if "tips_share" in sector.columns:
        tips_positive = int((pd.to_numeric(sector["tips_share"], errors="coerce").fillna(0.0) > 1e-12).sum())
        lines.append(f"- Rows with positive `tips_share`: `{_format_count_ratio(tips_positive, len(sector))}`")
    else:
        lines.append("- Rows with positive `tips_share`: `n/a`")

    if "frn_share" in sector.columns:
        frn_positive = int((pd.to_numeric(sector["frn_share"], errors="coerce").fillna(0.0) > 1e-12).sum())
        lines.append(f"- Rows with positive `frn_share`: `{_format_count_ratio(frn_positive, len(sector))}`")
    else:
        lines.append("- Rows with positive `frn_share`: `n/a`")

    if fed_summary is not None:
        factor_block = fed_summary.get("factor_cols") or []
        if factor_block:
            lines.append(f"- Fed calibration factor block: `{_summarize_labels([str(col) for col in factor_block])}`")

    return lines


def _render_fed_calibration_section(fed_summary: dict[str, Any]) -> list[str]:
    interval = dict(fed_summary.get("interval_calibration") or {})
    lines = [
        f"- Status: `{fed_summary.get('status', 'unknown')}`",
        f"- Revaluation fit RMSE: {_format_scalar(fed_summary.get('revaluation_fit_rmse'))}",
    ]
    factor_block = fed_summary.get("factor_cols") or []
    if factor_block:
        lines.append(f"- Factor block: `{_summarize_labels([str(col) for col in factor_block])}`")
    if interval:
        lines.extend(
            [
                f"- Interval calibration status: `{interval.get('status', 'unknown')}`",
                f"- Interval calibration rows: `{interval.get('n_obs', 'n/a')}`",
                f"- Absolute-error quantile: `{interval.get('abs_error_quantile', 'n/a')}`",
                f"- Fit RMSE reference: {_format_scalar(interval.get('fit_rmse_reference'))}",
                "",
                _render_markdown_table(_build_fed_metric_rows(interval.get('metrics') or {})),
            ]
        )
    else:
        lines.extend(["- Interval calibration metadata unavailable."])
    return lines


def _build_fed_metric_rows(metrics: dict[str, Any]) -> list[dict[str, str]]:
    if not metrics:
        return [{"metric": "none", "note": "No metric summaries found."}]

    rows: list[dict[str, str]] = []
    for metric_name in sorted(metrics):
        metric = metrics[metric_name] or {}
        rows.append(
            {
                "metric": metric_name,
                "n_obs": str(metric.get("n_obs", "n/a")),
                "half_width": _format_scalar(metric.get("half_width")),
                "median_abs_error": _format_scalar(metric.get("median_abs_error")),
                "rmse": _format_scalar(metric.get("rmse")),
                "max_abs_error": _format_scalar(metric.get("max_abs_error")),
            }
        )
    return rows


def _render_markdown_table(rows: list[dict[str, str]]) -> str:
    if not rows:
        return "_No rows._"

    columns = list(rows[0].keys())
    header = "| " + " | ".join(columns) + " |"
    divider = "| " + " | ".join(["---"] * len(columns)) + " |"
    body = [
        "| " + " | ".join(_escape_cell(str(row.get(column, ""))) for column in columns) + " |"
        for row in rows
    ]
    return "\n".join([header, divider, *body])


def _escape_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


def _join_unique(series: pd.Series | None) -> str:
    if series is None:
        return "n/a"
    values = [str(value) for value in pd.Series(series).dropna().astype(str).unique() if str(value).strip()]
    if not values:
        return "n/a"
    return ", ".join(sorted(values))


def _format_date_span(series: pd.Series) -> str:
    clean = pd.to_datetime(series, errors="coerce").dropna()
    if clean.empty:
        return "n/a"
    return f"{clean.min().date()} to {clean.max().date()}"


def _format_ratio_range(series: pd.Series | None) -> str:
    if series is None:
        return "n/a"
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return "n/a"
    low = float(clean.min())
    high = float(clean.max())
    if abs(high - low) < 1e-12:
        return f"{low:.1%}"
    return f"{low:.1%}-{high:.1%}"


def _format_range(series: pd.Series) -> str:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return "n/a"
    low = float(clean.min())
    high = float(clean.max())
    if abs(high - low) < 1e-12:
        return f"{low:.2f}"
    return f"{low:.2f}-{high:.2f}"


def _format_count_ratio(count: int, total: int) -> str:
    if total <= 0:
        return "0/0 (0.0%)"
    return f"{count}/{total} ({(count / total):.1%})"


def _format_scalar(value: Any) -> str:
    try:
        parsed = float(value)
    except Exception:
        return "n/a"
    if abs(parsed) >= 100:
        return f"{parsed:.2f}"
    if abs(parsed) >= 1:
        return f"{parsed:.3f}"
    return f"{parsed:.4f}"


def _numeric_series(frame: pd.DataFrame, column: str, default: float) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce").fillna(default)


def _bool_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(False, index=frame.index, dtype=bool)
    return frame[column].fillna(False).astype(bool)


def _summarize_labels(labels: list[str], max_items: int = 8) -> str:
    if not labels:
        return "none"
    if len(labels) <= max_items:
        return ", ".join(labels)
    return f"{', '.join(labels[:max_items])}, ... (+{len(labels) - max_items} more)"


def _fallback_suffix(is_fallback: bool) -> str:
    return " (toy fallback)" if is_fallback else ""
