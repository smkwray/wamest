from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from .h15 import build_benchmark_panel, curve_block_config, load_h15_curve_file
from .providers import default_normalized_dir, fetch_h15_curves


def parse_curve_file_overrides(entries: Iterable[str] | None) -> dict[str, Path]:
    overrides: dict[str, Path] = {}
    for entry in entries or []:
        text = str(entry).strip()
        if not text:
            continue
        if "=" not in text:
            raise ValueError("--curve-file entries must use CURVE_KEY=PATH format.")
        curve_key, raw_path = text.split("=", 1)
        curve_key = curve_key.strip()
        raw_path = raw_path.strip()
        if not curve_key or not raw_path:
            raise ValueError("--curve-file entries must use CURVE_KEY=PATH format.")
        overrides[curve_key] = Path(raw_path)
    return overrides


def normalized_family_list(values: Iterable[str] | None, default: list[str] | None = None) -> list[str]:
    raw_values = list(default or []) if values is None else list(values)
    out: list[str] = []
    for value in raw_values:
        text = str(value).strip()
        if text and text not in out:
            out.append(text)
    return out


def merge_benchmark_panels(panels: list[pd.DataFrame]) -> pd.DataFrame:
    if not panels:
        raise ValueError("At least one benchmark panel is required.")

    merged = panels[0].copy()
    if "date" not in merged.columns:
        raise ValueError("Benchmark panels must include a date column.")

    for panel in panels[1:]:
        current = panel.copy()
        if "date" not in current.columns:
            raise ValueError("Benchmark panels must include a date column.")
        overlap = sorted((set(merged.columns) & set(current.columns)) - {"date"})
        if overlap:
            raise ValueError(f"Benchmark panel columns overlap across families: {', '.join(overlap)}")
        merged = merged.merge(current, on="date", how="outer")

    return merged.sort_values("date").reset_index(drop=True)


def build_estimation_benchmark_blocks(
    *,
    series_config_path: str | Path,
    provider: str,
    holdings_families: list[str],
    factor_families: list[str] | None = None,
    curve_file_overrides: dict[str, Path] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame | None]:
    holdings = normalized_family_list(holdings_families)
    factors = normalized_family_list(factor_families)
    if not holdings:
        raise ValueError("At least one holdings benchmark family is required.")

    curve_files = dict(curve_file_overrides or {})
    source_curve_cache: dict[str, pd.DataFrame] = {}
    family_panels: dict[str, pd.DataFrame] = {}

    for family in [*holdings, *factors]:
        if family in family_panels:
            continue

        curve_block = curve_block_config(series_config_path, family)
        source_curve_key = str(curve_block.get("source_curve_key") or family)

        if source_curve_key not in source_curve_cache:
            if source_curve_key in curve_files:
                source_path = curve_files[source_curve_key]
                source_curve_cache[source_curve_key] = load_h15_curve_file(
                    source_path,
                    series_config_path=series_config_path,
                    curve_key=source_curve_key,
                )
            else:
                artifact = fetch_h15_curves(
                    provider=provider,
                    series_config_path=series_config_path,
                    curve_key=source_curve_key,
                    normalized_out=default_normalized_dir() / f"h15_curves_{provider}_{source_curve_key}.csv",
                )
                source_curve_cache[source_curve_key] = load_h15_curve_file(
                    artifact.normalized_path,
                    series_config_path=series_config_path,
                    curve_key=source_curve_key,
                )

        family_panels[family] = build_benchmark_panel(
            source_curve_cache[source_curve_key],
            curve_block=curve_block,
        )

    holdings_panel = merge_benchmark_panels([family_panels[family] for family in holdings])
    factor_panel = merge_benchmark_panels([family_panels[family] for family in factors]) if factors else None
    return holdings_panel, factor_panel
