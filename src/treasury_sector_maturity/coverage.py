from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .utils import load_yaml, project_root

DEFAULT_COVERAGE_REGISTRY_PATH = "configs/coverage_registry.yaml"
DEFAULT_Z1_SERIES_CATALOG_PATH = "configs/z1_series_catalog.yaml"
FULL_Z1_SERIES_CATALOG_PATH = "configs/z1_series_catalog_full.yaml"
DEFAULT_SECTOR_DEFINITIONS_PATH = "configs/sector_definitions.yaml"
FULL_SECTOR_DEFINITIONS_PATH = "configs/sector_definitions_full.yaml"


@dataclass(frozen=True)
class CoverageNode:
    key: str
    label: str
    node_type: str
    sector_family: str | None = None
    parent_key: str | None = None
    is_canonical: bool = False
    required_for_full_coverage: bool = False
    included_in_public_preview_default: bool = False
    included_in_optional_bank_paths: bool = False
    concept_risk: str | None = None
    history_start_reason: str | None = None
    release_window_promotion_eligible: bool = False


def resolve_coverage_registry_path(path: str | Path = DEFAULT_COVERAGE_REGISTRY_PATH) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return project_root() / candidate


def load_coverage_registry(path: str | Path = DEFAULT_COVERAGE_REGISTRY_PATH) -> dict[str, CoverageNode]:
    data = load_yaml(resolve_coverage_registry_path(path))
    nodes: dict[str, CoverageNode] = {}
    for key, raw in (data.get("nodes") or {}).items():
        spec = dict(raw or {})
        nodes[key] = CoverageNode(
            key=key,
            label=str(spec.get("label", key)),
            node_type=str(spec.get("node_type", "atomic")),
            sector_family=_optional_text(spec.get("sector_family")),
            parent_key=_optional_text(spec.get("parent_key")),
            is_canonical=bool(spec.get("is_canonical", False)),
            required_for_full_coverage=bool(spec.get("required_for_full_coverage", False)),
            included_in_public_preview_default=bool(spec.get("included_in_public_preview_default", False)),
            included_in_optional_bank_paths=bool(spec.get("included_in_optional_bank_paths", False)),
            concept_risk=_optional_text(spec.get("concept_risk")),
            history_start_reason=_optional_text(spec.get("history_start_reason")),
            release_window_promotion_eligible=bool(spec.get("release_window_promotion_eligible", False)),
        )
    return nodes


def public_preview_sector_keys(path: str | Path = DEFAULT_COVERAGE_REGISTRY_PATH) -> list[str]:
    return [
        key
        for key, node in load_coverage_registry(path).items()
        if node.included_in_public_preview_default
    ]


def optional_bank_sector_keys(path: str | Path = DEFAULT_COVERAGE_REGISTRY_PATH) -> list[str]:
    return [
        key
        for key, node in load_coverage_registry(path).items()
        if node.included_in_optional_bank_paths
    ]


def preview_catalog_sector_keys(path: str | Path = DEFAULT_COVERAGE_REGISTRY_PATH) -> list[str]:
    return public_preview_sector_keys(path) + optional_bank_sector_keys(path)


def canonical_atomic_sector_keys(path: str | Path = DEFAULT_COVERAGE_REGISTRY_PATH) -> list[str]:
    return [
        key
        for key, node in load_coverage_registry(path).items()
        if node.is_canonical and node.node_type == "atomic"
    ]


def required_canonical_sector_keys(path: str | Path = DEFAULT_COVERAGE_REGISTRY_PATH) -> list[str]:
    return [
        key
        for key, node in load_coverage_registry(path).items()
        if node.is_canonical and node.required_for_full_coverage
    ]


def required_full_coverage_sector_keys(path: str | Path = DEFAULT_COVERAGE_REGISTRY_PATH) -> list[str]:
    return [
        key
        for key, node in load_coverage_registry(path).items()
        if node.required_for_full_coverage
    ]


def resolve_z1_build_scope(scope: str) -> dict[str, str]:
    normalized = str(scope or "default").strip().lower()
    if normalized == "default":
        return {
            "scope": "default",
            "catalog_path": DEFAULT_Z1_SERIES_CATALOG_PATH,
            "sector_defs_path": DEFAULT_SECTOR_DEFINITIONS_PATH,
            "series_out": "data/interim/z1_series_panel.csv",
            "sector_out": "data/interim/z1_sector_panel.csv",
        }
    if normalized == "full":
        return {
            "scope": "full",
            "catalog_path": FULL_Z1_SERIES_CATALOG_PATH,
            "sector_defs_path": FULL_SECTOR_DEFINITIONS_PATH,
            "series_out": "data/interim/z1_series_panel_full.csv",
            "sector_out": "data/interim/z1_sector_panel_full.csv",
        }
    raise ValueError(f"Unsupported coverage scope: {scope}")


def resolve_z1_fetch_provider(scope: str, provider: str) -> str:
    normalized_scope = str(scope or "default").strip().lower()
    normalized_provider = str(provider or "auto").strip().lower()

    if normalized_scope == "full":
        if normalized_provider == "auto":
            return "fed"
        if normalized_provider == "fred":
            raise ValueError(
                "Full coverage scope currently requires Fed Z.1 sourcing because the full catalog does not yet have complete explicit FRED mappings."
            )
    return normalized_provider


def resolve_fed_calibration_scope(scope: str) -> dict[str, str]:
    normalized = str(scope or "default").strip().lower()
    if normalized == "default":
        return {
            "scope": "default",
            "z1_panel": "data/interim/z1_sector_panel.csv",
            "exact_out": "data/processed/fed_exact_metrics.csv",
            "interval_calibration_out": "data/processed/fed_interval_calibration.csv",
            "summary_out": "outputs/fed_calibration_summary.json",
        }
    if normalized == "full":
        return {
            "scope": "full",
            "z1_panel": "data/interim/z1_sector_panel_full.csv",
            "exact_out": "data/processed/fed_exact_metrics_full.csv",
            "interval_calibration_out": "data/processed/fed_interval_calibration_full.csv",
            "summary_out": "outputs/fed_calibration_summary_full.json",
        }
    raise ValueError(f"Unsupported calibration scope: {scope}")


def resolve_estimation_scope(scope: str) -> dict[str, str]:
    normalized = str(scope or "default").strip().lower()
    if normalized == "default":
        return {
            "scope": "default",
            "z1_panel": "data/interim/z1_sector_panel.csv",
            "sector_defs_path": DEFAULT_SECTOR_DEFINITIONS_PATH,
            "interval_calibration_file": "data/processed/fed_interval_calibration.csv",
            "out": "data/processed/sector_effective_maturity.csv",
        }
    if normalized == "full":
        return {
            "scope": "full",
            "z1_panel": "data/interim/z1_sector_panel_full.csv",
            "sector_defs_path": FULL_SECTOR_DEFINITIONS_PATH,
            "interval_calibration_file": "data/processed/fed_interval_calibration_full.csv",
            "out": "data/processed/sector_effective_maturity_full.csv",
        }
    raise ValueError(f"Unsupported estimation scope: {scope}")


def coverage_registry_frame(path: str | Path = DEFAULT_COVERAGE_REGISTRY_PATH) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for key, node in load_coverage_registry(path).items():
        rows.append(
            {
                "sector_key": key,
                "registry_label": node.label,
                "node_type": node.node_type,
                "sector_family": node.sector_family,
                "parent_key": node.parent_key,
                "is_canonical": node.is_canonical,
                "required_for_full_coverage": node.required_for_full_coverage,
                "included_in_public_preview_default": node.included_in_public_preview_default,
                "included_in_optional_bank_paths": node.included_in_optional_bank_paths,
                "concept_risk": node.concept_risk,
                "history_start_reason": node.history_start_reason,
                "release_window_promotion_eligible": node.release_window_promotion_eligible,
            }
        )
    return pd.DataFrame(rows)


def attach_coverage_metadata(
    frame: pd.DataFrame,
    path: str | Path = DEFAULT_COVERAGE_REGISTRY_PATH,
) -> pd.DataFrame:
    if frame.empty or "sector_key" not in frame.columns:
        return frame.copy()

    metadata = coverage_registry_frame(path)
    if metadata.empty:
        return frame.copy()

    return frame.merge(metadata, on="sector_key", how="left")


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
