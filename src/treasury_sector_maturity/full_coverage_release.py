from __future__ import annotations

from dataclasses import dataclass, replace
import json
from pathlib import Path
import re
from typing import Any

import pandas as pd

from .benchmark_sets import build_estimation_benchmark_blocks, parse_curve_file_overrides
from .calibration import (
    build_fed_interval_calibration,
    calibrate_fed_revaluation_mapping,
    summarize_interval_calibration,
)
from .coverage import (
    load_coverage_registry,
    required_canonical_sector_keys,
    resolve_estimation_scope,
    resolve_fed_calibration_scope,
    resolve_z1_build_scope,
    resolve_z1_fetch_provider,
)
from .estimation import EstimationSettings, attach_revaluation_returns, estimate_effective_maturity_panel
from .ffiec import build_bank_constraint_panel
from .h15 import load_h15_curve_file
from .output_metadata import annotate_estimated_output
from .providers import (
    fetch_fred_series_observations,
    fetch_h15_curves,
    fetch_soma_holdings,
    fetch_z1_series,
    normalize_fred_observations,
)
from .soma import read_soma_holdings, summarize_soma_quarterly
from .tic import build_foreign_monthly_nowcast, extract_shl_total_foreign_benchmark, load_extracted_shl_issue_mix, load_shl_historical_treasury_benchmark, load_slt_short_long
from .utils import dump_json, ensure_parent, load_yaml, read_table, write_table
from .z1 import build_sector_panel, compute_identity_errors, load_series_catalog, materialize_series_panel, parse_z1_ddp_csv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SUMMARY_JSON = Path("outputs/full_coverage_release/full_coverage_summary.json")
DEFAULT_REPORT_PATH = Path("outputs/full_coverage_release/full_coverage_report.md")
DEFAULT_RELEASE_DIR = Path("outputs/full_coverage_release")
DEFAULT_REGISTRY_PATH = Path("configs/coverage_registry.yaml")
DEFAULT_RELEASE_CONFIG_PATH = Path("configs/full_coverage_release.yaml")
RECONCILIATION_TOLERANCE = 1e-6
PRIMARY_ESTIMATE_COLUMNS = [
    "bill_share",
    "effective_duration_years",
    "zero_coupon_equivalent_years",
]
SECONDARY_ESTIMATE_COLUMNS = [
    "short_share_le_1y",
    "coupon_share",
    "tips_share",
    "frn_share",
    "coupon_only_maturity_years",
]
INTERVAL_ESTIMATE_COLUMNS = [
    "bill_share_lower",
    "bill_share_upper",
    "short_share_le_1y_lower",
    "short_share_le_1y_upper",
    "effective_duration_years_lower",
    "effective_duration_years_upper",
    "zero_coupon_equivalent_years_lower",
    "zero_coupon_equivalent_years_upper",
]
ESTIMATE_METADATA_COLUMNS = [
    "estimand_class",
    "estimator_family",
    "selection_reason",
    "high_confidence_flag",
    "estimate_origin_includes_short_window_promotion",
    "short_window_promotion_quarters",
    "level_evidence_tier",
    "maturity_evidence_tier",
    "level_measurement_basis",
    "maturity_measurement_basis",
    "anchor_type",
    "concept_match",
    "coverage_ratio",
    "coverage_measurement_basis",
    "coverage_label",
    "effective_duration_status",
    "point_estimate_origin",
    "interval_origin",
]


@dataclass(frozen=True)
class FullCoverageReleaseArtifacts:
    canonical_sector_maturity_path: Path
    latest_sector_snapshot_path: Path
    high_confidence_sector_maturity_path: Path
    reconciliation_nodes_path: Path
    fed_exact_overlay_path: Path
    required_sector_inventory_path: Path
    report_path: Path
    manifest_path: Path
    summary_json_path: Path


@dataclass(frozen=True)
class _BuiltSectorInputs:
    raw_long_df: pd.DataFrame
    long_df: pd.DataFrame
    series_panel: pd.DataFrame
    sector_panel: pd.DataFrame
    catalog: dict[str, Any]


def build_full_coverage_release(
    *,
    out_dir: str | Path = DEFAULT_RELEASE_DIR,
    source_provider: str = "fed",
    coverage_scope: str = "full",
    end_date: str | None = None,
    quarters: int | None = None,
    summary_json_out: str | Path | None = None,
    command: str | None = None,
    z1_file: str | Path | None = None,
    h15_file: str | Path | None = None,
    curve_file: list[str] | None = None,
    soma_file: str | Path | None = None,
    foreign_shl_file: str | Path | None = None,
    foreign_slt_file: str | Path | None = None,
    bank_constraint_file: str | Path | None = None,
    ffiec_file: str | Path | None = None,
    ffiec002_file: str | Path | None = None,
    ncua_file: str | Path | None = None,
    bank_supplement_file: str | Path | None = None,
    series_catalog: str | Path = "configs/z1_series_catalog_full.yaml",
    sector_defs: str | Path = "configs/sector_definitions_full.yaml",
    model_config: str | Path = "configs/model_defaults.yaml",
    series_config: str | Path = "configs/h15_series.yaml",
    bank_constraints_config: str | Path = "configs/bank_constraints.yaml",
    release_config: str | Path = DEFAULT_RELEASE_CONFIG_PATH,
    supplement_missing_z1_levels_from_fred: bool = False,
) -> FullCoverageReleaseArtifacts:
    if str(coverage_scope).strip().lower() != "full":
        raise ValueError("The full-coverage release builder only supports coverage_scope='full'.")

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    summary_json_path = Path(summary_json_out) if summary_json_out is not None else out_dir / "full_coverage_summary.json"
    report_path = out_dir / "full_coverage_report.md"
    canonical_path = out_dir / "canonical_sector_maturity.csv"
    latest_path = out_dir / "latest_sector_snapshot.csv"
    high_confidence_path = out_dir / "high_confidence_sector_maturity.csv"
    reconciliation_path = out_dir / "reconciliation_nodes.csv"
    fed_exact_overlay_path = out_dir / "fed_exact_overlay.csv"
    required_inventory_path = out_dir / "required_sector_inventory.csv"
    manifest_path = out_dir / "run_manifest.json"

    run_started = pd.Timestamp.now("UTC")
    source_artifacts: dict[str, Any] = {}
    provider_summary: dict[str, str] = {}
    intermediate_artifacts: dict[str, Any] = {}

    z1_scope = resolve_z1_build_scope("full")
    z1_provider = resolve_z1_fetch_provider("full", source_provider)
    z1_path = _resolve_z1_input(
        z1_file=z1_file,
        provider=z1_provider,
        source_artifacts=source_artifacts,
        provider_summary=provider_summary,
        full_scope_normalized_out=Path(z1_scope["series_out"]),
    )
    sector_inputs = _build_sector_panel(
        z1_path=z1_path,
        series_catalog=series_catalog,
        sector_defs=sector_defs,
        series_out=Path(z1_scope["series_out"]),
        sector_out=Path(z1_scope["sector_out"]),
        intermediate_artifacts=intermediate_artifacts,
        supplement_missing_z1_levels_from_fred=supplement_missing_z1_levels_from_fred,
    )
    sector_panel = sector_inputs.sector_panel
    sector_definitions = dict((load_yaml(sector_defs).get("sectors") or {}))
    coverage_registry = load_coverage_registry(DEFAULT_REGISTRY_PATH)
    release_config_data = load_yaml(release_config)
    promotion_window_quarters = _resolve_release_promotion_window_quarters(release_config_data)

    model_cfg = load_yaml(model_config)
    est_cfg = model_cfg.get("estimation", {})
    interval_cfg = model_cfg.get("interval_calibration", {})
    settings = EstimationSettings(
        rolling_window_quarters=int(est_cfg.get("rolling_window_quarters", 12)),
        smoothness_penalty=float(est_cfg.get("smoothness_penalty", 10.0)),
        turnover_penalty=float(est_cfg.get("turnover_penalty", 2.0)),
        ridge_penalty=float(est_cfg.get("ridge_penalty", 0.01)),
        bill_share_penalty=float(est_cfg.get("bill_share_penalty", 0.0)),
        factor_ridge_penalty=float(est_cfg.get("factor_ridge_penalty", 0.1)),
        factor_turnover_penalty=float(est_cfg.get("factor_turnover_penalty", 0.0)),
    )

    benchmark, factor_benchmark = _build_benchmark_blocks(
        h15_file=h15_file,
        curve_file_overrides=parse_curve_file_overrides(curve_file),
        source_provider=source_provider,
        model_cfg=model_cfg,
        series_config=series_config,
    )
    intermediate_artifacts["benchmark_returns"] = {
        "holdings_family": "nominal_treasury_constant_maturity",
        "source_provider": source_provider,
    }

    fed_summary, interval_calibration, fed_exact_metrics = _build_fed_calibration(
        sector_panel=sector_panel,
        benchmark=benchmark,
        factor_benchmark=factor_benchmark,
        settings=settings,
        interval_cfg=interval_cfg,
        source_provider=source_provider,
        series_config=series_config,
        h15_file=h15_file,
        soma_file=soma_file,
        source_artifacts=source_artifacts,
        provider_summary=provider_summary,
        intermediate_artifacts=intermediate_artifacts,
    )

    foreign_nowcast = _build_foreign_nowcast(
        foreign_shl_file=foreign_shl_file,
        foreign_slt_file=foreign_slt_file,
        source_artifacts=source_artifacts,
        provider_summary=provider_summary,
        intermediate_artifacts=intermediate_artifacts,
    )

    bank_constraints = _build_bank_constraints(
        bank_constraint_file=bank_constraint_file,
        ffiec_file=ffiec_file,
        ffiec002_file=ffiec002_file,
        ncua_file=ncua_file,
        bank_supplement_file=bank_supplement_file,
        bank_constraints_config=bank_constraints_config,
        source_artifacts=source_artifacts,
        provider_summary=provider_summary,
        intermediate_artifacts=intermediate_artifacts,
    )

    estimated = estimate_effective_maturity_panel(
        sector_panel,
        benchmark,
        factor_returns=factor_benchmark,
        settings=settings,
        interval_calibration=interval_calibration,
        interval_settings=interval_cfg,
        foreign_nowcast=foreign_nowcast,
        bank_constraints=bank_constraints,
        sector_config_path=str(sector_defs),
        annotation_mode="full_coverage",
    )
    estimated = _merge_promoted_release_estimates(
        estimated=estimated,
        sector_panel=sector_panel,
        benchmark=benchmark,
        factor_benchmark=factor_benchmark,
        settings=settings,
        interval_calibration=interval_calibration,
        interval_cfg=interval_cfg,
        foreign_nowcast=foreign_nowcast,
        bank_constraints=bank_constraints,
        sector_defs_path=sector_defs,
        sector_definitions=sector_definitions,
        coverage_registry=coverage_registry,
        promotion_window_quarters=promotion_window_quarters,
    )
    intermediate_artifacts["sector_effective_maturity"] = str(resolve_estimation_scope("full")["out"])
    write_table(estimated, intermediate_artifacts["sector_effective_maturity"])

    registry_path = DEFAULT_REGISTRY_PATH
    required_canonical = required_canonical_sector_keys(registry_path)

    canonical = _annotate_publication_status(
        _apply_history_preserving_backfill(
            _build_required_canonical_panel(
                sector_panel=sector_panel,
                estimated=estimated,
                required_canonical=required_canonical,
            )
        )
    )
    latest_quarter = _resolve_latest_snapshot_quarter(sector_panel, required_canonical)
    latest_required_sectors = _latest_required_sector_keys(sector_panel, required_canonical, latest_quarter)
    if latest_quarter is not None:
        latest = canonical[
            canonical["date"].eq(latest_quarter)
            & canonical["sector_key"].astype(str).isin(latest_required_sectors)
        ].copy()
    else:
        latest = canonical.iloc[0:0].copy()
    high_confidence = canonical[canonical["high_confidence_flag"].fillna(False)].copy()
    reconciliation = estimated[~estimated["is_canonical"].fillna(False)].copy()

    for frame in (canonical, latest, high_confidence, reconciliation):
        if "warnings" in frame.columns:
            frame.drop(columns=["warnings"], inplace=True)
    export_columns = [
        "date",
        "sector_key",
        "node_type",
        "sector_family",
        "required_for_full_coverage",
        "concept_risk",
        "history_start_reason",
        "estimand_class",
        "estimator_family",
        "selection_reason",
        "high_confidence_flag",
        "history_preserving_backfill",
        "publication_status",
        "publication_status_reason",
        "row_is_short_window_estimate",
        "estimate_origin_includes_short_window_promotion",
        "short_window_promotion_quarters",
        "level_evidence_tier",
        "maturity_evidence_tier",
        "level_measurement_basis",
        "maturity_measurement_basis",
        "anchor_type",
        "concept_match",
        "coverage_ratio",
        "coverage_measurement_basis",
        "coverage_label",
        "level_source_provider_used",
        "level_supplemented_from_fred",
        "effective_duration_status",
        "point_estimate_origin",
        "interval_origin",
        "bill_share",
        "short_share_le_1y",
        "coupon_share",
        "tips_share",
        "frn_share",
        "effective_duration_years",
        "zero_coupon_equivalent_years",
        "coupon_only_maturity_years",
        "bill_share_lower",
        "bill_share_upper",
        "short_share_le_1y_lower",
        "short_share_le_1y_upper",
        "effective_duration_years_lower",
        "effective_duration_years_upper",
        "zero_coupon_equivalent_years_lower",
        "zero_coupon_equivalent_years_upper",
    ]
    canonical_export = _project_columns(canonical, export_columns)
    latest_export = _project_columns(latest, export_columns)
    high_confidence_export = _project_columns(high_confidence, export_columns)
    reconciliation_export = _project_columns(reconciliation, export_columns)
    fed_exact_overlay = _build_fed_exact_overlay(fed_exact_metrics)
    required_sector_inventory = _build_required_sector_inventory(
        canonical=canonical,
        sector_panel=sector_panel,
        sector_definitions=sector_definitions,
        coverage_registry=coverage_registry,
        catalog=sector_inputs.catalog,
        raw_long_df=sector_inputs.raw_long_df,
        long_df=sector_inputs.long_df,
    )
    source_series_audit = _build_source_series_audit(required_sector_inventory)

    write_table(canonical_export, canonical_path)
    write_table(latest_export, latest_path)
    write_table(high_confidence_export, high_confidence_path)
    write_table(reconciliation_export, reconciliation_path)
    write_table(fed_exact_overlay, fed_exact_overlay_path)
    write_table(required_sector_inventory, required_inventory_path)

    release_summary = _build_release_summary(
        canonical=canonical,
        latest=latest,
        high_confidence=high_confidence,
        requested_end_date=end_date,
        resolved_latest_snapshot_date=None if latest_quarter is None else pd.Timestamp(latest_quarter).date().isoformat(),
        quarters=quarters,
        coverage_scope=coverage_scope,
        source_provider=source_provider,
        model_config_path=model_config,
        release_config_path=release_config,
        sector_defs=sector_defs,
        series_catalog=series_catalog,
        summary_json_out=summary_json_path,
        reconciliation=reconciliation,
        required_sector_inventory=required_sector_inventory,
    )
    history_spans = _build_history_spans(canonical, sector_panel, required_canonical)
    coverage_completeness = _build_coverage_completeness(canonical, sector_panel, required_canonical)
    weakest_sectors = _build_weakest_sectors(canonical, sector_panel)
    reconciliation_diagnostics = _build_reconciliation_diagnostics(
        sector_panel=sector_panel,
        sector_definitions=sector_definitions,
        coverage_registry=coverage_registry,
    )
    latest_snapshot_summary = _build_latest_snapshot_summary(latest, latest_quarter)
    fed_exact_overlay_summary = _build_fed_exact_overlay_summary(fed_exact_overlay)
    history_backfill_summary = _build_history_backfill_summary(canonical)
    validation = _build_validation_summary(
        canonical=canonical,
        latest=latest,
        high_confidence=high_confidence,
        sector_panel=sector_panel,
        coverage_completeness=coverage_completeness,
        required_canonical=required_canonical,
        latest_quarter=latest_quarter,
        latest_required_sectors=latest_required_sectors,
        reconciliation=reconciliation,
        reconciliation_diagnostics=reconciliation_diagnostics,
    )
    _raise_on_failed_validations(validation)
    provenance = _build_provenance_summary(
        source_artifacts=source_artifacts,
        intermediate_artifacts=intermediate_artifacts,
        manifest_path=manifest_path,
        report_path=report_path,
        summary_json_path=summary_json_path,
    )

    summary = {
        "schema_version": "v0.2-full-coverage",
        "release_summary": release_summary,
        "coverage_completeness": coverage_completeness,
        "source_series_audit": source_series_audit,
        "history_spans": history_spans,
        "history_preserving_backfill": history_backfill_summary,
        "latest_snapshot_summary": latest_snapshot_summary,
        "fed_exact_overlay_summary": fed_exact_overlay_summary,
        "high_confidence_subset": {
            "count": int(len(high_confidence)),
            "sector_keys": sorted(str(value) for value in high_confidence["sector_key"].dropna().astype(str).unique()),
        },
        "weakest_sectors": weakest_sectors,
        "reconciliation_diagnostics": reconciliation_diagnostics,
        "validation": validation,
        "provenance": provenance,
        "machine_readable_outputs": {
            "canonical_sector_maturity": str(canonical_path),
            "latest_sector_snapshot": str(latest_path),
            "high_confidence_sector_maturity": str(high_confidence_path),
            "reconciliation_nodes": str(reconciliation_path),
            "fed_exact_overlay": str(fed_exact_overlay_path),
            "required_sector_inventory": str(required_inventory_path),
            "full_coverage_report": str(report_path),
            "full_coverage_summary": str(summary_json_path),
            "run_manifest": str(manifest_path),
        },
        "source_artifact_paths": dict(source_artifacts),
    }
    dump_json(summary, summary_json_path)

    manifest = {
        "schema_version": "v0.2-full-coverage",
        "run_timestamp_utc": run_started.isoformat(),
        "command": command,
        "coverage_scope": coverage_scope,
        "source_provider_requested": source_provider,
        "source_provider_used": dict(provider_summary),
        "model_config_path": str(model_config),
        "release_config_path": str(release_config),
        "series_catalog_path": str(series_catalog),
        "sector_defs_path": str(sector_defs),
        "benchmark_contract": {
            "holdings_benchmark_families": list(est_cfg.get("holdings_benchmark_families") or ["nominal_treasury_constant_maturity"]),
            "factor_benchmark_families": list(est_cfg.get("factor_benchmark_families") or []),
        },
        "end_date": end_date,
        "quarters": quarters,
        "resolved_latest_snapshot_date": None if latest_quarter is None else pd.Timestamp(latest_quarter).date().isoformat(),
        "source_artifact_paths": dict(source_artifacts),
        "intermediate_artifact_paths": dict(intermediate_artifacts),
        "output_paths": {
            "canonical_sector_maturity": str(canonical_path),
            "latest_sector_snapshot": str(latest_path),
            "high_confidence_sector_maturity": str(high_confidence_path),
            "reconciliation_nodes": str(reconciliation_path),
            "fed_exact_overlay": str(fed_exact_overlay_path),
            "required_sector_inventory": str(required_inventory_path),
            "full_coverage_report": str(report_path),
            "full_coverage_summary": str(summary_json_path),
            "run_manifest": str(manifest_path),
        },
    }
    dump_json(manifest, manifest_path)

    report = _render_release_report(
        release_summary=release_summary,
        coverage_completeness=coverage_completeness,
        source_series_audit=source_series_audit,
        history_spans=history_spans,
        history_backfill_summary=history_backfill_summary,
        latest_snapshot_summary=latest_snapshot_summary,
        fed_exact_overlay=fed_exact_overlay,
        fed_exact_overlay_summary=fed_exact_overlay_summary,
        high_confidence=high_confidence,
        weakest_sectors=weakest_sectors,
        reconciliation_diagnostics=reconciliation_diagnostics,
        validation=validation,
        provenance=provenance,
        canonical=canonical,
        latest=latest,
        high_confidence_path=high_confidence_path,
        canonical_path=canonical_path,
        latest_path=latest_path,
        reconciliation_path=reconciliation_path,
        fed_exact_overlay_path=fed_exact_overlay_path,
        required_inventory_path=required_inventory_path,
        required_sector_inventory=required_sector_inventory,
        summary_json_path=summary_json_path,
        manifest_path=manifest_path,
    )
    report_path.write_text(report, encoding="utf-8")

    return FullCoverageReleaseArtifacts(
        canonical_sector_maturity_path=canonical_path,
        latest_sector_snapshot_path=latest_path,
        high_confidence_sector_maturity_path=high_confidence_path,
        reconciliation_nodes_path=reconciliation_path,
        fed_exact_overlay_path=fed_exact_overlay_path,
        required_sector_inventory_path=required_inventory_path,
        report_path=report_path,
        manifest_path=manifest_path,
        summary_json_path=summary_json_path,
    )


def _resolve_z1_input(
    *,
    z1_file: str | Path | None,
    provider: str,
    source_artifacts: dict[str, Any],
    provider_summary: dict[str, str],
    full_scope_normalized_out: Path,
) -> Path:
    if z1_file is not None:
        path = Path(z1_file)
        source_artifacts["z1"] = {"provided_path": str(path)}
        provider_summary["z1"] = "provided"
        return path

    artifact = fetch_z1_series(
        provider=provider,
        series_catalog_path="configs/z1_series_catalog_full.yaml",
        normalized_out=full_scope_normalized_out.with_name(f"z1_series_{provider}_full.csv"),
    )
    source_artifacts["z1"] = {"normalized_path": str(artifact.normalized_path), "provider": artifact.provider}
    provider_summary["z1"] = artifact.provider
    return artifact.normalized_path


def _build_sector_panel(
    *,
    z1_path: Path,
    series_catalog: str | Path,
    sector_defs: str | Path,
    series_out: Path,
    sector_out: Path,
    intermediate_artifacts: dict[str, Any],
    supplement_missing_z1_levels_from_fred: bool,
) -> _BuiltSectorInputs:
    raw_long_df = parse_z1_ddp_csv(z1_path)
    long_df = raw_long_df.copy()
    catalog = load_series_catalog(series_catalog)
    sector_defs_data = load_yaml(sector_defs).get("sectors") or {}
    supplement_summary: dict[str, Any] = {}
    if supplement_missing_z1_levels_from_fred:
        long_df, supplement_summary = _supplement_missing_z1_levels_from_fred(
            long_df=long_df,
            catalog=catalog,
            sector_defs_path=sector_defs,
        )
        intermediate_artifacts["z1_level_fred_supplement"] = supplement_summary
    series_panel = materialize_series_panel(long_df, catalog)
    series_panel = compute_identity_errors(series_panel)
    write_table(series_panel, series_out)
    intermediate_artifacts["z1_series_panel"] = str(series_out)

    sector_panel = build_sector_panel(series_panel, sector_defs)
    sector_panel = compute_identity_errors(
        sector_panel.rename(columns={"sector_key": "series_key"}).copy()
    ).rename(columns={"series_key": "sector_key"})
    sector_panel = attach_revaluation_returns(sector_panel, group_col="sector_key")
    sector_panel = _attach_level_source_provenance(
        sector_panel,
        sector_definitions=sector_defs_data,
        supplement_summary=supplement_summary,
    )
    write_table(sector_panel, sector_out)
    intermediate_artifacts["z1_sector_panel"] = str(sector_out)
    return _BuiltSectorInputs(
        raw_long_df=raw_long_df,
        long_df=long_df,
        series_panel=series_panel,
        sector_panel=sector_panel,
        catalog=catalog,
    )


def _supplement_missing_z1_levels_from_fred(
    *,
    long_df: pd.DataFrame,
    catalog: dict[str, Any],
    sector_defs_path: str | Path,
    coverage_registry_path: str | Path = DEFAULT_REGISTRY_PATH,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    sector_defs = load_yaml(sector_defs_path).get("sectors") or {}
    coverage_registry = load_coverage_registry(coverage_registry_path)
    required_atomic = sorted(
        key for key, node in coverage_registry.items() if node.required_for_full_coverage and node.node_type == "atomic"
    )
    base_long_df = long_df[["series_code", "date", "value"]].copy()
    available_codes = set(base_long_df["series_code"].dropna().astype(str))
    supplement_frames: list[pd.DataFrame] = []
    supplemented_sector_keys: list[str] = []
    supplemented_series_codes: list[str] = []
    supplemented_level_rows: list[dict[str, Any]] = []

    for sector_key in required_atomic:
        level_series_key = _as_text((sector_defs.get(sector_key) or {}).get("level_series"))
        if level_series_key is None:
            continue
        spec = catalog.get(level_series_key)
        if spec is None:
            continue
        series_code = _as_text(getattr(spec, "level", None))
        fred_id = _as_text((getattr(spec, "fred_ids", None) or {}).get("level"))
        if series_code is None or fred_id is None or series_code in available_codes:
            continue

        payload = fetch_fred_series_observations(fred_id)
        frame = normalize_fred_observations(payload, value_name="value", frequency_suffix="Q")
        if frame.empty or frame["value"].notna().sum() == 0:
            continue

        supplement = frame[["date", "value"]].copy()
        supplement["series_code"] = series_code
        supplement = supplement[["series_code", "date", "value"]]
        supplement_frames.append(supplement)
        supplemented_sector_keys.append(sector_key)
        supplemented_series_codes.append(series_code)
        supplemented_level_rows.extend(
            {
                "sector_key": sector_key,
                "series_code": series_code,
                "date": pd.Timestamp(date).normalize(),
                "level_source_provider_used": "fred_level_supplement",
                "level_supplemented_from_fred": True,
            }
            for date in supplement["date"].dropna().unique()
        )
        available_codes.add(series_code)

    if supplement_frames:
        base_long_df = pd.concat([base_long_df, *supplement_frames], ignore_index=True)
        base_long_df = base_long_df.sort_values(["series_code", "date"]).drop_duplicates(
            ["series_code", "date"], keep="last"
        )

    return base_long_df.reset_index(drop=True), {
        "supplemented_sector_keys": supplemented_sector_keys,
        "supplemented_series_codes": supplemented_series_codes,
        "supplemented_series_count": len(supplemented_series_codes),
        "supplemented_level_rows": [
            {
                "sector_key": item["sector_key"],
                "series_code": item["series_code"],
                "date": pd.Timestamp(item["date"]).date().isoformat(),
                "level_source_provider_used": item["level_source_provider_used"],
                "level_supplemented_from_fred": bool(item["level_supplemented_from_fred"]),
            }
            for item in supplemented_level_rows
        ],
    }


def _attach_level_source_provenance(
    sector_panel: pd.DataFrame,
    *,
    sector_definitions: dict[str, Any],
    supplement_summary: dict[str, Any] | None,
) -> pd.DataFrame:
    out = sector_panel.copy()
    out["date"] = pd.to_datetime(out.get("date"), errors="coerce")
    out["level_source_provider_used"] = pd.NA
    out["level_supplemented_from_fred"] = False

    default_provider_map: dict[str, str] = {}
    for sector_key, raw_spec in sector_definitions.items():
        spec = dict(raw_spec or {})
        method_priority = list(spec.get("method_priority") or [])
        primary = str(method_priority[0]).strip() if method_priority else ""
        if spec.get("formula_level"):
            default_provider_map[str(sector_key)] = "computed_identity"
        elif primary == "computed_series_proxy":
            default_provider_map[str(sector_key)] = "computed_proxy"
        elif spec.get("level_series"):
            default_provider_map[str(sector_key)] = "fed_z1"

    if default_provider_map:
        default_mask = out["level"].notna() & out["sector_key"].astype(str).isin(default_provider_map)
        out.loc[default_mask, "level_source_provider_used"] = out.loc[default_mask, "sector_key"].astype(str).map(default_provider_map)

    rows = list((supplement_summary or {}).get("supplemented_level_rows") or [])
    if not rows:
        return out

    supplement_df = pd.DataFrame(rows)
    supplement_df["date"] = pd.to_datetime(supplement_df.get("date"), errors="coerce")
    supplement_df["sector_key"] = supplement_df["sector_key"].astype(str)
    supplement_df["level_supplemented_from_fred"] = supplement_df["level_supplemented_from_fred"].fillna(False).astype(bool)
    supplement_df = supplement_df.drop_duplicates(["date", "sector_key"])
    out = out.merge(
        supplement_df[["date", "sector_key", "level_source_provider_used", "level_supplemented_from_fred"]].rename(
            columns={
                "level_source_provider_used": "_supplement_level_source_provider_used",
                "level_supplemented_from_fred": "_supplement_level_supplemented_from_fred",
            }
        ),
        on=["date", "sector_key"],
        how="left",
    )
    supplement_mask = out["_supplement_level_supplemented_from_fred"].fillna(False).astype(bool)
    out.loc[supplement_mask, "level_source_provider_used"] = out.loc[supplement_mask, "_supplement_level_source_provider_used"]
    out.loc[supplement_mask, "level_supplemented_from_fred"] = True
    out.drop(
        columns=["_supplement_level_source_provider_used", "_supplement_level_supplemented_from_fred"],
        inplace=True,
    )
    return out


def _build_benchmark_blocks(
    *,
    h15_file: str | Path | None,
    curve_file_overrides: dict[str, Path] | None,
    source_provider: str,
    model_cfg: dict[str, Any],
    series_config: str | Path,
) -> tuple[pd.DataFrame, pd.DataFrame | None]:
    est_cfg = model_cfg.get("estimation", {})
    holdings_families = list(est_cfg.get("holdings_benchmark_families") or ["nominal_treasury_constant_maturity"])
    factor_families = list(est_cfg.get("factor_benchmark_families") or [])
    curve_files = dict(curve_file_overrides or {})
    if h15_file is not None:
        curve_files["nominal_treasury_constant_maturity"] = Path(h15_file)
    benchmark, factor_benchmark = build_estimation_benchmark_blocks(
        series_config_path=series_config,
        provider=source_provider,
        holdings_families=holdings_families,
        factor_families=factor_families,
        curve_file_overrides=curve_files,
    )
    return benchmark, factor_benchmark


def _build_fed_calibration(
    *,
    sector_panel: pd.DataFrame,
    benchmark: pd.DataFrame,
    factor_benchmark: pd.DataFrame | None,
    settings: EstimationSettings,
    interval_cfg: dict[str, Any],
    source_provider: str,
    series_config: str | Path,
    h15_file: str | Path | None,
    soma_file: str | Path | None,
    source_artifacts: dict[str, Any],
    provider_summary: dict[str, str],
    intermediate_artifacts: dict[str, Any],
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    fed_panel = sector_panel[sector_panel["sector_key"] == "fed"].copy()
    if fed_panel.empty:
        raise ValueError("Sector panel does not contain the required 'fed' sector.")

    if soma_file is not None:
        soma_path = Path(soma_file)
        source_artifacts["soma"] = {"provided_path": str(soma_path)}
        provider_summary["soma"] = "provided"
    else:
        requested_dates = sorted(pd.to_datetime(fed_panel["date"]).dropna().unique())
        artifact = fetch_soma_holdings(
            as_of_dates=requested_dates,
            normalized_out="data/external/normalized/soma_holdings_fed.csv",
        )
        soma_path = artifact.normalized_path
        source_artifacts["soma"] = {"normalized_path": str(artifact.normalized_path), "provider": artifact.provider}
        provider_summary["soma"] = artifact.provider

    h15_source = h15_file
    if h15_source is None:
        artifact = fetch_h15_curves(
            provider=source_provider,
            series_config_path=series_config,
            curve_key="nominal_treasury_constant_maturity",
            normalized_out=f"data/external/normalized/h15_curves_{source_provider}.csv",
        )
        h15_source = artifact.normalized_path
        source_artifacts["h15_nominal"] = {"normalized_path": str(artifact.normalized_path), "provider": artifact.provider}
        provider_summary["h15_nominal"] = artifact.provider
    else:
        source_artifacts["h15_nominal"] = {"provided_path": str(Path(h15_source))}
        provider_summary["h15_nominal"] = "provided"

    curves = load_h15_curve_file(
        h15_source,
        series_config_path=series_config,
        curve_key="nominal_treasury_constant_maturity",
    )
    soma = read_soma_holdings(soma_path)
    exact_metrics = summarize_soma_quarterly(soma, curve_df=curves)
    exact_path = resolve_fed_calibration_scope("full")["exact_out"]
    write_table(exact_metrics, exact_path)
    intermediate_artifacts["fed_exact_metrics"] = exact_path

    summary = calibrate_fed_revaluation_mapping(
        fed_panel,
        exact_metrics,
        benchmark,
        factor_returns=factor_benchmark,
        smoothness_penalty=settings.smoothness_penalty,
        ridge_penalty=settings.ridge_penalty,
        factor_ridge_penalty=settings.factor_ridge_penalty,
    )
    interval_calibration = build_fed_interval_calibration(
        fed_panel,
        exact_metrics,
        benchmark,
        factor_returns=factor_benchmark,
        settings=settings,
        strict_duration=True,
    )
    interval_path = resolve_fed_calibration_scope("full")["interval_calibration_out"]
    write_table(interval_calibration, interval_path)
    summary["interval_calibration"] = summarize_interval_calibration(interval_calibration, settings=interval_cfg)
    summary_path = resolve_fed_calibration_scope("full")["summary_out"]
    dump_json(summary, summary_path)
    intermediate_artifacts["fed_interval_calibration"] = interval_path
    intermediate_artifacts["fed_calibration_summary"] = summary_path
    return summary, interval_calibration, exact_metrics


def _build_fed_exact_overlay(exact_metrics: pd.DataFrame) -> pd.DataFrame:
    overlay = exact_metrics.copy()
    if overlay.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "sector_key",
                "node_type",
                "level",
                "exact_wam_years",
                "approx_modified_duration_years",
                "bill_share",
                "coupon_share",
                "tips_share",
                "frn_share",
            ]
        )

    overlay["date"] = pd.to_datetime(overlay.get("date"), errors="coerce")
    overlay["sector_key"] = "fed"
    overlay["node_type"] = "atomic"
    overlay = overlay[
        [
            "date",
            "sector_key",
            "node_type",
            "level",
            "exact_wam_years",
            "approx_modified_duration_years",
            "bill_share",
            "coupon_share",
            "tips_share",
            "frn_share",
        ]
    ].copy()
    return overlay.sort_values("date").reset_index(drop=True)


def _build_fed_exact_overlay_summary(fed_exact_overlay: pd.DataFrame) -> dict[str, Any]:
    if fed_exact_overlay.empty:
        return {"row_count": 0, "date_start": None, "date_end": None}
    dates = pd.to_datetime(fed_exact_overlay["date"], errors="coerce").dropna()
    return {
        "row_count": int(len(fed_exact_overlay)),
        "date_start": None if dates.empty else pd.Timestamp(dates.min()).date().isoformat(),
        "date_end": None if dates.empty else pd.Timestamp(dates.max()).date().isoformat(),
    }


def _resolve_release_promotion_window_quarters(release_config_data: dict[str, Any]) -> int:
    promotion_cfg = dict(release_config_data.get("promotion") or {})
    raw_value = promotion_cfg.get("window_quarters", 4)
    try:
        window_quarters = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid full-coverage release promotion window: {raw_value!r}") from exc
    if window_quarters <= 0:
        raise ValueError("Full-coverage release promotion window must be positive.")
    return window_quarters


def _merge_promoted_release_estimates(
    *,
    estimated: pd.DataFrame,
    sector_panel: pd.DataFrame,
    benchmark: pd.DataFrame,
    factor_benchmark: pd.DataFrame | None,
    settings: EstimationSettings,
    interval_calibration: pd.DataFrame | None,
    interval_cfg: dict[str, Any],
    foreign_nowcast: pd.DataFrame,
    bank_constraints: pd.DataFrame,
    sector_defs_path: str | Path,
    sector_definitions: dict[str, Any],
    coverage_registry: dict[str, Any],
    promotion_window_quarters: int,
) -> pd.DataFrame:
    out = estimated.copy()
    out["row_is_short_window_estimate"] = False
    out["estimate_origin_includes_short_window_promotion"] = False
    out["short_window_promotion_quarters"] = pd.NA

    promoted_sectors = _select_promoted_release_sectors(
        sector_panel=sector_panel,
        coverage_registry=coverage_registry,
    )
    if not promoted_sectors or settings.rolling_window_quarters <= promotion_window_quarters:
        return out

    promoted_settings = replace(settings, rolling_window_quarters=promotion_window_quarters)
    promoted = estimate_effective_maturity_panel(
        sector_panel,
        benchmark,
        factor_returns=factor_benchmark,
        settings=promoted_settings,
        sectors=promoted_sectors,
        interval_calibration=interval_calibration,
        interval_settings=interval_cfg,
        foreign_nowcast=foreign_nowcast,
        bank_constraints=bank_constraints,
        sector_config_path=str(sector_defs_path),
        annotation_mode="full_coverage",
    )
    if promoted.empty:
        return out

    promoted = promoted.copy()
    promoted["row_is_short_window_estimate"] = True
    promoted["estimate_origin_includes_short_window_promotion"] = True
    promoted["short_window_promotion_quarters"] = promotion_window_quarters
    promoted["date"] = pd.to_datetime(promoted["date"], errors="coerce")
    out["date"] = pd.to_datetime(out["date"], errors="coerce")

    existing_pairs = out[["date", "sector_key"]].drop_duplicates().assign(_existing=True)
    promoted = promoted.merge(existing_pairs, on=["date", "sector_key"], how="left")
    promoted = promoted[promoted["_existing"] != True].drop(columns=["_existing"]).copy()
    if promoted.empty:
        return out

    if "high_confidence_flag" in promoted.columns:
        promoted["high_confidence_flag"] = False
    if "maturity_evidence_tier" in promoted.columns:
        promoted["maturity_evidence_tier"] = promoted["maturity_evidence_tier"].map(_downgrade_evidence_tier)
    if "selection_reason" in promoted.columns:
        promoted["selection_reason"] = promoted["selection_reason"].map(
            lambda value: _append_short_window_reason(value, promotion_window_quarters)
        )
    return _sort_release_frame(pd.concat([out, promoted], ignore_index=True, sort=False))


def _select_promoted_release_sectors(
    *,
    sector_panel: pd.DataFrame,
    coverage_registry: dict[str, Any],
) -> list[str]:
    if sector_panel.empty:
        return []

    selected: list[str] = []
    for sector_key in sector_panel["sector_key"].dropna().astype(str).unique():
        registry_node = coverage_registry.get(sector_key)
        if registry_node is None or registry_node.node_type != "atomic":
            continue
        if not registry_node.release_window_promotion_eligible:
            continue
        selected.append(sector_key)
    return sorted(set(selected))


def _build_foreign_nowcast(
    *,
    foreign_shl_file: str | Path | None,
    foreign_slt_file: str | Path | None,
    source_artifacts: dict[str, Any],
    provider_summary: dict[str, str],
    intermediate_artifacts: dict[str, Any],
) -> pd.DataFrame:
    if foreign_shl_file is not None:
        shl = load_extracted_shl_issue_mix(foreign_shl_file)
        source_artifacts["foreign_shl"] = {"provided_path": str(Path(foreign_shl_file))}
        provider_summary["foreign_shl"] = "provided"
    else:
        shl = extract_shl_total_foreign_benchmark(load_shl_historical_treasury_benchmark("https://ticdata.treasury.gov/Publish/shlhistdat.csv"))
        source_artifacts["foreign_shl"] = {"url": "https://ticdata.treasury.gov/Publish/shlhistdat.csv"}
        provider_summary["foreign_shl"] = "official"

    if foreign_slt_file is not None:
        slt = load_slt_short_long(foreign_slt_file)
        source_artifacts["foreign_slt"] = {"provided_path": str(Path(foreign_slt_file))}
        provider_summary["foreign_slt"] = "provided"
    else:
        slt = None
        source_artifacts["foreign_slt"] = {"note": "not provided"}
        provider_summary["foreign_slt"] = "unavailable"

    foreign_nowcast = build_foreign_monthly_nowcast(shl, slt)
    out_path = Path("data/processed/foreign_nowcast_panel_full.csv")
    write_table(foreign_nowcast, out_path)
    intermediate_artifacts["foreign_nowcast_panel"] = str(out_path)
    return foreign_nowcast


def _build_bank_constraints(
    *,
    bank_constraint_file: str | Path | None,
    ffiec_file: str | Path | None,
    ffiec002_file: str | Path | None,
    ncua_file: str | Path | None,
    bank_supplement_file: str | Path | None,
    bank_constraints_config: str | Path,
    source_artifacts: dict[str, Any],
    provider_summary: dict[str, str],
    intermediate_artifacts: dict[str, Any],
) -> pd.DataFrame:
    if bank_constraint_file is not None:
        panel = read_table(bank_constraint_file)
        if "date" in panel.columns:
            panel["date"] = pd.to_datetime(panel["date"], errors="coerce")
        source_artifacts["bank_constraints"] = {"provided_path": str(Path(bank_constraint_file))}
        provider_summary["bank_constraints"] = "provided"
        return panel

    if any(value is not None for value in [ffiec_file, ffiec002_file, ncua_file, bank_supplement_file]):
        institutions = _load_bank_institution_files(ffiec_file=ffiec_file, ffiec002_file=ffiec002_file, ncua_file=ncua_file)
        supplement = read_table(bank_supplement_file) if bank_supplement_file is not None else None
        panel = build_bank_constraint_panel(
            institutions,
            constraints_config_path=bank_constraints_config,
            supplement_df=supplement,
        )
        out_path = Path("data/processed/bank_constraint_panel_full.csv")
        write_table(panel, out_path)
        source_artifacts["bank_constraints"] = {"normalized_path": str(out_path)}
        provider_summary["bank_constraints"] = "derived"
        intermediate_artifacts["bank_constraint_panel"] = str(out_path)
        return panel

    source_artifacts["bank_constraints"] = {"note": "unavailable"}
    provider_summary["bank_constraints"] = "unavailable"
    return pd.DataFrame()


def _load_bank_institution_files(
    *,
    ffiec_file: str | Path | None,
    ffiec002_file: str | Path | None,
    ncua_file: str | Path | None,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in [ffiec_file, ffiec002_file, ncua_file]:
        if path is None:
            continue
        frame = read_table(path)
        if "date" in frame.columns:
            frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)


def _build_required_canonical_panel(
    *,
    sector_panel: pd.DataFrame,
    estimated: pd.DataFrame,
    required_canonical: list[str],
) -> pd.DataFrame:
    base = _sort_release_frame(
        sector_panel[sector_panel["sector_key"].astype(str).isin(required_canonical)].copy()
    )
    estimated_overlay = estimated.drop(
        columns=[col for col in base.columns if col in estimated.columns and col not in {"date", "sector_key"}],
        errors="ignore",
    )
    return _sort_release_frame(base.merge(estimated_overlay, on=["date", "sector_key"], how="left"))


def _annotate_publication_status(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if out.empty:
        out["publication_status"] = pd.Series(dtype=str)
        out["publication_status_reason"] = pd.Series(dtype=str)
        return out

    has_estimate = _has_published_estimate(out)
    has_level = out["level"].notna() if "level" in out.columns else pd.Series(False, index=out.index)
    is_backfill = out["history_preserving_backfill"].fillna(False) if "history_preserving_backfill" in out.columns else pd.Series(False, index=out.index)

    out["publication_status"] = "status_only_no_level_or_estimate"
    out.loc[has_level, "publication_status"] = "level_present_no_publishable_estimate"
    out.loc[has_estimate, "publication_status"] = "published_estimate"
    out.loc[is_backfill & has_estimate, "publication_status"] = "history_preserving_backfill"

    out["publication_status_reason"] = out["publication_status"].map(
        {
            "history_preserving_backfill": "Carried nearest available sector estimate into a leading warmup gap.",
            "published_estimate": "Published best-available sector estimate for this quarter.",
            "level_present_no_publishable_estimate": "Sector has a level row but no publishable maturity estimate for this quarter.",
            "status_only_no_level_or_estimate": "Sector has neither a public level row nor a publishable maturity estimate for this quarter.",
        }
    )
    return out


def _resolve_latest_snapshot_quarter(sector_panel: pd.DataFrame, required_canonical: list[str]) -> pd.Timestamp | None:
    frame = sector_panel.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    subset = frame[
        frame["sector_key"].isin(required_canonical)
    ][["sector_key", "date"]].dropna()
    if subset.empty:
        return None
    per_sector_latest = subset.groupby("sector_key", sort=False)["date"].max()
    if per_sector_latest.empty:
        return None
    return pd.Timestamp(per_sector_latest.min()).normalize()


def _latest_required_sector_keys(
    sector_panel: pd.DataFrame,
    required_canonical: list[str],
    latest_quarter: pd.Timestamp | None,
) -> set[str]:
    if latest_quarter is None:
        return set()
    frame = sector_panel.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    covered = frame[
        frame["sector_key"].isin(required_canonical)
        & frame["date"].eq(pd.Timestamp(latest_quarter))
    ]["sector_key"].dropna().astype(str).unique().tolist()
    return set(covered)


def _build_release_summary(
    *,
    canonical: pd.DataFrame,
    latest: pd.DataFrame,
    high_confidence: pd.DataFrame,
    requested_end_date: str | None,
    resolved_latest_snapshot_date: str | None,
    quarters: int | None,
    coverage_scope: str,
    source_provider: str,
    model_config_path: str | Path,
    release_config_path: str | Path,
    sector_defs: str | Path,
    series_catalog: str | Path,
    summary_json_out: Path,
    reconciliation: pd.DataFrame,
    required_sector_inventory: pd.DataFrame,
) -> dict[str, Any]:
    date_range = _date_range(canonical)
    return {
        "coverage_scope": coverage_scope,
        "source_provider_requested": source_provider,
        "requested_end_date": requested_end_date or "",
        "resolved_latest_snapshot_date": resolved_latest_snapshot_date or (date_range[1] or ""),
        "quarters": quarters,
        "canonical_row_count": int(len(canonical)),
        "latest_snapshot_row_count": int(len(latest)),
        "high_confidence_row_count": int(len(high_confidence)),
        "reconciliation_row_count": int(len(reconciliation)),
        "required_sector_inventory_row_count": int(len(required_sector_inventory)),
        "history_preserving_backfill_rows": int(canonical["history_preserving_backfill"].fillna(False).sum()) if "history_preserving_backfill" in canonical.columns else 0,
        "short_window_estimate_rows": int(canonical["row_is_short_window_estimate"].fillna(False).sum()) if "row_is_short_window_estimate" in canonical.columns else 0,
        "short_window_origin_rows": int(canonical["estimate_origin_includes_short_window_promotion"].fillna(False).sum()) if "estimate_origin_includes_short_window_promotion" in canonical.columns else 0,
        "date_start": date_range[0],
        "date_end": date_range[1],
        "model_config_path": str(model_config_path),
        "release_config_path": str(release_config_path),
        "sector_defs_path": str(sector_defs),
        "series_catalog_path": str(series_catalog),
        "summary_json_out": str(summary_json_out),
}


def _build_history_spans(
    canonical: pd.DataFrame,
    sector_panel: pd.DataFrame,
    required_canonical: list[str],
) -> list[dict[str, Any]]:
    registry = sector_panel.drop_duplicates("sector_key").set_index("sector_key")
    out: list[dict[str, Any]] = []
    required_set = set(required_canonical)
    for sector_key, sub in canonical.groupby("sector_key", sort=True):
        sector_rows = sector_panel[sector_panel["sector_key"] == sector_key].copy()
        history_start = None if sector_rows.empty else pd.Timestamp(sector_rows["date"].min()).date().isoformat()
        history_end = None if sector_rows.empty else pd.Timestamp(sector_rows["date"].max()).date().isoformat()
        row0 = sub.iloc[0]
        out.append(
            {
                "sector_key": str(sector_key),
                "included": True,
                "date_start": history_start,
                "date_end": history_end,
                "rows": int(len(sub)),
                "node_type": str(row0.get("node_type") or ""),
                "required_for_full_coverage": bool(row0.get("required_for_full_coverage", False)),
                "concept_risk": _as_text(row0.get("concept_risk")),
                "estimand_class": _as_text(row0.get("estimand_class")),
                "publication_status": _single_or_mixed(sub.get("publication_status")),
                "high_confidence_flag": bool(sub["high_confidence_flag"].fillna(False).any()),
                "history_start_reason": _as_text(row0.get("history_start_reason")),
                "history_preserving_backfill_rows": int(sub["history_preserving_backfill"].fillna(False).sum()) if "history_preserving_backfill" in sub.columns else 0,
                "short_window_estimate_rows": int(sub["row_is_short_window_estimate"].fillna(False).sum()) if "row_is_short_window_estimate" in sub.columns else 0,
                "short_window_origin_rows": int(sub["estimate_origin_includes_short_window_promotion"].fillna(False).sum()) if "estimate_origin_includes_short_window_promotion" in sub.columns else 0,
            }
        )

    for sector_key, row in registry.iterrows():
        if sector_key not in required_set or sector_key in set(canonical["sector_key"].astype(str)):
            continue
        out.append(
            {
                "sector_key": str(sector_key),
                "included": False,
                "date_start": None,
                "date_end": None,
                "rows": 0,
                "node_type": _as_text(row.get("node_type")),
                "required_for_full_coverage": bool(row.get("required_for_full_coverage", False)),
                "concept_risk": _as_text(row.get("concept_risk")),
                "estimand_class": None,
                "publication_status": None,
                "high_confidence_flag": False,
                "history_start_reason": _as_text(row.get("history_start_reason")),
                "history_preserving_backfill_rows": 0,
                "short_window_estimate_rows": 0,
                "short_window_origin_rows": 0,
            }
        )

    return sorted(out, key=lambda item: (item["included"] is False, item["sector_key"]))


def _build_coverage_completeness(
    canonical: pd.DataFrame,
    sector_panel: pd.DataFrame,
    required_canonical: list[str],
) -> dict[str, Any]:
    canonical_keys = set(canonical["sector_key"].astype(str).dropna())
    required_set = set(required_canonical)
    missing = sorted(required_set - canonical_keys)
    sector_frame = sector_panel[sector_panel["sector_key"].isin(required_set)].copy()
    estimate_rows = canonical[
        canonical["sector_key"].isin(required_set)
        & _has_published_estimate(canonical)
    ][["date", "sector_key"]].drop_duplicates()
    publication_rows = canonical[
        canonical["sector_key"].isin(required_set)
        & canonical["publication_status"].notna()
    ][["date", "sector_key"]].drop_duplicates()
    required_rows = sector_frame[["date", "sector_key"]].drop_duplicates()
    missing_publication_rows = required_rows.merge(
        publication_rows,
        on=["date", "sector_key"],
        how="left",
        indicator=True,
    )
    missing_publication_rows = missing_publication_rows[missing_publication_rows["_merge"] != "both"]
    return {
        "required_canonical_total": int(len(required_set)),
        "required_canonical_covered": int(len(required_set) - len(missing)),
        "required_row_count": int(len(sector_frame)),
        "coverage_ratio": 0.0 if not required_set else float((len(required_set) - len(missing)) / len(required_set)),
        "missing_required_sectors": missing,
        "required_sector_rows": int(len(sector_frame)),
        "required_rows_with_estimates": int(len(estimate_rows)),
        "required_rows_with_publication_status": int(len(publication_rows)),
        "published_estimate_coverage_ratio": (
            0.0 if len(required_rows) == 0 else float(len(estimate_rows) / len(required_rows))
        ),
        "missing_required_publication_rows": int(len(missing_publication_rows)),
    }


def _build_required_sector_inventory(
    *,
    canonical: pd.DataFrame,
    sector_panel: pd.DataFrame,
    sector_definitions: dict[str, Any],
    coverage_registry: dict[str, Any],
    catalog: dict[str, Any],
    raw_long_df: pd.DataFrame,
    long_df: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    canonical_groups = {
        str(sector_key): sub.copy()
        for sector_key, sub in canonical.groupby("sector_key", sort=True)
    }
    sector_panel = sector_panel.copy()
    sector_panel["date"] = pd.to_datetime(sector_panel["date"], errors="coerce")
    raw_available_codes = set(raw_long_df["series_code"].dropna().astype(str)) if "series_code" in raw_long_df.columns else set()
    available_codes = set(long_df["series_code"].dropna().astype(str)) if "series_code" in long_df.columns else set()

    for sector_key, node in sorted(coverage_registry.items()):
        if not (node.required_for_full_coverage and node.is_canonical):
            continue
        sector_spec = dict(sector_definitions.get(sector_key) or {})
        level_series_key = _as_text(sector_spec.get("level_series"))
        bills_series_key = _as_text(sector_spec.get("bills_series"))
        series_spec = catalog.get(level_series_key) if level_series_key else None
        bills_spec = catalog.get(bills_series_key) if bills_series_key else None
        base_code = _as_text(getattr(series_spec, "base_code", None)) if series_spec is not None else None
        level_source_code = _as_text(getattr(series_spec, "level", None)) if series_spec is not None else None
        transactions_source_code = _as_text(getattr(series_spec, "transactions", None)) if series_spec is not None else None
        revaluation_source_code = _as_text(getattr(series_spec, "revaluation", None)) if series_spec is not None else None
        level_fred_id = _as_text((getattr(series_spec, "fred_ids", None) or {}).get("level")) if series_spec is not None else None
        transactions_fred_id = _as_text((getattr(series_spec, "fred_ids", None) or {}).get("transactions")) if series_spec is not None else None
        revaluation_fred_id = _as_text((getattr(series_spec, "fred_ids", None) or {}).get("revaluation")) if series_spec is not None else None
        bills_source_code = _as_text(getattr(bills_spec, "level", None)) if bills_spec is not None else None
        bills_fred_id = _as_text((getattr(bills_spec, "fred_ids", None) or {}).get("level")) if bills_spec is not None else None
        level_code_present = bool(level_source_code and level_source_code in raw_available_codes)
        transactions_code_present = bool(transactions_source_code and transactions_source_code in raw_available_codes)
        revaluation_code_present = bool(revaluation_source_code and revaluation_source_code in raw_available_codes)
        bills_code_present = bool(bills_source_code and bills_source_code in raw_available_codes)
        post_supplement_level_code_present = bool(level_source_code and level_source_code in available_codes)
        same_base_codes = _same_base_source_codes(raw_available_codes, base_code)
        sector_rows_all = sector_panel[
            sector_panel["sector_key"].astype(str).eq(sector_key)
        ].copy()
        history_rows = sector_rows_all.copy()
        canonical_rows = canonical_groups.get(sector_key, pd.DataFrame())
        date_start = None if history_rows.empty else pd.Timestamp(history_rows["date"].min()).date().isoformat()
        date_end = None if history_rows.empty else pd.Timestamp(history_rows["date"].max()).date().isoformat()
        latest_estimand_class = None
        latest_estimator_family = None
        latest_level_source_provider_used = None
        latest_level_supplemented_from_fred = False
        latest_point_estimate_origin = None
        latest_interval_origin = None
        latest_publication_status = None
        latest_publication_status_reason = None
        if not canonical_rows.empty:
            latest_row = canonical_rows.sort_values("date").iloc[-1]
            latest_estimand_class = _as_text(latest_row.get("estimand_class"))
            latest_estimator_family = _as_text(latest_row.get("estimator_family"))
            latest_level_source_provider_used = _as_text(latest_row.get("level_source_provider_used"))
            latest_level_supplemented_from_fred = bool(latest_row.get("level_supplemented_from_fred", False))
            latest_point_estimate_origin = _as_text(latest_row.get("point_estimate_origin"))
            latest_interval_origin = _as_text(latest_row.get("interval_origin"))
            latest_publication_status = _as_text(latest_row.get("publication_status"))
            latest_publication_status_reason = _as_text(latest_row.get("publication_status_reason"))
        rows.append(
            {
                "sector_key": sector_key,
                "sector_family": node.sector_family,
                "node_type": node.node_type,
                "concept_risk": node.concept_risk,
                "method_priority": ", ".join(str(value) for value in (sector_spec.get("method_priority") or [])),
                "level_series_key": level_series_key,
                "level_source_code": level_source_code,
                "transactions_source_code": transactions_source_code,
                "revaluation_source_code": revaluation_source_code,
                "level_fred_id": level_fred_id,
                "transactions_fred_id": transactions_fred_id,
                "revaluation_fred_id": revaluation_fred_id,
                "bills_series_key": bills_series_key,
                "bills_source_code": bills_source_code,
                "bills_fred_id": bills_fred_id,
                "bills_series_available": bool(sector_spec.get("bills_series")),
                "release_window_promotion_eligible": bool(node.release_window_promotion_eligible),
                "source_level_code_present": level_code_present,
                "source_transactions_code_present": transactions_code_present,
                "source_revaluation_code_present": revaluation_code_present,
                "source_bills_code_present": bills_code_present,
                "post_supplement_level_code_present": post_supplement_level_code_present,
                "source_level_status": _classify_source_level_status(
                    level_source_code=level_source_code,
                    transactions_source_code=transactions_source_code,
                    level_code_present=level_code_present,
                    transactions_code_present=transactions_code_present,
                    same_base_codes=same_base_codes,
                ),
                "post_supplement_level_status": _classify_source_level_status(
                    level_source_code=level_source_code,
                    transactions_source_code=transactions_source_code,
                    level_code_present=post_supplement_level_code_present,
                    transactions_code_present=transactions_code_present,
                    same_base_codes=same_base_codes,
                ),
                "same_base_source_codes": ", ".join(same_base_codes),
                "history_start": date_start,
                "history_end": date_end,
                "level_rows_available": int(sector_rows_all["level"].notna().sum()) if "level" in sector_rows_all.columns else 0,
                "transactions_rows_available": int(sector_rows_all["transactions"].notna().sum()) if "transactions" in sector_rows_all.columns else 0,
                "revaluation_rows_available": int(sector_rows_all["revaluation"].notna().sum()) if "revaluation" in sector_rows_all.columns else 0,
                "bills_rows_available": int(sector_rows_all["bills_level"].notna().sum()) if "bills_level" in sector_rows_all.columns else 0,
                "history_row_count": int(len(history_rows)),
                "canonical_row_count": int(len(canonical_rows)),
                "history_preserving_backfill_rows": int(canonical_rows["history_preserving_backfill"].fillna(False).sum()) if not canonical_rows.empty and "history_preserving_backfill" in canonical_rows.columns else 0,
                "short_window_estimate_rows": int(canonical_rows["row_is_short_window_estimate"].fillna(False).sum()) if not canonical_rows.empty and "row_is_short_window_estimate" in canonical_rows.columns else 0,
                "short_window_origin_rows": int(canonical_rows["estimate_origin_includes_short_window_promotion"].fillna(False).sum()) if not canonical_rows.empty and "estimate_origin_includes_short_window_promotion" in canonical_rows.columns else 0,
                "currently_backfilled": bool(canonical_rows["history_preserving_backfill"].fillna(False).any()) if not canonical_rows.empty and "history_preserving_backfill" in canonical_rows.columns else False,
                "currently_short_window_estimated": bool(canonical_rows["row_is_short_window_estimate"].fillna(False).any()) if not canonical_rows.empty and "row_is_short_window_estimate" in canonical_rows.columns else False,
                "currently_short_window_origin": bool(canonical_rows["estimate_origin_includes_short_window_promotion"].fillna(False).any()) if not canonical_rows.empty and "estimate_origin_includes_short_window_promotion" in canonical_rows.columns else False,
                "latest_estimand_class": latest_estimand_class,
                "latest_estimator_family": latest_estimator_family,
                "latest_level_source_provider_used": latest_level_source_provider_used,
                "latest_level_supplemented_from_fred": latest_level_supplemented_from_fred,
                "latest_point_estimate_origin": latest_point_estimate_origin,
                "latest_interval_origin": latest_interval_origin,
                "latest_publication_status": latest_publication_status,
                "latest_publication_status_reason": latest_publication_status_reason,
            }
        )

    return pd.DataFrame(rows).sort_values(["concept_risk", "sector_key"], kind="stable").reset_index(drop=True)


def _same_base_source_codes(available_codes: set[str], base_code: str | None) -> list[str]:
    if not base_code:
        return []
    return sorted(code for code in available_codes if base_code in code)


def _classify_source_level_status(
    *,
    level_source_code: str | None,
    transactions_source_code: str | None,
    level_code_present: bool,
    transactions_code_present: bool,
    same_base_codes: list[str],
) -> str:
    if level_code_present:
        return "present"
    if level_source_code is None:
        return "computed_proxy"
    if transactions_source_code and transactions_code_present:
        return "transactions_only"
    if same_base_codes:
        return "same_base_other_only"
    return "absent"


def _build_source_series_audit(required_sector_inventory: pd.DataFrame) -> dict[str, Any]:
    inventory = required_sector_inventory.copy()
    raw_status_counts = {
        str(status): int(count)
        for status, count in inventory["source_level_status"].fillna("unknown").astype(str).value_counts().sort_index().items()
    }
    post_supplement_status_counts = {
        str(status): int(count)
        for status, count in inventory["post_supplement_level_status"].fillna("unknown").astype(str).value_counts().sort_index().items()
    }
    transactions_only = inventory[inventory["source_level_status"].astype(str).eq("transactions_only")]
    absent = inventory[inventory["source_level_status"].astype(str).eq("absent")]
    present = inventory[inventory["source_level_status"].astype(str).eq("present")]
    post_supplement_present = inventory[inventory["post_supplement_level_status"].astype(str).eq("present")]
    post_supplement_absent = inventory[inventory["post_supplement_level_status"].astype(str).eq("absent")]
    transactions_only_with_fred_mapping = transactions_only[
        transactions_only["level_fred_id"].notna() & transactions_only["level_fred_id"].astype(str).ne("")
    ]
    return {
        "required_sector_count": int(len(inventory)),
        "source_level_status_counts": raw_status_counts,
        "source_level_present_count": int(len(present)),
        "source_level_transactions_only_count": int(len(transactions_only)),
        "source_level_absent_count": int(len(absent)),
        "post_supplement_level_status_counts": post_supplement_status_counts,
        "post_supplement_level_present_count": int(len(post_supplement_present)),
        "post_supplement_level_absent_count": int(len(post_supplement_absent)),
        "transactions_only_sector_keys": sorted(transactions_only["sector_key"].dropna().astype(str).unique()),
        "transactions_only_with_level_fred_mapping_count": int(len(transactions_only_with_fred_mapping)),
        "transactions_only_with_level_fred_mapping_sector_keys": sorted(
            transactions_only_with_fred_mapping["sector_key"].dropna().astype(str).unique()
        ),
        "absent_sector_keys": sorted(absent["sector_key"].dropna().astype(str).unique()),
    }


def _build_weakest_sectors(canonical: pd.DataFrame, sector_panel: pd.DataFrame) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for sector_key, sub in canonical.groupby("sector_key", sort=True):
        row0 = sub.iloc[0]
        sector_rows = sector_panel[sector_panel["sector_key"] == sector_key]
        records.append(
            {
                "sector_key": str(sector_key),
                "node_type": _as_text(row0.get("node_type")),
                "concept_risk": _as_text(row0.get("concept_risk")),
                "estimand_class": _as_text(row0.get("estimand_class")),
                "publication_status": _single_or_mixed(sub.get("publication_status")),
                "level_evidence_tier": _as_text(row0.get("level_evidence_tier")),
                "maturity_evidence_tier": _as_text(row0.get("maturity_evidence_tier")),
                "high_confidence_flag": bool(sub["high_confidence_flag"].fillna(False).any()),
                "history_preserving_backfill": bool(sub["history_preserving_backfill"].fillna(False).any()),
                "row_is_short_window_estimate": bool(sub["row_is_short_window_estimate"].fillna(False).any()) if "row_is_short_window_estimate" in sub.columns else False,
                "estimate_origin_includes_short_window_promotion": bool(sub["estimate_origin_includes_short_window_promotion"].fillna(False).any()) if "estimate_origin_includes_short_window_promotion" in sub.columns else False,
                "date_start": None if sector_rows.empty else pd.Timestamp(sector_rows["date"].min()).date().isoformat(),
                "date_end": None if sector_rows.empty else pd.Timestamp(sector_rows["date"].max()).date().isoformat(),
                "rows": int(len(sub)),
                "history_start_reason": _as_text(row0.get("history_start_reason")),
            }
        )

    return sorted(
        records,
        key=lambda item: (
            item["high_confidence_flag"],
            item["maturity_evidence_tier"] or "",
            item["level_evidence_tier"] or "",
            item["concept_risk"] or "",
            not item["history_preserving_backfill"],
            not item["estimate_origin_includes_short_window_promotion"],
            item["sector_key"],
        ),
    )


def _build_latest_snapshot_summary(latest: pd.DataFrame, latest_quarter: pd.Timestamp | None) -> dict[str, Any]:
    if latest.empty or latest_quarter is None:
        return {
            "latest_common_quarter": None,
            "row_count": 0,
            "high_confidence_rows": 0,
            "history_preserving_backfill_rows": 0,
            "short_window_estimate_rows": 0,
            "short_window_origin_rows": 0,
            "sector_keys": [],
        }
    return {
        "latest_common_quarter": pd.Timestamp(latest_quarter).date().isoformat(),
        "row_count": int(len(latest)),
        "high_confidence_rows": int(latest["high_confidence_flag"].fillna(False).sum()),
        "history_preserving_backfill_rows": int(latest["history_preserving_backfill"].fillna(False).sum()) if "history_preserving_backfill" in latest.columns else 0,
        "short_window_estimate_rows": int(latest["row_is_short_window_estimate"].fillna(False).sum()) if "row_is_short_window_estimate" in latest.columns else 0,
        "short_window_origin_rows": int(latest["estimate_origin_includes_short_window_promotion"].fillna(False).sum()) if "estimate_origin_includes_short_window_promotion" in latest.columns else 0,
        "sector_keys": sorted(latest["sector_key"].dropna().astype(str).unique()),
    }


def _build_history_backfill_summary(canonical: pd.DataFrame) -> dict[str, Any]:
    if canonical.empty:
        return {
            "history_preserving_backfill_rows": 0,
            "short_window_estimate_rows": 0,
            "short_window_origin_rows": 0,
            "sectors_with_backfill": [],
            "sectors_with_short_window_estimate": [],
            "sectors_with_short_window_origin": [],
        }
    return {
        "history_preserving_backfill_rows": int(canonical["history_preserving_backfill"].fillna(False).sum()) if "history_preserving_backfill" in canonical.columns else 0,
        "short_window_estimate_rows": int(canonical["row_is_short_window_estimate"].fillna(False).sum()) if "row_is_short_window_estimate" in canonical.columns else 0,
        "short_window_origin_rows": int(canonical["estimate_origin_includes_short_window_promotion"].fillna(False).sum()) if "estimate_origin_includes_short_window_promotion" in canonical.columns else 0,
        "sectors_with_backfill": sorted(canonical.loc[canonical["history_preserving_backfill"].fillna(False), "sector_key"].dropna().astype(str).unique()) if "history_preserving_backfill" in canonical.columns else [],
        "sectors_with_short_window_estimate": sorted(canonical.loc[canonical["row_is_short_window_estimate"].fillna(False), "sector_key"].dropna().astype(str).unique()) if "row_is_short_window_estimate" in canonical.columns else [],
        "sectors_with_short_window_origin": sorted(canonical.loc[canonical["estimate_origin_includes_short_window_promotion"].fillna(False), "sector_key"].dropna().astype(str).unique()) if "estimate_origin_includes_short_window_promotion" in canonical.columns else [],
    }


def _build_reconciliation_diagnostics(
    *,
    sector_panel: pd.DataFrame,
    sector_definitions: dict[str, Any],
    coverage_registry: dict[str, Any],
) -> dict[str, Any]:
    levels = sector_panel[["date", "sector_key", "level"]].copy()
    levels["date"] = pd.to_datetime(levels["date"], errors="coerce")
    levels["level"] = pd.to_numeric(levels["level"], errors="coerce")
    levels = levels.dropna(subset=["date", "sector_key"])

    level_env: dict[pd.Timestamp, dict[str, float]] = {}
    for _, row in levels.iterrows():
        if pd.isna(row["level"]):
            continue
        level_env.setdefault(pd.Timestamp(row["date"]).normalize(), {})[str(row["sector_key"])] = float(row["level"])

    formula_rows_checked = 0
    formula_rows_failing = 0
    formula_max_abs_gap = 0.0
    for sector_key, spec in sector_definitions.items():
        formula = spec.get("formula_level")
        if not formula:
            continue
        for _, env in level_env.items():
            actual = env.get(sector_key)
            if actual is None:
                continue
            expected = _safe_formula_eval(str(formula), env)
            if expected is None:
                continue
            formula_rows_checked += 1
            gap = abs(float(actual) - float(expected))
            formula_max_abs_gap = max(formula_max_abs_gap, gap)
            if gap > RECONCILIATION_TOLERANCE:
                formula_rows_failing += 1

    children_by_parent: dict[str, list[str]] = {}
    for key, node in coverage_registry.items():
        if node.parent_key:
            children_by_parent.setdefault(node.parent_key, []).append(key)

    parent_rows_checked = 0
    parent_rows_failing = 0
    parent_max_abs_gap = 0.0
    for parent_key, child_keys in children_by_parent.items():
        if len(child_keys) < 2:
            continue
        for _, env in level_env.items():
            actual = env.get(parent_key)
            if actual is None or not all(child in env for child in child_keys):
                continue
            expected = sum(float(env[child]) for child in child_keys)
            parent_rows_checked += 1
            gap = abs(float(actual) - float(expected))
            parent_max_abs_gap = max(parent_max_abs_gap, gap)
            if gap > RECONCILIATION_TOLERANCE:
                parent_rows_failing += 1

    return {
        "formula_nodes_checked": int(sum(1 for spec in sector_definitions.values() if spec.get("formula_level"))),
        "formula_rows_checked": int(formula_rows_checked),
        "formula_rows_failing": int(formula_rows_failing),
        "formula_max_abs_gap": float(formula_max_abs_gap),
        "parent_rollups_checked": int(sum(1 for children in children_by_parent.values() if len(children) >= 2)),
        "parent_rows_checked": int(parent_rows_checked),
        "parent_rows_failing": int(parent_rows_failing),
        "parent_max_abs_gap": float(parent_max_abs_gap),
    }


def _build_validation_summary(
    *,
    canonical: pd.DataFrame,
    latest: pd.DataFrame,
    high_confidence: pd.DataFrame,
    sector_panel: pd.DataFrame,
    coverage_completeness: dict[str, Any],
    required_canonical: list[str],
    latest_quarter: pd.Timestamp | None,
    latest_required_sectors: set[str],
    reconciliation: pd.DataFrame,
    reconciliation_diagnostics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    canonical_required_only = bool(canonical["required_for_full_coverage"].fillna(False).all()) if not canonical.empty else True
    reconciliation_noncanonical_only = bool((~reconciliation["is_canonical"].fillna(False)).all()) if not reconciliation.empty else True
    high_confidence_match = True
    if not high_confidence.empty:
        filtered = canonical[canonical["high_confidence_flag"].fillna(False)].copy()
        high_confidence_match = _frames_equal(filtered, high_confidence)
    canonical_unique_pairs = len(canonical[["date", "sector_key"]].drop_duplicates()) == len(canonical)
    latest_unique_pairs = len(latest[["date", "sector_key"]].drop_duplicates()) == len(latest)
    required_rows = sector_panel[sector_panel["sector_key"].isin(required_canonical)]
    canonical_pairs = set(zip(canonical["date"].astype(str), canonical["sector_key"].astype(str)))
    required_pairs = set(zip(required_rows["date"].astype(str), required_rows["sector_key"].astype(str)))
    required_coverage = required_pairs.issubset(canonical_pairs)
    published_status_rows = canonical[
        canonical["sector_key"].isin(required_canonical)
        & canonical["publication_status"].notna()
    ][["date", "sector_key"]].drop_duplicates()
    published_status_pairs = set(zip(published_status_rows["date"].astype(str), published_status_rows["sector_key"].astype(str)))
    required_publication_complete = required_pairs.issubset(published_status_pairs)
    published_estimate_ratio = float(coverage_completeness.get("published_estimate_coverage_ratio", 0.0))
    published_estimate_ratio_bounded = 0.0 <= published_estimate_ratio <= 1.0
    latest_matches = True
    if latest_quarter is not None:
        latest_matches = (
            latest["date"].nunique() == 1
            and bool(pd.Timestamp(latest["date"].iloc[0]).normalize() == pd.Timestamp(latest_quarter).normalize())
            and set(latest["sector_key"].astype(str)) == set(latest_required_sectors)
        )
    reconciliation_diagnostics = reconciliation_diagnostics or {}
    formula_reconciliation_passes = int(reconciliation_diagnostics.get("formula_rows_failing", 0)) == 0
    parent_child_reconciliation_passes = int(reconciliation_diagnostics.get("parent_rows_failing", 0)) == 0
    checks = [
        {
            "check": "canonical_required_only",
            "status": "pass" if canonical_required_only else "fail",
            "hard_failure": True,
            "details": "Canonical artifact contains only required canonical sectors." if canonical_required_only else "Canonical artifact contains at least one non-required or non-canonical sector row.",
        },
        {
            "check": "reconciliation_nodes_only_noncanonical",
            "status": "pass" if reconciliation_noncanonical_only else "fail",
            "hard_failure": True,
            "details": "Reconciliation artifact contains only non-canonical nodes." if reconciliation_noncanonical_only else "Reconciliation artifact contains at least one canonical node.",
        },
        {
            "check": "high_confidence_is_filtered_subset",
            "status": "pass" if high_confidence_match else "fail",
            "hard_failure": True,
            "details": "High-confidence artifact is an exact filter of the canonical artifact." if high_confidence_match else "High-confidence artifact diverges from the canonical filter.",
        },
        {
            "check": "canonical_sector_dates_unique",
            "status": "pass" if canonical_unique_pairs else "fail",
            "hard_failure": True,
            "details": "Canonical artifact has at most one row per sector/date." if canonical_unique_pairs else "Canonical artifact contains duplicate sector/date rows.",
        },
        {
            "check": "latest_snapshot_sector_dates_unique",
            "status": "pass" if latest_unique_pairs else "fail",
            "hard_failure": True,
            "details": "Latest snapshot artifact has at most one row per sector/date." if latest_unique_pairs else "Latest snapshot artifact contains duplicate sector/date rows.",
        },
        {
            "check": "required_sector_coverage_complete",
            "status": "pass" if required_coverage else "fail",
            "hard_failure": True,
            "details": "Every required canonical sector/date row appears in the canonical artifact." if required_coverage else "At least one required canonical sector/date row is missing from the canonical artifact.",
        },
        {
            "check": "required_sector_publication_complete",
            "status": "pass" if required_publication_complete else "fail",
            "hard_failure": True,
            "details": "Every required sector/date row has an explicit publication status." if required_publication_complete else "At least one required sector/date row lacks a publication status.",
        },
        {
            "check": "published_estimate_coverage_ratio_bounded",
            "status": "pass" if published_estimate_ratio_bounded else "fail",
            "hard_failure": True,
            "details": "Published-estimate coverage ratio stays within [0, 1]." if published_estimate_ratio_bounded else "Published-estimate coverage ratio fell outside [0, 1], which indicates inconsistent artifact accounting.",
        },
        {
            "check": "latest_snapshot_matches_required_rows",
            "status": "pass" if latest_matches else "fail",
            "hard_failure": True,
            "details": "Latest snapshot matches the latest required-sector common quarter." if latest_matches else "Latest snapshot does not match the required-sector common-quarter rows.",
        },
        {
            "check": "formula_reconciliation_passes",
            "status": "pass" if formula_reconciliation_passes else "fail",
            "hard_failure": True,
            "details": "Formula-defined reconciliation nodes match their configured level formulas." if formula_reconciliation_passes else "At least one formula-defined reconciliation node does not match its configured level formula.",
        },
        {
            "check": "parent_child_reconciliation_passes",
            "status": "pass" if parent_child_reconciliation_passes else "fail",
            "hard_failure": True,
            "details": "Parent rollups match the sum of their tracked children." if parent_child_reconciliation_passes else "At least one parent rollup does not match the sum of its tracked children.",
        },
    ]
    overall = all(check["status"] == "pass" for check in checks)
    return {
        "canonical_required_only": canonical_required_only,
        "reconciliation_nodes_only_noncanonical": reconciliation_noncanonical_only,
        "high_confidence_is_filtered_subset": high_confidence_match,
        "canonical_sector_dates_unique": canonical_unique_pairs,
        "latest_snapshot_sector_dates_unique": latest_unique_pairs,
        "required_sector_coverage_complete": required_coverage,
        "required_sector_publication_complete": required_publication_complete,
        "published_estimate_coverage_ratio_bounded": published_estimate_ratio_bounded,
        "latest_snapshot_matches_required_rows": latest_matches,
        "formula_reconciliation_passes": formula_reconciliation_passes,
        "parent_child_reconciliation_passes": parent_child_reconciliation_passes,
        "overall_status": "PASS" if overall else "FAIL",
        "checks": checks,
    }


def _raise_on_failed_validations(validation: dict[str, Any]) -> None:
    failed = [
        check
        for check in validation.get("checks", [])
        if check.get("hard_failure") and check.get("status") != "pass"
    ]
    if not failed:
        return
    details = "; ".join(f"{check['check']}: {check['details']}" for check in failed)
    raise ValueError(f"Full coverage release validation failed: {details}")


def _apply_history_preserving_backfill(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        out = frame.copy()
        out["history_preserving_backfill"] = pd.Series(dtype=bool)
        return out

    out = _sort_release_frame(frame)
    out["history_preserving_backfill"] = False
    fill_columns = [
        column
        for column in [
            *PRIMARY_ESTIMATE_COLUMNS,
            *SECONDARY_ESTIMATE_COLUMNS,
            *INTERVAL_ESTIMATE_COLUMNS,
            *ESTIMATE_METADATA_COLUMNS,
        ]
        if column in out.columns
    ]
    available_estimate_columns = [column for column in PRIMARY_ESTIMATE_COLUMNS if column in out.columns]
    if not available_estimate_columns:
        return out
    grouped: list[pd.DataFrame] = []

    for _, sub in out.groupby("sector_key", sort=False):
        sector = sub.copy()
        original_has_estimate = sector[available_estimate_columns].notna().any(axis=1)
        if not original_has_estimate.any():
            grouped.append(sector)
            continue

        leading_gap_mask = original_has_estimate.cumsum().eq(0) & ~original_has_estimate
        if leading_gap_mask.any():
            for column in fill_columns:
                sector.loc[leading_gap_mask, column] = sector[column].bfill().loc[leading_gap_mask]
        filled_has_estimate = sector[available_estimate_columns].notna().any(axis=1)
        fallback_rows = leading_gap_mask & filled_has_estimate
        if fallback_rows.any():
            sector.loc[fallback_rows, "history_preserving_backfill"] = True
            if "high_confidence_flag" in sector.columns:
                sector.loc[fallback_rows, "high_confidence_flag"] = False
            if "maturity_evidence_tier" in sector.columns:
                sector.loc[fallback_rows, "maturity_evidence_tier"] = sector.loc[fallback_rows, "maturity_evidence_tier"].map(_downgrade_evidence_tier)
            if "selection_reason" in sector.columns:
                sector.loc[fallback_rows, "selection_reason"] = sector.loc[fallback_rows, "selection_reason"].map(_append_history_backfill_reason)
        grouped.append(sector)

    return _sort_release_frame(pd.concat(grouped, ignore_index=True, sort=False))


def _single_or_mixed(series: pd.Series | None) -> str | None:
    if series is None or len(series) == 0:
        return None
    values = pd.Series(series).dropna().astype(str).unique().tolist()
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    return "mixed"


def _downgrade_evidence_tier(value: Any) -> str | Any:
    if value is None or pd.isna(value):
        return value
    tier = str(value).strip().upper()
    if not tier:
        return value
    return {"A": "B", "B": "C", "C": "D"}.get(tier, "D")


def _append_history_backfill_reason(value: Any) -> str:
    base = "" if value is None or pd.isna(value) else str(value).strip()
    suffix = "history-preserving release backfill from nearest available sector estimate"
    if not base:
        return suffix
    if suffix in base:
        return base
    return f"{base}; {suffix}"


def _append_short_window_reason(value: Any, promotion_window_quarters: int) -> str:
    base = "" if value is None or pd.isna(value) else str(value).strip()
    suffix = f"release short-window promotion ({promotion_window_quarters}q window)"
    if not base:
        return suffix
    if suffix in base:
        return base
    return f"{base}; {suffix}"


def _has_published_estimate(frame: pd.DataFrame) -> pd.Series:
    available_columns = [column for column in PRIMARY_ESTIMATE_COLUMNS if column in frame.columns]
    if not available_columns:
        return pd.Series(False, index=frame.index)
    return frame[available_columns].notna().any(axis=1)


def _safe_formula_eval(expression: str, env: dict[str, float]) -> float | None:
    names = set(re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expression))
    if not names or not names.issubset(set(env)):
        return None
    try:
        return float(eval(expression, {"__builtins__": {}}, {name: float(env[name]) for name in names}))
    except Exception:
        return None


def _build_provenance_summary(
    *,
    source_artifacts: dict[str, Any],
    intermediate_artifacts: dict[str, Any],
    manifest_path: Path,
    report_path: Path,
    summary_json_path: Path,
) -> dict[str, Any]:
    return {
        "manifest_path": str(manifest_path),
        "report_path": str(report_path),
        "summary_json_path": str(summary_json_path),
        "source_artifacts": source_artifacts,
        "intermediate_artifacts": intermediate_artifacts,
    }


def _render_release_report(
    *,
    release_summary: dict[str, Any],
    coverage_completeness: dict[str, Any],
    source_series_audit: dict[str, Any],
    history_spans: list[dict[str, Any]],
    history_backfill_summary: dict[str, Any],
    latest_snapshot_summary: dict[str, Any],
    fed_exact_overlay: pd.DataFrame,
    fed_exact_overlay_summary: dict[str, Any],
    high_confidence: pd.DataFrame,
    weakest_sectors: list[dict[str, Any]],
    reconciliation_diagnostics: dict[str, Any],
    validation: dict[str, Any],
    provenance: dict[str, Any],
    canonical: pd.DataFrame,
    latest: pd.DataFrame,
    high_confidence_path: Path,
    canonical_path: Path,
    latest_path: Path,
    reconciliation_path: Path,
    fed_exact_overlay_path: Path,
    required_inventory_path: Path,
    required_sector_inventory: pd.DataFrame,
    summary_json_path: Path,
    manifest_path: Path,
) -> str:
    lines = [
        "# Full Coverage Research Release",
        "",
        "## Release Summary",
        "",
        _render_bullets(
            [
                f"Coverage scope: `{release_summary['coverage_scope']}`",
                f"Requested provider: `{release_summary['source_provider_requested']}`",
                f"Requested end date: `{release_summary['requested_end_date'] or 'none'}`",
                f"Resolved latest snapshot date: `{release_summary['resolved_latest_snapshot_date']}`",
                "Canonical panel preserves the longest feasible sector history even when the latest common-quarter snapshot is narrower.",
                f"Canonical rows: `{release_summary['canonical_row_count']}`",
                f"Latest snapshot rows: `{release_summary['latest_snapshot_row_count']}`",
                f"High-confidence rows: `{release_summary['high_confidence_row_count']}`",
                f"Reconciliation rows: `{release_summary['reconciliation_row_count']}`",
                f"Required inventory rows: `{release_summary['required_sector_inventory_row_count']}`",
                f"History-preserving backfill rows: `{release_summary['history_preserving_backfill_rows']}`",
                f"Short-window estimate rows: `{release_summary['short_window_estimate_rows']}`",
                f"Short-window origin rows: `{release_summary['short_window_origin_rows']}`",
            ]
        ),
        "",
        "## Latest Common-Quarter Snapshot",
        "",
        _render_bullets(
            [
                f"Resolved latest common quarter: `{latest_snapshot_summary['latest_common_quarter']}`",
                f"Snapshot row count: `{latest_snapshot_summary['row_count']}`",
                f"Snapshot high-confidence rows: `{latest_snapshot_summary['high_confidence_rows']}`",
                f"Snapshot history-preserving backfill rows: `{latest_snapshot_summary['history_preserving_backfill_rows']}`",
                f"Snapshot short-window estimate rows: `{latest_snapshot_summary['short_window_estimate_rows']}`",
                f"Snapshot short-window origin rows: `{latest_snapshot_summary['short_window_origin_rows']}`",
                "This snapshot is a separate common-quarter cross-section, not the definition of the canonical history span.",
            ]
        ),
        "",
        _render_markdown_table(
            _summarize_frame(latest, ["sector_key", "date", "publication_status", "high_confidence_flag", "history_preserving_backfill", "row_is_short_window_estimate", "estimate_origin_includes_short_window_promotion"]),
            columns=["sector_key", "date", "publication_status", "high_confidence_flag", "history_preserving_backfill", "row_is_short_window_estimate", "estimate_origin_includes_short_window_promotion"],
        ),
        "",
        "## Fed Exact Overlay",
        "",
        _render_bullets(
            [
                f"Artifact: `{fed_exact_overlay_path}`",
                f"Rows: `{fed_exact_overlay_summary['row_count']}`",
                f"Date start: `{fed_exact_overlay_summary['date_start']}`",
                f"Date end: `{fed_exact_overlay_summary['date_end']}`",
                "This artifact is the direct SOMA-based Fed companion. The canonical `fed` row in `canonical_sector_maturity.csv` remains the cross-sector-comparable inferred series.",
            ]
        ),
        "",
        _render_markdown_table(
            _summarize_frame(fed_exact_overlay, ["date", "sector_key", "exact_wam_years", "approx_modified_duration_years", "bill_share"]),
            columns=["date", "sector_key", "exact_wam_years", "approx_modified_duration_years", "bill_share"],
        ),
        "",
        "## Coverage Completeness",
        "",
        _render_markdown_table([coverage_completeness], columns=[
            "required_canonical_total",
            "required_canonical_covered",
            "required_row_count",
            "coverage_ratio",
            "required_rows_with_estimates",
            "required_rows_with_publication_status",
            "published_estimate_coverage_ratio",
            "missing_required_publication_rows",
            "missing_required_sectors",
        ]),
        "",
        "## Source Series Audit",
        "",
        _render_bullets(
            [
                f"Required sectors audited: `{source_series_audit['required_sector_count']}`",
                f"Raw parsed Z.1 level codes present: `{source_series_audit['source_level_present_count']}`",
                f"Raw parsed Z.1 transactions-only sectors: `{source_series_audit['source_level_transactions_only_count']}`",
                f"Raw parsed Z.1 level codes absent: `{source_series_audit['source_level_absent_count']}`",
                f"Post-supplement level codes present: `{source_series_audit['post_supplement_level_present_count']}`",
                f"Post-supplement level codes absent: `{source_series_audit['post_supplement_level_absent_count']}`",
                f"Transactions-only sectors: `{', '.join(source_series_audit['transactions_only_sector_keys']) or 'none'}`",
                f"Transactions-only sectors with configured level FRED mappings: `{', '.join(source_series_audit['transactions_only_with_level_fred_mapping_sector_keys']) or 'none'}`",
                f"Absent sectors: `{', '.join(source_series_audit['absent_sector_keys']) or 'none'}`",
            ]
        ),
        "",
        _render_markdown_table(required_sector_inventory.to_dict(orient="records"), columns=[
            "sector_key",
            "source_level_status",
            "level_source_code",
            "level_fred_id",
            "transactions_source_code",
            "source_level_code_present",
            "post_supplement_level_code_present",
            "source_transactions_code_present",
            "same_base_source_codes",
        ]),
        "",
        "## Required Sector Inventory",
        "",
        _render_bullets(
            [
                f"Artifact: `{required_inventory_path}`",
                "Inventory tracks raw parsed-source availability, post-supplement level availability, method priority, bills-series availability, history span, latest backfill/promotion usage, publication status, and latest provenance fields for every required canonical sector.",
            ]
        ),
        "",
        _render_markdown_table(required_sector_inventory.to_dict(orient="records"), columns=[
            "sector_key",
            "sector_family",
            "concept_risk",
            "source_level_status",
            "method_priority",
            "bills_series_available",
            "release_window_promotion_eligible",
            "history_start",
            "history_end",
            "level_rows_available",
            "transactions_rows_available",
            "revaluation_rows_available",
            "history_preserving_backfill_rows",
            "short_window_estimate_rows",
            "short_window_origin_rows",
            "latest_publication_status",
            "latest_level_source_provider_used",
            "latest_level_supplemented_from_fred",
            "latest_point_estimate_origin",
            "latest_interval_origin",
        ]),
        "",
        "## History-Preserving Backfill",
        "",
        _render_bullets(
            [
                f"Backfilled rows: `{history_backfill_summary['history_preserving_backfill_rows']}`",
                f"Short-window estimate rows: `{history_backfill_summary['short_window_estimate_rows']}`",
                f"Short-window origin rows: `{history_backfill_summary['short_window_origin_rows']}`",
                f"Sectors with backfill: `{', '.join(history_backfill_summary['sectors_with_backfill']) or 'none'}`",
                f"Sectors with short-window estimates: `{', '.join(history_backfill_summary['sectors_with_short_window_estimate']) or 'none'}`",
                f"Sectors with short-window origins: `{', '.join(history_backfill_summary['sectors_with_short_window_origin']) or 'none'}`",
            ]
        ),
        "",
        "## History Spans",
        "",
        _render_markdown_table(history_spans[:25], columns=[
            "sector_key",
            "included",
            "date_start",
            "date_end",
            "rows",
            "node_type",
            "required_for_full_coverage",
            "concept_risk",
            "estimand_class",
            "publication_status",
            "high_confidence_flag",
            "history_preserving_backfill_rows",
            "short_window_estimate_rows",
            "short_window_origin_rows",
        ]),
        "",
        "## High-Confidence Subset",
        "",
        _render_bullets(
            [
                f"Rows: `{len(high_confidence)}`",
                f"Artifact: `{high_confidence_path}`",
                f"Derived from canonical panel: `{canonical_path}`",
            ]
        ),
        "",
        _render_markdown_table(
            _summarize_frame(high_confidence, ["sector_key", "date", "node_type", "high_confidence_flag"]),
            columns=["sector_key", "date", "node_type", "high_confidence_flag"],
        ),
        "",
        "## Weakest Sectors",
        "",
        _render_markdown_table(weakest_sectors[:25], columns=[
            "sector_key",
            "node_type",
            "concept_risk",
            "estimand_class",
            "publication_status",
            "level_evidence_tier",
            "maturity_evidence_tier",
            "high_confidence_flag",
            "history_preserving_backfill",
            "row_is_short_window_estimate",
            "estimate_origin_includes_short_window_promotion",
            "date_start",
            "date_end",
        ]),
        "",
        "## Reconciliation Diagnostics",
        "",
        _render_markdown_table([reconciliation_diagnostics], columns=[
            "formula_nodes_checked",
            "formula_rows_checked",
            "formula_rows_failing",
            "formula_max_abs_gap",
            "parent_rollups_checked",
            "parent_rows_checked",
            "parent_rows_failing",
            "parent_max_abs_gap",
        ]),
        "",
        "## Validation",
        "",
        _render_markdown_table([validation], columns=[
            "canonical_required_only",
            "reconciliation_nodes_only_noncanonical",
            "high_confidence_is_filtered_subset",
            "canonical_sector_dates_unique",
            "latest_snapshot_sector_dates_unique",
            "required_sector_coverage_complete",
            "required_sector_publication_complete",
            "published_estimate_coverage_ratio_bounded",
            "latest_snapshot_matches_required_rows",
            "formula_reconciliation_passes",
            "parent_child_reconciliation_passes",
            "overall_status",
        ]),
        "",
        "## Provenance",
        "",
        _render_markdown_table(
            [
                {
                    "canonical_sector_maturity": str(canonical_path),
                    "latest_sector_snapshot": str(latest_path),
                    "high_confidence_sector_maturity": str(high_confidence_path),
                    "reconciliation_nodes": str(reconciliation_path),
                    "fed_exact_overlay": str(fed_exact_overlay_path),
                    "required_sector_inventory": str(required_inventory_path),
                    "full_coverage_summary": str(summary_json_path),
                    "run_manifest": str(manifest_path),
                }
            ],
            columns=[
                "canonical_sector_maturity",
                "latest_sector_snapshot",
                "high_confidence_sector_maturity",
                "reconciliation_nodes",
                "fed_exact_overlay",
                "required_sector_inventory",
                "full_coverage_summary",
                "run_manifest",
            ],
        ),
        "",
        _render_markdown_table([{
            "manifest_path": provenance["manifest_path"],
            "report_path": provenance["report_path"],
            "summary_json_path": provenance["summary_json_path"],
        }], columns=["manifest_path", "report_path", "summary_json_path"]),
    ]
    return "\n".join(lines).rstrip() + "\n"


def _render_bullets(items: list[str]) -> str:
    return "\n".join(f"- {item}" for item in items)


def _render_markdown_table(rows: list[dict[str, Any]], columns: list[str] | None = None) -> str:
    if not rows:
        return "_No rows._"
    frame = pd.DataFrame(rows)
    if columns is not None:
        for column in columns:
            if column not in frame.columns:
                frame[column] = pd.NA
        frame = frame[columns]
    headers = list(frame.columns)
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for _, row in frame.iterrows():
        lines.append("| " + " | ".join(_markdown_cell(row.get(col)) for col in headers) + " |")
    return "\n".join(lines)


def _summarize_frame(frame: pd.DataFrame, columns: list[str]) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    subset = frame.copy()
    for column in columns:
        if column not in subset.columns:
            subset[column] = pd.NA
    return subset[columns].drop_duplicates().head(25).to_dict(orient="records")


def _project_columns(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=columns)
    subset = frame.copy()
    for column in columns:
        if column not in subset.columns:
            subset[column] = pd.NA
    for column in [
        "required_for_full_coverage",
        "high_confidence_flag",
        "history_preserving_backfill",
        "row_is_short_window_estimate",
        "estimate_origin_includes_short_window_promotion",
    ]:
        if column in subset.columns:
            subset[column] = subset[column].fillna(False).astype(bool)
    return subset[columns].copy()


def _markdown_cell(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if isinstance(value, (list, tuple, set)):
        return ", ".join(_markdown_cell(item) for item in value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value).replace("|", "\\|")


def _sort_release_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    out = frame.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    return out.sort_values(["sector_key", "date"]).reset_index(drop=True)


def _date_range(frame: pd.DataFrame) -> tuple[str | None, str | None]:
    if frame.empty or "date" not in frame.columns:
        return None, None
    dates = pd.to_datetime(frame["date"], errors="coerce").dropna()
    if dates.empty:
        return None, None
    return pd.Timestamp(dates.min()).date().isoformat(), pd.Timestamp(dates.max()).date().isoformat()


def _as_text(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _frames_equal(left: pd.DataFrame, right: pd.DataFrame) -> bool:
    if left.shape != right.shape:
        return False
    left_df = left.copy().reset_index(drop=True)
    right_df = right.copy().reset_index(drop=True)
    if list(left_df.columns) != list(right_df.columns):
        return False
    left_df = left_df.fillna(pd.NA)
    right_df = right_df.fillna(pd.NA)
    return left_df.equals(right_df)
