from __future__ import annotations

import shlex
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .benchmark_sets import build_estimation_benchmark_blocks
from .calibration import (
    apply_fed_wam_correction,
    build_fed_interval_calibration,
    calibrate_fed_revaluation_mapping,
    fit_fed_wam_correction,
    recenter_estimate_interval,
    summarize_interval_calibration,
)
from .coverage import (
    DEFAULT_COVERAGE_REGISTRY_PATH,
    load_coverage_registry,
    optional_bank_sector_keys,
    preview_catalog_sector_keys,
    public_preview_sector_keys,
)
from .estimation import EstimationSettings, attach_revaluation_returns, estimate_effective_maturity_panel
from .ffiec import build_bank_constraint_panel as build_bank_constraint_panel_frame
from .h15 import load_h15_curve_file
from .providers import (
    FetchArtifacts,
    fetch_ffiec_call_reports,
    fetch_h15_curves,
    fetch_ncua_call_reports,
    fetch_soma_holdings,
    fetch_z1_series,
)
from .soma import read_soma_holdings, summarize_soma_quarterly
from .tic import (
    DEFAULT_SHL_HISTORICAL_URL,
    DEFAULT_SLT_TABLE3_URL,
    build_foreign_monthly_nowcast,
    build_slt_foreign_holder_panel,
    extract_shl_total_foreign_benchmark,
    load_extracted_shl_issue_mix,
    load_shl_historical_treasury_benchmark,
    load_slt_short_long,
    load_slt_table3,
)
from .utils import dump_json, ensure_parent, load_yaml, read_table, write_table
from .z1 import (
    build_sector_panel,
    compute_identity_errors,
    load_series_catalog,
    materialize_series_panel,
    parse_z1_ddp_csv,
)

DEFAULT_PUBLIC_PREVIEW_SECTORS = public_preview_sector_keys(DEFAULT_COVERAGE_REGISTRY_PATH)
DEFAULT_OPTIONAL_BANK_SECTORS = optional_bank_sector_keys(DEFAULT_COVERAGE_REGISTRY_PATH)
PUBLIC_PREVIEW_CATALOG_SECTORS = preview_catalog_sector_keys(DEFAULT_COVERAGE_REGISTRY_PATH)

PUBLIC_PREVIEW_SCHEMA_VERSION = "1.0.0"

REQUIRED_PUBLIC_PREVIEW_COLUMNS = [
    "date",
    "sector_key",
    "bill_share",
    "short_share_le_1y",
    "coupon_share",
    "effective_duration_years",
    "zero_coupon_equivalent_years",
    "coupon_only_maturity_years",
    "method",
    "window_obs",
    "level_evidence_tier",
    "maturity_evidence_tier",
    "concept_match",
    "uncertainty_band_method",
    "uncertainty_calibration_source",
    "identified_set_source",
    "bill_share_lower",
    "bill_share_upper",
    "short_share_le_1y_lower",
    "short_share_le_1y_upper",
]

SECTOR_INTERPRETATION_RULES = {
    "fed": {
        "interpretation_class": "observed",
        "basis": "CUSIP-level SOMA holdings summarized directly against the public benchmark ladder.",
        "interpretation_boundary": "Closest object in the repo to observed legal maturity.",
    },
    "foreigners_total": {
        "interpretation_class": "survey_anchored",
        "basis": "Annual SHL survey anchors plus monthly SLT totals and short-vs-long support.",
        "interpretation_boundary": "Do not read as exact legal WAM between survey anchors.",
    },
    "foreigners_official": {
        "interpretation_class": "survey_anchored",
        "basis": "Annual SHL survey anchors plus monthly SLT totals and short-vs-long support.",
        "interpretation_boundary": "Do not read as exact legal WAM between survey anchors.",
    },
    "foreigners_private": {
        "interpretation_class": "survey_anchored",
        "basis": "Annual SHL survey anchors plus monthly SLT totals and short-vs-long support.",
        "interpretation_boundary": "Do not read as exact legal WAM between survey anchors.",
    },
    "bank_us_chartered": {
        "interpretation_class": "constrained_inference",
        "basis": "Observed levels with public bank-constraint support; maturity remains revaluation-based inference.",
        "interpretation_boundary": "Do not read as exact legal WAM.",
    },
    "credit_unions_marketable_proxy": {
        "interpretation_class": "constrained_inference",
        "basis": "Proxy level path with public bank-style short-end support where available.",
        "interpretation_boundary": "Do not read as exact legal WAM.",
    },
    "bank_foreign_banking_offices_us": {
        "interpretation_class": "constrained_inference",
        "basis": "Optional FFIEC 002-backed perimeter with public short-end support where supplied.",
        "interpretation_boundary": "Do not read as exact legal WAM.",
    },
    "bank_reserve_access_core": {
        "interpretation_class": "constrained_inference",
        "basis": "Optional supplement-backed perimeter with explicit short-end support where supplied.",
        "interpretation_boundary": "Do not read as exact legal WAM.",
    },
    "bank_broad_private_depositories_marketable_proxy": {
        "interpretation_class": "constrained_inference",
        "basis": "Optional supplement-backed perimeter with explicit short-end support where supplied.",
        "interpretation_boundary": "Do not read as exact legal WAM.",
    },
    "bank_us_affiliated_areas": {
        "interpretation_class": "constrained_inference",
        "basis": "Optional supplement-backed perimeter with explicit short-end support where supplied.",
        "interpretation_boundary": "Do not read as exact legal WAM.",
    },
    "nonfinancial_corporates": {
        "interpretation_class": "constrained_inference",
        "basis": "Observed level series with maturity inferred from revaluations and benchmark structure.",
        "interpretation_boundary": "Do not read as exact legal WAM.",
    },
    "state_local_governments": {
        "interpretation_class": "constrained_inference",
        "basis": "Observed level series with maturity inferred from revaluations and benchmark structure.",
        "interpretation_boundary": "Do not read as exact legal WAM.",
    },
    "all_holders_total": {
        "interpretation_class": "constrained_inference",
        "basis": "Aggregate holder block with maturity inferred from revaluations and benchmark structure.",
        "interpretation_boundary": "Do not read as exact legal WAM.",
    },
    "households_nonprofits": {
        "interpretation_class": "residual_inference",
        "basis": "Residual-style or weakly observed block with maturity recovered from the inverse problem.",
        "interpretation_boundary": "Do not read as exact legal WAM.",
    },
    "deposit_user_narrow_proxy": {
        "interpretation_class": "residual_inference",
        "basis": "Proxy or residual-style block assembled from component sectors.",
        "interpretation_boundary": "Do not read as exact legal WAM.",
    },
    "domestic_nonbank_residual_broad": {
        "interpretation_class": "residual_inference",
        "basis": "Residual closure sector with maturity recovered from the inverse problem.",
        "interpretation_boundary": "Do not read as exact legal WAM.",
    },
}


@dataclass(frozen=True)
class PublicReleaseArtifacts:
    report_path: Path
    sector_output_path: Path
    manifest_path: Path
    summary_json_path: Path | None = None


def build_public_release_report(
    *,
    out_dir: str | Path = "outputs/public_preview",
    source_provider: str = "fed",
    end_date: str | None = None,
    quarters: int | None = None,
    include_optional_bank_paths: bool = False,
    summary_json_out: str | Path | None = None,
    command: str | None = None,
    z1_file: str | Path | None = None,
    h15_file: str | Path | None = None,
    soma_file: str | Path | None = None,
    foreign_shl_file: str | Path | None = None,
    foreign_slt_file: str | Path | None = None,
    bank_constraint_file: str | Path | None = None,
    ffiec_file: str | Path | None = None,
    ffiec002_file: str | Path | None = None,
    ncua_file: str | Path | None = None,
    bank_supplement_file: str | Path | None = None,
    series_catalog: str | Path = "configs/z1_series_catalog.yaml",
    sector_defs: str | Path = "configs/sector_definitions.yaml",
    model_config: str | Path = "configs/model_public_preview.yaml",
    series_config: str | Path = "configs/h15_series.yaml",
    bank_constraints_config: str | Path = "configs/bank_constraints.yaml",
) -> PublicReleaseArtifacts:
    out_dir = Path(out_dir)
    report_path = out_dir / "public_release_report.md"
    sector_output_path = out_dir / "sector_effective_maturity.csv"
    manifest_path = out_dir / "run_manifest.json"
    summary_json_path = Path(summary_json_out) if summary_json_out is not None else None

    run_started = pd.Timestamp.now("UTC")
    selected_sectors = list(DEFAULT_PUBLIC_PREVIEW_SECTORS)
    if include_optional_bank_paths:
        selected_sectors.extend(DEFAULT_OPTIONAL_BANK_SECTORS)

    source_artifacts: dict[str, dict[str, Any]] = {}
    provider_summary: dict[str, str] = {}

    z1_path = _resolve_or_fetch_z1(
        z1_file=z1_file,
        source_provider=source_provider,
        series_catalog=series_catalog,
        source_artifacts=source_artifacts,
        provider_summary=provider_summary,
    )
    sector_panel = _build_sector_panel(z1_path, series_catalog=series_catalog, sector_defs=sector_defs)

    report_date = pd.Timestamp(end_date) if end_date else pd.to_datetime(sector_panel["date"]).max()
    if pd.isna(report_date):
        raise ValueError("Unable to infer a report date from the sector panel.")
    report_date = pd.Timestamp(report_date).normalize()

    h15_path = _resolve_or_fetch_h15(
        h15_file=h15_file,
        source_provider=source_provider,
        series_config=series_config,
        source_artifacts=source_artifacts,
        provider_summary=provider_summary,
    )
    benchmark = _build_nominal_benchmark_block(
        h15_path=h15_path,
        source_provider=source_provider,
        series_config=series_config,
    )

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

    fed_summary, interval_calibration = _build_fed_calibration(
        sector_panel=sector_panel,
        benchmark=benchmark,
        settings=settings,
        interval_cfg=interval_cfg,
        source_provider=source_provider,
        series_config=series_config,
        h15_path=h15_path,
        soma_file=soma_file,
        source_artifacts=source_artifacts,
        provider_summary=provider_summary,
    )
    fed_wam_correction = fit_fed_wam_correction(interval_calibration)

    foreign_nowcast = _build_foreign_nowcast(
        foreign_shl_file=foreign_shl_file,
        foreign_slt_file=foreign_slt_file,
        source_artifacts=source_artifacts,
        provider_summary=provider_summary,
    )

    bank_constraints = _build_bank_constraints(
        report_date=report_date,
        source_provider=source_provider,
        bank_constraint_file=bank_constraint_file,
        ffiec_file=ffiec_file,
        ffiec002_file=ffiec002_file,
        ncua_file=ncua_file,
        bank_supplement_file=bank_supplement_file,
        include_optional_bank_paths=include_optional_bank_paths,
        bank_constraints_config=bank_constraints_config,
        source_artifacts=source_artifacts,
        provider_summary=provider_summary,
    )

    estimated = estimate_effective_maturity_panel(
        sector_panel,
        benchmark,
        settings=settings,
        sectors=selected_sectors,
        interval_calibration=interval_calibration,
        interval_settings=interval_cfg,
        foreign_nowcast=foreign_nowcast,
        bank_constraints=bank_constraints,
        sector_config_path=str(sector_defs),
    )
    estimated = _apply_fed_wam_correction_to_public_output(estimated, fed_wam_correction)
    _validate_required_sectors_present(estimated, DEFAULT_PUBLIC_PREVIEW_SECTORS)

    resolved_end_date = _resolve_latest_common_quarter(estimated, selected_sectors, requested=end_date)
    filtered = _filter_public_output(
        estimated,
        selected_sectors=selected_sectors,
        end_date=resolved_end_date,
        quarters=quarters,
    )
    validation = _build_validation_summary(
        filtered,
        selected_sectors=selected_sectors,
        include_optional_bank_paths=include_optional_bank_paths,
        resolved_end_date=resolved_end_date,
    )
    _raise_on_failed_validations(validation)
    write_table(filtered, sector_output_path)

    manifest = {
        "schema_version": PUBLIC_PREVIEW_SCHEMA_VERSION,
        "run_timestamp_utc": run_started.isoformat(),
        "command": command or _current_command(),
        "source_provider_requested": source_provider,
        "source_provider_used": provider_summary,
        "model_config_path": str(model_config),
        "benchmark_contract": {
            "holdings_benchmark_families": ["nominal_treasury_constant_maturity"],
            "factor_benchmark_families": [],
        },
        "end_date": resolved_end_date.date().isoformat(),
        "resolved_common_quarter_date": resolved_end_date.date().isoformat(),
        "quarter_count": int(filtered["date"].nunique()),
        "sector_keys_included": selected_sectors,
        "optional_bank_paths_included": bool(include_optional_bank_paths),
        "optional_bank_sectors_skipped": (
            [] if include_optional_bank_paths else list(DEFAULT_OPTIONAL_BANK_SECTORS)
        ),
        "source_artifact_paths": source_artifacts,
        "output_paths": {
            "public_release_report": str(report_path),
            "sector_effective_maturity": str(sector_output_path),
            "run_manifest": str(manifest_path),
            "public_release_summary": str(summary_json_path) if summary_json_path is not None else None,
        },
    }
    dump_json(manifest, manifest_path)

    structured_summary = build_public_release_summary(
        sector=filtered,
        foreign_nowcast=foreign_nowcast,
        manifest=manifest,
        report_date=resolved_end_date,
        sector_output_path=sector_output_path,
        report_path=report_path,
        manifest_path=manifest_path,
        fed_summary=fed_summary,
        include_optional_bank_paths=include_optional_bank_paths,
        summary_json_path=summary_json_path,
        validation=validation,
    )
    if summary_json_path is not None:
        dump_json(structured_summary, summary_json_path)

    markdown = render_public_release_report(
        sector=filtered,
        foreign_nowcast=foreign_nowcast,
        manifest=manifest,
        report_date=resolved_end_date,
        sector_output_path=sector_output_path,
        report_path=report_path,
        manifest_path=manifest_path,
        fed_summary=fed_summary,
        include_optional_bank_paths=include_optional_bank_paths,
        summary_json_path=summary_json_path,
        validation=validation,
    )
    ensure_parent(report_path).write_text(markdown, encoding="utf-8")

    return PublicReleaseArtifacts(
        report_path=report_path,
        sector_output_path=sector_output_path,
        manifest_path=manifest_path,
        summary_json_path=summary_json_path,
    )


def build_public_release_summary(
    *,
    sector: pd.DataFrame,
    foreign_nowcast: pd.DataFrame,
    manifest: dict[str, Any],
    report_date: pd.Timestamp,
    sector_output_path: str | Path,
    report_path: str | Path,
    manifest_path: str | Path,
    fed_summary: dict[str, Any],
    include_optional_bank_paths: bool,
    summary_json_path: str | Path | None,
    validation: dict[str, Any],
) -> dict[str, Any]:
    sector_frame = sector.copy()
    sector_frame["date"] = pd.to_datetime(sector_frame["date"], errors="coerce")
    provenance = _build_provenance_summary(manifest)
    interpretations = _build_sector_interpretation_summary(sector_frame)
    return {
        "schema_version": PUBLIC_PREVIEW_SCHEMA_VERSION,
        "release_summary": {
            "report_end_date": report_date.date().isoformat(),
            "quarter_count": int(manifest["quarter_count"]),
            "source_provider_requested": manifest["source_provider_requested"],
            "source_provider_used": dict(manifest.get("source_provider_used") or {}),
            "model_config_path": manifest.get("model_config_path"),
            "benchmark_contract": dict(manifest.get("benchmark_contract") or {}),
            "optional_bank_paths_included": bool(include_optional_bank_paths),
            "command": manifest.get("command"),
            "resolved_common_quarter_date": manifest.get("resolved_common_quarter_date"),
        },
        "sector_coverage": _build_sector_coverage_summary(sector_frame),
        "sector_interpretation": interpretations,
        "evidence_tiers": _build_evidence_summary(sector_frame),
        "uncertainty_identified_sets": _build_uncertainty_summary(sector_frame),
        "validation": validation,
        "provenance": provenance,
        "bank_sector_caveats": [
            "Default public preview includes only non-interactive bank paths: bank_us_chartered plus credit_unions_marketable_proxy.",
            "Optional bank perimeters remain excluded from the stable default path because they depend on FFIEC 002 and/or supplement-backed inputs.",
        ],
        "foreign_holder_caveats": [
            "The foreign block is a foreign-holder estimate, not a foreign-bank census.",
            "Monthly foreign maturity support is anchored by annual SHL observations and SLT monthly totals, with assumption-band envelopes between anchors.",
        ],
        "excluded_optional_sectors": [] if include_optional_bank_paths else list(DEFAULT_OPTIONAL_BANK_SECTORS),
        "foreign_support_snapshot": _build_foreign_support_summary(foreign_nowcast),
        "fed_calibration_snapshot": _build_fed_calibration_snapshot(fed_summary),
        "machine_readable_outputs": {
            "sector_effective_maturity": str(sector_output_path),
            "run_manifest": str(manifest_path),
            "public_release_report": str(report_path),
            "public_release_summary": str(summary_json_path) if summary_json_path is not None else None,
        },
        "source_artifact_paths": dict(manifest.get("source_artifact_paths") or {}),
    }


def render_public_release_report(
    *,
    sector: pd.DataFrame,
    foreign_nowcast: pd.DataFrame,
    manifest: dict[str, Any],
    report_date: pd.Timestamp,
    sector_output_path: str | Path,
    report_path: str | Path,
    manifest_path: str | Path,
    fed_summary: dict[str, Any],
    include_optional_bank_paths: bool,
    summary_json_path: str | Path | None,
    validation: dict[str, Any],
) -> str:
    sector_frame = sector.copy()
    sector_frame["date"] = pd.to_datetime(sector_frame["date"], errors="coerce")
    coverage_rows = _build_sector_coverage_rows(sector_frame)
    interpretation_rows = _build_sector_interpretation_rows(sector_frame)
    evidence_rows = _build_evidence_rows(sector_frame)
    uncertainty_rows = _build_uncertainty_rows(sector_frame)
    validation_rows = _build_validation_rows(validation)
    provenance_rows = _build_provenance_rows(manifest)

    lines = [
        "# Public Release Preview Report",
        "",
        "## Release Summary",
        "",
        (
            "This artifact is the first public research preview: a nominal-only, non-interactive "
            "build of sector Treasury maturity outputs from public sources."
        ),
        "",
        f"- Report end date: `{report_date.date().isoformat()}`",
        f"- Schema version: `{PUBLIC_PREVIEW_SCHEMA_VERSION}`",
        f"- Quarters included: `{manifest['quarter_count']}`",
        f"- Requested provider: `{manifest['source_provider_requested']}`",
        f"- Public model config: `{manifest['model_config_path']}`",
        f"- Benchmark contract: `{', '.join(manifest['benchmark_contract']['holdings_benchmark_families'])}`",
        f"- Optional bank paths included: `{include_optional_bank_paths}`",
        "",
        "## Exact Command",
        "",
        "```bash",
        manifest["command"],
        "```",
        "",
        "## Sector Coverage",
        "",
        _render_markdown_table(coverage_rows),
        "",
        "## Sector Interpretation",
        "",
        _render_markdown_table(interpretation_rows),
        "",
        "## Evidence Tiers",
        "",
        _render_markdown_table(evidence_rows),
        "",
        "## Uncertainty and Identified Sets",
        "",
        _render_markdown_table(uncertainty_rows),
        "",
        "## Bank-Sector Caveats",
        "",
        (
            "The default public preview includes only the non-interactive bank path: "
            "`bank_us_chartered` plus `credit_unions_marketable_proxy`. "
            "Bill share is directly anchored when public ladder constraints are present, "
            "but duration-style metrics remain inferred from Z.1 revaluations and Fed/SOMA calibration."
        ),
        "",
        (
            "Optional bank perimeters such as `bank_foreign_banking_offices_us`, "
            "`bank_reserve_access_core`, `bank_broad_private_depositories_marketable_proxy`, "
            "and `bank_us_affiliated_areas` are excluded from the stable default path because they depend on "
            "FFIEC 002 and/or supplemental perimeter inputs."
        ),
        "",
        "## Foreign-Holder Caveats",
        "",
        (
            "The foreign block is a foreign-holder estimate, not a foreign-bank census. "
            "Official/private comes from TIC concepts, which do not map cleanly onto banks versus non-banks, "
            "and custody effects can affect attribution."
        ),
        "",
        (
            "Monthly foreign maturity support is anchored by annual SHL benchmark observations and SLT monthly totals. "
            "Between anchor dates, the preview publishes assumption-band envelopes rather than statistical confidence intervals."
        ),
        "",
        "## Excluded Optional Sectors",
        "",
        _render_bullets(
            [
                "`bank_foreign_banking_offices_us`",
                "`bank_reserve_access_core`",
                "`bank_broad_private_depositories_marketable_proxy`",
                "`bank_us_affiliated_areas`",
            ]
            if not include_optional_bank_paths
            else ["None. Optional bank paths were explicitly included for this run."]
        ),
        "",
        "## Machine-Readable Outputs",
        "",
        _render_bullets(
            [
                f"`{sector_output_path}`",
                f"`{manifest_path}`",
                f"`{report_path}`",
                *([f"`{summary_json_path}`"] if summary_json_path is not None else []),
            ]
        ),
        "",
        "## Release Notes",
        "",
        "See `docs/release_notes.md`, `docs/release_limitations.md`, and `docs/output_schema.md` for the public boundary and contract.",
        "",
        "## Validation",
        "",
        f"Overall validation status: `{validation['overall_status']}`",
        "",
        _render_markdown_table(validation_rows),
        "",
        "## Provenance",
        "",
        f"Resolved common quarter date: `{manifest.get('resolved_common_quarter_date')}`",
        "",
        _render_markdown_table(provenance_rows),
    ]

    if not foreign_nowcast.empty:
        support_row = _build_foreign_support_note(foreign_nowcast)
        lines.extend(["", "## Foreign Support Snapshot", "", support_row])

    interval = dict(fed_summary.get("interval_calibration") or {})
    if interval:
        lines.extend(
            [
                "",
                "## Fed Calibration Snapshot",
                "",
                _render_bullets(
                    [
                        f"Status: `{fed_summary.get('status', 'unknown')}`",
                        f"Revaluation fit RMSE: `{_format_scalar(fed_summary.get('revaluation_fit_rmse'))}`",
                        f"Interval calibration rows: `{interval.get('n_obs', 'n/a')}`",
                        f"Interval quantile: `{interval.get('abs_error_quantile', 'n/a')}`",
                    ]
                ),
            ]
        )

    return "\n".join(lines).rstrip() + "\n"


def _resolve_or_fetch_z1(
    *,
    z1_file: str | Path | None,
    source_provider: str,
    series_catalog: str | Path,
    source_artifacts: dict[str, dict[str, Any]],
    provider_summary: dict[str, str],
) -> Path:
    if z1_file is not None:
        path = Path(z1_file)
        source_artifacts["z1"] = {"provided_path": str(path)}
        provider_summary["z1"] = "provided"
        return path

    artifact = fetch_z1_series(
        provider=source_provider,
        series_catalog_path=series_catalog,
        normalized_out=f"data/external/normalized/z1_series_{source_provider}.csv",
    )
    source_artifacts["z1"] = _artifact_record(artifact)
    provider_summary["z1"] = artifact.provider
    return artifact.normalized_path


def _resolve_or_fetch_h15(
    *,
    h15_file: str | Path | None,
    source_provider: str,
    series_config: str | Path,
    source_artifacts: dict[str, dict[str, Any]],
    provider_summary: dict[str, str],
) -> Path:
    if h15_file is not None:
        path = Path(h15_file)
        source_artifacts["h15_nominal"] = {"provided_path": str(path)}
        provider_summary["h15_nominal"] = "provided"
        return path

    artifact = fetch_h15_curves(
        provider=source_provider,
        series_config_path=series_config,
        curve_key="nominal_treasury_constant_maturity",
        normalized_out=f"data/external/normalized/h15_curves_{source_provider}.csv",
    )
    source_artifacts["h15_nominal"] = _artifact_record(artifact)
    provider_summary["h15_nominal"] = artifact.provider
    return artifact.normalized_path


def _build_sector_panel(
    z1_path: Path,
    *,
    series_catalog: str | Path,
    sector_defs: str | Path,
) -> pd.DataFrame:
    long_df = parse_z1_ddp_csv(z1_path)
    catalog = load_series_catalog(series_catalog)
    series_panel = materialize_series_panel(long_df, catalog)
    series_panel = compute_identity_errors(series_panel)

    sector_panel = build_sector_panel(series_panel, sector_defs)
    sector_panel = compute_identity_errors(
        sector_panel.rename(columns={"sector_key": "series_key"}).copy()
    ).rename(columns={"series_key": "sector_key"})
    sector_panel = attach_revaluation_returns(sector_panel, group_col="sector_key")
    return sector_panel


def _build_nominal_benchmark_block(
    *,
    h15_path: Path,
    source_provider: str,
    series_config: str | Path,
) -> pd.DataFrame:
    benchmark, _factor = build_estimation_benchmark_blocks(
        series_config_path=series_config,
        provider=source_provider,
        holdings_families=["nominal_treasury_constant_maturity"],
        factor_families=[],
        curve_file_overrides={"nominal_treasury_constant_maturity": h15_path},
    )
    return benchmark


def _build_fed_calibration(
    *,
    sector_panel: pd.DataFrame,
    benchmark: pd.DataFrame,
    settings: EstimationSettings,
    interval_cfg: dict[str, Any],
    source_provider: str,
    series_config: str | Path,
    h15_path: Path,
    soma_file: str | Path | None,
    source_artifacts: dict[str, dict[str, Any]],
    provider_summary: dict[str, str],
) -> tuple[dict[str, Any], pd.DataFrame]:
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
        source_artifacts["soma"] = _artifact_record(artifact)
        provider_summary["soma"] = artifact.provider

    curves = load_h15_curve_file(
        h15_path,
        series_config_path=series_config,
        curve_key="nominal_treasury_constant_maturity",
    )
    soma = read_soma_holdings(soma_path)
    exact_metrics = summarize_soma_quarterly(soma, curve_df=curves)
    summary = calibrate_fed_revaluation_mapping(
        fed_panel,
        exact_metrics,
        benchmark,
        factor_returns=None,
        smoothness_penalty=settings.smoothness_penalty,
        ridge_penalty=settings.ridge_penalty,
        factor_ridge_penalty=settings.factor_ridge_penalty,
    )
    interval_calibration = build_fed_interval_calibration(
        fed_panel,
        exact_metrics,
        benchmark,
        factor_returns=None,
        settings=settings,
    )
    summary["interval_calibration"] = summarize_interval_calibration(interval_calibration, settings=interval_cfg)
    summary["wam_correction"] = fit_fed_wam_correction(interval_calibration)
    return summary, interval_calibration


def _apply_fed_wam_correction_to_public_output(
    estimated: pd.DataFrame,
    fed_wam_correction: dict | None,
) -> pd.DataFrame:
    if estimated.empty or not fed_wam_correction or fed_wam_correction.get("status") != "ok":
        return estimated

    out = estimated.copy()
    fed_mask = out["sector_key"].astype(str).eq("fed")
    if not bool(fed_mask.any()):
        return out

    fed_only = out.loc[fed_mask].copy()
    fed_only["raw_zero_coupon_equivalent_years"] = pd.to_numeric(
        fed_only["zero_coupon_equivalent_years"],
        errors="coerce",
    )
    fed_only = apply_fed_wam_correction(
        fed_only,
        fed_wam_correction,
        estimated_wam_col="raw_zero_coupon_equivalent_years",
        tips_share_col="tips_share",
        frn_share_col="frn_share",
        out_wam_col="zero_coupon_equivalent_years",
        coupon_share_col="coupon_share",
        bill_share_col="bill_share",
        coupon_only_out_col="coupon_only_maturity_years",
    )
    fed_only = recenter_estimate_interval(
        fed_only,
        raw_point_col="raw_zero_coupon_equivalent_years",
        corrected_point_col="zero_coupon_equivalent_years",
        lower_col="zero_coupon_equivalent_years_lower",
        upper_col="zero_coupon_equivalent_years_upper",
    )
    if "point_estimate_origin" in fed_only.columns:
        fed_only["point_estimate_origin"] = "fed_soma_bias_corrected_revaluation_inference"

    fed_only.drop(columns=["raw_zero_coupon_equivalent_years"], inplace=True, errors="ignore")
    out.loc[fed_mask, fed_only.columns] = fed_only
    return out


def _build_foreign_nowcast(
    *,
    foreign_shl_file: str | Path | None,
    foreign_slt_file: str | Path | None,
    source_artifacts: dict[str, dict[str, Any]],
    provider_summary: dict[str, str],
) -> pd.DataFrame:
    if foreign_shl_file is not None:
        shl = load_extracted_shl_issue_mix(foreign_shl_file)
        source_artifacts["foreign_shl"] = {"provided_path": str(Path(foreign_shl_file))}
        provider_summary["foreign_shl"] = "provided"
    else:
        shl = extract_shl_total_foreign_benchmark(
            load_shl_historical_treasury_benchmark(DEFAULT_SHL_HISTORICAL_URL)
        )
        source_artifacts["foreign_shl"] = {"url": DEFAULT_SHL_HISTORICAL_URL}
        provider_summary["foreign_shl"] = "official"

    if foreign_slt_file is not None:
        slt = load_slt_short_long(foreign_slt_file)
        source_artifacts["foreign_slt"] = {"provided_path": str(Path(foreign_slt_file))}
        provider_summary["foreign_slt"] = "provided"
    else:
        slt = build_slt_foreign_holder_panel(load_slt_table3(DEFAULT_SLT_TABLE3_URL))
        source_artifacts["foreign_slt"] = {"url": DEFAULT_SLT_TABLE3_URL}
        provider_summary["foreign_slt"] = "official"

    return build_foreign_monthly_nowcast(shl, slt)


def _build_bank_constraints(
    *,
    report_date: pd.Timestamp,
    source_provider: str,
    bank_constraint_file: str | Path | None,
    ffiec_file: str | Path | None,
    ffiec002_file: str | Path | None,
    ncua_file: str | Path | None,
    bank_supplement_file: str | Path | None,
    include_optional_bank_paths: bool,
    bank_constraints_config: str | Path,
    source_artifacts: dict[str, dict[str, Any]],
    provider_summary: dict[str, str],
) -> pd.DataFrame | None:
    if bank_constraint_file is not None:
        path = Path(bank_constraint_file)
        source_artifacts["bank_constraints"] = {"provided_path": str(path)}
        provider_summary["bank_constraints"] = "provided"
        return read_table(path)

    frames: list[pd.DataFrame] = []
    if ffiec_file is not None:
        path = Path(ffiec_file)
        source_artifacts["ffiec_call_reports"] = {"provided_path": str(path)}
        provider_summary["ffiec_call_reports"] = "provided"
        frames.append(read_table(path))
    else:
        artifact = fetch_ffiec_call_reports(
            report_date=report_date,
            provider="ffiec" if source_provider == "fed" else "auto",
            normalized_out="data/external/normalized/ffiec_call_reports_ffiec.csv",
        )
        source_artifacts["ffiec_call_reports"] = _artifact_record(artifact)
        provider_summary["ffiec_call_reports"] = artifact.provider
        frames.append(read_table(artifact.normalized_path))

    if ncua_file is not None:
        path = Path(ncua_file)
        source_artifacts["ncua_call_reports"] = {"provided_path": str(path)}
        provider_summary["ncua_call_reports"] = "provided"
        frames.append(read_table(path))
    else:
        artifact = fetch_ncua_call_reports(
            report_date=report_date,
            provider="ncua",
            normalized_out="data/external/normalized/ncua_call_reports_ncua.csv",
        )
        source_artifacts["ncua_call_reports"] = _artifact_record(artifact)
        provider_summary["ncua_call_reports"] = artifact.provider
        frames.append(read_table(artifact.normalized_path))

    if include_optional_bank_paths and ffiec002_file is not None:
        path = Path(ffiec002_file)
        source_artifacts["ffiec002_call_reports"] = {"provided_path": str(path)}
        provider_summary["ffiec002_call_reports"] = "provided"
        frames.append(read_table(path))

    supplement = None
    if include_optional_bank_paths and bank_supplement_file is not None:
        supplement_path = Path(bank_supplement_file)
        source_artifacts["bank_constraint_supplement"] = {"provided_path": str(supplement_path)}
        provider_summary["bank_constraint_supplement"] = "provided"
        supplement = read_table(supplement_path)

    institutions = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()
    panel = build_bank_constraint_panel_frame(
        institutions,
        constraints_config_path=bank_constraints_config,
        supplement_df=supplement,
    )
    source_artifacts["bank_constraints"] = {"derived_from": sorted(source_artifacts.keys())}
    provider_summary["bank_constraints"] = "derived"
    return panel


def _validate_required_sectors_present(frame: pd.DataFrame, required: list[str]) -> None:
    present = set(frame["sector_key"].dropna().astype(str))
    missing = [sector for sector in required if sector not in present]
    if missing:
        raise ValueError(f"Estimated output is missing required public-preview sectors: {', '.join(missing)}")


def _resolve_latest_common_quarter(
    frame: pd.DataFrame,
    selected_sectors: list[str],
    requested: str | None,
) -> pd.Timestamp:
    if requested is not None:
        return pd.Timestamp(requested).normalize()

    subset = frame[frame["sector_key"].isin(selected_sectors)].copy()
    if subset.empty:
        raise ValueError("Cannot resolve the latest common quarter from an empty output.")
    maxima = subset.groupby("sector_key")["date"].max()
    return pd.Timestamp(maxima.min()).normalize()


def _filter_public_output(
    frame: pd.DataFrame,
    *,
    selected_sectors: list[str],
    end_date: pd.Timestamp,
    quarters: int | None,
) -> pd.DataFrame:
    out = frame.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out[(out["sector_key"].isin(selected_sectors)) & (out["date"] <= end_date)].copy()
    out = out.sort_values(["date", "sector_key"]).reset_index(drop=True)
    if quarters is not None:
        keep_dates = sorted(out["date"].dropna().unique())[-quarters:]
        out = out[out["date"].isin(keep_dates)].copy()
    return out.reset_index(drop=True)


def _artifact_record(artifact: FetchArtifacts) -> dict[str, Any]:
    return {
        "provider": artifact.provider,
        "dataset": artifact.dataset,
        "raw_path": str(artifact.raw_path) if artifact.raw_path is not None else None,
        "normalized_path": str(artifact.normalized_path),
    }


def _build_sector_coverage_rows(frame: pd.DataFrame) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    registry = load_coverage_registry(DEFAULT_COVERAGE_REGISTRY_PATH)
    for sector_key in PUBLIC_PREVIEW_CATALOG_SECTORS:
        node = registry.get(sector_key)
        sub = frame[frame["sector_key"] == sector_key].copy()
        if sub.empty:
            rows.append(
                {
                    "sector_key": sector_key,
                    "node_type": node.node_type if node else "n/a",
                    "required": str(bool(node.required_for_full_coverage)) if node else "n/a",
                    "dates": "not included",
                    "rows": "0",
                    "level": "n/a",
                    "maturity": "n/a",
                    "concept": "n/a",
                    "coverage": "n/a",
                }
            )
            continue
        rows.append(
            {
                "sector_key": sector_key,
                "node_type": node.node_type if node else "n/a",
                "required": str(bool(node.required_for_full_coverage)) if node else "n/a",
                "dates": _format_date_span(pd.to_datetime(sub["date"], errors="coerce")),
                "rows": str(len(sub)),
                "level": _join_unique(sub.get("level_evidence_tier")),
                "maturity": _join_unique(sub.get("maturity_evidence_tier")),
                "concept": _join_unique(sub.get("concept_match")),
                "coverage": _format_ratio_range(sub.get("coverage_ratio")),
            }
        )
    return rows


def _build_sector_coverage_summary(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    registry = load_coverage_registry(DEFAULT_COVERAGE_REGISTRY_PATH)
    for sector_key in PUBLIC_PREVIEW_CATALOG_SECTORS:
        node = registry.get(sector_key)
        sub = frame[frame["sector_key"] == sector_key].copy()
        coverage_min, coverage_max = _numeric_min_max(sub.get("coverage_ratio"))
        rows.append(
            {
                "sector_key": sector_key,
                "node_type": node.node_type if node else None,
                "sector_family": node.sector_family if node else None,
                "parent_key": node.parent_key if node else None,
                "is_canonical": bool(node.is_canonical) if node else False,
                "required_for_full_coverage": bool(node.required_for_full_coverage) if node else False,
                "concept_risk": node.concept_risk if node else None,
                "history_start_reason": node.history_start_reason if node else None,
                "included": not sub.empty,
                "date_start": _series_date_start(sub.get("date")),
                "date_end": _series_date_end(sub.get("date")),
                "row_count": int(len(sub)),
                "level_evidence_tiers": _unique_list(sub.get("level_evidence_tier")),
                "maturity_evidence_tiers": _unique_list(sub.get("maturity_evidence_tier")),
                "concept_matches": _unique_list(sub.get("concept_match")),
                "coverage_ratio_min": coverage_min,
                "coverage_ratio_max": coverage_max,
            }
        )
    return rows


def _build_evidence_rows(frame: pd.DataFrame) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    grouped = (
        frame.assign(
            level_evidence_tier=frame.get("level_evidence_tier", pd.Series(dtype="object")).fillna("n/a"),
            maturity_evidence_tier=frame.get("maturity_evidence_tier", pd.Series(dtype="object")).fillna("n/a"),
        )
        .groupby(["level_evidence_tier", "maturity_evidence_tier"], dropna=False)
    )
    for (level, maturity), sub in grouped:
        rows.append(
            {
                "level": str(level),
                "maturity": str(maturity),
                "sectors": _summarize_labels(sorted(sub["sector_key"].dropna().astype(str).unique())),
                "rows": str(len(sub)),
            }
        )
    return rows or [{"level": "n/a", "maturity": "n/a", "sectors": "none", "rows": "0"}]


def _build_evidence_summary(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    grouped = (
        frame.assign(
            level_evidence_tier=frame.get("level_evidence_tier", pd.Series(dtype="object")).fillna("n/a"),
            maturity_evidence_tier=frame.get("maturity_evidence_tier", pd.Series(dtype="object")).fillna("n/a"),
        )
        .groupby(["level_evidence_tier", "maturity_evidence_tier"], dropna=False)
    )
    for (level, maturity), sub in grouped:
        rows.append(
            {
                "level_evidence_tier": str(level),
                "maturity_evidence_tier": str(maturity),
                "sector_keys": sorted(sub["sector_key"].dropna().astype(str).unique()),
                "row_count": int(len(sub)),
            }
        )
    return rows


def _build_uncertainty_rows(frame: pd.DataFrame) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for sector_key, sub in frame.groupby("sector_key", sort=True):
        clipped = _bool_series(sub.get("identified_set_point_clipped"))
        rows.append(
            {
                "sector_key": str(sector_key),
                "band_method": _join_unique(sub.get("uncertainty_band_method")),
                "calibration": _join_unique(sub.get("uncertainty_calibration_source")),
                "identified_set": _join_unique(sub.get("identified_set_source")),
                "point_clipped_rows": _format_count_ratio(int(clipped.sum()), len(sub)),
            }
        )
    return rows or [{"sector_key": "none", "band_method": "n/a", "calibration": "n/a", "identified_set": "n/a", "point_clipped_rows": "0/0"}]


def _build_uncertainty_summary(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for sector_key, sub in frame.groupby("sector_key", sort=True):
        clipped = _bool_series(sub.get("identified_set_point_clipped"))
        rows.append(
            {
                "sector_key": str(sector_key),
                "band_methods": _unique_list(sub.get("uncertainty_band_method")),
                "calibration_sources": _unique_list(sub.get("uncertainty_calibration_source")),
                "identified_set_sources": _unique_list(sub.get("identified_set_source")),
                "point_clipped_rows": int(clipped.sum()),
                "total_rows": int(len(sub)),
                "point_clipped_ratio": (float(clipped.sum()) / float(len(sub))) if len(sub) else 0.0,
            }
        )
    return rows


def _build_foreign_support_note(frame: pd.DataFrame) -> str:
    active = _bool_series(frame.get("uncertainty_band_active"))
    support_kind = frame.get("uncertainty_support_kind")
    support_counts = []
    if support_kind is not None:
        for label in [
            "direct_support",
            "two_sided_between_supports",
            "one_sided_flat_fill",
            "no_support",
        ]:
            support_counts.append(f"{label}={int((support_kind == label).sum())}")
    return (
        f"Foreign nowcast rows with active assumption bands: `{int(active.sum())}/{len(frame)}`; "
        f"support mix: `{', '.join(support_counts)}`."
    )


def _build_foreign_support_summary(frame: pd.DataFrame) -> dict[str, Any] | None:
    if frame.empty:
        return None
    active = _bool_series(frame.get("uncertainty_band_active"))
    support_kind = frame.get("uncertainty_support_kind")
    support_kind_counts: dict[str, int] = {}
    if support_kind is not None:
        for label in [
            "direct_support",
            "two_sided_between_supports",
            "one_sided_flat_fill",
            "no_support",
        ]:
            support_kind_counts[label] = int((support_kind == label).sum())
    return {
        "active_assumption_band_rows": int(active.sum()),
        "total_rows": int(len(frame)),
        "support_kind_counts": support_kind_counts,
    }


def _build_fed_calibration_snapshot(fed_summary: dict[str, Any]) -> dict[str, Any] | None:
    if not fed_summary:
        return None
    interval = dict(fed_summary.get("interval_calibration") or {})
    return {
        "status": fed_summary.get("status"),
        "revaluation_fit_rmse": _safe_float(fed_summary.get("revaluation_fit_rmse")),
        "interval_calibration_rows": interval.get("n_obs"),
        "interval_quantile": interval.get("abs_error_quantile"),
    }


def _build_validation_summary(
    frame: pd.DataFrame,
    *,
    selected_sectors: list[str],
    include_optional_bank_paths: bool,
    resolved_end_date: pd.Timestamp,
) -> dict[str, Any]:
    present = set(frame.get("sector_key", pd.Series(dtype="object")).dropna().astype(str))
    missing_required = [sector for sector in DEFAULT_PUBLIC_PREVIEW_SECTORS if sector not in present]
    missing_columns = [column for column in REQUIRED_PUBLIC_PREVIEW_COLUMNS if column not in frame.columns]
    optional_present = sorted(present & set(DEFAULT_OPTIONAL_BANK_SECTORS))

    checks = [
        {
            "check": "required_public_preview_sectors_present",
            "status": "pass" if not missing_required else "fail",
            "hard_failure": True,
            "details": (
                "All required default public-preview sectors are present."
                if not missing_required
                else "Missing sectors: " + ", ".join(missing_required)
            ),
        },
        {
            "check": "required_public_columns_present",
            "status": "pass" if not missing_columns else "fail",
            "hard_failure": True,
            "details": (
                "All required schema columns are present."
                if not missing_columns
                else "Missing columns: " + ", ".join(missing_columns)
            ),
        },
        {
            "check": "resolved_common_quarter_present_for_all_selected_sectors",
            "status": (
                "pass"
                if _sectors_present_at_date(frame, selected_sectors, resolved_end_date)
                else "fail"
            ),
            "hard_failure": True,
            "details": (
                f"All selected sectors contain rows at {resolved_end_date.date().isoformat()}."
                if _sectors_present_at_date(frame, selected_sectors, resolved_end_date)
                else f"At least one selected sector is missing the resolved common quarter {resolved_end_date.date().isoformat()}."
            ),
        },
        {
            "check": "optional_bank_sector_policy",
            "status": (
                "pass"
                if include_optional_bank_paths or not optional_present
                else "fail"
            ),
            "hard_failure": True,
            "details": (
                "Optional bank sectors are absent from the stable default output."
                if (include_optional_bank_paths or not optional_present)
                else "Optional sectors unexpectedly present: " + ", ".join(optional_present)
            ),
        },
        {
            "check": "published_interval_bounds_are_ordered",
            "status": "pass" if _bounds_are_valid(frame) else "fail",
            "hard_failure": True,
            "details": (
                "Published lower/upper interval columns are ordered and contain their points when all values are present."
                if _bounds_are_valid(frame)
                else "At least one published lower/upper interval pair is invalid or excludes its point estimate."
            ),
        },
    ]
    return {
        "overall_status": "pass" if all(check["status"] == "pass" for check in checks) else "fail",
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
    raise ValueError(f"Public preview validation failed: {details}")


def _build_validation_rows(validation: dict[str, Any]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for check in validation.get("checks", []):
        rows.append(
            {
                "check": str(check.get("check")),
                "status": str(check.get("status")),
                "hard_failure": str(bool(check.get("hard_failure"))),
                "details": str(check.get("details", "")),
            }
        )
    return rows or [{"check": "n/a", "status": "n/a", "hard_failure": "False", "details": "No validation rows."}]


def _build_provenance_summary(manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "resolved_common_quarter_date": manifest.get("resolved_common_quarter_date"),
        "sources": _build_provenance_rows(manifest),
    }


def _build_provenance_rows(manifest: dict[str, Any]) -> list[dict[str, str | None]]:
    rows: list[dict[str, str | None]] = []
    source_artifacts = dict(manifest.get("source_artifact_paths") or {})
    providers = dict(manifest.get("source_provider_used") or {})
    for source_key in sorted(source_artifacts):
        artifact = dict(source_artifacts[source_key] or {})
        access_mode, reference = _artifact_access_mode_and_reference(artifact)
        date_start, date_end, vintage = _artifact_temporal_metadata(artifact)
        rows.append(
            {
                "source_key": source_key,
                "provider": str(providers.get(source_key) or "n/a"),
                "access_mode": access_mode,
                "reference": reference,
                "date_span": _format_nullable_date_span(date_start, date_end),
                "vintage": vintage,
            }
        )
    return rows or [
        {
            "source_key": "n/a",
            "provider": "n/a",
            "access_mode": "n/a",
            "reference": "n/a",
            "date_span": "n/a",
            "vintage": "n/a",
        }
    ]


def _artifact_access_mode_and_reference(artifact: dict[str, Any]) -> tuple[str, str]:
    if artifact.get("provided_path"):
        return "provided", str(artifact["provided_path"])
    if artifact.get("normalized_path"):
        return "fetched", str(artifact["normalized_path"])
    if artifact.get("url"):
        return "official_url", str(artifact["url"])
    if artifact.get("derived_from"):
        derived_from = ", ".join(str(value) for value in artifact["derived_from"])
        return "derived", derived_from
    if artifact.get("raw_path"):
        return "fetched_raw", str(artifact["raw_path"])
    return "unknown", "n/a"


def _artifact_temporal_metadata(artifact: dict[str, Any]) -> tuple[str | None, str | None, str | None]:
    path_str = artifact.get("normalized_path") or artifact.get("provided_path")
    if not path_str:
        return None, None, None
    path = Path(path_str)
    if not path.exists() or path.suffix.lower() not in {".csv", ".txt", ".xlsx", ".xls", ".parquet"}:
        return None, None, None
    try:
        frame = read_table(path)
    except Exception:
        return None, None, None

    date_start, date_end = _detect_frame_date_span(frame)
    vintage = _detect_frame_vintage(frame)
    return date_start, date_end, vintage


def _detect_frame_date_span(frame: pd.DataFrame) -> tuple[str | None, str | None]:
    for candidate in [
        "date",
        "report_date",
        "as_of_date",
        "observation_date",
        "quarter_end",
        "quarter_end_date",
        "quarter",
    ]:
        if candidate not in frame.columns:
            continue
        series = pd.to_datetime(frame[candidate], errors="coerce").dropna().sort_values()
        if not series.empty:
            return series.iloc[0].date().isoformat(), series.iloc[-1].date().isoformat()
    return None, None


def _detect_frame_vintage(frame: pd.DataFrame) -> str | None:
    if "vintage" not in frame.columns:
        return None
    values = sorted({str(value) for value in frame["vintage"].dropna().astype(str) if str(value)})
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    return f"{values[0]} -> {values[-1]}"


def _build_sector_interpretation_summary(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    registry = load_coverage_registry(DEFAULT_COVERAGE_REGISTRY_PATH)
    for sector_key in PUBLIC_PREVIEW_CATALOG_SECTORS:
        node = registry.get(sector_key)
        rule = dict(
            SECTOR_INTERPRETATION_RULES.get(
                sector_key,
                {
                    "interpretation_class": "constrained_inference",
                    "basis": "Sector maturity is inferred from the public benchmark/revaluation stack.",
                    "interpretation_boundary": "Do not read as exact legal WAM.",
                },
            )
        )
        included = sector_key in set(frame.get("sector_key", pd.Series(dtype="object")).dropna().astype(str))
        rows.append(
            {
                "sector_key": sector_key,
                "node_type": node.node_type if node else None,
                "required_for_full_coverage": bool(node.required_for_full_coverage) if node else False,
                "included": included,
                "interpretation_class": rule["interpretation_class"],
                "basis": rule["basis"],
                "interpretation_boundary": rule["interpretation_boundary"],
            }
        )
    return rows


def _build_sector_interpretation_rows(frame: pd.DataFrame) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for row in _build_sector_interpretation_summary(frame):
        rows.append(
            {
                "sector_key": str(row["sector_key"]),
                "included": str(bool(row["included"])),
                "class": str(row["interpretation_class"]),
                "basis": str(row["basis"]),
                "boundary": str(row["interpretation_boundary"]),
            }
        )
    return rows


def _sectors_present_at_date(frame: pd.DataFrame, sectors: list[str], date: pd.Timestamp) -> bool:
    if frame.empty:
        return False
    normalized_date = pd.Timestamp(date).normalize()
    rows = frame.copy()
    rows["date"] = pd.to_datetime(rows["date"], errors="coerce").dt.normalize()
    present = set(rows.loc[rows["date"] == normalized_date, "sector_key"].dropna().astype(str))
    return set(sectors).issubset(present)


def _bounds_are_valid(frame: pd.DataFrame) -> bool:
    bound_sets = [
        ("bill_share", "bill_share_lower", "bill_share_upper"),
        ("short_share_le_1y", "short_share_le_1y_lower", "short_share_le_1y_upper"),
        ("effective_duration_years", "effective_duration_years_lower", "effective_duration_years_upper"),
        (
            "zero_coupon_equivalent_years",
            "zero_coupon_equivalent_years_lower",
            "zero_coupon_equivalent_years_upper",
        ),
    ]
    for point_col, lower_col, upper_col in bound_sets:
        if not {point_col, lower_col, upper_col}.issubset(frame.columns):
            continue
        subset = frame[[point_col, lower_col, upper_col]].apply(pd.to_numeric, errors="coerce").dropna()
        if subset.empty:
            continue
        if ((subset[lower_col] > subset[upper_col]) | (subset[point_col] < subset[lower_col]) | (subset[point_col] > subset[upper_col])).any():
            return False
    return True


def _format_nullable_date_span(start: str | None, end: str | None) -> str:
    if start and end:
        return f"{start} -> {end}"
    if start:
        return start
    if end:
        return end
    return "n/a"


def _render_markdown_table(rows: list[dict[str, str]]) -> str:
    if not rows:
        return "_No rows._"

    columns = list(rows[0].keys())
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    body = [
        "| "
        + " | ".join(str(row.get(column, "")).replace("\n", " ").replace("|", "\\|") for column in columns)
        + " |"
        for row in rows
    ]
    return "\n".join([header, separator, *body])


def _render_bullets(items: list[str]) -> str:
    return "\n".join(f"- {item}" for item in items)


def _format_date_span(series: pd.Series) -> str:
    series = pd.to_datetime(series, errors="coerce").dropna().sort_values()
    if series.empty:
        return "n/a"
    return f"{series.iloc[0].date().isoformat()} -> {series.iloc[-1].date().isoformat()}"


def _series_date_start(series: pd.Series | None) -> str | None:
    if series is None:
        return None
    normalized = pd.to_datetime(series, errors="coerce").dropna().sort_values()
    if normalized.empty:
        return None
    return normalized.iloc[0].date().isoformat()


def _series_date_end(series: pd.Series | None) -> str | None:
    if series is None:
        return None
    normalized = pd.to_datetime(series, errors="coerce").dropna().sort_values()
    if normalized.empty:
        return None
    return normalized.iloc[-1].date().isoformat()


def _format_ratio_range(series: pd.Series | None) -> str:
    if series is None:
        return "n/a"
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return "n/a"
    return f"{numeric.min():.3f} -> {numeric.max():.3f}"


def _join_unique(series: pd.Series | None) -> str:
    if series is None:
        return "n/a"
    values = [str(value) for value in pd.Series(series).dropna().astype(str).unique() if str(value).strip()]
    return ", ".join(values) if values else "n/a"


def _unique_list(series: pd.Series | None) -> list[str]:
    if series is None:
        return []
    return [str(value) for value in pd.Series(series).dropna().astype(str).unique() if str(value).strip()]


def _numeric_min_max(series: pd.Series | None) -> tuple[float | None, float | None]:
    if series is None:
        return None, None
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return None, None
    return float(numeric.min()), float(numeric.max())


def _summarize_labels(labels: list[str], limit: int = 5) -> str:
    if not labels:
        return "n/a"
    head = labels[:limit]
    if len(labels) <= limit:
        return ", ".join(head)
    return f"{', '.join(head)} + {len(labels) - limit} more"


def _bool_series(series: pd.Series | None) -> pd.Series:
    if series is None:
        return pd.Series(dtype=bool)
    if pd.api.types.is_bool_dtype(series):
        return pd.Series(series).fillna(False).astype(bool)
    mapped = (
        pd.Series(series)
        .astype("string")
        .str.strip()
        .str.lower()
        .map({"true": True, "false": False, "1": True, "0": False, "yes": True, "no": False})
    )
    return mapped.fillna(False).astype(bool)


def _format_count_ratio(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "0/0"
    return f"{numerator}/{denominator} ({numerator / denominator:.1%})"


def _format_scalar(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        numeric = float(value)
    except Exception:
        return str(value)
    if pd.isna(numeric):
        return "n/a"
    return f"{numeric:.6g}"


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    if pd.isna(numeric):
        return None
    return numeric


def _current_command() -> str:
    return " ".join(shlex.quote(part) for part in sys.argv)
