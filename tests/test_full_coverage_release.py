from __future__ import annotations

import importlib
import json
from pathlib import Path

import pandas as pd

from treasury_sector_maturity.coverage import canonical_atomic_sector_keys, required_full_coverage_sector_keys
from treasury_sector_maturity.z1 import build_sector_panel, compute_identity_errors, load_series_catalog, materialize_series_panel, parse_z1_ddp_csv


ROOT = Path(__file__).resolve().parents[1]
TOY_Z1 = ROOT / "data" / "examples" / "toy_z1_selected_series.csv"
TOY_H15 = ROOT / "data" / "examples" / "toy_h15_curves.csv"
TOY_TIPS = ROOT / "data" / "examples" / "toy_tips_real_yields.csv"
TOY_SOMA = ROOT / "data" / "examples" / "toy_soma_holdings.csv"
TOY_SHL = ROOT / "data" / "examples" / "toy_shl_issue_mix.csv"
TOY_SLT = ROOT / "data" / "examples" / "toy_slt_short_long.csv"
TOY_BANK_CONSTRAINTS = ROOT / "data" / "examples" / "toy_bank_constraint_panel.csv"
FULL_CATALOG = ROOT / "configs" / "z1_series_catalog_full.yaml"
FULL_SECTOR_DEFS = ROOT / "configs" / "sector_definitions_full.yaml"


def _build_full_scope_toy_sector_panel() -> pd.DataFrame:
    long_df = parse_z1_ddp_csv(TOY_Z1)
    catalog = load_series_catalog(FULL_CATALOG)
    series_panel = materialize_series_panel(long_df, catalog)
    series_panel = compute_identity_errors(series_panel)
    sector_panel = build_sector_panel(series_panel, FULL_SECTOR_DEFS)
    sector_panel = compute_identity_errors(
        sector_panel.rename(columns={"sector_key": "series_key"}).copy()
    ).rename(columns={"series_key": "sector_key"})
    return sector_panel


def test_full_coverage_release_builder_emits_expected_artifacts(tmp_path):
    module = importlib.import_module("treasury_sector_maturity.full_coverage_release")
    builder = getattr(module, "build_full_coverage_release")

    out_dir = tmp_path / "full_coverage_release"
    artifacts = builder(
        out_dir=out_dir,
        source_provider="fed",
        coverage_scope="full",
        z1_file=TOY_Z1,
        h15_file=TOY_H15,
        curve_file=[f"tips_real_yield_constant_maturity={TOY_TIPS}"],
        soma_file=TOY_SOMA,
        foreign_shl_file=TOY_SHL,
        foreign_slt_file=TOY_SLT,
        bank_constraint_file=TOY_BANK_CONSTRAINTS,
        series_catalog=FULL_CATALOG,
        sector_defs=FULL_SECTOR_DEFS,
        model_config="configs/model_defaults.yaml",
        series_config="configs/h15_series.yaml",
        bank_constraints_config="configs/bank_constraints.yaml",
        summary_json_out=out_dir / "full_coverage_summary.json",
    )

    assert artifacts is not None
    expected_paths = [
        out_dir / "canonical_atomic_sector_maturity.csv",
        out_dir / "latest_atomic_sector_snapshot.csv",
        out_dir / "high_confidence_sector_maturity.csv",
        out_dir / "reconciliation_nodes.csv",
        out_dir / "required_sector_inventory.csv",
        out_dir / "full_coverage_report.md",
        out_dir / "run_manifest.json",
        out_dir / "full_coverage_summary.json",
    ]
    for path in expected_paths:
        assert path.exists(), path

    canonical = pd.read_csv(out_dir / "canonical_atomic_sector_maturity.csv", parse_dates=["date"])
    latest = pd.read_csv(out_dir / "latest_atomic_sector_snapshot.csv", parse_dates=["date"])
    high_confidence = pd.read_csv(out_dir / "high_confidence_sector_maturity.csv", parse_dates=["date"])
    reconciliation = pd.read_csv(out_dir / "reconciliation_nodes.csv", parse_dates=["date"])
    inventory = pd.read_csv(out_dir / "required_sector_inventory.csv")
    summary = json.loads((out_dir / "full_coverage_summary.json").read_text(encoding="utf-8"))
    report = (out_dir / "full_coverage_report.md").read_text(encoding="utf-8")

    assert "node_type" in canonical.columns
    assert "high_confidence_flag" in canonical.columns
    assert "history_preserving_backfill" in canonical.columns
    assert "release_window_override" in canonical.columns
    assert canonical["node_type"].eq("atomic").all()
    assert "node_type" in reconciliation.columns
    assert reconciliation["node_type"].ne("atomic").all()
    assert len(canonical[["date", "sector_key"]].drop_duplicates()) == len(canonical)
    assert len(latest[["date", "sector_key"]].drop_duplicates()) == len(latest)

    sector_panel = _build_full_scope_toy_sector_panel()
    registry_atomic = set(canonical_atomic_sector_keys(FULL_CATALOG.parent / "coverage_registry.yaml"))
    registry_required = set(required_full_coverage_sector_keys(FULL_CATALOG.parent / "coverage_registry.yaml"))
    required_atomic = registry_atomic & registry_required
    latest_quarter = pd.Timestamp(sector_panel.loc[sector_panel["sector_key"].isin(required_atomic) & sector_panel["level"].notna(), "date"].max())
    if pd.notna(latest_quarter):
        expected_latest_sectors = set(
            sector_panel[
                (sector_panel["sector_key"].isin(required_atomic))
                & (sector_panel["level"].notna())
                & (sector_panel["date"] == latest_quarter)
            ]["sector_key"].dropna().astype(str)
        )
        assert latest["date"].nunique() == 1
        assert latest["date"].iloc[0] == latest_quarter
        assert set(latest["sector_key"].astype(str)) == expected_latest_sectors

    expected_required_rows = sector_panel[
        (sector_panel["sector_key"].isin(required_atomic)) & sector_panel["level"].notna()
    ][["date", "sector_key"]].drop_duplicates()
    observed_required_rows = canonical[["date", "sector_key"]].drop_duplicates()
    merged = expected_required_rows.merge(observed_required_rows, on=["date", "sector_key"], how="left", indicator=True)
    assert merged["_merge"].eq("both").all()
    required_with_estimates = canonical[
        canonical["sector_key"].isin(required_atomic)
        & canonical[["bill_share", "effective_duration_years", "zero_coupon_equivalent_years"]].notna().any(axis=1)
    ][["date", "sector_key"]].drop_duplicates()
    estimate_merged = expected_required_rows.merge(required_with_estimates, on=["date", "sector_key"], how="left", indicator=True)
    assert estimate_merged["_merge"].eq("both").all()
    assert canonical["history_preserving_backfill"].fillna(False).any()
    assert canonical["release_window_override"].fillna(False).any()

    filtered_high_confidence = canonical[canonical["high_confidence_flag"].fillna(False)].copy()
    canonical_cols = list(canonical.columns)
    pd.testing.assert_frame_equal(
        high_confidence.sort_values(["date", "sector_key"]).reset_index(drop=True)[canonical_cols],
        filtered_high_confidence.sort_values(["date", "sector_key"]).reset_index(drop=True)[canonical_cols],
        check_like=False,
    )

    expected_report_sections = [
        "# Full Coverage Research Release",
        "## Release Summary",
        "## Coverage Completeness",
        "## Source Series Audit",
        "## Required Sector Inventory",
        "## Latest Common-Quarter Snapshot",
        "## History-Preserving Backfill",
        "## History Spans",
        "## High-Confidence Subset",
        "## Weakest Sectors",
        "## Reconciliation Diagnostics",
        "## Validation",
        "## Provenance",
    ]
    for heading in expected_report_sections:
        assert heading in report

    expected_summary_keys = {
        "schema_version",
        "release_summary",
        "coverage_completeness",
        "source_series_audit",
        "history_spans",
        "high_confidence_subset",
        "weakest_sectors",
        "validation",
        "provenance",
        "machine_readable_outputs",
        "source_artifact_paths",
    }
    assert expected_summary_keys.issubset(summary)
    assert summary["machine_readable_outputs"]["full_coverage_summary"] == str(out_dir / "full_coverage_summary.json")
    assert summary["machine_readable_outputs"]["full_coverage_report"] == str(out_dir / "full_coverage_report.md")
    assert summary["machine_readable_outputs"]["required_sector_inventory"] == str(out_dir / "required_sector_inventory.csv")
    assert summary["high_confidence_subset"]["count"] == int(len(high_confidence))
    assert summary["release_summary"]["canonical_row_count"] == int(len(canonical))
    assert summary["release_summary"]["latest_snapshot_row_count"] == int(len(latest))
    assert summary["release_summary"]["high_confidence_row_count"] == int(len(high_confidence))
    assert summary["release_summary"]["reconciliation_row_count"] == int(len(reconciliation))
    assert summary["release_summary"]["required_sector_inventory_row_count"] == int(len(inventory))
    expected_required_covered = len(set(canonical["sector_key"].astype(str)) & required_atomic)
    assert summary["coverage_completeness"]["required_atomic_covered"] == expected_required_covered
    assert summary["coverage_completeness"]["missing_required_estimate_rows"] == 0
    assert summary["coverage_completeness"]["required_estimate_coverage_ratio"] <= 1.0
    assert summary["source_series_audit"]["required_sector_count"] == int(len(inventory))
    assert summary["source_series_audit"]["source_level_status_counts"]["present"] > 0
    assert summary["source_series_audit"]["source_level_absent_count"] > 0
    assert "transactions_only_with_level_fred_mapping_count" in summary["source_series_audit"]
    assert summary["latest_snapshot_summary"]["latest_common_quarter"] == latest["date"].iloc[0].date().isoformat()
    assert summary["history_preserving_backfill"]["history_preserving_backfill_rows"] > 0
    assert summary["history_preserving_backfill"]["release_window_override_rows"] > 0
    assert summary["reconciliation_diagnostics"]["formula_rows_checked"] > 0
    assert summary["reconciliation_diagnostics"]["parent_rows_checked"] > 0
    assert summary["reconciliation_diagnostics"]["formula_rows_failing"] == 0
    assert summary["reconciliation_diagnostics"]["parent_rows_failing"] == 0
    assert summary["validation"]["required_sector_estimates_complete"] is True
    assert summary["validation"]["canonical_atomic_sector_dates_unique"] is True
    assert summary["validation"]["latest_snapshot_sector_dates_unique"] is True
    assert summary["validation"]["required_estimate_coverage_ratio_bounded"] is True
    assert summary["validation"]["formula_reconciliation_passes"] is True
    assert summary["validation"]["parent_child_reconciliation_passes"] is True
    assert set(inventory["sector_key"].astype(str)) == required_atomic
    assert inventory["release_window_promotion_eligible"].all()
    assert {
        "level_rows_available",
        "transactions_rows_available",
        "revaluation_rows_available",
        "bills_rows_available",
        "level_source_code",
        "level_fred_id",
        "transactions_source_code",
        "transactions_fred_id",
        "source_level_code_present",
        "source_transactions_code_present",
        "source_level_status",
        "same_base_source_codes",
    }.issubset(inventory.columns)
    fed_inventory = inventory[inventory["sector_key"] == "fed"].iloc[0]
    assert fed_inventory["source_level_status"] == "present"
    assert isinstance(fed_inventory["level_fred_id"], str) and fed_inventory["level_fred_id"]
    life_inventory = inventory[inventory["sector_key"] == "life_insurers"].iloc[0]
    assert life_inventory["source_level_status"] == "absent"
    promoted_inventory = inventory[inventory["release_window_override_rows"] > 0]
    assert not promoted_inventory.empty


def test_classify_source_level_status_prefers_transactions_only_when_level_is_missing():
    module = importlib.import_module("treasury_sector_maturity.full_coverage_release")
    classifier = getattr(module, "_classify_source_level_status")

    assert (
        classifier(
            level_source_code="FL543061105.Q",
            transactions_source_code="FU543061105.Q",
            level_code_present=False,
            transactions_code_present=True,
            same_base_codes=["FU543061105.Q"],
        )
        == "transactions_only"
    )


def test_supplement_missing_z1_levels_from_fred_adds_configured_required_level_series(monkeypatch):
    module = importlib.import_module("treasury_sector_maturity.full_coverage_release")
    helper = getattr(module, "_supplement_missing_z1_levels_from_fred")

    long_df = pd.DataFrame(
        {
            "series_code": ["FU763061100.Q"],
            "date": [pd.Timestamp("2025-12-31")],
            "value": [5.0],
        }
    )
    catalog = load_series_catalog(FULL_CATALOG)
    observed_codes = set(long_df["series_code"].astype(str))
    assert "FL763061100.Q" not in observed_codes

    calls: list[str] = []

    def fake_fetch(series_id: str):
        calls.append(series_id)
        return {"series_id": series_id}

    def fake_normalize(payload, value_name="value", frequency_suffix=None):
        return pd.DataFrame({"date": [pd.Timestamp("2025-12-31")], value_name: [42.0]})

    monkeypatch.setattr(module, "fetch_fred_series_observations", fake_fetch)
    monkeypatch.setattr(module, "normalize_fred_observations", fake_normalize)

    supplemented, summary = helper(
        long_df=long_df,
        catalog=catalog,
        sector_defs_path=FULL_SECTOR_DEFS,
    )

    supplemented_codes = set(supplemented["series_code"].astype(str))
    assert "FL763061100.Q" in supplemented_codes
    assert summary["supplemented_series_count"] >= 1
    assert "bank_us_chartered" in summary["supplemented_sector_keys"]
    assert "BOGZ1FL763061100Q" in calls


def test_full_coverage_release_summary_tracks_required_sector_history_spans(tmp_path):
    module = importlib.import_module("treasury_sector_maturity.full_coverage_release")
    builder = getattr(module, "build_full_coverage_release")

    out_dir = tmp_path / "full_coverage_release"
    builder(
        out_dir=out_dir,
        source_provider="fed",
        coverage_scope="full",
        z1_file=TOY_Z1,
        h15_file=TOY_H15,
        curve_file=[f"tips_real_yield_constant_maturity={TOY_TIPS}"],
        soma_file=TOY_SOMA,
        foreign_shl_file=TOY_SHL,
        foreign_slt_file=TOY_SLT,
        bank_constraint_file=TOY_BANK_CONSTRAINTS,
        series_catalog=FULL_CATALOG,
        sector_defs=FULL_SECTOR_DEFS,
        model_config="configs/model_defaults.yaml",
        series_config="configs/h15_series.yaml",
        bank_constraints_config="configs/bank_constraints.yaml",
        summary_json_out=out_dir / "full_coverage_summary.json",
    )

    summary = json.loads((out_dir / "full_coverage_summary.json").read_text(encoding="utf-8"))
    sector_panel = _build_full_scope_toy_sector_panel()
    canonical = pd.read_csv(out_dir / "canonical_atomic_sector_maturity.csv", parse_dates=["date"])

    history_spans = {row["sector_key"]: row for row in summary["history_spans"]}
    required_atomic = set(required_full_coverage_sector_keys(FULL_CATALOG.parent / "coverage_registry.yaml")) & set(
        canonical_atomic_sector_keys(FULL_CATALOG.parent / "coverage_registry.yaml")
    )
    for sector_key in required_atomic:
        sector_rows = sector_panel[(sector_panel["sector_key"] == sector_key) & sector_panel["level"].notna()]
        if sector_rows.empty:
            continue
        assert sector_key in history_spans
        span = history_spans[sector_key]
        assert span["included"] is True
        assert span["date_start"] == pd.Timestamp(sector_rows["date"].min()).date().isoformat()
        assert span["date_end"] == pd.Timestamp(sector_rows["date"].max()).date().isoformat()


def test_full_coverage_release_raises_when_required_sector_estimates_remain_missing(tmp_path, monkeypatch):
    module = importlib.import_module("treasury_sector_maturity.full_coverage_release")
    builder = getattr(module, "build_full_coverage_release")
    original_estimator = getattr(module, "estimate_effective_maturity_panel")

    def broken_estimator(*args, **kwargs):
        estimated = original_estimator(*args, **kwargs)
        sector_mask = estimated["sector_key"].eq("fed")
        for column in [
            "bill_share",
            "effective_duration_years",
            "zero_coupon_equivalent_years",
            "estimand_class",
            "estimator_family",
            "selection_reason",
            "level_evidence_tier",
            "maturity_evidence_tier",
            "anchor_type",
            "concept_match",
            "coverage_ratio",
        ]:
            if column in estimated.columns:
                estimated.loc[sector_mask, column] = pd.NA
        if "high_confidence_flag" in estimated.columns:
            estimated.loc[sector_mask, "high_confidence_flag"] = False
        return estimated

    monkeypatch.setattr(module, "estimate_effective_maturity_panel", broken_estimator)

    out_dir = tmp_path / "full_coverage_release"
    try:
        builder(
            out_dir=out_dir,
            source_provider="fed",
            coverage_scope="full",
            z1_file=TOY_Z1,
            h15_file=TOY_H15,
            curve_file=[f"tips_real_yield_constant_maturity={TOY_TIPS}"],
            soma_file=TOY_SOMA,
            foreign_shl_file=TOY_SHL,
            foreign_slt_file=TOY_SLT,
            bank_constraint_file=TOY_BANK_CONSTRAINTS,
            series_catalog=FULL_CATALOG,
            sector_defs=FULL_SECTOR_DEFS,
            model_config="configs/model_defaults.yaml",
            series_config="configs/h15_series.yaml",
            bank_constraints_config="configs/bank_constraints.yaml",
            summary_json_out=out_dir / "full_coverage_summary.json",
        )
        raise AssertionError("expected the full-coverage builder to fail when required-sector estimates remain missing")
    except ValueError as exc:
        assert "required_sector_estimates_complete" in str(exc)


def test_full_coverage_release_benchmark_builder_does_not_inject_toy_curve_fallbacks(monkeypatch):
    module = importlib.import_module("treasury_sector_maturity.full_coverage_release")

    captured: dict[str, object] = {}

    def fake_build_estimation_benchmark_blocks(**kwargs):
        captured.update(kwargs)
        return pd.DataFrame({"date": pd.to_datetime(["2025-12-31"]), "1y": [0.0]}), None

    monkeypatch.setattr(module, "build_estimation_benchmark_blocks", fake_build_estimation_benchmark_blocks)

    benchmark, factor_benchmark = module._build_benchmark_blocks(
        h15_file=None,
        curve_file_overrides=None,
        source_provider="auto",
        model_cfg={"estimation": {"holdings_benchmark_families": ["nominal_treasury_constant_maturity"]}},
        series_config="configs/h15_series.yaml",
    )

    assert factor_benchmark is None
    assert "curve_file_overrides" in captured
    assert captured["curve_file_overrides"] == {}
    assert not benchmark.empty


def test_full_coverage_release_bank_constraints_do_not_fall_back_to_toy_panel():
    module = importlib.import_module("treasury_sector_maturity.full_coverage_release")

    artifacts: dict[str, object] = {}
    providers: dict[str, str] = {}
    intermediate: dict[str, object] = {}

    panel = module._build_bank_constraints(
        bank_constraint_file=None,
        ffiec_file=None,
        ffiec002_file=None,
        ncua_file=None,
        bank_supplement_file=None,
        bank_constraints_config="configs/bank_constraints.yaml",
        source_artifacts=artifacts,
        provider_summary=providers,
        intermediate_artifacts=intermediate,
    )

    assert panel.empty
    assert providers["bank_constraints"] == "unavailable"
