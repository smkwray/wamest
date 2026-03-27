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
    assert "level_measurement_basis" in canonical.columns
    assert "maturity_measurement_basis" in canonical.columns
    assert "coverage_measurement_basis" in canonical.columns
    assert "coverage_label" in canonical.columns
    assert "level_source_provider_used" in canonical.columns
    assert "level_supplemented_from_fred" in canonical.columns
    assert "effective_duration_status" in canonical.columns
    assert "point_estimate_origin" in canonical.columns
    assert "interval_origin" in canonical.columns
    assert "short_share_le_1y" in canonical.columns
    assert "coupon_share" in canonical.columns
    assert "tips_share" in canonical.columns
    assert "frn_share" in canonical.columns
    assert "coupon_only_maturity_years" in canonical.columns
    assert "bill_share_lower" in canonical.columns
    assert "bill_share_upper" in canonical.columns
    assert "short_share_le_1y_lower" in canonical.columns
    assert "short_share_le_1y_upper" in canonical.columns
    assert "effective_duration_years_lower" in canonical.columns
    assert "effective_duration_years_upper" in canonical.columns
    assert "zero_coupon_equivalent_years_lower" in canonical.columns
    assert "zero_coupon_equivalent_years_upper" in canonical.columns
    assert canonical["node_type"].eq("atomic").all()
    assert "node_type" in reconciliation.columns
    assert reconciliation["node_type"].ne("atomic").all()
    assert len(canonical[["date", "sector_key"]].drop_duplicates()) == len(canonical)
    assert len(latest[["date", "sector_key"]].drop_duplicates()) == len(latest)
    assert canonical["coverage_ratio"].isna().all()
    assert canonical["coverage_measurement_basis"].eq("qualitative_placeholder").all()
    assert canonical["effective_duration_status"].eq("not_separately_estimated").all()
    assert canonical["effective_duration_years"].isna().all()

    sector_panel = _build_full_scope_toy_sector_panel()
    registry_atomic = set(canonical_atomic_sector_keys(FULL_CATALOG.parent / "coverage_registry.yaml"))
    registry_required = set(required_full_coverage_sector_keys(FULL_CATALOG.parent / "coverage_registry.yaml"))
    required_atomic = registry_atomic & registry_required
    latest_by_sector = sector_panel.loc[
        sector_panel["sector_key"].isin(required_atomic) & sector_panel["level"].notna(),
        ["sector_key", "date"],
    ].groupby("sector_key")["date"].max()
    latest_quarter = pd.Timestamp(latest_by_sector.min())
    if pd.notna(latest_quarter):
        expected_latest_sectors = set(latest_by_sector.index.astype(str))
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
    assert "requested_end_date" in summary["release_summary"]
    assert "resolved_latest_snapshot_date" in summary["release_summary"]
    expected_required_covered = len(set(canonical["sector_key"].astype(str)) & required_atomic)
    assert summary["coverage_completeness"]["required_atomic_covered"] == expected_required_covered
    assert summary["coverage_completeness"]["missing_required_estimate_rows"] == 0
    assert summary["coverage_completeness"]["required_estimate_coverage_ratio"] <= 1.0
    assert summary["source_series_audit"]["required_sector_count"] == int(len(inventory))
    assert summary["source_series_audit"]["source_level_status_counts"]["present"] > 0
    assert summary["source_series_audit"]["source_level_absent_count"] > 0
    assert "post_supplement_level_status_counts" in summary["source_series_audit"]
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
        "post_supplement_level_code_present",
        "post_supplement_level_status",
        "same_base_source_codes",
        "latest_level_source_provider_used",
        "latest_level_supplemented_from_fred",
        "latest_point_estimate_origin",
        "latest_interval_origin",
    }.issubset(inventory.columns)
    fed_inventory = inventory[inventory["sector_key"] == "fed"].iloc[0]
    assert fed_inventory["source_level_status"] == "present"
    assert isinstance(fed_inventory["level_fred_id"], str) and fed_inventory["level_fred_id"]
    assert fed_inventory["latest_point_estimate_origin"] == "rolling_benchmark_weights_plus_factors"
    life_inventory = inventory[inventory["sector_key"] == "life_insurers"].iloc[0]
    assert life_inventory["source_level_status"] == "absent"
    promoted_inventory = inventory[inventory["release_window_override_rows"] > 0]
    assert not promoted_inventory.empty
    fed_row = canonical[canonical["sector_key"] == "fed"].iloc[0]
    assert fed_row["anchor_type"] == "soma_calibration_context"
    assert fed_row["estimand_class"] == "soma_calibrated_duration_equivalent_inferred"
    assert fed_row["point_estimate_origin"] == "rolling_benchmark_weights_plus_factors"
    assert fed_row["interval_origin"] == "fed_soma_calibrated_uncertainty_band"


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
    assert any(row["sector_key"] == "bank_us_chartered" for row in summary["supplemented_level_rows"])


def test_resolve_latest_snapshot_quarter_uses_min_of_per_sector_latest_dates():
    module = importlib.import_module("treasury_sector_maturity.full_coverage_release")
    resolver = getattr(module, "_resolve_latest_snapshot_quarter")

    sector_panel = pd.DataFrame(
        {
            "sector_key": [
                "fed",
                "fed",
                "foreigners_total",
                "foreigners_total",
                "bank_us_chartered",
            ],
            "date": pd.to_datetime(
                [
                    "2025-09-30",
                    "2025-12-31",
                    "2025-09-30",
                    "2025-12-31",
                    "2025-09-30",
                ]
            ),
            "level": [1.0, 1.1, 2.0, 2.1, 3.0],
        }
    )

    latest = resolver(sector_panel, ["fed", "foreigners_total", "bank_us_chartered"])
    assert latest == pd.Timestamp("2025-09-30")


def test_apply_history_preserving_backfill_only_fills_leading_gaps():
    module = importlib.import_module("treasury_sector_maturity.full_coverage_release")
    helper = getattr(module, "_apply_history_preserving_backfill")

    frame = pd.DataFrame(
        {
            "date": pd.to_datetime(["2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31"]),
            "sector_key": ["fed", "fed", "fed", "fed"],
            "bill_share": [pd.NA, 0.10, pd.NA, 0.20],
            "effective_duration_years": [pd.NA, pd.NA, pd.NA, pd.NA],
            "zero_coupon_equivalent_years": [pd.NA, 1.0, pd.NA, 2.0],
            "selection_reason": ["base", "base", "base", "base"],
            "maturity_evidence_tier": ["B", "B", "B", "B"],
            "high_confidence_flag": [True, True, True, True],
        }
    )

    out = helper(frame)
    assert bool(out.loc[out["date"] == pd.Timestamp("2025-03-31"), "history_preserving_backfill"].iloc[0]) is True
    assert pd.isna(out.loc[out["date"] == pd.Timestamp("2025-09-30"), "bill_share"].iloc[0])
    assert pd.isna(out.loc[out["date"] == pd.Timestamp("2025-09-30"), "zero_coupon_equivalent_years"].iloc[0])
    assert bool(out.loc[out["date"] == pd.Timestamp("2025-09-30"), "history_preserving_backfill"].iloc[0]) is False


def test_full_coverage_release_supports_deterministic_fully_covered_supplemented_surface(tmp_path, monkeypatch):
    module = importlib.import_module("treasury_sector_maturity.full_coverage_release")
    builder = getattr(module, "build_full_coverage_release")

    registry_path = FULL_CATALOG.parent / "coverage_registry.yaml"
    required_atomic = sorted(
        set(required_full_coverage_sector_keys(registry_path)) & set(canonical_atomic_sector_keys(registry_path))
    )
    coverage_registry = module.load_coverage_registry(registry_path)
    sector_defs = module.load_yaml(FULL_SECTOR_DEFS).get("sectors") or {}
    catalog = load_series_catalog(FULL_CATALOG)
    long_df = parse_z1_ddp_csv(TOY_Z1)
    date = pd.Timestamp("2025-12-31")
    observed_codes = set(long_df["series_code"].dropna().astype(str))
    supplemented: set[str] = set()
    supplemented_long_df = long_df.copy()
    for sector_key in required_atomic:
        level_series_key = sector_defs.get(sector_key, {}).get("level_series")
        if not level_series_key:
            continue
        spec = catalog.get(level_series_key)
        if spec is None:
            continue
        level_code = getattr(spec, "level", None)
        fred_id = (getattr(spec, "fred_ids", None) or {}).get("level")
        if not level_code or not fred_id or str(level_code) in observed_codes:
            continue
        supplemented.add(sector_key)
        supplemented_long_df = pd.concat(
            [
                supplemented_long_df,
                pd.DataFrame(
                    {
                        "series_code": [str(level_code)],
                        "date": [date],
                        "value": [float(200 + len(supplemented))],
                    }
                ),
            ],
            ignore_index=True,
        )
        if len(supplemented) >= 3:
            break
    assert supplemented

    synthetic_sector_rows = []
    synthetic_estimated_rows = []
    for idx, sector_key in enumerate(required_atomic, start=1):
        node = coverage_registry[sector_key]
        synthetic_sector_rows.append(
            {
                "date": date,
                "sector_key": sector_key,
                "label": sector_defs.get(sector_key, {}).get("label", sector_key),
                "method_priority": "|".join(sector_defs.get(sector_key, {}).get("method_priority", [])),
                "warnings": "",
                "level": float(100 + idx),
                "transactions": float(idx),
                "revaluation": float(idx) / 10.0,
                "bills_level": float(idx) / 5.0,
                "node_type": node.node_type,
                "sector_family": node.sector_family,
                "required_for_full_coverage": node.required_for_full_coverage,
                "concept_risk": node.concept_risk,
                "history_start_reason": node.history_start_reason,
                "level_source_provider_used": "fred_level_supplement" if sector_key in supplemented else "fed_z1",
                "level_supplemented_from_fred": sector_key in supplemented,
                "revaluation_return": 0.001 * idx,
            }
        )
        synthetic_estimated_rows.append(
            {
                "date": date,
                "sector_key": sector_key,
                "node_type": "atomic",
                "sector_family": node.sector_family,
                "required_for_full_coverage": node.required_for_full_coverage,
                "concept_risk": node.concept_risk,
                "history_start_reason": node.history_start_reason,
                "estimand_class": "soma_calibrated_duration_equivalent_inferred" if sector_key == "fed" else "duration_equivalent_inferred",
                "estimator_family": "direct_level_plus_revaluation_inference",
                "selection_reason": "synthetic deterministic release test",
                "high_confidence_flag": sector_key == "fed",
                "release_window_override": False,
                "release_window_override_quarters": pd.NA,
                "level_evidence_tier": "A",
                "maturity_evidence_tier": "B" if sector_key == "fed" else "C",
                "anchor_type": "soma_calibration_context" if sector_key == "fed" else "direct_z1_revaluation",
                "concept_match": "calibrated" if sector_key == "fed" else "direct",
                "coverage_ratio": pd.NA,
                "coverage_measurement_basis": "qualitative_placeholder",
                "coverage_label": "observed_level_with_soma_calibration" if sector_key == "fed" else "observed_level_with_inferred_maturity",
                "level_source_provider_used": "fred_level_supplement" if sector_key in supplemented else "fed_z1",
                "level_supplemented_from_fred": sector_key in supplemented,
                "effective_duration_status": "not_separately_estimated",
                "point_estimate_origin": "rolling_benchmark_weights_plus_factors",
                "interval_origin": "fed_soma_calibrated_uncertainty_band",
                "bill_share": min(0.95, 0.01 * idx),
                "effective_duration_years": pd.NA,
                "zero_coupon_equivalent_years": float(1 + idx / 10.0),
                "method": "rolling_benchmark_weights_plus_factors",
                "window_obs": 4,
                "fit_rmse_window": 0.01,
            }
        )

    synthetic_sector_panel = pd.DataFrame(synthetic_sector_rows)
    synthetic_estimated = pd.DataFrame(synthetic_estimated_rows)

    def fake_build_sector_panel(**kwargs):
        return module._BuiltSectorInputs(
            raw_long_df=long_df,
            long_df=supplemented_long_df,
            series_panel=pd.DataFrame(),
            sector_panel=synthetic_sector_panel.copy(),
            catalog=catalog,
        )

    monkeypatch.setattr(module, "_build_sector_panel", fake_build_sector_panel)
    monkeypatch.setattr(module, "_build_benchmark_blocks", lambda **kwargs: (pd.DataFrame({"date": [date], "1y": [0.0]}), None))
    monkeypatch.setattr(module, "_build_fed_calibration", lambda **kwargs: ({"status": "ok", "interval_calibration": {"status": "empty"}}, pd.DataFrame()))
    monkeypatch.setattr(module, "_build_foreign_nowcast", lambda **kwargs: pd.DataFrame())
    monkeypatch.setattr(module, "_build_bank_constraints", lambda **kwargs: pd.DataFrame())
    monkeypatch.setattr(module, "estimate_effective_maturity_panel", lambda *args, **kwargs: synthetic_estimated.copy())
    monkeypatch.setattr(module, "_merge_promoted_release_estimates", lambda **kwargs: kwargs["estimated"])

    out_dir = tmp_path / "full_coverage_release_deterministic"
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

    canonical = pd.read_csv(out_dir / "canonical_atomic_sector_maturity.csv")
    latest = pd.read_csv(out_dir / "latest_atomic_sector_snapshot.csv")
    inventory = pd.read_csv(out_dir / "required_sector_inventory.csv")
    summary = json.loads((out_dir / "full_coverage_summary.json").read_text(encoding="utf-8"))

    assert summary["coverage_completeness"]["required_atomic_covered"] == len(required_atomic)
    assert summary["release_summary"]["latest_snapshot_row_count"] == len(required_atomic)
    assert len(canonical) == len(required_atomic)
    assert len(latest) == len(required_atomic)
    supplemented_rows = canonical[canonical["sector_key"].isin(supplemented)]
    assert supplemented_rows["level_supplemented_from_fred"].all()
    assert set(supplemented_rows["level_source_provider_used"]) == {"fred_level_supplement"}
    supplemented_inventory = inventory[inventory["sector_key"].isin(supplemented)]
    assert not supplemented_inventory["source_level_code_present"].any()
    assert supplemented_inventory["post_supplement_level_code_present"].all()
    assert supplemented_inventory["latest_level_supplemented_from_fred"].all()
    assert summary["source_series_audit"]["post_supplement_level_present_count"] > summary["source_series_audit"]["source_level_present_count"]


def test_build_fed_calibration_passes_factor_returns_through(monkeypatch, tmp_path):
    module = importlib.import_module("treasury_sector_maturity.full_coverage_release")

    captured: dict[str, pd.DataFrame | None] = {}

    monkeypatch.setattr(module, "load_h15_curve_file", lambda *args, **kwargs: pd.DataFrame({"date": [pd.Timestamp("2025-12-31")], "rate": [0.01]}))
    monkeypatch.setattr(module, "read_soma_holdings", lambda path: pd.DataFrame({"cusip": []}))
    monkeypatch.setattr(
        module,
        "summarize_soma_quarterly",
        lambda soma, curve_df: pd.DataFrame(
            {
                "date": [pd.Timestamp("2025-12-31")],
                "approx_modified_duration_years": [5.0],
                "exact_wam_years": [6.0],
                "bill_share": [0.2],
            }
        ),
    )
    monkeypatch.setattr(module, "write_table", lambda frame, path: None)
    monkeypatch.setattr(module, "dump_json", lambda payload, path: None)
    monkeypatch.setattr(
        module,
        "resolve_fed_calibration_scope",
        lambda scope: {
            "exact_out": tmp_path / "exact.csv",
            "interval_calibration_out": tmp_path / "interval.csv",
            "summary_out": tmp_path / "summary.json",
        },
    )

    def fake_calibrate(*args, factor_returns=None, **kwargs):
        captured["summary_factor_returns"] = factor_returns
        return {"status": "ok"}

    def fake_interval(*args, factor_returns=None, **kwargs):
        captured["interval_factor_returns"] = factor_returns
        return pd.DataFrame({"date": [pd.Timestamp("2025-12-31")]})

    monkeypatch.setattr(module, "calibrate_fed_revaluation_mapping", fake_calibrate)
    monkeypatch.setattr(module, "build_fed_interval_calibration", fake_interval)
    monkeypatch.setattr(module, "summarize_interval_calibration", lambda interval_calibration, settings=None: {"status": "ok"})

    factor_benchmark = pd.DataFrame({"date": [pd.Timestamp("2025-12-31")], "kr_5y": [0.01]})
    summary, interval_calibration = module._build_fed_calibration(
        sector_panel=pd.DataFrame(
            {
                "sector_key": ["fed"],
                "date": [pd.Timestamp("2025-12-31")],
                "revaluation_return": [0.001],
            }
        ),
        benchmark=pd.DataFrame({"date": [pd.Timestamp("2025-12-31")], "1y": [0.0]}),
        factor_benchmark=factor_benchmark,
        settings=module.EstimationSettings(),
        interval_cfg={},
        source_provider="fed",
        series_config="configs/h15_series.yaml",
        h15_file=TOY_H15,
        soma_file=TOY_SOMA,
        source_artifacts={},
        provider_summary={},
        intermediate_artifacts={},
    )

    assert summary["status"] == "ok"
    assert not interval_calibration.empty
    pd.testing.assert_frame_equal(captured["summary_factor_returns"], factor_benchmark)
    pd.testing.assert_frame_equal(captured["interval_factor_returns"], factor_benchmark)


def test_required_sector_inventory_latest_fields_use_latest_canonical_row():
    module = importlib.import_module("treasury_sector_maturity.full_coverage_release")
    inventory_builder = getattr(module, "_build_required_sector_inventory")

    coverage_registry = module.load_coverage_registry(FULL_CATALOG.parent / "coverage_registry.yaml")
    sector_defs = module.load_yaml(FULL_SECTOR_DEFS).get("sectors") or {}
    catalog = load_series_catalog(FULL_CATALOG)

    canonical_atomic = pd.DataFrame(
        {
            "date": pd.to_datetime(["2025-09-30", "2025-12-31"]),
            "sector_key": ["fed", "fed"],
            "estimand_class": ["old_estimand", "new_estimand"],
            "estimator_family": ["old_family", "new_family"],
            "level_source_provider_used": ["fed_z1", "fred_level_supplement"],
            "level_supplemented_from_fred": [False, True],
            "point_estimate_origin": ["rolling_benchmark_weights", "rolling_benchmark_weights_plus_factors"],
            "interval_origin": ["old_interval", "new_interval"],
            "history_preserving_backfill": [False, False],
            "release_window_override": [False, False],
        }
    )
    sector_panel = pd.DataFrame(
        {
            "date": pd.to_datetime(["2025-09-30", "2025-12-31"]),
            "sector_key": ["fed", "fed"],
            "level": [1.0, 2.0],
            "transactions": [0.1, 0.2],
            "revaluation": [0.01, 0.02],
            "bills_level": [0.3, 0.4],
        }
    )

    inventory = inventory_builder(
        canonical_atomic=canonical_atomic,
        sector_panel=sector_panel,
        sector_definitions=sector_defs,
        coverage_registry=coverage_registry,
        catalog=catalog,
        raw_long_df=pd.DataFrame(columns=["series_code", "date", "value"]),
        long_df=pd.DataFrame(columns=["series_code", "date", "value"]),
    )

    fed_row = inventory[inventory["sector_key"] == "fed"].iloc[0]
    assert fed_row["latest_estimand_class"] == "new_estimand"
    assert fed_row["latest_estimator_family"] == "new_family"
    assert fed_row["latest_level_source_provider_used"] == "fred_level_supplement"
    assert bool(fed_row["latest_level_supplemented_from_fred"]) is True
    assert fed_row["latest_point_estimate_origin"] == "rolling_benchmark_weights_plus_factors"
    assert fed_row["latest_interval_origin"] == "new_interval"


def test_full_coverage_release_summary_distinguishes_requested_end_date_from_resolved_snapshot(tmp_path):
    module = importlib.import_module("treasury_sector_maturity.full_coverage_release")
    builder = getattr(module, "build_full_coverage_release")

    out_dir = tmp_path / "full_coverage_release_requested_vs_resolved"
    builder(
        out_dir=out_dir,
        source_provider="fed",
        coverage_scope="full",
        end_date="2026-12-31",
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
    report = (out_dir / "full_coverage_report.md").read_text(encoding="utf-8")

    assert summary["release_summary"]["requested_end_date"] == "2026-12-31"
    assert summary["release_summary"]["resolved_latest_snapshot_date"] == "2025-12-31"
    assert "Requested end date: `2026-12-31`" in report
    assert "Resolved latest snapshot date: `2025-12-31`" in report


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
