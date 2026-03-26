from pathlib import Path

import pandas as pd

from treasury_sector_maturity.coverage import (
    attach_coverage_metadata,
    load_coverage_registry,
    optional_bank_sector_keys,
    preview_catalog_sector_keys,
    public_preview_sector_keys,
    resolve_estimation_scope,
    resolve_fed_calibration_scope,
    resolve_z1_build_scope,
    resolve_z1_fetch_provider,
)


ROOT = Path(__file__).resolve().parents[1]


def test_coverage_registry_tracks_preview_and_future_full_coverage_nodes():
    registry = load_coverage_registry(ROOT / "configs" / "coverage_registry.yaml")

    assert registry["fed"].included_in_public_preview_default is True
    assert registry["bank_us_affiliated_areas"].included_in_optional_bank_paths is True
    assert registry["money_market_funds"].required_for_full_coverage is True
    assert registry["money_market_funds"].included_in_public_preview_default is False
    assert registry["money_market_funds"].release_window_promotion_eligible is True
    assert registry["households_nonprofits"].release_window_promotion_eligible is False
    assert registry["foreigners_private"].is_canonical is False

    assert public_preview_sector_keys(ROOT / "configs" / "coverage_registry.yaml") == [
        "all_holders_total",
        "fed",
        "foreigners_total",
        "foreigners_official",
        "foreigners_private",
        "bank_us_chartered",
        "credit_unions_marketable_proxy",
        "households_nonprofits",
        "nonfinancial_corporates",
        "state_local_governments",
        "deposit_user_narrow_proxy",
        "domestic_nonbank_residual_broad",
    ]
    assert optional_bank_sector_keys(ROOT / "configs" / "coverage_registry.yaml") == [
        "bank_foreign_banking_offices_us",
        "bank_us_affiliated_areas",
        "bank_reserve_access_core",
        "bank_broad_private_depositories_marketable_proxy",
    ]
    assert preview_catalog_sector_keys(ROOT / "configs" / "coverage_registry.yaml")[-1] == (
        "bank_broad_private_depositories_marketable_proxy"
    )


def test_attach_coverage_metadata_adds_registry_fields():
    frame = pd.DataFrame(
        {
            "sector_key": ["fed", "money_market_funds"],
            "value": [1.0, 2.0],
        }
    )

    out = attach_coverage_metadata(frame, ROOT / "configs" / "coverage_registry.yaml")

    assert {"node_type", "required_for_full_coverage", "history_start_reason", "concept_risk", "release_window_promotion_eligible"}.issubset(
        out.columns
    )
    fed = out[out["sector_key"] == "fed"].iloc[0]
    mmf = out[out["sector_key"] == "money_market_funds"].iloc[0]

    assert fed["node_type"] == "atomic"
    assert bool(fed["required_for_full_coverage"]) is True
    assert "SOMA overlay" in str(fed["history_start_reason"])
    assert bool(fed["release_window_promotion_eligible"]) is True
    assert mmf["sector_family"] == "fund"
    assert "N-MFP" in str(mmf["history_start_reason"])


def test_z1_build_scope_helpers_keep_default_and_full_paths_separate():
    default_scope = resolve_z1_build_scope("default")
    full_scope = resolve_z1_build_scope("full")

    assert default_scope["catalog_path"] == "configs/z1_series_catalog.yaml"
    assert default_scope["sector_defs_path"] == "configs/sector_definitions.yaml"
    assert full_scope["catalog_path"] == "configs/z1_series_catalog_full.yaml"
    assert full_scope["sector_defs_path"] == "configs/sector_definitions_full.yaml"
    assert full_scope["sector_out"].endswith("_full.csv")

    assert resolve_z1_fetch_provider("default", "auto") == "auto"
    assert resolve_z1_fetch_provider("full", "auto") == "fed"


def test_full_scope_rejects_fred_provider_until_full_fred_mappings_exist():
    try:
        resolve_z1_fetch_provider("full", "fred")
    except ValueError as exc:
        assert "requires Fed Z.1 sourcing" in str(exc)
    else:
        raise AssertionError("Expected full coverage scope to reject the FRED provider.")


def test_estimation_and_calibration_scope_helpers_keep_full_outputs_separate():
    estimate_default = resolve_estimation_scope("default")
    estimate_full = resolve_estimation_scope("full")
    calibration_default = resolve_fed_calibration_scope("default")
    calibration_full = resolve_fed_calibration_scope("full")

    assert estimate_default["z1_panel"] == "data/interim/z1_sector_panel.csv"
    assert estimate_default["sector_defs_path"] == "configs/sector_definitions.yaml"
    assert estimate_full["z1_panel"] == "data/interim/z1_sector_panel_full.csv"
    assert estimate_full["sector_defs_path"] == "configs/sector_definitions_full.yaml"
    assert estimate_full["out"].endswith("_full.csv")

    assert calibration_default["z1_panel"] == "data/interim/z1_sector_panel.csv"
    assert calibration_full["z1_panel"] == "data/interim/z1_sector_panel_full.csv"
    assert calibration_full["interval_calibration_out"].endswith("_full.csv")
    assert calibration_full["summary_out"].endswith("_full.json")
