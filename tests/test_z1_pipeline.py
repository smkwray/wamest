from pathlib import Path

import pandas as pd

from treasury_sector_maturity.estimation import attach_revaluation_returns
from treasury_sector_maturity.z1 import (
    build_sector_panel,
    compute_identity_errors,
    expand_base_code,
    load_series_catalog,
    materialize_series_panel,
    parse_z1_ddp_csv,
)


ROOT = Path(__file__).resolve().parents[1]


def test_expand_base_code():
    codes = expand_base_code("713061103")
    assert codes["level"] == "FL713061103.Q"
    assert codes["transactions"] == "FU713061103.Q"
    assert codes["revaluation"] == "FR713061103.Q"


def test_build_sector_panel_from_toy():
    long_df = parse_z1_ddp_csv(ROOT / "data" / "examples" / "toy_z1_selected_series.csv")
    catalog = load_series_catalog(ROOT / "configs" / "z1_series_catalog.yaml")
    series_panel = materialize_series_panel(long_df, catalog)
    series_panel = compute_identity_errors(series_panel)

    sector_panel = build_sector_panel(series_panel, ROOT / "configs" / "sector_definitions.yaml")
    sector_panel = compute_identity_errors(sector_panel.rename(columns={"sector_key": "series_key"})).rename(
        columns={"series_key": "sector_key"}
    )
    sector_panel = attach_revaluation_returns(sector_panel, group_col="sector_key")

    fed = sector_panel[sector_panel["sector_key"] == "fed"]
    assert not fed.empty
    assert fed["bill_share_observed"].notna().any()

    resid = sector_panel[sector_panel["sector_key"] == "domestic_nonbank_residual_broad"]
    assert not resid.empty
    assert resid["level"].notna().all()


def test_build_full_coverage_sector_panel_from_toy_scaffold():
    long_df = parse_z1_ddp_csv(ROOT / "data" / "examples" / "toy_z1_selected_series.csv")
    catalog = load_series_catalog(ROOT / "configs" / "z1_series_catalog_full.yaml")
    series_panel = materialize_series_panel(long_df, catalog)
    series_panel = compute_identity_errors(series_panel)

    sector_panel = build_sector_panel(series_panel, ROOT / "configs" / "sector_definitions_full.yaml")

    assert "money_market_funds" in set(sector_panel["sector_key"])
    assert "life_insurers" in set(sector_panel["sector_key"])
    assert "security_brokers_and_dealers" in set(sector_panel["sector_key"])
    assert "discrepancy" in set(sector_panel["sector_key"])

    mmf = sector_panel[sector_panel["sector_key"] == "money_market_funds"].iloc[0]
    discrepancy = sector_panel[sector_panel["sector_key"] == "discrepancy"].iloc[0]

    assert mmf["required_for_full_coverage"]
    assert mmf["node_type"] == "atomic"
    assert discrepancy["node_type"] == "rollup"
