from pathlib import Path

from treasury_sector_maturity.coverage import (
    canonical_atomic_sector_keys,
    required_canonical_sector_keys,
    required_full_coverage_sector_keys,
)
from treasury_sector_maturity.utils import load_yaml
from treasury_sector_maturity.z1 import load_series_catalog


ROOT = Path(__file__).resolve().parents[1]


def test_full_coverage_sector_definitions_cover_required_registry_nodes():
    registry_required = set(required_full_coverage_sector_keys(ROOT / "configs" / "coverage_registry.yaml"))
    registry_atomic = set(canonical_atomic_sector_keys(ROOT / "configs" / "coverage_registry.yaml"))
    registry_required_canonical = set(required_canonical_sector_keys(ROOT / "configs" / "coverage_registry.yaml"))
    defs = load_yaml(ROOT / "configs" / "sector_definitions_full.yaml").get("sectors", {})

    assert registry_required.issubset(defs)
    assert registry_atomic.issubset(defs)
    assert registry_required_canonical.issubset(defs)


def test_full_coverage_sector_definitions_reference_known_series_or_formulas():
    defs = load_yaml(ROOT / "configs" / "sector_definitions_full.yaml").get("sectors", {})
    catalog = load_series_catalog(ROOT / "configs" / "z1_series_catalog_full.yaml")

    for sector_key, spec in defs.items():
        if "level_series" in spec:
            assert spec["level_series"] in catalog, sector_key
        else:
            assert "formula_level" in spec, sector_key

        bills_series = spec.get("bills_series")
        if bills_series is not None:
            assert bills_series in catalog, sector_key


def test_full_catalog_contains_key_l210_holder_series():
    catalog = load_series_catalog(ROOT / "configs" / "z1_series_catalog_full.yaml")

    for key in [
        "nonfinancial_noncorporate_treasuries",
        "property_casualty_insurers_treasuries",
        "life_insurers_treasuries",
        "private_defined_benefit_pension_treasuries",
        "money_market_funds_treasuries",
        "mutual_funds_treasuries",
        "closed_end_funds_treasuries",
        "exchange_traded_funds_treasuries",
        "government_sponsored_enterprises_treasuries",
        "asset_backed_securities_issuers_treasuries",
        "security_brokers_and_dealers_treasuries",
        "holding_companies_treasuries",
        "other_financial_business_treasuries",
        "discrepancy_treasuries",
    ]:
        assert key in catalog


def test_full_catalog_includes_level_fred_candidates_for_live_transactions_only_sectors():
    catalog = load_series_catalog(ROOT / "configs" / "z1_series_catalog_full.yaml")

    for key in [
        "property_casualty_insurers_treasuries",
        "life_insurers_treasuries",
        "private_defined_benefit_pension_treasuries",
        "private_defined_contribution_pension_treasuries",
        "federal_defined_benefit_pension_treasuries",
        "federal_defined_contribution_pension_treasuries",
        "state_local_employee_defined_benefit_pension_treasuries",
        "mutual_funds_treasuries",
        "closed_end_funds_treasuries",
        "exchange_traded_funds_treasuries",
        "government_sponsored_enterprises_treasuries",
        "security_brokers_and_dealers_treasuries",
        "holding_companies_treasuries",
        "nonfinancial_noncorporate_treasuries",
    ]:
        assert catalog[key].fred_ids.get("level")
