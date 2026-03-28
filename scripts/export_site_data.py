#!/usr/bin/env python3
"""Export full-coverage release artifacts into a compact JSON for the frontend site."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FC_DIR = PROJECT_ROOT / "outputs" / "full_coverage_release"
OUT = PROJECT_ROOT / "web" / "public" / "data" / "site_data.json"
FED_INTERVAL_CALIBRATION = PROJECT_ROOT / "data" / "processed" / "fed_interval_calibration_full.csv"

HUMAN_SECTOR_NAMES = {
    "fed": "Federal Reserve (SOMA)",
    "foreigners_total": "Foreign Holders (Total)",
    "foreigners_official": "Foreign Official",
    "foreigners_private": "Foreign Private",
    "bank_us_chartered": "U.S. Chartered Banks",
    "bank_foreign_banking_offices_us": "Foreign Banking Offices (U.S.)",
    "bank_reserve_access_core": "Reserve-Access Core",
    "bank_broad_private_depositories_marketable_proxy": "Broad Private Depositories",
    "bank_us_affiliated_areas": "U.S. Affiliated-Area Banks",
    "credit_unions_marketable_proxy": "Credit Unions",
    "households_nonprofits": "Households & Nonprofits",
    "nonfinancial_corporates": "Nonfinancial Corporates",
    "state_local_governments": "State & Local Governments",
    "deposit_user_narrow_proxy": "Deposit Users (Narrow)",
    "domestic_nonbank_residual_broad": "Domestic Non-Bank Residual",
    "all_holders_total": "All Holders (Total)",
    "mutual_funds": "Mutual Funds",
    "exchange_traded_funds": "Exchange-Traded Funds",
    "closed_end_funds": "Closed-End Funds",
    "life_insurers": "Life Insurers",
    "property_casualty_insurers": "Property & Casualty Insurers",
    "private_defined_benefit_pensions": "Private Defined-Benefit Pensions",
    "private_defined_contribution_pensions": "Private Defined-Contribution Pensions",
    "federal_defined_benefit_pensions": "Federal Defined-Benefit Pensions",
    "federal_defined_contribution_pensions": "Federal Defined-Contribution Pensions",
    "government_sponsored_enterprises": "Government-Sponsored Enterprises",
    "holding_companies": "Holding Companies",
    "security_brokers_and_dealers": "Security Brokers & Dealers",
    "nonfinancial_noncorporate_business": "Nonfinancial Noncorporate Business",
}

LOW_INFORMATION_BILL_SHARE_WIDTH = 0.40
LOW_SIGNAL_STD_THRESHOLD = 1e-6


def human_name(key: str) -> str:
    return HUMAN_SECTOR_NAMES.get(key, key.replace("_", " ").title())


def safe_val(v):
    if pd.isna(v):
        return None
    if isinstance(v, str) and v == "not_applicable":
        return None
    if isinstance(v, float):
        return round(v, 4)
    return v


def safe_bool(v) -> bool:
    if pd.isna(v):
        return False
    return bool(v)


def safe_text(v) -> str:
    if pd.isna(v) or v == "not_applicable":
        return ""
    return str(v)


def bill_share_interval_width(row: pd.Series) -> float | None:
    lower = pd.to_numeric(pd.Series([row.get("bill_share_lower")]), errors="coerce").iloc[0]
    upper = pd.to_numeric(pd.Series([row.get("bill_share_upper")]), errors="coerce").iloc[0]
    if pd.isna(lower) or pd.isna(upper):
        return None
    return float(upper - lower)


def bill_share_low_information(row: pd.Series) -> tuple[bool, str]:
    width = bill_share_interval_width(row)
    if width is None:
        return False, ""
    if width >= LOW_INFORMATION_BILL_SHARE_WIDTH:
        return True, f"bill_share_interval_width_ge_{int(LOW_INFORMATION_BILL_SHARE_WIDTH * 100)}pp"
    return False, ""


def maturity_low_identification(row: pd.Series, bill_share_low_info: bool) -> tuple[bool, str]:
    signal_std = pd.to_numeric(pd.Series([row.get("revaluation_signal_std_window")]), errors="coerce").iloc[0]
    if pd.notna(signal_std) and float(signal_std) <= LOW_SIGNAL_STD_THRESHOLD:
        return True, "revaluation_signal_near_zero"
    if bill_share_low_info and not safe_bool(row.get("high_confidence_flag", False)):
        return True, "wide_bill_share_interval_without_high_confidence"
    return False, ""


def _safe_quantile(values: pd.Series, q: float) -> float | None:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return None
    return safe_val(float(numeric.quantile(q)))


def build_validation_export() -> dict[str, object]:
    if not FED_INTERVAL_CALIBRATION.exists():
        return {"fed_calibration": {"dates": [], "bill_share_abs_error": [], "maturity_abs_error": [], "summary": {}}}

    calibration = pd.read_csv(FED_INTERVAL_CALIBRATION)
    required_cols = {"date", "bill_share_abs_error", "zero_coupon_equivalent_years_abs_error"}
    if not required_cols.issubset(calibration.columns):
        return {"fed_calibration": {"dates": [], "bill_share_abs_error": [], "maturity_abs_error": [], "summary": {}}}

    valid = calibration[
        calibration["bill_share_abs_error"].notna() & calibration["zero_coupon_equivalent_years_abs_error"].notna()
    ].copy()
    valid = valid.sort_values("date")

    bill_abs_error = pd.to_numeric(valid["bill_share_abs_error"], errors="coerce")
    maturity_abs_error = pd.to_numeric(valid["zero_coupon_equivalent_years_abs_error"], errors="coerce")

    return {
        "fed_calibration": {
            "dates": valid["date"].astype(str).tolist(),
            "bill_share_abs_error": [safe_val(v) for v in bill_abs_error],
            "maturity_abs_error": [safe_val(v) for v in maturity_abs_error],
            "summary": {
                "bill_share_median_ae": _safe_quantile(bill_abs_error, 0.5),
                "bill_share_p90_ae": _safe_quantile(bill_abs_error, 0.9),
                "bill_share_max_ae": safe_val(float(bill_abs_error.max())) if bill_abs_error.notna().any() else None,
                "maturity_median_ae": _safe_quantile(maturity_abs_error, 0.5),
                "maturity_p90_ae": _safe_quantile(maturity_abs_error, 0.9),
                "maturity_max_ae": safe_val(float(maturity_abs_error.max())) if maturity_abs_error.notna().any() else None,
            },
        }
    }


def main() -> None:
    if not FC_DIR.exists():
        print(f"Run 'make full-coverage-contract' first — {FC_DIR} not found", file=sys.stderr)
        sys.exit(1)

    canonical = pd.read_csv(FC_DIR / "canonical_sector_maturity.csv")
    snapshot = pd.read_csv(FC_DIR / "latest_sector_snapshot.csv")
    fed_overlay = pd.read_csv(FC_DIR / "fed_exact_overlay.csv")
    inventory = pd.read_csv(FC_DIR / "required_sector_inventory.csv")

    # Try loading a richer live SOMA overlay if available
    live_soma = Path("/tmp/wamest_live_fed_exact_overlay.csv")
    if live_soma.exists():
        fed_overlay_for_compare = pd.read_csv(live_soma)
        print(f"Using live SOMA overlay ({len(fed_overlay_for_compare)} rows)")
    else:
        fed_overlay_for_compare = fed_overlay
    summary = json.loads((FC_DIR / "full_coverage_summary.json").read_text())
    inventory_lookup = inventory.set_index("sector_key").to_dict(orient="index") if not inventory.empty else {}

    # --- Hero stats ---
    cc = summary.get("coverage_completeness", {})
    sectors_covered = f"{cc.get('required_canonical_covered', '?')}/{cc.get('required_canonical_total', '?')}"
    dates = sorted(canonical["date"].unique())
    quarter_count = len(dates)
    snapshot_quarter = summary.get("resolved_latest_snapshot_date", dates[-1] if dates else "")

    pub_rows = canonical[canonical.get("publication_status") == "published_estimate"] if "publication_status" in canonical.columns else canonical
    published_count = len(pub_rows)
    pub_quarter_count = len(sorted(pub_rows["date"].unique())) if not pub_rows.empty else quarter_count

    hero = {
        "sectors_covered": sectors_covered,
        "quarters": pub_quarter_count,
        "snapshot_quarter": snapshot_quarter,
        "published_rows": published_count,
        "data_sources": 6,
    }

    # --- Latest snapshot bar chart data ---
    snap_data = []
    for _, row in snapshot.iterrows():
        sk = row.get("sector_key", "")
        inventory_row = inventory_lookup.get(sk, {})
        interval_width = bill_share_interval_width(row)
        low_info_flag, low_info_reason = bill_share_low_information(row)
        maturity_low_id_flag, maturity_low_id_reason = maturity_low_identification(row, low_info_flag)
        snap_data.append({
            "sector": human_name(sk),
            "sector_key": sk,
            "bill_share": safe_val(row.get("bill_share")),
            "short_share": safe_val(row.get("short_share_le_1y")),
            "duration": safe_val(row.get("effective_duration_years")),
            "maturity": safe_val(row.get("zero_coupon_equivalent_years")),
            "bill_share_lower": safe_val(row.get("bill_share_lower")),
            "bill_share_upper": safe_val(row.get("bill_share_upper")),
            "maturity_lower": safe_val(row.get("zero_coupon_equivalent_years_lower")),
            "maturity_upper": safe_val(row.get("zero_coupon_equivalent_years_upper")),
            "bill_share_interval_width": safe_val(interval_width),
            "bill_share_low_information": low_info_flag,
            "bill_share_low_information_reason": low_info_reason,
            "window_obs": safe_val(row.get("window_obs")),
            "fit_rmse_window": safe_val(row.get("fit_rmse_window")),
            "revaluation_signal_std_window": safe_val(row.get("revaluation_signal_std_window")),
            "revaluation_signal_abs_max_window": safe_val(row.get("revaluation_signal_abs_max_window")),
            "revaluation_source_observed": safe_bool(
                inventory_row.get("source_revaluation_code_present", row.get("revaluation_source_observed", False))
            ),
            "maturity_low_identification": maturity_low_id_flag,
            "maturity_low_identification_reason": maturity_low_id_reason,
            "level_tier": row.get("level_evidence_tier", ""),
            "maturity_tier": row.get("maturity_evidence_tier", ""),
            "method": row.get("method") or row.get("point_estimate_origin") or row.get("estimator_family", ""),
            "estimator_family": row.get("estimator_family", ""),
            "point_estimate_origin": row.get("point_estimate_origin", ""),
            "interval_origin": row.get("interval_origin", ""),
            "fallback_peer_group": safe_text(row.get("fallback_peer_group", "")),
            "fallback_peer_count": safe_val(row.get("fallback_peer_count")),
            "fallback_reason": safe_text(row.get("fallback_reason", "")),
            "publication_status": row.get("publication_status", ""),
            "high_confidence": safe_bool(row.get("high_confidence_flag", False)),
            "sector_family": inventory_row.get("sector_family", ""),
            "concept_risk": inventory_row.get("concept_risk", ""),
            "source_revaluation_code_present": safe_bool(inventory_row.get("source_revaluation_code_present", False)),
            "source_bills_code_present": safe_bool(inventory_row.get("source_bills_code_present", False)),
        })

    # --- Time series for key sectors ---
    ts_sectors = sorted(canonical["sector_key"].unique())

    time_series = {}
    for sk in ts_sectors:
        sub = canonical[canonical["sector_key"] == sk].sort_values("date")
        pub = sub
        if "publication_status" in sub.columns:
            pub = sub[sub["publication_status"] == "published_estimate"]
        time_series[human_name(sk)] = {
            "dates": pub["date"].tolist(),
            "bill_share": [safe_val(v) for v in pub["bill_share"]],
            "maturity": [safe_val(v) for v in pub.get("zero_coupon_equivalent_years", [])],
            "short_share": [safe_val(v) for v in pub.get("short_share_le_1y", [])],
        }

    # --- Fed exact vs inferred comparison ---
    fed_canonical = canonical[
        (canonical["sector_key"] == "fed") &
        (canonical.get("publication_status", "published_estimate") == "published_estimate")
    ].sort_values("date") if "fed" in canonical["sector_key"].values else pd.DataFrame()

    fed_compare = {
        "dates": [],
        "inferred_maturity": [],
        "exact_maturity": [],
        "inferred_bill_share": [],
        "exact_bill_share": [],
    }

    if not fed_overlay_for_compare.empty:
        overlay = fed_overlay_for_compare.sort_values("date")
        ov_indexed = overlay.set_index("date")

        # For the exact overlay, use all available dates even if canonical is empty/zero
        if not fed_canonical.empty:
            can_indexed = fed_canonical.set_index("date")
            all_dates = sorted(set(can_indexed.index) | set(ov_indexed.index))
        else:
            can_indexed = pd.DataFrame()
            all_dates = sorted(ov_indexed.index)

        for d in all_dates:
            fed_compare["dates"].append(d)
            if not can_indexed.empty and d in can_indexed.index:
                fed_compare["inferred_maturity"].append(safe_val(can_indexed.loc[d].get("zero_coupon_equivalent_years")))
                fed_compare["inferred_bill_share"].append(safe_val(can_indexed.loc[d].get("bill_share")))
            else:
                fed_compare["inferred_maturity"].append(None)
                fed_compare["inferred_bill_share"].append(None)

            if d in ov_indexed.index:
                row_ov = ov_indexed.loc[d]
                # Use exact_wam_years or wam_years for maturity
                for col in ["exact_wam_years", "wam_years", "zero_coupon_equivalent_years"]:
                    if col in ov_indexed.columns:
                        fed_compare["exact_maturity"].append(safe_val(row_ov.get(col)))
                        break
                else:
                    fed_compare["exact_maturity"].append(None)
                fed_compare["exact_bill_share"].append(safe_val(row_ov.get("bill_share")))
            else:
                fed_compare["exact_maturity"].append(None)
                fed_compare["exact_bill_share"].append(None)

    # --- Evidence tier distribution ---
    tier_counts = {}
    if "maturity_evidence_tier" in snapshot.columns:
        for tier in sorted(snapshot["maturity_evidence_tier"].dropna().unique()):
            tier_counts[tier] = int((snapshot["maturity_evidence_tier"] == tier).sum())

    # --- Sector inventory summary ---
    inv_summary = []
    if not inventory.empty:
        for _, row in inventory.iterrows():
            sk = row.get("sector_key", "")
            inv_summary.append({
                "sector": human_name(sk),
                "sector_key": sk,
                "level_tier": row.get("level_evidence_tier", ""),
                "maturity_tier": row.get("maturity_evidence_tier", ""),
                "has_bills_series": bool(row.get("has_bills_series", False)),
                "publication_start": row.get("publication_range_start", ""),
                "publication_end": row.get("publication_range_end", ""),
                "source_level_status": row.get("source_level_status", ""),
            })

    # --- SOMA exact time series ---
    soma_ts = {"dates": [], "wam_years": [], "duration_years": [], "bill_share": [], "holdings_trillions": []}
    soma_src = fed_overlay_for_compare if not fed_overlay_for_compare.empty else fed_overlay
    if not soma_src.empty:
        for _, row in soma_src.sort_values("date").iterrows():
            soma_ts["dates"].append(row["date"])
            for col_try in ["exact_wam_years", "wam_years"]:
                if col_try in soma_src.columns:
                    soma_ts["wam_years"].append(safe_val(row.get(col_try)))
                    break
            else:
                soma_ts["wam_years"].append(None)
            for col_try in ["approx_modified_duration_years", "duration_years"]:
                if col_try in soma_src.columns:
                    soma_ts["duration_years"].append(safe_val(row.get(col_try)))
                    break
            else:
                soma_ts["duration_years"].append(None)
            soma_ts["bill_share"].append(safe_val(row.get("bill_share")))
            level = row.get("level", None)
            soma_ts["holdings_trillions"].append(round(level / 1e12, 3) if pd.notna(level) and level > 0 else None)

    # --- Assemble ---
    site_data = {
        "hero": hero,
        "snapshot": snap_data,
        "time_series": time_series,
        "fed_comparison": fed_compare,
        "soma_exact": soma_ts,
        "validation": build_validation_export(),
        "evidence_tiers": tier_counts,
        "inventory": inv_summary,
        "build_info": {
            "schema_version": summary.get("schema_version", ""),
            "snapshot_quarter": snapshot_quarter,
            "source": "full-coverage release" if summary.get("release_summary", {}).get("source_provider_requested") in ("auto", "fred") else "full-coverage contract build",
        },
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(site_data, indent=2))
    print(f"Wrote {OUT} ({OUT.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
