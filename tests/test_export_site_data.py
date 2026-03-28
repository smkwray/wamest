import importlib.util
import json
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]


def _load_export_module():
    path = ROOT / "scripts" / "export_site_data.py"
    spec = importlib.util.spec_from_file_location("export_site_data", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_export_site_data_exposes_provenance_and_low_information_flags(tmp_path):
    module = _load_export_module()
    fc_dir = tmp_path / "full_coverage_release"
    out = tmp_path / "site_data.json"
    fc_dir.mkdir()

    canonical = pd.DataFrame(
        [
            {
                "date": "2025-12-31",
                "sector_key": "fed",
                "publication_status": "published_estimate",
                "bill_share": 0.06,
                "zero_coupon_equivalent_years": 9.56,
                "short_share_le_1y": 0.32,
            }
        ]
    )
    snapshot = pd.DataFrame(
        [
            {
                "date": "2025-12-31",
                "sector_key": "fed",
                "bill_share": 0.06,
                "short_share_le_1y": 0.32,
                "effective_duration_years": None,
                "zero_coupon_equivalent_years": 9.56,
                "bill_share_lower": 0.05,
                "bill_share_upper": 0.07,
                "zero_coupon_equivalent_years_lower": 9.10,
                "zero_coupon_equivalent_years_upper": 10.05,
                "level_evidence_tier": "A",
                "maturity_evidence_tier": "A",
                "point_estimate_origin": "rolling_benchmark_weights_plus_factors",
                "interval_origin": "fed_soma_calibrated_uncertainty_band",
                "estimator_family": "direct_level_plus_revaluation_inference",
                "publication_status": "published_estimate",
                "high_confidence_flag": True,
                "window_obs": 8,
                "fit_rmse_window": 0.0036,
                "revaluation_signal_std_window": 0.012,
                "revaluation_signal_abs_max_window": 0.021,
                "revaluation_source_observed": True,
                "fallback_peer_group": "",
                "fallback_peer_count": None,
                "fallback_reason": "",
            },
            {
                "date": "2025-12-31",
                "sector_key": "holding_companies",
                "bill_share": 0.29,
                "short_share_le_1y": 0.29,
                "effective_duration_years": None,
                "zero_coupon_equivalent_years": 7.26,
                "bill_share_lower": 0.00,
                "bill_share_upper": 0.60,
                "zero_coupon_equivalent_years_lower": 3.20,
                "zero_coupon_equivalent_years_upper": 11.80,
                "level_evidence_tier": "A",
                "maturity_evidence_tier": "C",
                "point_estimate_origin": "rolling_benchmark_weights_plus_factors",
                "interval_origin": "fed_soma_calibrated_uncertainty_band",
                "estimator_family": "direct_level_plus_revaluation_inference",
                "publication_status": "published_estimate",
                "high_confidence_flag": False,
                "window_obs": 8,
                "fit_rmse_window": 0.0057,
                "revaluation_signal_std_window": 0.0,
                "revaluation_signal_abs_max_window": 0.0,
                "revaluation_source_observed": False,
                "fallback_peer_group": "other_financial",
                "fallback_peer_count": 3,
                "fallback_reason": "peer_group_envelope_for_low_confidence_interval",
            },
        ]
    )
    fed_overlay = pd.DataFrame(
        [
            {
                "date": "2025-12-31",
                "sector_key": "fed",
                "exact_wam_years": 8.87,
                "approx_modified_duration_years": 6.20,
                "bill_share": 0.173,
                "level": 4.1e12,
            }
        ]
    )
    fed_interval_calibration = pd.DataFrame(
        [
            {
                "date": "2025-09-30",
                "bill_share_abs_error": 0.0123,
                "zero_coupon_equivalent_years_abs_error": 0.4567,
            },
            {
                "date": "2025-12-31",
                "bill_share_abs_error": 0.0456,
                "zero_coupon_equivalent_years_abs_error": 0.8912,
            },
        ]
    )
    inventory = pd.DataFrame(
        [
            {
                "sector_key": "fed",
                "level_evidence_tier": "A",
                "maturity_evidence_tier": "A",
                "has_bills_series": True,
                "publication_range_start": "1945-12-31",
                "publication_range_end": "2025-12-31",
                "source_level_status": "present",
                "sector_family": "official",
                "concept_risk": "low",
                "source_revaluation_code_present": True,
                "source_bills_code_present": True,
            },
            {
                "sector_key": "holding_companies",
                "level_evidence_tier": "A",
                "maturity_evidence_tier": "C",
                "has_bills_series": False,
                "publication_range_start": "1945-12-31",
                "publication_range_end": "2025-12-31",
                "source_level_status": "present",
                "sector_family": "other_financial",
                "concept_risk": "high",
                "source_revaluation_code_present": False,
                "source_bills_code_present": False,
            }
        ]
    )
    summary = {
        "coverage_completeness": {
            "required_canonical_covered": 2,
            "required_canonical_total": 2,
        },
        "resolved_latest_snapshot_date": "2025-12-31",
        "schema_version": "v0.2-full-coverage",
    }

    canonical.to_csv(fc_dir / "canonical_sector_maturity.csv", index=False)
    snapshot.to_csv(fc_dir / "latest_sector_snapshot.csv", index=False)
    fed_overlay.to_csv(fc_dir / "fed_exact_overlay.csv", index=False)
    inventory.to_csv(fc_dir / "required_sector_inventory.csv", index=False)
    (fc_dir / "full_coverage_summary.json").write_text(json.dumps(summary))

    module.FC_DIR = fc_dir
    module.OUT = out
    module.FED_INTERVAL_CALIBRATION = tmp_path / "fed_interval_calibration_full.csv"
    fed_interval_calibration.to_csv(module.FED_INTERVAL_CALIBRATION, index=False)
    module.main()

    site_data = json.loads(out.read_text())
    fed = next(item for item in site_data["snapshot"] if item["sector_key"] == "fed")
    weak = next(item for item in site_data["snapshot"] if item["sector_key"] == "holding_companies")

    assert fed["method"] == "rolling_benchmark_weights_plus_factors"
    assert fed["point_estimate_origin"] == "rolling_benchmark_weights_plus_factors"
    assert fed["interval_origin"] == "fed_soma_calibrated_uncertainty_band"
    assert fed["publication_status"] == "published_estimate"
    assert fed["high_confidence"] is True
    assert fed["bill_share_low_information"] is False
    assert fed["maturity_low_identification"] is False
    assert fed["maturity_lower"] == 9.1
    assert fed["maturity_upper"] == 10.05
    assert fed["revaluation_source_observed"] is True
    assert fed["sector_family"] == "official"
    assert fed["source_revaluation_code_present"] is True

    assert weak["bill_share_interval_width"] == 0.6
    assert weak["bill_share_low_information"] is True
    assert weak["bill_share_low_information_reason"] == "bill_share_interval_width_ge_40pp"
    assert weak["maturity_low_identification"] is True
    assert weak["maturity_low_identification_reason"] == "revaluation_signal_near_zero"
    assert weak["fallback_peer_group"] == "other_financial"
    assert weak["fallback_peer_count"] == 3
    assert weak["fallback_reason"] == "peer_group_envelope_for_low_confidence_interval"
    assert weak["revaluation_source_observed"] is False
    assert weak["concept_risk"] == "high"
    assert weak["source_revaluation_code_present"] is False

    validation = site_data["validation"]["fed_calibration"]
    assert validation["dates"] == ["2025-09-30", "2025-12-31"]
    assert validation["bill_share_abs_error"] == [0.0123, 0.0456]
    assert validation["maturity_abs_error"] == [0.4567, 0.8912]
    assert validation["summary"]["bill_share_median_ae"] == 0.029
    assert validation["summary"]["bill_share_p90_ae"] == 0.0423
    assert validation["summary"]["bill_share_max_ae"] == 0.0456
    assert validation["summary"]["maturity_median_ae"] == 0.674
    assert validation["summary"]["maturity_p90_ae"] == 0.8478
    assert validation["summary"]["maturity_max_ae"] == 0.8912
