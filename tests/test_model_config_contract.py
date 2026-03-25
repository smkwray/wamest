from pathlib import Path

from treasury_sector_maturity.utils import load_yaml


ROOT = Path(__file__).resolve().parents[1]


def test_model_defaults_uses_hybrid_research_contract():
    cfg = load_yaml(ROOT / "configs" / "model_defaults.yaml")
    estimation = cfg["estimation"]

    assert estimation["holdings_benchmark_families"] == [
        "nominal_treasury_constant_maturity",
        "tips_real_yield_constant_maturity",
        "frn_proxy_from_nominal",
    ]
    assert estimation["factor_benchmark_families"] == ["key_rate_buckets_from_nominal"]


def test_public_preview_model_config_stays_nominal_only():
    cfg = load_yaml(ROOT / "configs" / "model_public_preview.yaml")
    estimation = cfg["estimation"]

    assert estimation["holdings_benchmark_families"] == ["nominal_treasury_constant_maturity"]
    assert estimation["factor_benchmark_families"] == []
