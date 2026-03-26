from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
FULL_RELEASE_SCRIPT = ROOT / "scripts" / "build_full_coverage_release.py"
MAKEFILE = ROOT / "Makefile"


def _full_release_surface_available() -> bool:
    if not FULL_RELEASE_SCRIPT.exists():
        return False
    return importlib.util.find_spec("treasury_sector_maturity.full_coverage_release") is not None


def _makefile_has_full_targets() -> bool:
    if not MAKEFILE.exists():
        return False
    text = MAKEFILE.read_text(encoding="utf-8")
    return "full-coverage-release" in text and "full-coverage-contract" in text


@pytest.mark.xfail(not _full_release_surface_available(), reason="full-coverage release surface is not implemented yet")
def test_full_coverage_release_builder_surface_is_exposed():
    assert FULL_RELEASE_SCRIPT.exists()

    help_text = subprocess.run(
        [sys.executable, "-B", str(FULL_RELEASE_SCRIPT), "--help"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout

    assert "--coverage-scope" in help_text
    assert "full" in help_text
    assert "full_coverage_report.md" in help_text
    assert "canonical_atomic_sector_maturity.csv" in help_text
    assert "latest_atomic_sector_snapshot.csv" in help_text
    assert "high_confidence_sector_maturity.csv" in help_text
    assert "reconciliation_nodes.csv" in help_text
    assert "required_sector_inventory.csv" in help_text
    assert "--supplement-missing-z1-levels-from-fred" in help_text


@pytest.mark.xfail(not _makefile_has_full_targets(), reason="Makefile does not yet expose full-coverage targets")
def test_makefile_declares_full_coverage_targets_and_commands():
    text = MAKEFILE.read_text(encoding="utf-8")

    assert "full-coverage-release" in text
    assert "full-coverage-contract" in text
    assert "scripts/build_full_coverage_release.py" in text
    assert "--coverage-scope full" in text
    assert '--source-provider "$(FULL_COVERAGE_RELEASE_SOURCE_PROVIDER)"' in text
    assert "--supplement-missing-z1-levels-from-fred" in text
    assert '--curve-file "tips_real_yield_constant_maturity=$(FULL_COVERAGE_TIPS_FILE)"' in text
    assert "outputs/full_coverage_release" in text
    assert "public-preview" in text
    assert "public-preview-contract" in text


@pytest.mark.xfail(not _full_release_surface_available(), reason="full-coverage release surface is not implemented yet")
def test_full_coverage_builder_contract_uses_expected_module_name():
    spec = importlib.util.find_spec("treasury_sector_maturity.full_coverage_release")
    assert spec is not None
    module = importlib.import_module("treasury_sector_maturity.full_coverage_release")

    builder = getattr(module, "build_full_coverage_release", None)
    assert callable(builder)
