.PHONY: test toy public-preview release-check clean-release-hygiene

PYTHON ?= python3
PUBLIC_PREVIEW_DIR ?= outputs/public_preview
PUBLIC_PREVIEW_SOURCE_PROVIDER ?= fed
PUBLIC_PREVIEW_ARGS ?=

test:
	$(PYTHON) -B -m pytest -q

toy:
	$(PYTHON) -B scripts/run_toy_pipeline.py

public-preview:
	$(PYTHON) -B scripts/build_public_release_report.py --out-dir "$(PUBLIC_PREVIEW_DIR)" --source-provider "$(PUBLIC_PREVIEW_SOURCE_PROVIDER)" $(PUBLIC_PREVIEW_ARGS)

release-check:
	$(PYTHON) -B scripts/check_public_release_hygiene.py

clean-release-hygiene:
	rm -rf src/treasury_sector_maturity/__pycache__ src/treasury_sector_maturity.egg-info outputs/public_preview
