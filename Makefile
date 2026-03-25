.PHONY: test toy public-preview public-preview-contract release-check clean-release-hygiene

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

public-preview-contract:
	$(PYTHON) -B scripts/build_public_release_report.py \
		--out-dir "$(PUBLIC_PREVIEW_DIR)" \
		--source-provider fed \
		--end-date 2025-12-31 \
		--quarters 4 \
		--summary-json-out "$(PUBLIC_PREVIEW_DIR)/public_release_summary.json" \
		--z1-file data/examples/toy_z1_selected_series.csv \
		--h15-file data/examples/toy_h15_curves.csv \
		--soma-file data/examples/toy_soma_holdings.csv \
		--foreign-shl-file data/examples/toy_shl_issue_mix.csv \
		--foreign-slt-file data/examples/toy_slt_short_long.csv \
		--bank-constraint-file data/examples/toy_bank_constraint_panel.csv

release-check:
	$(PYTHON) -B scripts/check_public_release_hygiene.py

clean-release-hygiene:
	rm -rf src/treasury_sector_maturity/__pycache__ src/treasury_sector_maturity.egg-info outputs/public_preview
