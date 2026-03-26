.PHONY: test toy public-preview public-preview-contract full-coverage-release full-coverage-contract release-check clean-release-hygiene

PYTHON ?= python3
PUBLIC_PREVIEW_DIR ?= outputs/public_preview
PUBLIC_PREVIEW_SOURCE_PROVIDER ?= fed
PUBLIC_PREVIEW_ARGS ?=
FULL_COVERAGE_RELEASE_DIR ?= outputs/full_coverage_release
FULL_COVERAGE_RELEASE_SOURCE_PROVIDER ?= auto
FULL_COVERAGE_RELEASE_ARGS ?=
FULL_COVERAGE_Z1_FILE ?= data/examples/toy_z1_selected_series.csv
FULL_COVERAGE_H15_FILE ?= data/examples/toy_h15_curves.csv
FULL_COVERAGE_TIPS_FILE ?= data/examples/toy_tips_real_yields.csv
FULL_COVERAGE_SOMA_FILE ?= data/examples/toy_soma_holdings.csv
FULL_COVERAGE_FOREIGN_SHL_FILE ?= data/examples/toy_shl_issue_mix.csv
FULL_COVERAGE_FOREIGN_SLT_FILE ?= data/examples/toy_slt_short_long.csv
FULL_COVERAGE_BANK_CONSTRAINT_FILE ?= data/examples/toy_bank_constraint_panel.csv
FULL_COVERAGE_SUMMARY_JSON ?= $(FULL_COVERAGE_RELEASE_DIR)/full_coverage_summary.json
FULL_COVERAGE_RELEASE_CONFIG ?= configs/full_coverage_release.yaml
FULL_COVERAGE_ARGS ?=

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

full-coverage-release: release-check
	$(PYTHON) -B scripts/build_full_coverage_release.py \
		--out-dir "$(FULL_COVERAGE_RELEASE_DIR)" \
		--coverage-scope full \
		--source-provider "$(FULL_COVERAGE_RELEASE_SOURCE_PROVIDER)" \
		--release-config "$(FULL_COVERAGE_RELEASE_CONFIG)" \
		--supplement-missing-z1-levels-from-fred \
		--summary-json-out "$(FULL_COVERAGE_SUMMARY_JSON)" \
		$(FULL_COVERAGE_RELEASE_ARGS)

full-coverage-contract: release-check
	$(PYTHON) -B scripts/build_full_coverage_release.py \
		--out-dir "$(FULL_COVERAGE_RELEASE_DIR)" \
		--coverage-scope full \
		--source-provider fed \
		--z1-file "$(FULL_COVERAGE_Z1_FILE)" \
		--h15-file "$(FULL_COVERAGE_H15_FILE)" \
		--curve-file "tips_real_yield_constant_maturity=$(FULL_COVERAGE_TIPS_FILE)" \
		--soma-file "$(FULL_COVERAGE_SOMA_FILE)" \
		--foreign-shl-file "$(FULL_COVERAGE_FOREIGN_SHL_FILE)" \
		--foreign-slt-file "$(FULL_COVERAGE_FOREIGN_SLT_FILE)" \
		--bank-constraint-file "$(FULL_COVERAGE_BANK_CONSTRAINT_FILE)" \
		--series-catalog "configs/z1_series_catalog_full.yaml" \
		--sector-defs "configs/sector_definitions_full.yaml" \
		--model-config "configs/model_defaults.yaml" \
		--series-config "configs/h15_series.yaml" \
		--bank-constraints-config "configs/bank_constraints.yaml" \
		--release-config "$(FULL_COVERAGE_RELEASE_CONFIG)" \
		--summary-json-out "$(FULL_COVERAGE_SUMMARY_JSON)" \
		$(FULL_COVERAGE_ARGS)

release-check:
	$(PYTHON) -B scripts/check_public_release_hygiene.py

clean-release-hygiene:
	rm -rf src/treasury_sector_maturity/__pycache__ src/treasury_sector_maturity.egg-info outputs/public_preview outputs/full_coverage_release
