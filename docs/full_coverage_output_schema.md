# Full-Coverage Output Schema

This document defines the artifact contract for the separate `v0.2` full-coverage research release. It is distinct from the frozen `v0.1` public preview contract and may include weaker sectors, explicit publication-range metadata, and non-preview artifact names.

## Scope

The full-coverage path is a separate release surface for quarterly maturity estimation across the required canonical Z.1 holder sectors. The standard live workflow uses the full-scope Fed Z.1 build plus configured `fred_ids.level` supplements for missing required-sector level series, and keeps required sector/date rows in the main surface with explicit `publication_status` semantics instead of dropping them.

Canonical entrypoints:

- `make full-coverage-release` for the live Fed Z.1 build with the standard required-sector FRED level supplement
- `make full-coverage-contract` for the deterministic toy contract build
- `python3 -B scripts/build_full_coverage_release.py --coverage-scope full`

The release is designed for:

- full holder coverage across required canonical sectors
- a common required-sector/date scaffold with explicit publication-range endpoints and row-level `in_publication_range` flags
- explicit separation between required canonical sectors, non-canonical reconciliation nodes, and high-confidence subsets
- coverage-honest reporting rather than preview-minimal reporting

## Required artifacts

The full-coverage release writes these top-level artifacts:

1. `canonical_sector_maturity.csv`
2. `latest_sector_snapshot.csv`
3. `high_confidence_sector_maturity.csv`
4. `reconciliation_nodes.csv`
5. `fed_exact_overlay.csv`
6. `required_sector_inventory.csv`
7. `full_coverage_report.md`
8. `run_manifest.json`
9. `full_coverage_summary.json`

## Canonical panel

`canonical_sector_maturity.csv` is the main historical artifact.

Rules:

- contains one row per required canonical sector/date combination in the full-coverage release scope
- preserves each sector's actual `node_type`, including `atomic`, `proxy`, and `residual`
- includes weak sectors, partially identified sectors, and explicit status rows for sector/date combinations that lack a publishable maturity estimate
- uses the common required-sector/date scaffold rather than dropping out-of-range rows
- excludes non-canonical reconciliation or roll-up nodes
- retains the core maturity fields used in the preview path, and extends them with direct composition metrics, calibrated interval bands, and explicit measurement-basis fields
- includes `history_preserving_backfill` so leading warmup rows filled from the nearest available sector estimate remain explicit and easy to filter
- includes `publication_status` and `publication_status_reason` so every required row is explicit about whether it is a published estimate, a history-preserving backfill row, or a status-only row
- includes `in_publication_range` so the common-grid panel can be filtered to each sector's feasible publication span without losing audit visibility outside that span
- distinguishes `row_is_short_window_estimate` from `estimate_origin_includes_short_window_promotion`

Expected interpretation:

- point estimates are the best available estimates under the public-data stack
- calibrated uncertainty bands communicate uncertainty and weak identification
- `effective_duration_years` is only populated when a distinct duration map is actually supplied; otherwise `effective_duration_status` records that the metric is not separately estimated
- status-only rows are acceptable when a sector/date lacks a publishable maturity estimate under the current public-data stack
- the canonical panel is not truncated to the latest common quarter, but sector-level historical interpretation should follow `in_publication_range` plus the publication-range endpoints exported in the summary and inventory artifacts

## Latest snapshot

`latest_sector_snapshot.csv` is the quarter-aligned snapshot artifact.

Rules:

- one row per required canonical sector whose publication range reaches the resolved quarter
- the snapshot quarter is the minimum of the per-sector `latest_publication_date` values across required canonical sectors in the release build
- the row for each sector is taken from that common quarter
- sectors without a row at the resolved common quarter fail validation
- this artifact is a common-quarter cross-section companion to the canonical panel, not the canonical panel's history definition

## Fed exact overlay

`fed_exact_overlay.csv` is the direct SOMA-based Fed companion artifact.

Rules:

- contains only Fed rows with `sector_key == fed`
- is built from the already-produced quarterly SOMA exact metrics used during Fed calibration
- does not replace the canonical Fed row in `canonical_sector_maturity.csv`
- exists to expose the direct security-level Fed benchmark alongside the cross-sector-comparable inferred canonical panel

## High-confidence subset

`high_confidence_sector_maturity.csv` is a filtered view of the canonical panel.

Rules:

- exact row filter of the canonical panel where `high_confidence_flag == true`
- no re-estimation or separate contract
- may be empty for a date if no sector meets the high-confidence threshold

## Reconciliation nodes

`reconciliation_nodes.csv` captures the non-atomic nodes used for rollups, discrepancy accounting, and audit visibility.

Rules:

- contains only non-canonical nodes
- may include rollups, auxiliaries, and discrepancy objects
- exists to support transparency and hierarchy checks, not to expand the canonical panel

## Required sector inventory

`required_sector_inventory.csv` is the machine-readable audit artifact for the full-coverage surface.

Rules:

- contains one row per required canonical sector
- records the configured level / transactions / revaluation / bills source codes and any configured `fred_ids` fallbacks for each required sector where applicable
- records whether those configured source codes are present in the parsed Z.1 source file used for the release
- records whether the configured level code becomes available after the optional FRED level supplement used by the live full-coverage path
- records a compact `source_level_status` classification such as `present`, `transactions_only`, `same_base_other_only`, `absent`, or `computed_proxy`
- records any same-base source codes actually seen in the parsed Z.1 source file so catalog mismatches can be audited without re-scanning the raw release
- records the configured method-priority stack from `configs/sector_definitions_full.yaml`
- records whether a sector has a direct `bills_series`
- records whether the sector is explicitly eligible for release-window promotion in `configs/coverage_registry.yaml`
- records the publication-range span, underlying level/transactions/revaluation row availability, emitted `history_preserving_backfill` usage, short-window estimate/origin counts, and explicit historical `ever_*` usage flags from the canonical artifact
- distinguishes `latest_emitted_*` fields from `latest_published_*` fields so the common-grid terminal row is not conflated with the latest in-publication-range row
- records latest-row provenance fields such as point-estimate origin, uncertainty-band origin, and whether the latest published level path was supplemented from FRED

## Validation semantics

The full-coverage release should validate the following:

- every required canonical sector appears in the canonical panel for every date in its publication range
- every required canonical sector/date row carries a non-null `publication_status`
- the latest snapshot quarter is the minimum of the per-sector `latest_publication_date` values across required canonical sectors in the build
- the high-confidence subset is a strict filter of the canonical panel
- reconciliation nodes do not leak into the canonical artifact
- preview validation remains separate and unchanged

## Required summary content

`full_coverage_summary.json` should include:

- release metadata and schema version
- per-sector history spans and inclusion status
- `fed_exact_overlay_summary` with row count and date span
- required-sector completeness results
- source-series audit results, including raw parsed-Z.1 counts for sectors whose configured level code is present versus transactions-only or absent, plus post-supplement level-availability counts and the subset of transactions-only sectors that already have a configured level `fred_ids` mapping
- required-sector inventory artifact path and row count
- weakest-sector summary keyed by evidence tier, concept risk, and estimand class
- high-confidence subset counts and sector list
- validation results covering row presence, explicit publication-status coverage, and bounded published-estimate coverage ratios
- row-level provenance fields including `level_source_provider_used`, `level_supplemented_from_fred`, `point_estimate_origin`, `interval_origin`, `effective_duration_status`, `publication_status`, `row_is_short_window_estimate`, and `estimate_origin_includes_short_window_promotion`
- reconciliation diagnostics for formula nodes and parent/child rollups
- provenance and source-path summary

## Reporting language

Preferred language for the full-coverage release:

- full coverage
- explicit publication range
- common-grid canonical panel
- required canonical sectors
- high-confidence subset
- weakest sectors
- reconciliation nodes

Avoid implying that all sectors are equally observed or that the full-coverage release is a single-number preview product.
Avoid implying that the Fed exact overlay replaces the canonical Fed series; it is a companion artifact.
