# Full-Coverage Output Schema

This document defines the artifact contract for the separate `v0.2` full-coverage research release. It is distinct from the frozen `v0.1` public preview contract and may include ragged histories, weaker sectors, and non-preview artifact names.

## Scope

The full-coverage path is a separate release surface for quarterly maturity estimation across the required atomic Z.1 holder sectors. The standard live workflow uses the full-scope Fed Z.1 build plus configured `fred_ids.level` supplements for missing required-sector level series, and keeps weakly identified sectors in the output with explicit tiering instead of dropping them.

Canonical entrypoints:

- `make full-coverage-release` for the live Fed Z.1 build with the standard required-sector FRED level supplement
- `make full-coverage-contract` for the deterministic toy contract build
- `python3 -B scripts/build_full_coverage_release.py --coverage-scope full`

The release is designed for:

- full holder coverage across required atomic sectors
- ragged historical spans where source availability differs by sector
- explicit separation between atomic sectors, reconciliation nodes, and high-confidence subsets
- coverage-honest reporting rather than preview-minimal reporting

## Required artifacts

The full-coverage release writes these top-level artifacts:

1. `canonical_atomic_sector_maturity.csv`
2. `latest_atomic_sector_snapshot.csv`
3. `high_confidence_sector_maturity.csv`
4. `reconciliation_nodes.csv`
5. `required_sector_inventory.csv`
6. `full_coverage_report.md`
7. `run_manifest.json`
8. `full_coverage_summary.json`

## Canonical atomic panel

`canonical_atomic_sector_maturity.csv` is the main historical artifact.

Rules:

- contains one row per `node_type == atomic` sector/date combination in the full-coverage release scope
- includes weak sectors and partially identified sectors
- allows ragged histories when some sectors start later than others
- excludes non-atomic reconciliation or roll-up nodes
- retains the same core maturity fields used in the preview path, plus the richer coverage metadata
- includes `history_preserving_backfill` so early rows filled from the nearest available sector estimate remain explicit and easy to filter
- may include `release_window_override` rows where selected stronger sectors were estimated with a shorter window before any carry-style backfill was used

Expected interpretation:

- point estimates are the best available estimates under the public-data stack
- intervals communicate uncertainty and weak identification
- missing early history for a sector is allowed only when the sector has no feasible public estimate for those dates

## Latest snapshot

`latest_atomic_sector_snapshot.csv` is the quarter-aligned snapshot artifact.

Rules:

- one row per required atomic sector
- the snapshot quarter is the latest quarter shared across all required atomic sectors
- the row for each sector is taken from that common quarter
- sectors without a row at the resolved common quarter fail validation

## High-confidence subset

`high_confidence_sector_maturity.csv` is a filtered view of the canonical atomic panel.

Rules:

- exact row filter of the canonical atomic panel where `high_confidence_flag == true`
- no re-estimation or separate contract
- may be empty for a date if no sector meets the high-confidence threshold

## Reconciliation nodes

`reconciliation_nodes.csv` captures the non-atomic nodes used for rollups, discrepancy accounting, and audit visibility.

Rules:

- contains only non-atomic nodes
- may include rollups, residual nodes, proxies, and discrepancy objects
- exists to support transparency and hierarchy checks, not to expand the atomic panel

## Required sector inventory

`required_sector_inventory.csv` is the machine-readable planning and audit artifact for the full-coverage surface.

Rules:

- contains one row per required atomic sector
- records the configured level / transactions / revaluation / bills source codes and any configured `fred_ids` fallbacks for each required sector where applicable
- records whether those configured source codes are present in the parsed Z.1 source file used for the release
- records a compact `source_level_status` classification such as `present`, `transactions_only`, `same_base_other_only`, `absent`, or `computed_proxy`
- records any same-base source codes actually seen in the parsed Z.1 source file so catalog mismatches can be audited without re-scanning the raw release
- records the configured method-priority stack from `configs/sector_definitions_full.yaml`
- records whether a sector has a direct `bills_series`
- records whether the sector is explicitly eligible for release-window promotion in `configs/coverage_registry.yaml`
- records the feasible history span, underlying level/transactions/revaluation row availability, and current `history_preserving_backfill` / `release_window_override` usage from the emitted canonical artifact

## Validation semantics

The full-coverage release should validate the following:

- every required atomic sector appears in the canonical atomic panel for every date where that sector has a non-null full-scope level
- the latest snapshot quarter is the minimum of the per-sector latest available dates across required atomic sectors
- the high-confidence subset is a strict filter of the canonical atomic panel
- reconciliation nodes do not leak into the canonical atomic artifact
- preview validation remains separate and unchanged

## Required summary content

`full_coverage_summary.json` should include:

- release metadata and schema version
- per-sector history spans and inclusion status
- required-sector completeness results
- source-series audit results, including counts of sectors whose configured level code is present versus transactions-only or absent in the parsed source file, plus the subset of transactions-only sectors that already have a configured level `fred_ids` mapping
- required-sector inventory artifact path and row count
- weakest-sector summary keyed by evidence tier, concept risk, and estimand class
- high-confidence subset counts and sector list
- validation results covering row presence, non-null required-sector estimate coverage, and bounded estimate-coverage ratios
- reconciliation diagnostics for formula nodes and parent/child rollups
- provenance and source-path summary

## Reporting language

Preferred language for the full-coverage release:

- full coverage
- ragged history
- required atomic sectors
- high-confidence subset
- weakest sectors
- reconciliation nodes

Avoid implying that all sectors are equally observed or that the full-coverage release is a single-number preview product.
