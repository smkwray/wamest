# Release Notes

## Wave 1: Public Research Preview

Included in this release:

- one canonical script-first public-preview workflow via `make public-preview`
- one fixed reference-build acceptance path via `make public-preview-contract`
- a nominal-only default build that does not require FRED keys
- an explicit split between the nominal public-preview config (`configs/model_public_preview.yaml`) and the broader hybrid research default (`configs/model_defaults.yaml`)
- a non-interactive default sector set covering Fed, foreigners, core bank / credit-union paths, and domestic non-bank aggregates
- a human-readable markdown report plus machine-readable CSV and manifest outputs
- an optional machine-readable `public_release_summary.json` companion when explicitly requested
- a versioned output contract documented in `docs/output_schema.md`
- an explicit `make release-check` gate that rejects internal/public-surface leakage such as planning docs or internal orchestration references
- CI for `pytest -q`, the toy smoke pipeline, and the fixed public-preview contract build

Excluded from the stable default path:

- FFIEC 002 browser-backed paths
- supplemental uncovered bank-perimeter rows
- optional bank sectors such as `bank_foreign_banking_offices_us`, `bank_reserve_access_core`, `bank_broad_private_depositories_marketable_proxy`, and `bank_us_affiliated_areas`
- committed real-data output snapshots

Experimental or opt-in:

- the canonical public-preview build remains nominal-only even though the standalone estimator/calibration defaults now use the broader hybrid benchmark contract
- optional FFIEC 002 and supplement-backed bank perimeter support
- richer report formats beyond the markdown public-preview artifact set

Reference-build stance:

- the repo now treats the first public-preview milestone as a fixed reference build, not a rolling latest-data promise
- the GitHub Actions public-preview workflow is manual-only so a tagged release can remain the citable artifact boundary

## Wave 2: Full-Coverage Research Release

Included in this release path:

- a separate full-coverage release surface for required atomic Z.1 holder sectors
- a full-scope Fed Z.1 default for the sector panel, calibration, and estimation flow
- configured FRED level supplements for required sectors whose level series are missing from the Fed release zip
- ragged historical spans where sector source availability differs
- explicit tiering for weakly identified sectors instead of dropping them from output
- a canonical atomic panel, a latest common-quarter snapshot, a high-confidence subset, and reconciliation-node output
- short-window promotion for selected stronger sectors before history-preserving backfill is used
- explicit `history_preserving_backfill` flags plus reconciliation diagnostics for formula and parent/child rollups
- coverage-honest reporting and validation that remain separate from the frozen public-preview contract

Excluded from this release path:

- changes to the frozen `v0.1` public preview contract
- any promise that weak sectors become exact
- any requirement that all sectors share the same historical start date

Reference stance:

- the full-coverage path is a separate research release, not a rewrite of the existing preview baseline
- the preview remains unchanged and citable on its original artifact contract
