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
