# Release limitations

This repository is a **public research preview**, not a finished production package.

## Stable default path

The default public path is:

- nominal H.15 benchmark ladders only
- pinned to `configs/model_public_preview.yaml`
- public, non-interactive source acquisition
- script-first usage from `scripts/`
- release output focused on the markdown metadata report and the sector panel it summarizes

That path is the intended public baseline for the first release preview.

The frozen preview remains the stable public baseline even as the repository adds a separate `v0.2` full-coverage research release path.

## Optional and experimental paths

These paths are supported in the repository but are not part of the stable public baseline:

- TIPS real-yield benchmark ladders
- FRN proxy benchmark ladders
- key-rate factor buckets
- FFIEC 002 browser-backed fetching
- uncovered bank-perimeter supplement rows
- hybrid benchmark estimation runs that combine nominal, TIPS, FRN, and factor blocks

These workflows are useful for analysis, but they are more fragile than the default nominal-only path and may require additional inputs or manual steps.

The standalone `scripts/calibrate_fed.py` and `scripts/estimate_effective_maturity.py` defaults now use the broader hybrid research config in `configs/model_defaults.yaml`; the public preview remains pinned to the nominal-only config above.

The separate full-coverage research release path:

- uses the full-scope Fed Z.1 build by default
- in the standard live workflow, supplements missing required-sector level series from configured FRED mappings when the Fed release only exposes transactions
- keeps required sector/date rows in the main surface with explicit publication-status labeling
- emits a common required-sector/date panel and marks each row with explicit publication-range semantics instead of pretending every row is equally publishable
- may use short-window promotion for selected stronger sectors before falling back to history-preserving carry rows
- marks carried long-history rows explicitly instead of pretending they are equally observed
- separates canonical, snapshot, high-confidence, and reconciliation-node artifacts
- does not change the frozen preview path

## Sector caveats

- **Fed / SOMA**: strongest public benchmark and calibration set.
- **Foreigners**: annual SHL anchors plus monthly SLT nowcasts, with assumption bands between anchors.
- **Banks**: maturity composition is not fully observed from public sources; bill-share constraints and supplemental perimeter rows help, but they do not make the sector fully exact.
- **Domestic residuals**: useful and exact on levels by identity, but maturity inference is still an inverse problem and should be treated as such.

## What is intentionally not promised

- package-manager-grade CLI stability
- a committed public-data snapshot
- identical observability across all sectors
- one-number certainty for weakly observed sector maturity
- a guarantee that optional bank-perimeter workflows are available without additional public inputs
- a guarantee that every row in the common-grid canonical panel is equally in-range for publication
- a guarantee that weak sectors become exact or fully identified

## Public-facing language to prefer

Use:

- research preview
- script-first
- default nominal-only path
- separate full-coverage research release
- optional experimental extensions
- release limitations

Avoid:

- starter-project language
- production-ready
- complete sector observability
- exact maturity for every block
