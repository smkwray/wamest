# Public source notes

This project is built around free public sources. The default public preview path is nominal-only, non-interactive, and pinned to `configs/model_public_preview.yaml`; the broader hybrid estimator in `configs/model_defaults.yaml` is available for standalone research runs, while TIPS, FRN, key-rate, and FFIEC 002 workflows remain outside the stable public-preview baseline. A separate `v0.2` full-coverage research release path uses the full-scope Fed Z.1 build in its standard live workflow, supplements missing required-sector level series from configured FRED mappings, keeps required sector/date rows in the main surface with explicit publication-status labeling, and exports sector-level publication-range semantics rather than pretending all common-grid rows are equally publishable.

- Federal Reserve Z.1 Data Download Program / release tables
- Federal Reserve H.15 Treasury constant-maturity yields
- New York Fed SOMA holdings
- Treasury TIC SHL benchmark survey
- Treasury TIC SLT monthly holdings data
- later: FFIEC call reports and NCUA call reports

## Source role by component

### Z.1
Provides sector holdings levels, transactions, revaluations, and some bill splits for both the frozen preview path and the separate full-coverage research release path.

### H.15
Provides nominal Treasury constant-maturity yields used to build the default benchmark price-return ladder.
Optional extensions use H.15 real-yield and derived proxy ladders for TIPS, FRN, and key-rate workflows.

### SOMA
Provides the exact Fed portfolio benchmark and calibration target. It remains the core calibration anchor even when the full-coverage release widens the sector scope.

### SHL
Provides benchmark foreign maturity composition and WAM.

### SLT
Provides monthly short-vs-long and official-vs-private foreign composition.

### FFIEC / NCUA
Optional public bank-perimeter sources that strengthen bank and credit-union constraints when available.
They are not required for the default public preview path.
They remain optional in the full-coverage research release path as well.

## Current manifest

See `configs/source_manifest.yaml` for URLs and notes.
