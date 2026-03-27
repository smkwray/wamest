# Treasury Sector Maturity Research Preview

This repository is a public research preview for estimating maturity-related metrics for **marketable U.S. Treasury holdings** across a set of aggregate sectors using only **free public data**.

The repository is operated in a **script-first** way. The public contract is the set of documented scripts under `scripts/`; the package metadata exists for installation and reuse, but the primary entrypoints are the scripts themselves.

## Public preview scope

The default public path is **nominal-only** and **non-interactive**:

- sector levels and maturity metrics are built from public sources
- the benchmark ladder is nominal H.15 only
- the public-preview workflow is pinned to `configs/model_public_preview.yaml`
- FRED keys are optional, not required for the documented toy workflow
- FFIEC 002 / browser-backed bank perimeter paths are optional and not part of the stable public path

Advanced workflows are supported, but they are **optional** and more fragile:

- TIPS real-yield benchmark ladders
- FRN proxy benchmark ladders
- key-rate factor buckets
- FFIEC 002 and supplemental bank-perimeter inputs

See [docs/release_limitations.md](docs/release_limitations.md) for the release boundary and caveats, [docs/release_notes.md](docs/release_notes.md) for the Wave 1 public-preview summary, and [docs/output_schema.md](docs/output_schema.md) for the versioned output contract.

## Project goal

Estimate, as feasibly as possible given public data limits:

- par-weighted average remaining maturity / WAM where directly measurable
- bill share and bill-to-coupon ratio
- short-vs-long split
- TIPS share and FRN share when measurable
- effective duration / duration-equivalent maturity inferred from **Z.1 revaluations**

Target sector blocks:

1. **Central bank / monetary authority** (Fed / SOMA)
2. **Foreigners** (total, official, private)
3. **Banks**, under multiple perimeter definitions
   - U.S.-chartered depositories
   - foreign banking offices in the U.S. as an optional perimeter slice
   - banks in U.S.-affiliated areas as an optional perimeter slice
   - credit unions
   - reserve-access core
   - broad private depositories proxy
4. **Domestic non-banks / deposit users**
   - broad residual by identity
   - narrower proxy: households + nonfinancial corporates + state & local governments

## What is included

- methodology notes for the public release preview
- a source manifest pointed at the public source pages used by the project
- a Python package under `src/`
- provider-backed scripts for fetching sources, building sector panels, benchmark returns, Fed calibration inputs, foreign anchor panels, foreign monthly nowcast panels, and effective-maturity estimates
- a text-first output metadata report summarizing sector evidence tiers, uncertainty methods, foreign support kinds, and Fed calibration context
- an optional machine-readable public-release summary JSON companion when `--summary-json-out` is passed
- nominal, TIPS, FRN-proxy, and key-rate benchmark-return paths
- inferred-output metadata with evidence tiers, support-aware Fed/SOMA-calibrated uncertainty bands, and bill-share identified-set metadata for banks and residual sectors
- toy example data so the pipeline can run immediately
- tests

## Current modeling stance

- **Fed**: security-level SOMA provides the calibration truth set, but the current canonical maturity point estimate is still inferred from revaluation behavior unless an exact overlay is explicitly used
- **Foreigners**: annual SHL benchmark + monthly SLT monthly nowcast with alternative-prior assumption bands; long-end composition is only filled where the anchor inputs actually support it
- **Banks / credit unions**: exact levels; where public ladder constraints exist, bill share is anchored to bank-constraint identified sets and maturity remains support-aware Fed/SOMA-calibrated
- **Domestic non-banks residual**: exact by identity on levels; bill share now carries closure-derived identified sets, while maturity-style metrics remain support-aware Fed/SOMA-calibrated

## Default workflow

The recommended public workflow is:

1. Install dependencies.
2. Run the toy smoke test.
3. Run the public release hygiene check.
4. Build the canonical public preview.

Example setup and smoke:

```bash
python3 -m pip install -r requirements.txt
make test PYTHON=python3
make toy PYTHON=python3
make release-check PYTHON=python3
```

Canonical public preview:

```bash
make public-preview PYTHON=python3
```

That command defaults to the latest common quarter and writes exactly these top-level public artifacts under `outputs/public_preview/`:

- `public_release_report.md`
- `sector_effective_maturity.csv`
- `run_manifest.json`

If you want an additional machine-readable companion without changing the default three-artifact contract:

```bash
make public-preview PYTHON=python3 PUBLIC_PREVIEW_ARGS="--summary-json-out outputs/public_preview/public_release_summary.json"
```

For the first release-cycle reference example pinned to `2025-12-31`:

```bash
make public-preview PYTHON=python3 PUBLIC_PREVIEW_ARGS="--end-date 2025-12-31"
```

The stable public-preview path is nominal-only and does not require FRED keys or the FFIEC 002 browser workflow.
It is pinned to `configs/model_public_preview.yaml` even though the standalone estimator/calibration defaults now use the broader hybrid benchmark contract in `configs/model_defaults.yaml`.

The fixed reference release for this milestone is [`v0.1.0`](https://github.com/smkwray/wamest/releases/tag/v0.1.0).

## Canonical public preview

The default public-preview report includes only sectors that can be built through a non-interactive public-data path:

- `fed`
- `foreigners_total`
- `foreigners_official`
- `foreigners_private`
- `bank_us_chartered`
- `credit_unions_marketable_proxy`
- `households_nonprofits`
- `nonfinancial_corporates`
- `state_local_governments`
- `deposit_user_narrow_proxy`
- `domestic_nonbank_residual_broad`
- `all_holders_total`

The default public-preview report excludes these optional sectors:

- `bank_foreign_banking_offices_us`
- `bank_reserve_access_core`
- `bank_broad_private_depositories_marketable_proxy`
- `bank_us_affiliated_areas`

Those optional sectors remain supported through explicit FFIEC 002 and/or supplement-backed workflows, but they are not part of the first public-release acceptance bar.

## V1 acceptance contract

The first public-preview milestone is treated as a **fixed reference build**, not a rolling latest-data promise.

The V1 acceptance bundle is:

```bash
make test PYTHON=python3
make toy PYTHON=python3
make release-check PYTHON=python3
make public-preview-contract PYTHON=python3
```

That acceptance bundle must produce:

- `outputs/public_preview/public_release_report.md`
- `outputs/public_preview/sector_effective_maturity.csv`
- `outputs/public_preview/run_manifest.json`
- `outputs/public_preview/public_release_summary.json`

The acceptance build is pinned to:

- schema version `1.0.0`
- quarter end `2025-12-31`
- `configs/model_public_preview.yaml`
- the canonical default sector set listed above
- exclusion of `bank_foreign_banking_offices_us`, `bank_reserve_access_core`, `bank_broad_private_depositories_marketable_proxy`, and `bank_us_affiliated_areas`

The acceptance build fails if any required artifact is missing, any required default sector is missing, the schema-required columns or JSON fields are missing, or optional bank sectors appear in the stable default output.

## V0.2 full-coverage research release

The next release stage is a separate research surface. It does not change the frozen `v0.1` preview contract.

The standard live full-coverage build is:

```bash
make full-coverage-release PYTHON=python3
```

That target:

- uses `--coverage-scope full`
- defaults to the live Fed Z.1 path with `source-provider=auto`
- supplements missing required-sector level series from configured FRED mappings when the Fed release only exposes the matching transactions series
- requires `FRED_API_KEY` in the environment for that supplement path

The deterministic contract build for the same surface is:

```bash
make full-coverage-contract PYTHON=python3
```

That target stays pinned to the toy input bundle and does not depend on live FRED calls.

Both commands build the separate `v0.2` artifact set under `outputs/full_coverage_release/`:

- `canonical_sector_maturity.csv`
- `latest_sector_snapshot.csv`
- `high_confidence_sector_maturity.csv`
- `reconciliation_nodes.csv`
- `fed_exact_overlay.csv`
- `required_sector_inventory.csv`
- `full_coverage_report.md`
- `run_manifest.json`
- `full_coverage_summary.json`

The full-coverage release path:

- uses `--coverage-scope full`
- defaults to the Fed Z.1 source path
- in the standard live workflow, supplements required-sector level series that are missing from the Fed release zip but available through configured FRED mappings
- builds the main surface across required canonical sectors, including proxy and residual sectors where those are part of the configured full-coverage universe
- emits explicit `publication_status` rows when a required canonical sector/date lacks a publishable maturity estimate instead of dropping that row from the main artifact
- emits the canonical panel on the common required-sector/date grid while marking each row with `in_publication_range` and sector-level publication-range endpoints in the companion summary artifacts
- treats the latest snapshot as a separate common-quarter cross-section resolved from required-sector publication-range endpoints
- uses config-driven short-window promotion for explicitly allowlisted required atomic sectors before falling back to history-preserving fills
- marks leading warmup carry rows with `history_preserving_backfill`
- distinguishes row-level short-window estimates from rows whose estimate origin came from short-window promotion
- exports basis fields, direct composition metrics, and interval bands alongside the headline maturity estimates
- publishes `fed_exact_overlay.csv` as a direct SOMA companion while keeping the canonical Fed row cross-sector-comparable and inferred
- writes a `required_sector_inventory.csv` artifact covering raw parsed-source availability, post-supplement level availability, method priority, bills-series availability, publication-range endpoints, and latest provenance fields
- publishes reconciliation diagnostics for formula and parent/child rollups
- treats the high-confidence subset as a filter, not as the scope boundary

See [docs/full_coverage_output_schema.md](docs/full_coverage_output_schema.md) for the separate artifact contract.

## Advanced workflow outline

1. Fetch or provide a Z.1 input containing the selected Treasury series.
2. Build the sector panel:
   ```bash
   python3 scripts/build_sector_panel.py --source-provider auto
   ```
   The default Z.1 catalog uses an explicit FRED allowlist for `level`, `transactions`, and `revaluation`. `other_volume` remains explicitly unsupported in the FRED path and is carried as missing.
   To build the broader full-coverage scaffold instead:
   ```bash
   python3 scripts/build_sector_panel.py --coverage-scope full --source-provider fed
   ```
   That path uses `configs/z1_series_catalog_full.yaml` and `configs/sector_definitions_full.yaml`. The standard live release still starts from the Fed Z.1 source path, but it can now supplement missing required-sector level series from configured FRED mappings where the Fed release zip only exposes transactions.
3. Fetch or provide H.15 Treasury constant-maturity curves and build benchmark price returns:
   ```bash
   python3 scripts/build_benchmark_returns.py --source-provider auto
   ```
   To build the TIPS real-yield benchmark ladder instead:
   ```bash
   python3 -B scripts/build_benchmark_returns.py \
     --curve-key tips_real_yield_constant_maturity \
     --source-provider fred \
     --out data/interim/benchmark_returns_tips.csv
   ```
   The TIPS path emits prefixed columns such as `tips_5y` and `tips_10y` so they can coexist with nominal ladders in a merged benchmark panel later.
   To build the FRN proxy ladder instead:
   ```bash
   python3 -B scripts/build_benchmark_returns.py \
     --curve-key frn_proxy_from_nominal \
     --source-provider fred \
     --out data/interim/benchmark_returns_frn.csv
   ```
   The FRN path currently emits a derived `frn_3m` low-duration proxy from the nominal `3m` curve. It is useful for exposure recovery, but it is not an issue-level FRN total-return series.
   To build the key-rate bucket ladder instead:
   ```bash
   python3 -B scripts/build_benchmark_returns.py \
     --curve-key key_rate_buckets_from_nominal \
     --source-provider fred \
     --out data/interim/benchmark_returns_key_rate.csv
   ```
   The key-rate path emits prefixed columns such as `kr_2y`, `kr_10y`, and `kr_30y`. These are node-level repricing buckets without carry or roll-down, so they can coexist with the full nominal, TIPS, and FRN benchmark ladders.
4. Calibrate the Fed mapping with local or fetched SOMA holdings:
   ```bash
   python3 scripts/calibrate_fed.py \
     --z1-panel data/interim/z1_sector_panel.csv \
     --source-provider auto
   ```
   The default model config now uses the hybrid research benchmark contract: nominal holdings plus TIPS and FRN holdable families, with key-rate buckets as a factor block.
   By default the auto-fetch path keeps the 40 most recent quarter dates for SOMA. Override with `--soma-start`, `--soma-end`, or `--soma-max-quarters`.
   That step writes both the exact SOMA comparison table and a row-level Fed interval-calibration artifact that captures observed estimator errors against SOMA truth.
   To force nominal-only parity with the public preview, pass `--model-config configs/model_public_preview.yaml`.
   If you want to override the default hybrid benchmark contract explicitly, pass repeated holdable benchmark families plus any factor families you want so the interval artifact is aligned with the estimation run:
   ```bash
   python3 -B scripts/calibrate_fed.py \
     --z1-panel data/interim/z1_sector_panel.csv \
     --source-provider auto \
     --benchmark-family nominal_treasury_constant_maturity \
     --benchmark-family tips_real_yield_constant_maturity \
     --benchmark-family frn_proxy_from_nominal \
     --factor-family key_rate_buckets_from_nominal
   ```
   To calibrate from the full-coverage scaffold panel defaults instead:
   ```bash
   python3 scripts/calibrate_fed.py --coverage-scope full --source-provider fed
   ```
5. Estimate effective maturity metrics:
   ```bash
   python3 scripts/estimate_effective_maturity.py \
     --z1-panel data/interim/z1_sector_panel.csv \
     --source-provider auto
   ```
   If `data/processed/fed_interval_calibration.csv` exists, the estimator uses it by default to emit support-aware Fed/SOMA-calibrated uncertainty bands instead of the older sector-class heuristic bands. If `data/processed/foreign_nowcast_panel.csv` is present, the estimator also merges the foreign monthly short-end support envelope automatically. If `data/processed/bank_constraint_panel.csv` is present, the estimator also merges direct bank bill-share constraints, derives formula-based bank and residual bill-share identified sets, and projects the published `bill_share` point back into those feasible bounds when needed.
   The default model config now uses the richer hybrid benchmark contract. To force nominal-only parity with the public preview, pass `--model-config configs/model_public_preview.yaml`. To override the hybrid default explicitly, pass repeated holdable benchmark families plus any factor families you want:
   ```bash
   python3 -B scripts/estimate_effective_maturity.py \
     --z1-panel data/interim/z1_sector_panel.csv \
     --source-provider auto \
     --benchmark-family nominal_treasury_constant_maturity \
     --benchmark-family tips_real_yield_constant_maturity \
     --benchmark-family frn_proxy_from_nominal \
     --factor-family key_rate_buckets_from_nominal
   ```
   To estimate from the full-coverage scaffold panel defaults instead:
   ```bash
   python3 scripts/estimate_effective_maturity.py --coverage-scope full --source-provider fed
   ```
   Holdable families share the simplex and publish metrics like `tips_share` and `frn_share`. Factor families stay outside the simplex and publish `factor_exposure_kr_*` style outputs, so curve-shape buckets can enter without corrupting share metrics. As with holdable families, a block only enters once it is fully populated for the current rolling window.
6. Build the text summary report:
   ```bash
   python3 -B scripts/build_output_metadata_report.py
   ```
   If the canonical output files are absent but the toy files exist, the report builder falls back to the bundled toy artifacts and still writes `outputs/output_metadata_report.md`.

To prefetch normalized real inputs directly:

```bash
python3 scripts/fetch_public_sources.py --datasets z1,h15 --source-provider auto
```

To fetch and aggregate bank-constraint inputs:

```bash
python3 scripts/fetch_public_sources.py --datasets ffiec,ffiec002,ncua --ffiec-report-date 2025-12-31 --ffiec002-report-date 2025-12-31
python3 scripts/build_bank_constraint_panel.py
```

If you have an uncovered or composite-perimeter supplement such as affiliated-area bank rows, reserve-access core rows, or broad private-depositories rows, place it at `data/processed/bank_constraint_supplement.csv` or pass it explicitly:

```bash
python3 scripts/build_bank_constraint_panel.py \
  --supplement-file data/processed/bank_constraint_supplement.csv
```

That supplement file can add rows such as `bank_us_affiliated_areas`, `bank_reserve_access_core`, or `bank_broad_private_depositories_marketable_proxy` even when no FFIEC / FFIEC 002 / NCUA institution files exist for that perimeter, and the builder rejects duplicate date / sector pairs if a supplement tries to override an observed row.

Then pass those constraints into estimation explicitly if you want to override the default path:

```bash
python3 scripts/estimate_effective_maturity.py \
  --z1-panel data/interim/z1_sector_panel.csv \
  --bank-constraint-file data/processed/bank_constraint_panel.csv
```

To build the foreign-anchor panel directly from official TIC sources:

```bash
python3 scripts/build_foreign_anchor_panel.py \
  --source-provider official \
  --out data/processed/foreign_anchor_panel.csv
```

That path fetches and parses:
- the TIC SHL historical survey CSV for annual Treasury benchmark dates
- TIC `slt_table3.txt` for monthly total / official / private foreign Treasury holdings

To build the foreign monthly nowcast directly from official TIC sources:

```bash
python3 -B scripts/build_foreign_nowcast_panel.py \
  --source-provider official \
  --out data/processed/foreign_nowcast_panel.csv
```

That output carries:
- monthly `total`, `official`, and `private` holder groups
- interpolated monthly totals and short-vs-long shares between SHL benchmark dates and SLT monthly observations
- lower/upper assumption-band columns around the point nowcast from `linear`, `carry_previous`, and `carry_next` interpolation priors
- `has_shl_anchor`, `has_slt_observation`, `within_slt_window`, `uncertainty_band_active`, and `uncertainty_support_kind` so anchored rows, direct monthly observations, between-support rows, and one-sided flat fills stay distinguishable

The `ffiec002` fetch path is browser-backed because the official NIC CSV endpoints sit behind the FFIEC browser challenge. It uses Playwright only for that source, opens Chromium in headed mode by default, and may still require an interactive challenge pass on the FFIEC site.

If the direct FFIEC 002 fetch path is still challenged, use the real-Chrome CDP workflow instead:

```bash
python3 scripts/launch_ffiec_chrome.py --port 9222

# Warm that dedicated Chrome session manually on the FFIEC site, then:
python3 scripts/fetch_ffiec002_via_chrome_cdp.py \
  --search-export /path/to/ffiec002_search_results.csv \
  --report-date 2025-12-31 \
  --browser-url http://127.0.0.1:9222
```

Export `FRED_API_KEY` in your shell if you want `--source-provider fred` or `--source-provider auto` to use FRED.
The Z.1 FRED path is allowlist-based, not a generic code transformation. Keep using explicit `fred_ids` mappings in `configs/z1_series_catalog.yaml` for any added catalog series.

## Repo map

- `docs/` - methodology, source notes, release limitations, release notes, and the output schema contract
- `configs/` - sector definitions, source manifest, model defaults
- `src/treasury_sector_maturity/` - reusable code
- `scripts/` - pipeline entrypoints
- `data/examples/` - toy inputs for smoke tests
- `tests/` - basic test coverage

## Important caveats

- Some Z.1 Treasury revaluation series are sector-specific mark-to-market signals; others are partly or largely benchmark-imposed.
- Public-source sector maturity estimates are best treated as a layered evidence stack, not as equally strong measurements.
- The default public path is nominal-only and pinned to `configs/model_public_preview.yaml`; the standalone estimator/calibration defaults in `configs/model_defaults.yaml` now use the broader hybrid research contract, while FFIEC 002 workflows remain optional and experimental relative to the public baseline.
- `docs/release_limitations.md` describes what is intentionally outside the first public preview boundary.
- `docs/release_notes.md` summarizes what is included, excluded, and experimental in Wave 1.
- `docs/output_schema.md` defines the versioned public artifact contract.
