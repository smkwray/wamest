# WAMEST — Treasury Sector Maturity Estimation

> **[Live site →](https://smkwray.github.io/wamest/)** · [GitHub repo](https://github.com/smkwray/wamest)

Estimating maturity-related metrics for marketable U.S. Treasury holdings across holder sectors, using only free public data.

WAMEST produces quarterly estimates of weighted-average maturity, bill share, short-vs-long composition, effective duration, and related metrics for each sector block in the Federal Reserve's Z.1 Financial Accounts. All inputs are free public data: the Fed's Z.1 release, H.15 constant-maturity yields, SOMA holdings, Treasury International Capital (TIC) survey and monthly data, and optionally FFIEC and NCUA bank reports.

This is a **public research project**, not a finished product. The repo is explicit about uncertainty, evidence quality, and the distinction between directly observed and inferred quantities.

## What this repo estimates

- Par-weighted average remaining maturity / WAM where directly measurable
- Bill share and bill-to-coupon ratio
- Short-vs-long split (share maturing within one year)
- TIPS share and FRN share when measurable
- Effective duration / duration-equivalent maturity inferred from Z.1 revaluations
- Calibrated uncertainty bands anchored to the SOMA truth set

Target sectors span the full Z.1 holder universe: Fed/SOMA, foreigners (total, official, private), banks under multiple perimeter definitions, credit unions, and domestic non-bank holders including households, nonfinancial corporates, state & local governments, and residual aggregates.

**Some sectors are exact on levels but not exact on maturity.** The project surfaces this distinction through evidence tiers, measurement-basis labels, and explicit publication-status metadata rather than hiding it.

## Quick start

```bash
python3 -m pip install -r requirements.txt
make test PYTHON=python3
make toy PYTHON=python3
```

## Running the full-coverage release

```bash
# Deterministic contract build (toy inputs, no API key needed)
make full-coverage-contract PYTHON=python3

# Live release build (requires FRED_API_KEY)
export FRED_API_KEY=your_key_here
make full-coverage-release PYTHON=python3
```

The live build uses the Fed Z.1 source path and supplements missing required-sector level series from configured FRED mappings. The deterministic contract build stays pinned to toy fixtures. A free FRED API key is available from [FRED](https://fred.stlouisfed.org/docs/api/api_key.html).

## Frontend

The [live site](https://smkwray.github.io/wamest/) is built with Vite + React + TypeScript:

```bash
cd web
npm install
npm run dev        # development server at localhost:5173
npm run build      # production build → web/dist/
```

To regenerate the site data from a fresh release build:

```bash
python3 scripts/export_site_data.py
```

## Repository structure

```
configs/          Sector definitions, source manifest, model defaults, coverage registry
data/examples/    Toy inputs for smoke tests
docs/             Output schema, source notes, release limitations, release notes
scripts/          Pipeline entrypoints (the public contract)
src/              Python package: treasury_sector_maturity
tests/            Test coverage
web/              Frontend site (Vite + React + TypeScript)
Makefile          Primary build targets
```

## Estimate quality

The pipeline distinguishes three tiers of estimate quality:

- **High confidence** — direct calibration with security-level or survey-anchored data (Fed, Foreigners)
- **Estimated** — model-based estimate from revaluation behavior; uncertainty from peer-group envelope or calibrated bands
- **Peer fallback** — no sector-specific signal; estimate is a peer-group median with envelope bounds

Sectors without a sector-specific revaluation signal are explicitly labeled as peer fallbacks rather than presented as independent fits.

## Sector evidence quality

- **Fed/SOMA**: security-level truth set; strongest public benchmark (evidence tier A)
- **Foreigners**: annual SHL benchmark + monthly SLT nowcast; assumption bands between surveys (evidence tier B)
- **Banks**: exact levels; maturity composition not fully observed from public sources (evidence tier C–D)
- **Domestic non-banks**: exact by identity on levels; maturity inference relies on revaluation behavior calibrated against SOMA (evidence tier C–D)

See [docs/release_limitations.md](docs/release_limitations.md) for the full boundary.

## Limitations

This project does not claim:

- Equal observability across all sectors
- Exact maturity for weakly identified sectors
- That all estimates are equally reliable
- Production readiness or hosted API availability

Weak identification is surfaced through evidence tiers and calibrated uncertainty bands rather than hidden by omission. The project's design principle is that an honestly uncertain estimate is more useful than no estimate.

## Key docs

- [Output schema](docs/output_schema.md) — versioned v0.1 artifact contract
- [Full-coverage output schema](docs/full_coverage_output_schema.md) — v0.2 artifact contract
- [Source notes](docs/source_notes.md) — public data sources and their roles
- [Release limitations](docs/release_limitations.md) — what is intentionally not promised
- [Release notes](docs/release_notes.md) — Wave 1 preview and Wave 2 full-coverage summaries
