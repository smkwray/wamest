# WAMEST — Treasury Sector Maturity Estimation

> **[Live site →](https://smkwray.github.io/wamest/)** · [GitHub](https://github.com/smkwray/wamest)

Quarterly estimates of maturity structure, bill share, and short-vs-long composition for every holder sector in the Federal Reserve's Z.1 Financial Accounts — using only free public data.

## What this is

WAMEST infers the maturity exposure of each U.S. Treasury holder sector — whether they lean toward short-dated bills, long-dated coupons, or something in between — by analyzing revaluation behavior in the Z.1 Financial Accounts, calibrated against the Fed's exact SOMA portfolio. It covers 26 sectors — the Fed, foreign holders, banks, insurers, pensions, mutual funds, households, and more — with estimates going back to 2002.

All inputs are free public data: the Fed's Z.1 release, H.15 constant-maturity yields, SOMA holdings, and Treasury International Capital (TIC) survey data.

This is a **public research project**, not a finished product. The repo is explicit about uncertainty, evidence quality, and the distinction between directly observed and inferred quantities.

## How it works

1. **Build a sector panel** from Z.1 holdings, transactions, and revaluations
2. **Construct benchmark returns** from H.15 yield curves
3. **Calibrate against SOMA** — the Fed's security-level portfolio is the truth set
4. **Estimate maturity** by fitting each sector's revaluation behavior to benchmark price-return ladders
5. **Produce uncertainty bands** calibrated from SOMA estimation error, with peer-group envelopes for weaker sectors

The [live site](https://smkwray.github.io/wamest/) shows interactive visualizations of all results, including a date slider to explore how maturity structure has changed over time.

## Estimate quality

Not all sectors are equally well-observed. The pipeline distinguishes three tiers:

| | Description | Example |
|---|---|---|
| **Exact** | Direct security-level data | Fed (SOMA holdings) |
| **Estimated** | Model-based from revaluation behavior | Foreigners, banks, insurers |
| **Peer fallback** | No sector-specific signal; peer-group median | Asset-backed securities |

The Fed's canonical row uses direct SOMA holdings where public data exists. Other sectors are inferred from revaluation behavior and calibrated against the SOMA truth set. Sectors without a usable revaluation signal are explicitly labeled as peer fallbacks.

Many sectors cluster near similar maturity estimates (~7 years). This often reflects shared estimation anchors and regularization rather than precise sector-specific identification. The project surfaces this honestly through evidence tiers and estimate-quality labels.

## Sector evidence quality

| Sector | Evidence | Notes |
|---|---|---|
| Fed / SOMA | A | Security-level truth set; calibration anchor |
| Foreigners | B | Annual survey benchmark + monthly nowcast |
| Banks | C–D | Exact levels; maturity composition inferred |
| Domestic non-banks | C–D | Exact levels by identity; maturity from revaluation calibration |

See [release limitations](docs/release_limitations.md) for the full boundary.

## Quick start

```bash
python3 -m pip install -r requirements.txt
make test PYTHON=python3
make toy PYTHON=python3
```

## Full-coverage release

```bash
# Deterministic contract build (toy inputs, no API key needed)
make full-coverage-contract PYTHON=python3

# Live release build (requires free FRED API key)
export FRED_API_KEY=your_key_here
make full-coverage-release PYTHON=python3
```

A free FRED API key is available from [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html). The contract build uses toy fixtures and needs no key.

## Frontend

```bash
cd web && npm install && npm run dev    # localhost:5173
```

To regenerate site data from a fresh release build:

```bash
python3 scripts/export_site_data.py
```

## Limitations

This project does not claim equal observability across all sectors, exact maturity for weakly identified sectors, or that all estimates are equally reliable. Uncertainty is communicated through calibrated error envelopes (not formal confidence intervals) and evidence tiers. Weak identification is surfaced rather than hidden by omission.

## Repository structure

```
configs/       Sector definitions, source manifest, model defaults
data/examples/ Toy inputs for smoke tests
docs/          Output schema, source notes, limitations, release notes
scripts/       Pipeline entrypoints
src/           Python package (treasury_sector_maturity)
tests/         Test coverage
web/           Frontend site (Vite + React + TypeScript)
```

## Docs

- [Output schema](docs/output_schema.md) — artifact contract
- [Full-coverage schema](docs/full_coverage_output_schema.md) — full-coverage artifact contract
- [Source notes](docs/source_notes.md) — public data sources and roles
- [Release limitations](docs/release_limitations.md) — what is not promised
- [Release notes](docs/release_notes.md) — change summaries
