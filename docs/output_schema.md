# Output schema

This document defines the versioned public artifact contract for the canonical public preview.

## Schema version

- Current schema version: `1.0.0`
- Stability promise: the required files, required fields below, and their meanings are the stable public contract for the first reference preview
- Compatibility rule: additive fields and columns are allowed in later preview builds, but removing or redefining required fields requires a schema-version change

## Required files

The fixed reference build and CI acceptance bundle require these files under `outputs/public_preview/`:

1. `public_release_report.md`
2. `sector_effective_maturity.csv`
3. `run_manifest.json`
4. `public_release_summary.json`

The default user-facing `make public-preview` path still writes the first three files unless `--summary-json-out` is requested explicitly.

## `sector_effective_maturity.csv`

Required columns:

| column | type | nullability | meaning |
| --- | --- | --- | --- |
| `date` | ISO date | non-null | Quarter-end date for the row |
| `sector_key` | string | non-null | Canonical sector identifier |
| `bill_share` | float | nullable | Estimated Treasury bill share |
| `short_share_le_1y` | float | nullable | Estimated share maturing within one year |
| `coupon_share` | float | nullable | Estimated coupon-bearing share |
| `effective_duration_years` | float | nullable | Duration-style maturity estimate from the public model |
| `zero_coupon_equivalent_years` | float | nullable | Zero-coupon-equivalent maturity implied by the estimate |
| `coupon_only_maturity_years` | float | nullable | Coupon-only maturity measure when defined |
| `method` | string | non-null | Estimation method label |
| `window_obs` | integer | nullable | Rolling-window observation count used by the solver |
| `level_evidence_tier` | enum | non-null | Public evidence tier for sector levels |
| `maturity_evidence_tier` | enum | non-null | Public evidence tier for maturity information |
| `concept_match` | enum | non-null | Whether the public source concept is direct, proxy, residual, aggregate, or anchor-consistent |
| `uncertainty_band_method` | string | nullable | Method used to construct uncertainty bands |
| `uncertainty_calibration_source` | string | nullable | Calibration source for uncertainty scaling |
| `identified_set_source` | string | nullable | Source of any bill-share identified set |
| `bill_share_lower` | float | nullable | Lower bound for published bill-share interval |
| `bill_share_upper` | float | nullable | Upper bound for published bill-share interval |
| `short_share_le_1y_lower` | float | nullable | Lower bound for published short-share interval |
| `short_share_le_1y_upper` | float | nullable | Upper bound for published short-share interval |

Rules:

- Bounds are nullable, but when a lower and upper bound are both present they must be ordered and contain the published point estimate.
- Additional columns are allowed and should be treated as additive metadata unless promoted into a later required schema.

Enums used in the fixed reference preview:

- `level_evidence_tier`: `A`, `B`, `C`
- `maturity_evidence_tier`: `A`, `B`, `C`, `D`
- `concept_match`: `direct`, `aggregate`, `proxy`, `residual`, `residual_style`, `anchor_consistent`, `partial`

## `run_manifest.json`

Required top-level fields:

| field | type | meaning |
| --- | --- | --- |
| `schema_version` | string | Output schema version |
| `run_timestamp_utc` | string | UTC timestamp for the build |
| `command` | string | Reconstructed build command |
| `source_provider_requested` | string | Requested provider family |
| `source_provider_used` | object | Actual provider per source block |
| `model_config_path` | string | Public model config used for the build |
| `benchmark_contract` | object | Holdable and factor benchmark-family contract |
| `end_date` | string | Final report date |
| `resolved_common_quarter_date` | string | Latest quarter shared across included sectors |
| `quarter_count` | integer | Number of quarter dates retained |
| `sector_keys_included` | array | Included sector keys |
| `optional_bank_paths_included` | boolean | Whether optional bank paths were enabled |
| `optional_bank_sectors_skipped` | array | Optional sectors excluded from the stable default |
| `source_artifact_paths` | object | Source provenance map |
| `output_paths` | object | Output artifact paths |

Required `output_paths` keys:

- `public_release_report`
- `sector_effective_maturity`
- `run_manifest`
- `public_release_summary`

## `public_release_summary.json`

Required top-level fields:

| field | type | meaning |
| --- | --- | --- |
| `schema_version` | string | Output schema version |
| `release_summary` | object | High-level run metadata |
| `sector_coverage` | array | Included-sector coverage summary |
| `sector_interpretation` | array | Per-sector interpretation boundary table |
| `evidence_tiers` | array | Evidence-tier summary |
| `uncertainty_identified_sets` | array | Uncertainty / identified-set summary |
| `validation` | object | Named validation checks and overall status |
| `provenance` | object | Provider and source-reference summary |
| `excluded_optional_sectors` | array | Optional sectors excluded from the stable default |
| `foreign_support_snapshot` | object or null | Foreign-support summary |
| `fed_calibration_snapshot` | object or null | Fed-calibration summary |
| `machine_readable_outputs` | object | Output path summary |
| `source_artifact_paths` | object | Raw source-artifact map copied from the manifest |

Required `release_summary` fields:

- `report_end_date`
- `quarter_count`
- `source_provider_requested`
- `source_provider_used`
- `model_config_path`
- `benchmark_contract`
- `optional_bank_paths_included`
- `command`
- `resolved_common_quarter_date`

## `public_release_report.md`

Required headings:

1. `# Public Release Preview Report`
2. `## Release Summary`
3. `## Sector Coverage`
4. `## Sector Interpretation`
5. `## Evidence Tiers`
6. `## Uncertainty and Identified Sets`
7. `## Validation`
8. `## Provenance`

The markdown report is human-readable first, but the required headings above are part of the stable preview contract because downstream review tooling may assert their presence.
