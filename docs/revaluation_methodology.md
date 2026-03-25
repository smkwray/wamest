# Methodology: infer effective maturity from Z.1 revaluations

This note describes the public research-preview estimator. The canonical public-preview contract is nominal-only and pinned to `configs/model_public_preview.yaml`, while the standalone research estimator defaults in `configs/model_defaults.yaml` now include the broader hybrid contract with TIPS, FRN, and key-rate factors.

## 1. Target variable

Use Z.1 revaluations to infer a **market-value sensitivity object**:
- modified duration
- key-rate duration exposure
- duration-equivalent maturity

Do **not** assume that Z.1 revaluations identify legal WAM directly.

## 2. Identity

For a Z.1 series:

\[
Level_t = Level_{t-1} + Transactions_t + Revaluation_t + OtherVolume_t
\]

The revaluation-return object is:

\[
r^{rv}_{s,t} = \frac{Revaluation_{s,t}}{B_{s,t}}
\]

where \(B_{s,t}\) is an exposure base, ideally:

\[
B_{s,t} = Level_{s,t-1} + 0.5 \times (Transactions_{s,t} + OtherVolume_{s,t})
\]

This approximates average holdings at risk during the quarter.

## 3. Public benchmark return library

Build synthetic quarterly **price-only** Treasury returns from H.15 curves.

Buckets:
- 1m
- 3m
- 6m
- 1y
- 2y
- 3y
- 5y
- 7y
- 10y
- 20y
- 30y

Later extensions:
- TIPS real-yield return ladders
- FRN near-par short-duration proxy
- key-rate shock factors

These later extensions are supported for analysis, but they are not required for the stable public baseline.

## 4. Calibration sector

Use the Fed / SOMA as the main calibration set:
- exact quarter-end holdings at CUSIP level
- exact remaining maturity and instrument mix
- approximate duration from quarter-end curve

The calibration set is the cleanest public benchmark in the repository and should be treated as the primary reference for model error.

Fit the mapping from:
- benchmark price-return library
- Z.1 Fed revaluation return
- exact Fed maturity metrics

## 5. Core estimator

Estimate a rolling portfolio over benchmark maturity buckets:

\[
\hat{w}_{s,t}
=
\arg\min_{w \ge 0,\; 1'w=1}
\sum_{\tau=t-h+1}^{t}
\left(r^{rv}_{s,\tau} - R^{bench}_{\tau} w\right)^2
+ \lambda \| D w \|^2
+ \gamma \|w - w_{t-1}\|^2
\]

Where:
- \(R^{bench}\) is the benchmark-return matrix
- \(D\) is an adjacent-bucket smoothness operator
- \(\lambda\) is smoothness penalty
- \(\gamma\) is turnover penalty

Constraints:
- non-negative weights
- weights sum to one

## 6. Outputs from estimated weights

Convert the rolling weights into:
- effective duration
- zero-coupon-equivalent maturity
- bill share
- coupon share
- optional TIPS share

## 7. Sector application

### Fed
Exact / calibrated.

### Foreigners
Use SHL / SLT as anchor. Use revaluations weakly.

### U.S.-chartered banks
Good candidate for revaluation-based inference.

### Smaller bank segments
Use with caution; measurement quality weaker.

### Broad domestic non-bank residual
Infer as residual, but uncertainty is widest.

## 8. Validation

Minimum diagnostics:
- Fed fitted vs exact SOMA metrics
- SHL foreign WAM match on survey dates
- sensible behavior in rate shock episodes
- stability under rolling-window and penalty choices
- robustness to exposure-base definition

## 9. Reporting convention

Reserve the label **WAM** for:
- survey-backed measures
- security-level measures
- directly observed maturity distributions

Use **effective duration** or **maturity-equivalent** elsewhere.
