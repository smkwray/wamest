export default function Limitations() {
  return (
    <div className="page">
      <h1>Limitations &amp; Interpretation</h1>
      <p className="section-desc">
        What these estimates are and are not. This page explains evidence
        quality, the distinction between observed and inferred quantities, and
        the boundaries of what the project claims.
      </p>

      <h2>Evidence Quality by Sector</h2>
      <p>
        Different sectors have fundamentally different evidence quality. The
        project surfaces this through evidence tiers and uncertainty bands
        rather than hiding it by omission.
      </p>
      <div className="card-grid">
        <div className="card">
          <h3>Fed / SOMA <span className="tier tier-A">A</span></h3>
          <p>
            Security-level truth set from SOMA holdings. The strongest public
            benchmark and the calibration anchor for the entire pipeline.
          </p>
        </div>
        <div className="card">
          <h3>Foreigners <span className="tier tier-B">B</span></h3>
          <p>
            Annual benchmark survey plus monthly nowcast. Assumption bands grow
            between survey dates. Split into total, official, and private.
          </p>
        </div>
        <div className="card">
          <h3>Banks <span className="tier tier-C">C</span></h3>
          <p>
            Exact on aggregate levels. Maturity composition not fully observed
            from public sources. Bill-share constraints help but do not make the
            sector exact.
          </p>
        </div>
        <div className="card">
          <h3>
            Domestic Non-Banks{" "}
            <span className="tier tier-C">C</span>&ndash;<span className="tier tier-D">D</span>
          </h3>
          <p>
            Exact by identity on levels. Maturity inference relies on
            revaluation behavior calibrated against SOMA. Includes households,
            insurers, pensions, and residual aggregates.
          </p>
        </div>
      </div>

      <h2>What Is Directly Observed</h2>
      <ul>
        <li>
          <strong>Aggregate holdings levels</strong> for all sectors from the
          Federal Reserve Z.1 release
        </li>
        <li>
          <strong>Transactions and revaluations</strong> for most sectors
        </li>
        <li>
          <strong>Bill splits</strong> for sectors where the data source
          separates bills from coupons
        </li>
        <li>
          <strong>SOMA security-level portfolio</strong> &mdash; the Fed's exact
          maturity, duration, and composition at the individual-security level
        </li>
        <li>
          <strong>Foreign maturity anchors</strong> &mdash; annual benchmark
          composition and monthly short/long splits from TIC data
        </li>
      </ul>

      <h2>What Is Inferred</h2>
      <ul>
        <li>
          <strong>Maturity exposure</strong> from revaluation behavior &mdash;
          fitting each sector's revaluation series to benchmark price-return
          ladders
        </li>
        <li>
          <strong>Uncertainty bands</strong> calibrated by transferring SOMA
          estimation error to other sectors
        </li>
        <li>
          <strong>Duration-equivalent maturity</strong> when a distinct duration
          map is available
        </li>
        <li>
          <strong>Short-vs-long composition</strong> from rolling-window
          estimation and bill-share constraints
        </li>
      </ul>

      <h2>Proxy and Weakly Identified Sectors</h2>
      <div className="callout">
        <p>
          Some sectors are surfaced as proxies or carry high concept risk. These
          are included for coverage completeness, not because they have the same
          evidence quality as directly observed sectors.
        </p>
      </div>
      <p>
        <strong>Credit unions</strong>, for example, are estimated via a proxy
        assembled from public component series. This is not a direct maturity
        ladder &mdash; it carries high concept risk and should be treated as a
        first-order approximation, not a precise measurement.
      </p>
      <p>
        Other proxy and residual sectors are similarly labeled with their
        evidence quality in the output data. Rows without a publishable
        estimate are retained as status-only placeholders for transparency.
      </p>

      <h2>What Is Not Claimed</h2>
      <div className="claim-grid">
        <div className="claim-panel claim-can">
          <div className="claim-label">What the project provides</div>
          <ul>
            <li>
              Quarterly maturity estimates with calibrated uncertainty across
              all Z.1 holder sectors
            </li>
            <li>
              Explicit evidence quality labels and uncertainty bands on every
              estimate
            </li>
            <li>
              A security-level truth set (SOMA) used as calibration anchor
            </li>
            <li>Foreign maturity anchors from public survey data</li>
            <li>Reproducible builds from documented public inputs</li>
          </ul>
        </div>
        <div className="claim-panel claim-cannot">
          <div className="claim-label">What the project does not claim</div>
          <ul>
            <li>Equal observability across all sectors</li>
            <li>Exact maturity for weakly identified sectors</li>
            <li>That all estimates are equally reliable</li>
            <li>That the direct SOMA series replaces the inferred Fed estimate</li>
            <li>Production readiness or hosted API availability</li>
            <li>That weak sectors will become exact</li>
          </ul>
        </div>
      </div>

      <h2>Interpretation Guidance</h2>
      <ul>
        <li>
          <strong>Point estimates are best-available, not exact.</strong> They
          represent the best estimates under the public-data stack, with honestly
          communicated uncertainty.
        </li>
        <li>
          <strong>Evidence quality guides confidence.</strong> Sectors with
          moderate evidence (e.g., foreigners) have meaningfully different
          reliability than those with the weakest evidence (e.g., residual
          domestic non-banks).
        </li>
        <li>
          <strong>
            The direct SOMA series is a companion, not a replacement.
          </strong>{" "}
          The inferred Fed estimate uses the same cross-sector method as all
          other sectors. The SOMA series exposes the direct benchmark alongside
          it.
        </li>
        <li>
          <strong>Not all rows are equally publishable.</strong> Some sector/date
          combinations lack a reliable estimate. These are retained in the data
          for transparency but should not be treated as first-class results.
        </li>
      </ul>
    </div>
  );
}
