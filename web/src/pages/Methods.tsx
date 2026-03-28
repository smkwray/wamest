export default function Methods() {
  return (
    <div className="page">
      <h1>Methods</h1>
      <p className="section-desc">
        How the estimation pipeline works, where uncertainty enters, and what
        the project does not claim.
      </p>

      <h2>Pipeline</h2>
      <div className="pipeline-steps">
        <div className="step">
          <h3>Build sector panel</h3>
          <p>
            Parse the Z.1 release and construct a quarterly panel of
            sector-level holdings, transactions, revaluations, and bill splits.
            The full-coverage path supplements missing level series from FRED
            when the Fed release only publishes transactions.
          </p>
        </div>
        <div className="step">
          <h3>Build benchmark returns</h3>
          <p>
            Convert H.15 constant-maturity yield curves into quarterly
            price-return ladders. These returns form the decomposition basis
            for inferring maturity exposure from revaluation behavior.
          </p>
        </div>
        <div className="step">
          <h3>Calibrate against SOMA</h3>
          <p>
            Compare the revaluation-inferred Fed estimate to SOMA
            security-level truth. This produces interval calibration that
            quantifies estimator error and scales uncertainty for other sectors.
          </p>
        </div>
        <div className="step">
          <h3>Build foreign anchors</h3>
          <p>
            Merge annual SHL benchmark surveys and monthly SLT holdings to
            produce a monthly foreign maturity nowcast with assumption bands
            between survey dates.
          </p>
        </div>
        <div className="step">
          <h3>Estimate effective maturity</h3>
          <p>
            Fit each sector's revaluation series to the benchmark return ladder
            over a rolling window. The estimator merges calibration intervals,
            foreign support, and bank constraints to produce support-aware
            uncertainty bands.
          </p>
        </div>
        <div className="step">
          <h3>Assemble release artifacts</h3>
          <p>
            Produce the canonical panel, latest snapshot, high-confidence
            subset, reconciliation nodes, Fed exact overlay, and sector
            inventory with explicit publication-status semantics.
          </p>
        </div>
      </div>

      <h2>Data Sources</h2>
      <div className="card-grid">
        <div className="card">
          <h3>Federal Reserve Z.1</h3>
          <p>Sector-level levels, transactions, and revaluations for marketable Treasuries. The revaluations carry implicit duration information.</p>
        </div>
        <div className="card">
          <h3>H.15 Treasury Yields</h3>
          <p>Constant-maturity yield curves for constructing the benchmark price-return ladder. Nominal, TIPS, FRN, and key-rate extensions available.</p>
        </div>
        <div className="card">
          <h3>SOMA Holdings</h3>
          <p>Security-level Fed portfolio from the NY Fed. The only sector with CUSIP-level truth — the calibration anchor for the pipeline.</p>
        </div>
        <div className="card">
          <h3>TIC SHL / SLT</h3>
          <p>Annual foreign benchmark composition (SHL) and monthly short-vs-long splits (SLT). Provides the foreign sector's maturity anchors.</p>
        </div>
        <div className="card">
          <h3>FFIEC / NCUA</h3>
          <p>Optional bank call-report inputs that strengthen bill-share constraints. Not required for the default or full-coverage paths.</p>
        </div>
        <div className="card">
          <h3>FRED API</h3>
          <p>Supplements missing Z.1 level series for the live full-coverage build. Free API key required for the live path only.</p>
        </div>
      </div>

      <h2>Where Uncertainty Enters</h2>
      <div className="callout">
        <p>Different sectors have different evidence quality. The project does not
        claim uniform observability.</p>
      </div>
      <ul>
        <li><strong>Revaluation signal quality varies.</strong> Some Z.1 series are genuine mark-to-market; others are partly benchmark-imposed.</li>
        <li><strong>Rolling-window estimation is approximate.</strong> Results depend on window length, available maturity nodes, and composition stability.</li>
        <li><strong>Levels are not maturity.</strong> Bank and residual sectors have exact aggregate holdings but maturity composition is still inferred.</li>
        <li><strong>Calibration transfer is an assumption.</strong> Fed/SOMA error patterns may not transfer perfectly to sectors with different portfolios.</li>
        <li><strong>Foreign anchors are periodic.</strong> Between annual SHL surveys, maturity relies on interpolation with assumption bands.</li>
      </ul>

      <h2>Interpretation</h2>
      <p>
        Weak identification is labeled, not hidden. The design principle is that
        an honestly uncertain estimate — with calibrated bands and explicit
        evidence tiers — is more useful than no estimate.
      </p>
      <p>
        See <a href="#/limitations">Limitations &amp; Interpretation</a> for the
        full discussion of evidence quality, proxy sectors, and what the project
        does and does not claim.
      </p>
    </div>
  );
}
