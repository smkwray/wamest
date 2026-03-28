import { useState } from "react";
import { Link } from "react-router-dom";
import { useSiteData, type SiteData } from "../data";
import { useTheme, plotlyColors, TRACE_COLORS, TRACE_COLORS_DARK } from "../theme";
import Chart from "../components/Chart";

type Snap = SiteData["snapshot"][number];
type Quality = "high" | "estimated" | "fallback";

function getQuality(s: Snap): Quality {
  if (s.high_confidence) return "high";
  if (s.point_estimate_origin?.startsWith("peer_group")) return "fallback";
  return "estimated";
}

const QUALITY_LABEL: Record<Quality, string> = {
  high: "High confidence",
  estimated: "Estimated",
  fallback: "Peer fallback",
};

const TS_DEFAULTS = [
  "Federal Reserve (SOMA)", "Foreign Holders (Total)", "U.S. Chartered Banks",
  "Mutual Funds", "Life Insurers",
];

export default function Home() {
  const data = useSiteData();
  const { theme } = useTheme();
  const c = plotlyColors(theme);
  const traces = theme === "dark" ? TRACE_COLORS_DARK : TRACE_COLORS;

  const [tsOverride, setTsOverride] = useState<string[] | null>(null);
  const [scatterOverride, setScatterOverride] = useState<string[] | null>(null);

  if (!data) return <div className="loading">Loading results...</div>;

  const { hero, snapshot, time_series, fed_comparison } = data;
  const somaData = (data as any).soma_exact;

  // --- Quality-based colors ---
  const qColor = (q: Quality) =>
    q === "high" ? (theme === "dark" ? "#6bc47a" : "#3a8a4a")
    : q === "fallback" ? (theme === "dark" ? "#e0a050" : "#c07830")
    : (theme === "dark" ? "#5b9bd5" : "#3a72a4");

  // --- Sorted data ---
  const sorted = [...snapshot]
    .filter((s) => s.maturity != null && (s.maturity ?? 0) > 0.01)
    .sort((a, b) => (b.maturity ?? 0) - (a.maturity ?? 0));

  const sortedByBill = [...snapshot]
    .filter((s) => s.bill_share != null && (s.bill_share ?? 0) > 0.001)
    .sort((a, b) => (b.bill_share ?? 0) - (a.bill_share ?? 0));

  // --- Time series pills ---
  const nameToTier: Record<string, string> = {};
  for (const s of snapshot) nameToTier[s.sector] = s.maturity_tier;
  const tsNames = Object.keys(time_series).sort();
  const tsActive = new Set(tsOverride ?? TS_DEFAULTS.filter((n) => n in time_series));
  const toggleTs = (name: string) => {
    setTsOverride((prev) => {
      const cur = prev ?? [...tsActive];
      return cur.includes(name) ? cur.filter((n) => n !== name) : [...cur, name];
    });
  };

  // --- Scatter pills ---
  const scatterActive = new Set(scatterOverride ?? snapshot.map((s) => s.sector_key));
  const toggleScatter = (key: string) => {
    setScatterOverride((prev) => {
      const cur = prev ?? [...scatterActive];
      return cur.includes(key) ? cur.filter((k) => k !== key) : [...cur, key];
    });
  };

  const sectorCount = typeof hero.sectors_covered === "string"
    ? hero.sectors_covered.split("/")[0] : hero.sectors_covered;

  const layout = (title: string, extra?: Record<string, any>): Record<string, any> => ({
    font: { family: "Inter, system-ui, sans-serif", color: c.text, size: 12 },
    paper_bgcolor: c.paper, plot_bgcolor: c.bg,
    margin: { l: 50, r: 20, t: 40, b: 50 },
    title: { text: title, font: { size: 14, color: c.text }, x: 0, xanchor: "left" },
    xaxis: { gridcolor: c.grid, zerolinecolor: c.grid, color: c.text },
    yaxis: { gridcolor: c.grid, zerolinecolor: c.grid, color: c.text },
    hoverlabel: { bgcolor: c.hover_bg, font: { color: c.hover_font, size: 12 } },
    legend: { font: { color: c.text } },
    ...extra,
  });

  // --- Hover helpers ---
  const matHover = (s: Snap) => {
    const q = QUALITY_LABEL[getQuality(s)];
    let t = `${s.sector}<br>Maturity: ${(s.maturity ?? 0).toFixed(1)} years<br>${q}`;
    if (s.fallback_peer_group) t += `<br>Peer group: ${s.fallback_peer_group.replace(/_/g, " ")}`;
    if (s.maturity_low_identification) t += `<br>Low identification`;
    return t + "<extra></extra>";
  };

  const billHover = (s: Snap) => {
    const bs = (s.bill_share ?? 0) * 100;
    let t = `${s.sector}<br>Bill share: ${bs.toFixed(1)}%`;
    if (s.bill_share_lower != null && s.bill_share_upper != null)
      t += ` [${(s.bill_share_lower * 100).toFixed(1)}, ${(s.bill_share_upper * 100).toFixed(1)}]`;
    const q = QUALITY_LABEL[getQuality(s)];
    t += `<br>${q}`;
    if (s.interval_origin) t += `<br>Interval: ${s.interval_origin.replace(/_/g, " ")}`;
    return t + "<extra></extra>";
  };

  const scatterFiltered = snapshot.filter(
    (s) => scatterActive.has(s.sector_key) && s.bill_share != null && s.maturity != null
  );

  // --- Pill rendering ---
  const TsPills = (
    <div className="pill-row">
      {tsNames.map((name) => (
        <button key={name}
          className={`pill${tsActive.has(name) ? ` pill-active pill-tier-${nameToTier[name] || "C"}` : ""}`}
          onClick={() => toggleTs(name)}>{name}</button>
      ))}
    </div>
  );

  const ScatterPills = (
    <div className="pill-row">
      {[...snapshot].sort((a, b) => a.sector.localeCompare(b.sector)).map((s) => (
        <button key={s.sector_key}
          className={`pill${scatterActive.has(s.sector_key) ? ` pill-active pill-tier-${s.maturity_tier}` : ""}`}
          onClick={() => toggleScatter(s.sector_key)}>{s.sector}</button>
      ))}
    </div>
  );

  return (
    <>
      <section className="hero">
        <div className="hero-inner">
          <div className="hero-label">Public-Data Research</div>
          <h1>Treasury Sector Maturity Estimation</h1>
          <p className="subtitle">
            Quarterly estimates of maturity structure, bill share, duration, and
            short-vs-long composition across U.S. Treasury holder sectors. All
            inputs are free public data.
          </p>
          <div className="stats-bar">
            <div className="stat"><div className="stat-value">{sectorCount}</div><div className="stat-label">Sectors</div></div>
            <div className="stat"><div className="stat-value">{hero.quarters}</div><div className="stat-label">Quarters</div></div>
            <div className="stat"><div className="stat-value">{hero.published_rows.toLocaleString()}</div><div className="stat-label">Estimates</div></div>
            <div className="stat"><div className="stat-value">{hero.data_sources}</div><div className="stat-label">Data Sources</div></div>
            <div className="stat"><div className="stat-value">{hero.snapshot_quarter}</div><div className="stat-label">Latest Snapshot</div></div>
          </div>
        </div>
      </section>

      <div className="page page-wide">
        {/* --- Maturity by Sector --- */}
        <section className="section">
          <h2>Maturity Estimate by Sector</h2>
          <p className="section-desc">
            Zero-coupon-equivalent maturity (years) at {hero.snapshot_quarter}.
            Colors indicate estimate quality.
          </p>
          <div className="chart-box">
            <Chart
              data={[{
                type: "bar", orientation: "h",
                y: sorted.map((s) => s.sector),
                x: sorted.map((s) => s.maturity),
                marker: { color: sorted.map((s) => qColor(getQuality(s))) },
                hovertemplate: sorted.map(matHover),
              }]}
              layout={layout("", {
                height: Math.max(500, sorted.length * 26),
                margin: { l: 260, r: 30, t: 10, b: 40 },
                xaxis: { title: "Years", gridcolor: c.grid, color: c.text },
                yaxis: { autorange: "reversed" as const, color: c.text },
              })}
              config={{ responsive: true, displayModeBar: false }}
              style={{ width: "100%" }}
            />
            <details className="chart-legend">
              <summary>What do the colors mean?</summary>
              <ul>
                <li><span className="quality-badge quality-high">High confidence</span> Direct calibration with security-level or survey-anchored data (Fed, Foreigners)</li>
                <li><span className="quality-badge quality-estimated">Estimated</span> Model-based estimate from revaluation behavior; uncertainty from peer-group envelope or calibrated bands</li>
                <li><span className="quality-badge quality-fallback">Peer fallback</span> No sector-specific signal; estimate is a peer-group median with envelope bounds</li>
              </ul>
            </details>
          </div>
        </section>

        {/* --- Bill Share by Sector --- */}
        {sortedByBill.length > 0 && (
          <section className="section">
            <h2>Bill Share by Sector</h2>
            <p className="section-desc">
              Estimated share of holdings in Treasury bills. Error bars show
              uncertainty intervals. Hover for interval details.
            </p>
            <div className="chart-box">
              <Chart
                data={[{
                  type: "bar", orientation: "h",
                  y: sortedByBill.map((s) => s.sector),
                  x: sortedByBill.map((s) => (s.bill_share ?? 0) * 100),
                  error_x: {
                    type: "data", symmetric: false,
                    array: sortedByBill.map((s) => {
                      if (s.bill_share_upper == null || s.bill_share == null) return 0;
                      const d = (s.bill_share_upper - s.bill_share) * 100;
                      return d > 0.1 ? d : 0;
                    }),
                    arrayminus: sortedByBill.map((s) => {
                      if (s.bill_share_lower == null || s.bill_share == null) return 0;
                      const d = (s.bill_share - s.bill_share_lower) * 100;
                      return d > 0.1 ? d : 0;
                    }),
                    color: c.text, thickness: 1.5,
                  },
                  marker: { color: sortedByBill.map((s) => qColor(getQuality(s))) },
                  hovertemplate: sortedByBill.map(billHover),
                }]}
                layout={layout("", {
                  height: Math.max(500, sortedByBill.length * 26),
                  margin: { l: 260, r: 30, t: 10, b: 40 },
                  xaxis: { title: "Bill Share (%)", gridcolor: c.grid, color: c.text },
                  yaxis: { autorange: "reversed" as const, color: c.text },
                })}
                config={{ responsive: true, displayModeBar: false }}
                style={{ width: "100%" }}
              />
            </div>
          </section>
        )}

        {/* --- Bill Share Over Time --- */}
        <section className="section">
          <h2>Bill Share Over Time</h2>
          <p className="section-desc">
            Historical bill-share estimates. Click sectors to show or hide.
          </p>
          {TsPills}
          <div className="chart-box">
            <Chart
              data={tsNames.filter((n) => tsActive.has(n)).map((name, i) => ({
                type: "scatter" as const, mode: "lines+markers" as const, name,
                x: time_series[name].dates,
                y: time_series[name].bill_share.map((v) => v != null ? v * 100 : null),
                line: { color: traces[i % traces.length], width: 2 },
                marker: { size: 3, color: traces[i % traces.length] },
                hovertemplate: `${name}<br>%{x}<br>Bill share: %{y:.1f}%<extra></extra>`,
              }))}
              layout={layout("", {
                height: 420,
                yaxis: { title: "Bill Share (%)", gridcolor: c.grid, color: c.text },
                xaxis: { gridcolor: c.grid, color: c.text },
                legend: { orientation: "h" as const, y: -0.15, font: { size: 10, color: c.text } },
              })}
              config={{ responsive: true, displayModeBar: true, displaylogo: false }}
              style={{ width: "100%" }}
            />
          </div>
        </section>

        {/* --- Maturity Over Time --- */}
        <section className="section">
          <h2>Maturity Structure Over Time</h2>
          <p className="section-desc">
            Zero-coupon-equivalent maturity (years). Click sectors to show or hide.
          </p>
          {TsPills}
          <div className="chart-box">
            <Chart
              data={tsNames.filter((n) => tsActive.has(n)).map((name, i) => ({
                type: "scatter" as const, mode: "lines+markers" as const, name,
                x: time_series[name].dates,
                y: time_series[name].maturity,
                line: { color: traces[i % traces.length], width: 2 },
                marker: { size: 3, color: traces[i % traces.length] },
                hovertemplate: `${name}<br>%{x}<br>Maturity: %{y:.2f} years<extra></extra>`,
              }))}
              layout={layout("", {
                height: 420,
                yaxis: { title: "Maturity (years)", gridcolor: c.grid, color: c.text },
                xaxis: { gridcolor: c.grid, color: c.text },
                legend: { orientation: "h" as const, y: -0.15, font: { size: 10, color: c.text } },
              })}
              config={{ responsive: true, displayModeBar: true, displaylogo: false }}
              style={{ width: "100%" }}
            />
          </div>
        </section>

        {/* --- Fed: Inferred vs Exact --- */}
        {fed_comparison.dates.length > 0 && (
          <section className="section">
            <h2>Federal Reserve: Inferred vs. Exact</h2>
            <p className="section-desc">
              The Fed is the only sector with security-level truth (SOMA). This
              shows how closely the inferred estimate tracks the directly observed
              portfolio.
            </p>
            <div className="chart-box">
              <Chart
                data={[
                  { type: "scatter", mode: "lines+markers", name: "Inferred",
                    x: fed_comparison.dates,
                    y: fed_comparison.inferred_bill_share.map((v) => v != null ? v * 100 : null),
                    line: { color: traces[1], width: 2 }, marker: { size: 4, color: traces[1] } },
                  { type: "scatter", mode: "lines+markers", name: "Exact (SOMA)",
                    x: fed_comparison.dates,
                    y: fed_comparison.exact_bill_share.map((v) => v != null ? v * 100 : null),
                    line: { color: traces[0], width: 2, dash: "dot" },
                    marker: { size: 4, symbol: "diamond", color: traces[0] } },
                ]}
                layout={layout("Bill Share: Inferred vs. Exact", {
                  height: 380,
                  yaxis: { title: "Bill Share (%)", gridcolor: c.grid, color: c.text },
                  xaxis: { gridcolor: c.grid, color: c.text },
                  legend: { orientation: "h" as const, y: -0.15, font: { size: 11, color: c.text } },
                })}
                config={{ responsive: true, displayModeBar: true, displaylogo: false }}
                style={{ width: "100%" }}
              />
            </div>
          </section>
        )}

        {/* --- SOMA Portfolio --- */}
        {somaData && somaData.dates.length > 0 && (
          <section className="section">
            <h2>Federal Reserve SOMA Portfolio</h2>
            <p className="section-desc">
              Direct security-level data from the Fed's System Open Market Account &mdash;
              the calibration truth set for the estimation pipeline.
            </p>
            <div className="chart-box">
              <Chart
                data={[
                  { type: "scatter", mode: "lines+markers", name: "Weighted-Average Maturity",
                    x: somaData.dates, y: somaData.wam_years,
                    line: { color: traces[0], width: 2 }, marker: { size: 4, color: traces[0] },
                    yaxis: "y", hovertemplate: "%{x}<br>WAM: %{y:.1f} years<extra></extra>" },
                  { type: "scatter", mode: "lines+markers", name: "Modified Duration",
                    x: somaData.dates, y: somaData.duration_years,
                    line: { color: traces[1], width: 2, dash: "dot" }, marker: { size: 4, color: traces[1] },
                    yaxis: "y", hovertemplate: "%{x}<br>Duration: %{y:.1f} years<extra></extra>" },
                  { type: "bar", name: "Holdings ($T)",
                    x: somaData.dates, y: somaData.holdings_trillions,
                    marker: { color: traces[3], opacity: 0.3 }, yaxis: "y2",
                    hovertemplate: "%{x}<br>Holdings: $%{y:.2f}T<extra></extra>" },
                ]}
                layout={layout("", {
                  height: 420,
                  yaxis: { title: "Years", gridcolor: c.grid, color: c.text },
                  yaxis2: { title: "Holdings ($T)", overlaying: "y", side: "right", color: c.text, showgrid: false },
                  xaxis: { gridcolor: c.grid, color: c.text },
                  legend: { orientation: "h" as const, y: -0.15, font: { size: 11, color: c.text } },
                  barmode: "overlay",
                })}
                config={{ responsive: true, displayModeBar: true, displaylogo: false }}
                style={{ width: "100%" }}
              />
            </div>
          </section>
        )}

        {/* --- Bill Share vs Maturity Scatter --- */}
        {snapshot.length > 0 && (
          <section className="section">
            <h2>Bill Share vs. Maturity</h2>
            <p className="section-desc">
              Each dot is a sector at the latest quarter. Size and color indicate
              estimate quality. Click sectors to show or hide.
            </p>
            {ScatterPills}
            <div className="chart-box">
              <Chart
                data={[{
                  type: "scatter", mode: "markers",
                  x: scatterFiltered.map((s) => (s.bill_share ?? 0) * 100),
                  y: scatterFiltered.map((s) => s.maturity),
                  marker: {
                    size: scatterFiltered.map((s) => getQuality(s) === "high" ? 16 : getQuality(s) === "fallback" ? 8 : 12),
                    color: scatterFiltered.map((s) => qColor(getQuality(s))),
                    line: { width: 1, color: c.bg },
                  },
                  hovertemplate: scatterFiltered.map((s) => {
                    const q = QUALITY_LABEL[getQuality(s)];
                    let t = `${s.sector}<br>Bill share: ${((s.bill_share ?? 0) * 100).toFixed(1)}%<br>Maturity: ${(s.maturity ?? 0).toFixed(2)} yrs<br>${q}`;
                    if (s.fallback_peer_group) t += `<br>Peer group: ${s.fallback_peer_group.replace(/_/g, " ")}`;
                    if (s.maturity_low_identification) t += `<br>Low identification`;
                    return t + "<extra></extra>";
                  }),
                }]}
                layout={{
                  font: { family: "Inter, system-ui, sans-serif", color: c.text, size: 12 },
                  paper_bgcolor: c.paper, plot_bgcolor: c.bg,
                  margin: { l: 60, r: 20, t: 20, b: 50 },
                  height: 450,
                  xaxis: { title: "Bill Share (%)", gridcolor: c.grid, color: c.text },
                  yaxis: { title: "Maturity (years)", gridcolor: c.grid, color: c.text },
                  hoverlabel: { bgcolor: c.hover_bg, font: { color: c.hover_font, size: 12 } },
                }}
                config={{ responsive: true, displayModeBar: true, displaylogo: false }}
                style={{ width: "100%" }}
              />
            </div>
          </section>
        )}

        {/* --- Snapshot Table --- */}
        <section className="section">
          <h2>Latest Snapshot</h2>
          <p className="section-desc">All sectors at {hero.snapshot_quarter}.</p>
          <div style={{ overflowX: "auto" }}>
            <table className="data-table">
              <thead>
                <tr>
                  <th>Sector</th>
                  <th>Maturity (yrs)</th>
                  <th>Bill Share</th>
                  <th>Estimate</th>
                  <th>Evidence</th>
                </tr>
              </thead>
              <tbody>
                {sorted.map((s) => {
                  const q = getQuality(s);
                  return (
                    <tr key={s.sector_key}>
                      <td>
                        {s.sector}
                        {s.maturity_low_identification && <span className="low-id" title={s.maturity_low_identification_reason?.replace(/_/g, " ") || "Low identification"}> *</span>}
                      </td>
                      <td>{s.maturity != null ? s.maturity.toFixed(2) : "\u2014"}</td>
                      <td>
                        {s.bill_share != null ? `${(s.bill_share * 100).toFixed(1)}%` : "\u2014"}
                        {s.bill_share_lower != null && s.bill_share_upper != null && (
                          <span className="low-id"> [{(s.bill_share_lower * 100).toFixed(0)}\u2013{(s.bill_share_upper * 100).toFixed(0)}]</span>
                        )}
                      </td>
                      <td><span className={`quality-badge quality-${q}`}>{QUALITY_LABEL[q]}</span></td>
                      <td><span className={`tier tier-${s.maturity_tier}`}>{s.maturity_tier || "\u2014"}</span></td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          <p className="chart-note" style={{ marginTop: "var(--sp-sm)" }}>
            * Low identification: sector lacks specific revaluation signal; shown as peer-group fallback or with wide uncertainty.
          </p>
        </section>

        {/* --- Nav --- */}
        <section className="section">
          <div className="nav-cards">
            <Link to="/methods" className="nav-card">
              <div className="nav-card-label">Methods</div>
              <p className="nav-card-desc">Pipeline, sources, how uncertainty enters</p>
            </Link>
            <Link to="/limitations" className="nav-card">
              <div className="nav-card-label">Limitations</div>
              <p className="nav-card-desc">Evidence quality, what is and isn't claimed</p>
            </Link>
          </div>
        </section>
      </div>
    </>
  );
}
