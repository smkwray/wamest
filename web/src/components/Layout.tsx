import { NavLink, Outlet, useLocation } from "react-router-dom";
import { useEffect, useState } from "react";
import { useTheme } from "../theme";

const NAV = [
  { to: "/", label: "Results" },
  { to: "/methods", label: "Methods" },
  { to: "/limitations", label: "Limitations" },
];

export default function Layout() {
  const { pathname } = useLocation();
  const { theme, toggle } = useTheme();
  const [menuOpen, setMenuOpen] = useState(false);

  useEffect(() => {
    setMenuOpen(false);
    window.scrollTo(0, 0);
  }, [pathname]);

  return (
    <div className="site">
      <header className="site-header">
        <div className="header-inner">
          <NavLink to="/" className="wordmark">WAMEST</NavLink>
          <button className="menu-toggle" onClick={() => setMenuOpen(!menuOpen)} aria-label="Menu">
            <span /><span /><span />
          </button>
          <nav className={menuOpen ? "open" : ""}>
            {NAV.map((n) => (
              <NavLink key={n.to} to={n.to} end={n.to === "/"} className={({ isActive }) => isActive ? "active" : ""}>
                {n.label}
              </NavLink>
            ))}
            <a href="https://github.com/smkwray/wamest" target="_blank" rel="noopener noreferrer" className="gh-link">
              GitHub
            </a>
            <button className="theme-btn" onClick={toggle} aria-label="Toggle theme">
              {theme === "light" ? "\u263E" : "\u2600"}
            </button>
          </nav>
        </div>
      </header>
      <main><Outlet /></main>
      <footer className="site-footer">
        <div className="footer-inner">
          <p>WAMEST is an independent public-data research project. Not affiliated with or endorsed by any government agency.</p>
          <p className="footer-links">
            <a href="https://github.com/smkwray/wamest" target="_blank" rel="noopener noreferrer">Source</a>
            <span className="sep">&middot;</span>
            <a href="https://github.com/smkwray/wamest/releases/tag/v0.1.0" target="_blank" rel="noopener noreferrer">v0.1 Release</a>
            <span className="sep">&middot;</span>
            <span>Data: Federal Reserve Z.1 &middot; H.15 &middot; SOMA &middot; TIC SHL/SLT</span>
          </p>
        </div>
      </footer>
    </div>
  );
}
