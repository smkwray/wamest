import { createContext, useContext, useEffect, useState, type ReactNode } from "react";

type Theme = "light" | "dark";

interface ThemeCtx {
  theme: Theme;
  toggle: () => void;
}

const Ctx = createContext<ThemeCtx>({ theme: "light", toggle: () => {} });

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [theme, setTheme] = useState<Theme>(() => {
    const stored = localStorage.getItem("wamest-theme");
    if (stored === "dark" || stored === "light") return stored;
    return window.matchMedia?.("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  });

  useEffect(() => {
    document.documentElement.setAttribute("data-theme", theme);
    localStorage.setItem("wamest-theme", theme);
  }, [theme]);

  const toggle = () => setTheme((t) => (t === "light" ? "dark" : "light"));

  return <Ctx.Provider value={{ theme, toggle }}>{children}</Ctx.Provider>;
}

export function useTheme() {
  return useContext(Ctx);
}

/* Plotly layout colors keyed by theme */
export function plotlyColors(theme: Theme) {
  if (theme === "dark") {
    return {
      bg: "#1a1f2e",
      paper: "#1a1f2e",
      text: "#dce0e8",
      grid: "#2a3040",
      accent: "#d4b05c",
      accent2: "#5b9bd5",
      accent3: "#e07050",
      accent4: "#6bc47a",
      accent5: "#c27abf",
      accent6: "#e0a050",
      accent7: "#50b0c0",
      hover_bg: "#2a3040",
      hover_font: "#ffffff",
    };
  }
  return {
    bg: "#ffffff",
    paper: "#ffffff",
    text: "#2c2c2c",
    grid: "#e8e6e2",
    accent: "#b8963e",
    accent2: "#3a72a4",
    accent3: "#c05030",
    accent4: "#3a8a4a",
    accent5: "#8a4a8a",
    accent6: "#c07830",
    accent7: "#2a8a8a",
    hover_bg: "#1a2332",
    hover_font: "#ffffff",
  };
}

export const TRACE_COLORS = [
  "#b8963e", "#3a72a4", "#c05030", "#3a8a4a", "#8a4a8a", "#c07830", "#2a8a8a",
];

export const TRACE_COLORS_DARK = [
  "#d4b05c", "#5b9bd5", "#e07050", "#6bc47a", "#c27abf", "#e0a050", "#50b0c0",
];
