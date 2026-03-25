#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

import pandas as pd
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from treasury_sector_maturity.ffiec002 import (
    SEARCH_EXPORT_COLUMNS,
    load_ffiec002_search_export,
    normalize_ffiec002_browser_bundle,
    summarize_ffiec002_foreign_banking_offices,
)
from treasury_sector_maturity.utils import dump_json, write_table


NPW_URL = "https://www.ffiec.gov/npw/"
US_STATE_CODES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL",
    "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
    "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
    "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]


def _parse_states(value: str) -> list[str]:
    return [item.strip().upper() for item in value.split(",") if item.strip()]


def _expand_more_options(page) -> None:
    trigger = page.get_by_text("More Options", exact=True)
    if trigger.count():
        try:
            trigger.first.click(timeout=2_000)
            page.wait_for_timeout(300)
        except Exception:
            pass


def _set_search_filters(page, state_code: str) -> None:
    page.evaluate(
        """([entityGroup, stateCode]) => {
            const fire = (el) => {
                el.dispatchEvent(new Event('input', { bubbles: true }));
                el.dispatchEvent(new Event('change', { bubbles: true }));
            };
            const setSelect = (selector, value) => {
                const el = document.querySelector(selector);
                if (!el) return;
                el.value = value;
                fire(el);
            };
            setSelect('#EntityGroups', entityGroup);
            setSelect('#States', stateCode);
        }""",
        ["USBA", state_code],
    )


def _download_search_results(page, state_code: str) -> pd.DataFrame | None:
    candidates = page.locator("a,button").filter(has_text="Search Results")
    if candidates.count() == 0:
        return None

    with tempfile.TemporaryDirectory(prefix=f"ffiec-search-{state_code.lower()}-") as tmpdir:
        tmp_path = Path(tmpdir) / f"{state_code}.csv"
        try:
            with page.expect_download(timeout=5_000) as info:
                candidates.first.click()
            info.value.save_as(str(tmp_path))
            return load_ffiec002_search_export(tmp_path)
        except Exception:
            return None


def _parse_search_results_table(page) -> pd.DataFrame:
    raw_rows = page.locator("table tbody tr").evaluate_all(
        """rows => rows.map((row) =>
            Array.from(row.querySelectorAll('td')).map((td) => (td.innerText || '').trim())
        )"""
    )
    parsed: list[dict[str, str]] = []
    for cells in raw_rows:
        if len(cells) < 8:
            continue
        if "no matching" in cells[0].lower():
            continue
        parsed.append(dict(zip(SEARCH_EXPORT_COLUMNS, cells[:8])))
    return pd.DataFrame(parsed, columns=SEARCH_EXPORT_COLUMNS)


def collect_usba_manifest(browser_url: str, states: list[str]) -> pd.DataFrame:
    state_frames: list[pd.DataFrame] = []
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(browser_url)
        if not browser.contexts:
            raise RuntimeError("Chrome CDP session has no browser contexts. Warm the browser manually first.")
        page = browser.contexts[0].new_page()
        try:
            for state_code in states:
                page.goto(NPW_URL, wait_until="domcontentloaded", timeout=30_000)
                page.wait_for_timeout(700)
                _expand_more_options(page)
                _set_search_filters(page, state_code)
                page.get_by_role("button", name="Search").click()
                page.wait_for_timeout(1_500)
                df = _download_search_results(page, state_code)
                if df is None or df.empty:
                    df = _parse_search_results_table(page)
                if not df.empty:
                    state_frames.append(df)
        finally:
            page.close()
            browser.close()

    if not state_frames:
        raise RuntimeError("No FFIEC search results were collected from the trusted Chrome session.")

    manifest = pd.concat(state_frames, ignore_index=True)
    manifest["RssdID"] = manifest["RssdID"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    manifest = manifest.drop_duplicates(subset=["RssdID"]).sort_values(["StateOrCountry", "InstitutionName"]).reset_index(drop=True)
    return manifest.loc[:, SEARCH_EXPORT_COLUMNS]


def fetch_reports(browser_url: str, manifest_df: pd.DataFrame, report_date: str, raw_dir: Path) -> list[dict[str, str]]:
    raw_dir.mkdir(parents=True, exist_ok=True)
    report_key = pd.Timestamp(report_date).strftime("%Y%m%d")
    missing: list[dict[str, str]] = []

    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(browser_url)
        if not browser.contexts:
            raise RuntimeError("Chrome CDP session has no browser contexts. Warm the browser manually first.")
        page = browser.contexts[0].new_page()
        try:
            for _, row in manifest_df.iterrows():
                rssd = str(row["RssdID"]).strip()
                target = raw_dir / f"FFIEC002_{rssd}_{report_key}.csv"
                if target.exists() and target.stat().st_size > 0:
                    continue

                page.goto(f"https://www.ffiec.gov/npw/Institution/Profile/{rssd}", wait_until="domcontentloaded", timeout=30_000)
                page.wait_for_timeout(800)

                section = page.locator("button").filter(has_text="FFIEC 002")
                if section.count():
                    try:
                        section.first.click(timeout=2_000)
                        page.wait_for_timeout(250)
                    except Exception:
                        pass

                link = page.locator(
                    f'a[href*="ReturnFinancialReportCSV"][href*="rpt=FFIEC002"][href*="dt={report_key}"]'
                ).first
                if link.count() == 0:
                    missing.append({"rssd": rssd, "reason": "missing_link"})
                    continue

                try:
                    with page.expect_download(timeout=15_000) as info:
                        link.click()
                    info.value.save_as(str(target))
                except PlaywrightTimeoutError:
                    missing.append({"rssd": rssd, "reason": "timeout"})
        finally:
            page.close()
            browser.close()

    return missing


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch FFIEC 002 reports through a trusted Chrome CDP session.")
    parser.add_argument("--browser-url", default="http://127.0.0.1:9222")
    parser.add_argument("--report-date", required=True, help="Quarter-end date, for example 2025-12-31")
    parser.add_argument("--search-export", help="Optional NIC search-results CSV. If omitted, the script builds the manifest through Chrome.")
    parser.add_argument("--states", default=",".join(US_STATE_CODES), help="Comma-separated state codes used when --search-export is omitted.")
    parser.add_argument("--manifest-out", default="data/external/ffiec_usba_manifest.csv")
    parser.add_argument("--raw-dir", default="data/external/ffiec002_reports")
    parser.add_argument("--missing-out", default="data/external/ffiec002_missing.json")
    parser.add_argument("--normalized-out", default="data/interim/ffiec002_normalized.csv")
    parser.add_argument("--aggregate-out", default="data/processed/bank_foreign_banking_offices_us.csv")
    args = parser.parse_args()

    if args.search_export:
        manifest_df = load_ffiec002_search_export(args.search_export)
    else:
        manifest_df = collect_usba_manifest(args.browser_url, _parse_states(args.states))

    write_table(manifest_df, args.manifest_out)
    print(f"Wrote {args.manifest_out}")

    missing = fetch_reports(args.browser_url, manifest_df, args.report_date, Path(args.raw_dir))
    dump_json({"report_date": args.report_date, "missing": missing}, args.missing_out)
    print(f"Wrote {args.missing_out}")

    normalized = normalize_ffiec002_browser_bundle(
        manifest_df=manifest_df,
        reports_dir=args.raw_dir,
        report_date=args.report_date,
        missing_records=missing,
    )
    aggregate = summarize_ffiec002_foreign_banking_offices(normalized)
    write_table(normalized, args.normalized_out)
    write_table(aggregate, args.aggregate_out)
    print(f"Wrote {args.normalized_out}")
    print(f"Wrote {args.aggregate_out}")


if __name__ == "__main__":
    main()
