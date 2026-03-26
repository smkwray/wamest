#!/usr/bin/env python
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ALLOWED_PUBLIC_DOCS = {
    "docs/output_schema.md",
    "docs/full_coverage_output_schema.md",
    "docs/release_limitations.md",
    "docs/release_notes.md",
    "docs/revaluation_methodology.md",
    "docs/source_notes.md",
}
REQUIRED_GITIGNORE_ENTRIES = {
    "do/",
    "*.plan.md",
    "plan.md",
    "plans/",
    "data/external/raw/",
    "data/external/normalized/",
    "data/interim/",
    "data/processed/",
    "outputs/",
}
FORBIDDEN_TRACKED_PATH_PATTERNS = [
    re.compile(r"^do/"),
    re.compile(r"(^|/)\.env$"),
    re.compile(r"(^|/)plan\.md$"),
    re.compile(r"(^|/).*\.plan\.md$"),
    re.compile(r"^plans/"),
]
PUBLIC_TEXT_FILES = [
    "README.md",
    "LICENSE",
    "Makefile",
    "pyproject.toml",
    ".github/workflows/ci.yml",
    ".github/workflows/full-coverage-release.yml",
    ".github/workflows/public-preview.yml",
]
PUBLIC_TEXT_GLOBS = [
    "docs/*.md",
    "configs/*.yaml",
    "scripts/*.py",
    "src/**/*.py",
    "tests/*.py",
]
FORBIDDEN_TEXT_PATTERNS = [
    (re.compile(r"\bdo/"), "public files must not reference the internal do/ workspace"),
    (re.compile(r"\bsource\s+\.env\b"), "public files must not require a private .env file"),
    (re.compile(r"\$HOME/venvs/"), "public files must not depend on a home-directory-specific virtualenv path"),
    (re.compile(r"/Users/shanewray/"), "public files must not embed workstation-specific absolute paths"),
    (re.compile(r"\bhandoff\.md\b", flags=re.IGNORECASE), "public files must not reference internal handoff docs"),
    (re.compile(r"\btodo\.md\b", flags=re.IGNORECASE), "public files must not reference internal todo docs"),
    (re.compile(r"\bdontdo\.md\b", flags=re.IGNORECASE), "public files must not reference internal dontdo docs"),
    (re.compile(r"\bchanges\.md\b", flags=re.IGNORECASE), "public files must not reference internal change logs"),
    (re.compile(r"\borca\.md\b", flags=re.IGNORECASE), "public files must not reference internal orchestrator docs"),
    (re.compile(r"\bmako\.md\b", flags=re.IGNORECASE), "public files must not reference internal agent docs"),
    (re.compile(r"\bdairy\.md\b", flags=re.IGNORECASE), "public files must not reference internal agent docs"),
    (re.compile(r"\btandy\.md\b", flags=re.IGNORECASE), "public files must not reference internal agent docs"),
    (re.compile(r"\bAGENTS\.md\b"), "public files must not reference internal agent docs"),
    (re.compile(r"\bSTATUS\.md\b"), "public files must not reference internal status docs"),
]


def _public_text_paths() -> list[Path]:
    paths = [ROOT / relative for relative in PUBLIC_TEXT_FILES if (ROOT / relative).exists()]
    for pattern in PUBLIC_TEXT_GLOBS:
        paths.extend(path for path in ROOT.glob(pattern) if path.is_file())
    deduped: dict[Path, None] = {}
    for path in paths:
        if path == Path(__file__).resolve():
            continue
        deduped[path] = None
    return sorted(deduped)


def _check_allowed_docs(errors: list[str]) -> None:
    actual = {str(path.relative_to(ROOT)) for path in (ROOT / "docs").glob("*.md") if path.is_file()}
    unexpected = sorted(actual - ALLOWED_PUBLIC_DOCS)
    if unexpected:
        errors.append(
            "Unexpected public docs present outside the curated allowlist: " + ", ".join(unexpected)
        )


def _check_gitignore(errors: list[str]) -> None:
    gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    entries = {line.strip() for line in gitignore if line.strip() and not line.strip().startswith("#")}
    missing = sorted(REQUIRED_GITIGNORE_ENTRIES - entries)
    if missing:
        errors.append(".gitignore is missing required private-artifact entries: " + ", ".join(missing))


def _check_stray_files(errors: list[str]) -> None:
    tracked = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    ).stdout
    tracked_paths = [item for item in tracked.decode("utf-8", errors="replace").split("\0") if item]
    ds_store = sorted(path for path in tracked_paths if path.endswith(".DS_Store"))
    if ds_store:
        errors.append("Stray .DS_Store files must not be present: " + ", ".join(ds_store))
    forbidden = sorted(
        path
        for path in tracked_paths
        if any(pattern.search(path) for pattern in FORBIDDEN_TRACKED_PATH_PATTERNS)
    )
    if forbidden:
        errors.append("Tracked internal/private files must not be present: " + ", ".join(forbidden))


def _check_public_text(errors: list[str]) -> None:
    for path in _public_text_paths():
        text = path.read_text(encoding="utf-8")
        rel = str(path.relative_to(ROOT))
        for pattern, message in FORBIDDEN_TEXT_PATTERNS:
            if pattern.search(text):
                errors.append(f"{rel}: {message}")


def main() -> int:
    errors: list[str] = []
    _check_allowed_docs(errors)
    _check_gitignore(errors)
    _check_stray_files(errors)
    _check_public_text(errors)

    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    print("Public release hygiene checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
