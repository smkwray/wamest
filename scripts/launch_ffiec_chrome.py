#!/usr/bin/env python
from __future__ import annotations

import argparse
import os
import platform
import subprocess
from pathlib import Path


def default_chrome_path() -> str:
    system = platform.system()
    if system == "Darwin":
        return "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    if system == "Windows":
        return r"C:\Program Files\Google\Chrome\Application\chrome.exe"
    return "google-chrome"


def main() -> None:
    parser = argparse.ArgumentParser(description="Launch a dedicated Chrome session for FFIEC CDP automation.")
    parser.add_argument("--chrome-path", default=default_chrome_path())
    parser.add_argument("--port", type=int, default=9222)
    parser.add_argument("--profile-dir", default=str(Path.home() / ".cache" / "ffiec-chrome-profile"))
    parser.add_argument("--start-url", default="https://www.ffiec.gov/npw/")
    args = parser.parse_args()

    profile_dir = Path(os.path.expanduser(args.profile_dir))
    profile_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        args.chrome_path,
        f"--remote-debugging-port={args.port}",
        f"--user-data-dir={profile_dir}",
        args.start_url,
    ]
    subprocess.Popen(cmd)
    print(f"Launched Chrome on port {args.port}")
    print(f"Profile dir: {profile_dir}")
    print("Warm the NIC session manually in that browser before running the FFIEC 002 fetcher.")


if __name__ == "__main__":
    main()
