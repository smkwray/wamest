from __future__ import annotations

import argparse

from .utils import project_root


def main() -> None:
    parser = argparse.ArgumentParser(description="Treasury sector maturity research-preview helper")
    parser.add_argument(
        "--show-root",
        action="store_true",
        help="Print the detected project root and exit.",
    )
    args = parser.parse_args()

    if args.show_root:
        print(project_root())
    else:
        print("Treasury sector maturity research preview is installed. Use the documented scripts under scripts/.")


if __name__ == "__main__":
    main()
