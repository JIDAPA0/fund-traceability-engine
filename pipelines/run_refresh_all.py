"""Pipeline entrypoint: run all jobs in sequence."""

from __future__ import annotations

from run_build_mart import main as build_mart
from run_build_staging import main as build_staging


def main() -> None:
    build_staging()
    build_mart()


if __name__ == "__main__":
    main()
