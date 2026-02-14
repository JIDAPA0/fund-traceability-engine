"""Pipeline entrypoint: run smoke test, staging build, and mart build in sequence."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
import subprocess
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run full fund traceability refresh sequence.")
    parser.add_argument(
        "--as-of-date",
        default=date.today().isoformat(),
        help="Partition date in YYYY-MM-DD format (default: today).",
    )
    parser.add_argument("--max-depth", type=int, default=6, help="Max recursive trace depth.")
    parser.add_argument("--skip-smoke-test", action="store_true", help="Skip DB smoke test step.")
    return parser.parse_args()


def _run_step(step_name: str, command: list[str]) -> None:
    print(f"[RUN] {step_name}: {' '.join(command)}", flush=True)
    subprocess.run(command, cwd=REPO_ROOT, check=True)


def main() -> int:
    args = _parse_args()
    python_bin = sys.executable

    if not args.skip_smoke_test:
        _run_step("db_smoke_test", [python_bin, "pipelines/run_db_smoke_test.py"])

    _run_step(
        "build_staging",
        [python_bin, "pipelines/run_build_staging.py", "--as-of-date", args.as_of_date],
    )
    _run_step(
        "build_mart",
        [
            python_bin,
            "pipelines/run_build_mart.py",
            "--as-of-date",
            args.as_of_date,
            "--max-depth",
            str(max(1, args.max_depth)),
        ],
    )
    print("run_refresh_all completed", f"as_of_date={args.as_of_date}", f"max_depth={max(1, args.max_depth)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
