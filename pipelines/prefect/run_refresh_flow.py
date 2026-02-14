"""Prefect flow to orchestrate smoke test, staging build, and mart build."""

from __future__ import annotations

import argparse
from datetime import date
import os
from pathlib import Path
import subprocess
import sys
from typing import Sequence

from prefect import flow, get_run_logger, task

REPO_ROOT = Path(__file__).resolve().parents[2]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run fund traceability refresh flow via Prefect.")
    parser.add_argument(
        "--as-of-date",
        default=date.today().isoformat(),
        help="Partition date in YYYY-MM-DD format (default: today).",
    )
    parser.add_argument("--max-depth", type=int, default=6, help="Max recursive trace depth.")
    parser.add_argument(
        "--skip-smoke-test",
        action="store_true",
        help="Skip DB smoke test step.",
    )
    return parser.parse_args()


def _run_subprocess(command: Sequence[str]) -> tuple[int, str, str]:
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode, result.stdout, result.stderr


@task(name="run-shell-step", retries=2, retry_delay_seconds=30)
def run_shell_step(name: str, command: list[str]) -> None:
    logger = get_run_logger()
    logger.info("Running step=%s command=%s", name, " ".join(command))
    return_code, stdout, stderr = _run_subprocess(command)
    if stdout.strip():
        logger.info(stdout.strip())
    if return_code != 0:
        if stderr.strip():
            logger.error(stderr.strip())
        raise RuntimeError(f"Step {name} failed with exit code {return_code}")


@flow(name="fund-traceability-refresh-all", log_prints=True)
def refresh_all_flow(as_of_date: str, max_depth: int = 6, run_smoke_test: bool = True) -> None:
    python_bin = sys.executable

    if run_smoke_test:
        run_shell_step("db_smoke_test", [python_bin, "pipelines/run_db_smoke_test.py"])

    run_shell_step(
        "build_staging",
        [python_bin, "pipelines/run_build_staging.py", "--as-of-date", as_of_date],
    )

    run_shell_step(
        "build_mart",
        [
            python_bin,
            "pipelines/run_build_mart.py",
            "--as-of-date",
            as_of_date,
            "--max-depth",
            str(max_depth),
        ],
    )


if __name__ == "__main__":
    args = _parse_args()
    refresh_all_flow(
        as_of_date=args.as_of_date,
        max_depth=max(1, args.max_depth),
        run_smoke_test=not args.skip_smoke_test,
    )
