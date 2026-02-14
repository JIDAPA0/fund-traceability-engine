"""Pipeline utility: verify configured MySQL databases are reachable."""

from __future__ import annotations

from pathlib import Path
import sys

from sqlalchemy import text

# Allow running directly from repo root without package installation.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from db.connections import (  # noqa: E402
    create_global_mart_engine,
    create_global_raw_engine,
    create_global_staging_engine,
    create_traceability_mart_engine,
    create_traceability_staging_engine,
)


def _check(name: str, engine_factory) -> tuple[str, bool, str]:
    try:
        engine = engine_factory()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return name, True, "ok"
    except Exception as exc:  # intentionally broad for infra checks
        return name, False, str(exc)


def main() -> int:
    checks = [
        ("global_raw", create_global_raw_engine),
        ("global_staging", create_global_staging_engine),
        ("global_mart", create_global_mart_engine),
        ("traceability_staging", create_traceability_staging_engine),
        ("traceability_mart", create_traceability_mart_engine),
    ]

    failed = False
    for name, factory in checks:
        db_name, ok, detail = _check(name, factory)
        status = "PASS" if ok else "FAIL"
        print(f"[{status}] {db_name}: {detail}")
        if not ok:
            failed = True

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
