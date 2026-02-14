"""Pipeline entrypoint: compute and write true exposure mart output."""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date
from pathlib import Path
import sys

import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

# Allow running directly from repo root without package installation.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from db.connections import create_traceability_mart_engine, create_traceability_staging_engine  # noqa: E402
from utils.validation import require_columns  # noqa: E402

SQL_MART_TABLES = Path(__file__).resolve().parents[1] / "sql" / "20_mart_tables.sql"
FUND_LIKE_TYPES = {"fund", "etf"}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build true exposure mart table from staging tables.")
    parser.add_argument(
        "--as-of-date",
        default=date.today().isoformat(),
        help="Partition date in YYYY-MM-DD format (default: today).",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=6,
        help="Maximum recursive depth for feeder/master traversal.",
    )
    return parser.parse_args()


def _split_sql_statements(sql_text: str) -> list[str]:
    statements: list[str] = []
    buffer: list[str] = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        buffer.append(line)
        if stripped.endswith(";"):
            statement = "\n".join(buffer).strip()
            if statement:
                statements.append(statement)
            buffer = []
    tail = "\n".join(buffer).strip()
    if tail:
        statements.append(tail)
    return statements


def _run_sql_file(engine: Engine, path: Path) -> None:
    sql_text = path.read_text(encoding="utf-8")
    statements = _split_sql_statements(sql_text)
    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))


def _load_partition(engine: Engine, table_name: str, as_of_date: str) -> pd.DataFrame:
    existing = set(inspect(engine).get_table_names())
    if table_name not in existing:
        return pd.DataFrame()
    query = text(f"SELECT * FROM {table_name} WHERE as_of_date = :as_of_date")
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params={"as_of_date": as_of_date})


def _build_edge_map(holdings_df: pd.DataFrame, links_df: pd.DataFrame) -> dict[str, list[tuple[str, float, str]]]:
    edges: dict[str, list[tuple[str, float, str]]] = defaultdict(list)

    for row in holdings_df.itertuples(index=False):
        fund_id = str(row.fund_id).strip()
        asset_id = str(row.asset_id).strip()
        asset_type = str(row.asset_type).strip().lower()
        weight = float(row.weight)
        if not fund_id or not asset_id or weight <= 0:
            continue
        edges[fund_id].append((asset_id, weight, asset_type))

    for row in links_df.itertuples(index=False):
        feeder = str(row.feeder_fund_id).strip()
        master = str(row.master_fund_id).strip()
        confidence = float(row.confidence) if row.confidence is not None else 1.0
        link_weight = confidence if 0 < confidence <= 1 else 1.0
        if feeder and master:
            edges[feeder].append((master, link_weight, "fund"))

    return edges


def _traverse_paths(
    root_fund_id: str,
    current_fund_id: str,
    edges: dict[str, list[tuple[str, float, str]]],
    max_depth: int,
    current_weight: float,
    depth: int,
    visiting: set[str],
    outputs: list[dict[str, object]],
) -> None:
    if depth >= max_depth:
        return

    for asset_id, edge_weight, asset_type in edges.get(current_fund_id, []):
        effective_weight = current_weight * edge_weight
        if effective_weight <= 0:
            continue

        next_depth = depth + 1
        expands = asset_type in FUND_LIKE_TYPES or asset_id in edges
        in_cycle = asset_id in visiting

        if expands and not in_cycle:
            _traverse_paths(
                root_fund_id=root_fund_id,
                current_fund_id=asset_id,
                edges=edges,
                max_depth=max_depth,
                current_weight=effective_weight,
                depth=next_depth,
                visiting=visiting | {asset_id},
                outputs=outputs,
            )
            continue

        outputs.append(
            {
                "root_fund_id": root_fund_id,
                "final_asset_id": asset_id,
                "effective_weight": effective_weight,
                "path_depth": next_depth,
            }
        )


def _compute_true_exposure(holdings_df: pd.DataFrame, links_df: pd.DataFrame, max_depth: int) -> pd.DataFrame:
    output_columns = ["root_fund_id", "final_asset_id", "effective_weight", "path_depth"]
    if holdings_df.empty and links_df.empty:
        return pd.DataFrame(columns=output_columns)

    edges = _build_edge_map(holdings_df, links_df)
    root_funds = set(links_df["feeder_fund_id"].astype(str)) if not links_df.empty else set(edges.keys())
    root_funds |= set(holdings_df["fund_id"].astype(str)) if not holdings_df.empty else set()
    root_funds = {fund_id.strip() for fund_id in root_funds if fund_id and fund_id.strip()}

    rows: list[dict[str, object]] = []
    for root_fund in sorted(root_funds):
        _traverse_paths(
            root_fund_id=root_fund,
            current_fund_id=root_fund,
            edges=edges,
            max_depth=max_depth,
            current_weight=1.0,
            depth=0,
            visiting={root_fund},
            outputs=rows,
        )

    if not rows:
        return pd.DataFrame(columns=output_columns)

    exposure_df = pd.DataFrame(rows)
    exposure_df = (
        exposure_df.groupby(["root_fund_id", "final_asset_id"], as_index=False)
        .agg(effective_weight=("effective_weight", "sum"), path_depth=("path_depth", "max"))
        .sort_values(by=["root_fund_id", "effective_weight"], ascending=[True, False])
        .reset_index(drop=True)
    )
    return exposure_df[output_columns]


def _delete_partition(engine: Engine, table_name: str, as_of_date: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(f"DELETE FROM {table_name} WHERE as_of_date = :as_of_date"),
            {"as_of_date": as_of_date},
        )


def _write_partition(engine: Engine, as_of_date: str, exposure_df: pd.DataFrame) -> int:
    table_name = "mart_true_exposure"
    _delete_partition(engine, table_name, as_of_date)
    if exposure_df.empty:
        return 0
    to_write = exposure_df.copy()
    to_write["as_of_date"] = as_of_date
    to_write.to_sql(table_name, engine, if_exists="append", index=False, method="multi")
    return len(to_write)


def main() -> int:
    args = _parse_args()
    as_of_date = args.as_of_date
    max_depth = max(1, args.max_depth)

    staging_engine = create_traceability_staging_engine()
    mart_engine = create_traceability_mart_engine()

    _run_sql_file(mart_engine, SQL_MART_TABLES)

    holdings_df = _load_partition(staging_engine, "stg_holdings", as_of_date)
    links_df = _load_partition(staging_engine, "stg_fund_links", as_of_date)

    if not holdings_df.empty:
        require_columns(holdings_df, {"fund_id", "asset_id", "asset_type", "weight"})
    if not links_df.empty:
        require_columns(links_df, {"feeder_fund_id", "master_fund_id", "confidence"})

    exposure_df = _compute_true_exposure(holdings_df, links_df, max_depth=max_depth)
    written_rows = _write_partition(mart_engine, as_of_date, exposure_df)

    print(
        "run_build_mart completed",
        f"as_of_date={as_of_date}",
        f"max_depth={max_depth}",
        f"rows(stg_holdings)={len(holdings_df)}",
        f"rows(stg_fund_links)={len(links_df)}",
        f"rows(mart_true_exposure)={written_rows}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
