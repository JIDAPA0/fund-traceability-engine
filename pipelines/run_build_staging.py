"""Pipeline entrypoint: extract + normalize + write traceability staging tables."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
import sys

import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

# Allow running directly from repo root without package installation.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from db.connections import create_global_raw_engine, create_traceability_staging_engine  # noqa: E402
from transform.normalize.currency_normalizer import normalize_currency  # noqa: E402
from utils.validation import require_columns  # noqa: E402

SQL_STAGING_TABLES = Path(__file__).resolve().parents[1] / "sql" / "10_staging_tables.sql"

FUND_TABLE_CANDIDATES = ["raw_funds", "funds", "global_funds", "master_funds", "fund_master"]
HOLDINGS_TABLE_CANDIDATES = ["raw_holdings", "holdings", "global_holdings", "fund_holdings"]
LINK_TABLE_CANDIDATES = ["raw_fund_links", "fund_links", "feeder_master_links", "feeder_master_map"]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build traceability staging tables from global raw data.")
    parser.add_argument(
        "--as-of-date",
        default=date.today().isoformat(),
        help="Partition date in YYYY-MM-DD format (default: today).",
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


def _load_first_existing_table(engine: Engine, candidates: list[str]) -> tuple[pd.DataFrame, str | None]:
    existing = set(inspect(engine).get_table_names())
    for table_name in candidates:
        if table_name in existing:
            return pd.read_sql_table(table_name, engine), table_name
    return pd.DataFrame(), None


def _pick_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    by_lower = {col.lower(): col for col in df.columns}
    for candidate in candidates:
        if candidate.lower() in by_lower:
            return by_lower[candidate.lower()]
    return None


def _normalize_funds(raw_df: pd.DataFrame, as_of_date: str) -> pd.DataFrame:
    output_columns = ["fund_id", "fund_name", "source", "currency", "as_of_date"]
    if raw_df.empty:
        return pd.DataFrame(columns=output_columns)

    fund_id_col = _pick_column(raw_df, ["fund_id", "id", "master_fund_id", "isin", "fund_code", "ticker"])
    if fund_id_col is None:
        return pd.DataFrame(columns=output_columns)

    name_col = _pick_column(raw_df, ["fund_name", "name", "master_fund_name", "fund_title"])
    source_col = _pick_column(raw_df, ["source", "source_system", "provider"])
    currency_col = _pick_column(raw_df, ["currency", "currency_code", "ccy"])

    fund_ids = raw_df[fund_id_col].fillna("").astype(str).str.strip()
    fund_names = (
        raw_df[name_col].fillna("").astype(str).str.strip()
        if name_col is not None
        else raw_df[fund_id_col].fillna("").astype(str).str.strip()
    )
    sources = (
        raw_df[source_col].fillna("global").astype(str).str.strip().str.lower()
        if source_col is not None
        else pd.Series("global", index=raw_df.index)
    )
    currencies = (
        raw_df[currency_col].fillna("").astype(str).map(normalize_currency)
        if currency_col is not None
        else pd.Series("", index=raw_df.index)
    )

    normalized = pd.DataFrame(
        {
            "fund_id": fund_ids,
            "fund_name": fund_names.replace({"": pd.NA}).fillna(fund_ids),
            "source": sources.replace({"": "global"}),
            "currency": currencies,
            "as_of_date": as_of_date,
        }
    )
    normalized = normalized[normalized["fund_id"] != ""]
    normalized = normalized.drop_duplicates(subset=["fund_id"]).reset_index(drop=True)
    return normalized[output_columns]


def _normalize_holdings(raw_df: pd.DataFrame, as_of_date: str, known_fund_ids: set[str]) -> pd.DataFrame:
    output_columns = ["fund_id", "asset_id", "asset_name", "asset_type", "weight", "as_of_date"]
    if raw_df.empty:
        return pd.DataFrame(columns=output_columns)

    fund_id_col = _pick_column(raw_df, ["fund_id", "master_fund_id", "parent_fund_id", "portfolio_id"])
    asset_id_col = _pick_column(raw_df, ["asset_id", "holding_id", "ticker", "symbol", "security_id"])
    weight_col = _pick_column(raw_df, ["weight", "holding_weight", "allocation", "pct", "percentage"])
    asset_name_col = _pick_column(raw_df, ["asset_name", "holding_name", "security_name", "name"])
    asset_type_col = _pick_column(raw_df, ["asset_type", "holding_type", "security_type", "type"])

    if not fund_id_col or not asset_id_col or not weight_col:
        return pd.DataFrame(columns=output_columns)

    weights = pd.to_numeric(raw_df[weight_col], errors="coerce").fillna(0.0)
    if not weights.empty and weights.max() > 1.0:
        weights = weights / 100.0

    asset_ids = raw_df[asset_id_col].fillna("").astype(str).str.strip()
    inferred_types = asset_ids.map(lambda asset_id: "fund" if asset_id in known_fund_ids else "other")

    normalized = pd.DataFrame(
        {
            "fund_id": raw_df[fund_id_col].fillna("").astype(str).str.strip(),
            "asset_id": asset_ids,
            "asset_name": (
                raw_df[asset_name_col].fillna("").astype(str).str.strip()
                if asset_name_col is not None
                else asset_ids
            ),
            "asset_type": (
                raw_df[asset_type_col].fillna("").astype(str).str.strip().str.lower()
                if asset_type_col is not None
                else inferred_types
            ),
            "weight": weights.clip(lower=0.0, upper=1.0),
            "as_of_date": as_of_date,
        }
    )
    normalized["asset_type"] = normalized["asset_type"].replace({"": pd.NA}).fillna(inferred_types)
    normalized = normalized[(normalized["fund_id"] != "") & (normalized["asset_id"] != "")]
    normalized = normalized.drop_duplicates(subset=["fund_id", "asset_id"]).reset_index(drop=True)
    return normalized[output_columns]


def _normalize_links(raw_df: pd.DataFrame, as_of_date: str) -> pd.DataFrame:
    output_columns = ["feeder_fund_id", "master_fund_id", "confidence", "as_of_date"]
    if raw_df.empty:
        return pd.DataFrame(columns=output_columns)

    feeder_col = _pick_column(raw_df, ["feeder_fund_id", "feeder_id", "thai_fund_id", "fund_id"])
    master_col = _pick_column(raw_df, ["master_fund_id", "master_id", "target_fund_id", "linked_fund_id"])
    confidence_col = _pick_column(raw_df, ["confidence", "score", "match_score"])

    if not feeder_col or not master_col:
        return pd.DataFrame(columns=output_columns)

    confidence = (
        pd.to_numeric(raw_df[confidence_col], errors="coerce").fillna(1.0)
        if confidence_col is not None
        else pd.Series(1.0, index=raw_df.index)
    )

    normalized = pd.DataFrame(
        {
            "feeder_fund_id": raw_df[feeder_col].fillna("").astype(str).str.strip(),
            "master_fund_id": raw_df[master_col].fillna("").astype(str).str.strip(),
            "confidence": confidence.clip(lower=0.0, upper=1.0),
            "as_of_date": as_of_date,
        }
    )
    normalized = normalized[
        (normalized["feeder_fund_id"] != "") & (normalized["master_fund_id"] != "")
    ]
    normalized = normalized.drop_duplicates(subset=["feeder_fund_id", "master_fund_id"]).reset_index(drop=True)
    return normalized[output_columns]


def _delete_partition(engine: Engine, table_name: str, as_of_date: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(f"DELETE FROM {table_name} WHERE as_of_date = :as_of_date"),
            {"as_of_date": as_of_date},
        )


def _write_partition(engine: Engine, table_name: str, as_of_date: str, df: pd.DataFrame) -> int:
    _delete_partition(engine, table_name, as_of_date)
    if df.empty:
        return 0
    df.to_sql(table_name, engine, if_exists="append", index=False, method="multi")
    return len(df)


def main() -> int:
    args = _parse_args()
    as_of_date = args.as_of_date

    source_engine = create_global_raw_engine()
    staging_engine = create_traceability_staging_engine()

    _run_sql_file(staging_engine, SQL_STAGING_TABLES)

    raw_funds_df, funds_source = _load_first_existing_table(source_engine, FUND_TABLE_CANDIDATES)
    raw_holdings_df, holdings_source = _load_first_existing_table(source_engine, HOLDINGS_TABLE_CANDIDATES)
    raw_links_df, links_source = _load_first_existing_table(source_engine, LINK_TABLE_CANDIDATES)

    funds_df = _normalize_funds(raw_funds_df, as_of_date)
    fund_ids = set(funds_df["fund_id"].tolist())
    holdings_df = _normalize_holdings(raw_holdings_df, as_of_date, fund_ids)
    links_df = _normalize_links(raw_links_df, as_of_date)

    require_columns(funds_df, {"fund_id", "fund_name", "source", "currency", "as_of_date"})
    require_columns(holdings_df, {"fund_id", "asset_id", "asset_type", "weight", "as_of_date"})
    require_columns(links_df, {"feeder_fund_id", "master_fund_id", "confidence", "as_of_date"})

    funds_rows = _write_partition(staging_engine, "stg_funds", as_of_date, funds_df)
    holdings_rows = _write_partition(staging_engine, "stg_holdings", as_of_date, holdings_df)
    links_rows = _write_partition(staging_engine, "stg_fund_links", as_of_date, links_df)

    print(
        "run_build_staging completed",
        f"as_of_date={as_of_date}",
        f"funds_source={funds_source}",
        f"holdings_source={holdings_source}",
        f"links_source={links_source}",
        f"rows(stg_funds)={funds_rows}",
        f"rows(stg_holdings)={holdings_rows}",
        f"rows(stg_fund_links)={links_rows}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
