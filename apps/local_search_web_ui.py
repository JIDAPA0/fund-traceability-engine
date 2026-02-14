"""Fund traceability local web UI (Streamlit).

Features:

- Dashboard: freshness, row counts, and quick profiling
- Explorer: browse staging tables with search + filters
- Bidirectional search:
  - Fund -> assets (direct + true exposure)
  - Asset -> funds (who can buy / exposure)
- Top 10 views with bar charts

This app reads from:

- Staging DB: stg_funds, stg_holdings, stg_fund_links
- Mart DB: mart_true_exposure
"""

from __future__ import annotations

from collections import Counter, deque
import math
import re
from pathlib import Path
import sys
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st
from sqlalchemy import text

# Allow running directly from repo root without package installation.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from db.connections import create_traceability_mart_engine, create_traceability_staging_engine  # noqa: E402


DATASET_SPECS: dict[str, dict[str, Any]] = {
    "funds": {
        "label": "Funds",
        "from_sql": "FROM stg_funds t",
        "columns": [
            ("fund_id", "t.fund_id", "Fund ID"),
            ("fund_name", "t.fund_name", "Fund Name"),
            ("source", "t.source", "Source"),
            ("currency", "t.currency", "Currency"),
            ("as_of_date", "t.as_of_date", "As Of"),
            ("loaded_at", "t.loaded_at", "Loaded At"),
        ],
        "search_exprs": ["t.fund_id", "t.fund_name", "t.source", "t.currency"],
        "as_of_expr": "t.as_of_date",
        "source_expr": "t.source",
        "default_sort": "fund_id",
    },
    "holdings": {
        "label": "Holdings",
        "from_sql": (
            "FROM stg_holdings h "
            "LEFT JOIN stg_funds f ON h.fund_id = f.fund_id AND h.as_of_date = f.as_of_date"
        ),
        "columns": [
            ("fund_id", "h.fund_id", "Fund ID"),
            ("fund_name", "f.fund_name", "Fund Name"),
            ("source", "f.source", "Source"),
            ("asset_id", "h.asset_id", "Asset ID"),
            ("asset_name", "h.asset_name", "Asset Name"),
            ("asset_type", "h.asset_type", "Asset Type"),
            ("weight", "h.weight", "Weight"),
            ("as_of_date", "h.as_of_date", "As Of"),
        ],
        "search_exprs": ["h.fund_id", "f.fund_name", "f.source", "h.asset_id", "h.asset_name", "h.asset_type"],
        "as_of_expr": "h.as_of_date",
        "source_expr": "f.source",
        "default_sort": "fund_id",
    },
    "links": {
        "label": "Links",
        "from_sql": (
            "FROM stg_fund_links l "
            "LEFT JOIN stg_funds ff ON l.feeder_fund_id = ff.fund_id AND l.as_of_date = ff.as_of_date "
            "LEFT JOIN stg_funds mf ON l.master_fund_id = mf.fund_id AND l.as_of_date = mf.as_of_date"
        ),
        "columns": [
            ("feeder_fund_id", "l.feeder_fund_id", "Feeder Fund ID"),
            ("feeder_name", "ff.fund_name", "Feeder Name"),
            ("master_fund_id", "l.master_fund_id", "Master Fund ID"),
            ("master_name", "mf.fund_name", "Master Name"),
            ("confidence", "l.confidence", "Confidence"),
            ("as_of_date", "l.as_of_date", "As Of"),
            ("loaded_at", "l.loaded_at", "Loaded At"),
        ],
        "search_exprs": [
            "l.feeder_fund_id",
            "ff.fund_name",
            "l.master_fund_id",
            "mf.fund_name",
        ],
        "as_of_expr": "l.as_of_date",
        "source_expr": "COALESCE(ff.source, mf.source)",
        "default_sort": "feeder_fund_id",
    },
}

FUND_LIKE_TYPES = {"fund", "etf"}


def _inject_css() -> None:
    st.markdown(
        """
        <style>
          .stApp {
            background: linear-gradient(150deg, #f8fbff 0%, #eef4ff 45%, #f4fbf8 100%);
          }
          .hero {
            background: radial-gradient(circle at 8% 15%, #bfdbfe 0%, rgba(191,219,254,0) 35%),
                        radial-gradient(circle at 92% 20%, #a7f3d0 0%, rgba(167,243,208,0) 38%),
                        linear-gradient(135deg, #0f172a 0%, #1e293b 40%, #1e3a8a 100%);
            border-radius: 18px;
            padding: 20px 22px;
            color: #f8fafc;
            border: 1px solid rgba(148, 163, 184, 0.35);
            box-shadow: 0 14px 40px rgba(15, 23, 42, 0.25);
            margin-bottom: 12px;
          }
          .hero h1 {
            margin: 0;
            font-size: 1.65rem;
            line-height: 1.2;
          }
          .hero p {
            margin: 0.45rem 0 0 0;
            color: #dbeafe;
            max-width: 980px;
          }
          .top-card {
            background: rgba(255,255,255,0.92);
            border: 1px solid #d6e3ff;
            border-radius: 14px;
            padding: 14px 16px;
            box-shadow: 0 10px 28px rgba(15, 23, 42, 0.06);
          }
          .top-card .value {
            font-size: 1.55rem;
            font-weight: 700;
            color: #0f172a;
            margin-bottom: 0.1rem;
          }
          .top-card .label {
            color: #334155;
            font-size: 0.9rem;
          }
          .small-note {
            color: #64748b;
            font-size: 0.9rem;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_resource(show_spinner=False)
def _staging_engine():
    return create_traceability_staging_engine()


@st.cache_resource(show_spinner=False)
def _mart_engine():
    return create_traceability_mart_engine()


@st.cache_data(ttl=20, show_spinner=False)
def _list_as_of_dates() -> list[str]:
    query = text(
        """
        SELECT DISTINCT as_of_date
        FROM (
            SELECT as_of_date FROM stg_funds
            UNION
            SELECT as_of_date FROM stg_holdings
            UNION
            SELECT as_of_date FROM stg_fund_links
        ) d
        WHERE as_of_date IS NOT NULL
        ORDER BY as_of_date DESC
        """
    )
    with _staging_engine().connect() as conn:
        return [str(r[0]) for r in conn.execute(query).fetchall()]


def _default_as_of(as_of_dates: list[str]) -> str:
    return as_of_dates[0] if as_of_dates else "All"


@st.cache_data(ttl=20, show_spinner=False)
def _list_sources(as_of_date: str) -> list[str]:
    params: dict[str, Any] = {}
    where = "WHERE source IS NOT NULL AND TRIM(source) <> ''"
    if as_of_date != "All":
        where += " AND as_of_date = :as_of_date"
        params["as_of_date"] = as_of_date

    query = text(
        f"""
        SELECT DISTINCT source
        FROM stg_funds
        {where}
        ORDER BY source
        """
    )
    with _staging_engine().connect() as conn:
        values = [str(r[0]) for r in conn.execute(query, params if params else None).fetchall()]
    return ["All"] + values


@st.cache_data(ttl=20, show_spinner=False)
def _count_cards(as_of_date: str) -> dict[str, int]:
    params = {"as_of_date": as_of_date}
    filters = "" if as_of_date == "All" else "WHERE as_of_date = :as_of_date"

    out: dict[str, int] = {}
    with _staging_engine().connect() as conn:
        out["stg_funds"] = int(
            conn.execute(
                text(f"SELECT COUNT(*) FROM stg_funds {filters}"),
                params if as_of_date != "All" else None,
            ).scalar_one()
        )
        out["stg_holdings"] = int(
            conn.execute(
                text(f"SELECT COUNT(*) FROM stg_holdings {filters}"),
                params if as_of_date != "All" else None,
            ).scalar_one()
        )
        out["stg_fund_links"] = int(
            conn.execute(
                text(f"SELECT COUNT(*) FROM stg_fund_links {filters}"),
                params if as_of_date != "All" else None,
            ).scalar_one()
        )

    # Mart may be empty/not created yet.
    try:
        with _mart_engine().connect() as conn:
            mart_filters = "" if as_of_date == "All" else "WHERE as_of_date = :as_of_date"
            out["mart_true_exposure"] = int(
                conn.execute(
                    text(f"SELECT COUNT(*) FROM mart_true_exposure {mart_filters}"),
                    params if as_of_date != "All" else None,
                ).scalar_one()
            )
    except Exception:
        out["mart_true_exposure"] = 0

    return out


@st.cache_data(ttl=20, show_spinner=False)
def _freshness(as_of_date: str) -> pd.DataFrame:
    params = {"as_of_date": as_of_date}
    filters = "" if as_of_date == "All" else "WHERE as_of_date = :as_of_date"

    queries = [
        ("stg_funds", f"SELECT MAX(loaded_at) AS ts FROM stg_funds {filters}"),
        ("stg_holdings", f"SELECT MAX(loaded_at) AS ts FROM stg_holdings {filters}"),
        ("stg_fund_links", f"SELECT MAX(loaded_at) AS ts FROM stg_fund_links {filters}"),
    ]
    rows: list[dict[str, Any]] = []
    with _staging_engine().connect() as conn:
        for name, q in queries:
            ts = conn.execute(text(q), params if as_of_date != "All" else None).scalar_one()
            rows.append({"table": name, "max_loaded_at": ts})

    return pd.DataFrame(rows)


@st.cache_data(ttl=20, show_spinner=False)
def _fund_catalog(as_of_date: str) -> pd.DataFrame:
    params = {"as_of_date": as_of_date}
    where = "" if as_of_date == "All" else "WHERE as_of_date = :as_of_date"

    with _staging_engine().connect() as conn:
        funds = pd.read_sql(
            text(f"SELECT fund_id, fund_name, source, currency, as_of_date FROM stg_funds {where}"),
            conn,
            params=params if as_of_date != "All" else None,
        )

    if funds.empty:
        return funds

    funds["fund_id"] = funds["fund_id"].astype(str).str.strip()
    funds["fund_name"] = funds["fund_name"].astype(str).str.strip()
    funds["source"] = funds["source"].astype(str).str.strip()
    funds["currency"] = funds["currency"].astype(str).str.strip()
    return funds


@st.cache_data(ttl=20, show_spinner=False)
def _root_funds(as_of_date: str) -> list[str]:
    params = {"as_of_date": as_of_date}

    try:
        where = "" if as_of_date == "All" else "WHERE as_of_date = :as_of_date"
        query = text(f"SELECT DISTINCT root_fund_id FROM mart_true_exposure {where} ORDER BY root_fund_id")
        with _mart_engine().connect() as conn:
            values = [str(r[0]) for r in conn.execute(query, params if as_of_date != "All" else None).fetchall()]
        if values:
            return values
    except Exception:
        pass

    # Fallback to feeder ids from links.
    where = "" if as_of_date == "All" else "WHERE as_of_date = :as_of_date"
    query = text(f"SELECT DISTINCT feeder_fund_id FROM stg_fund_links {where} ORDER BY feeder_fund_id")
    with _staging_engine().connect() as conn:
        return [str(r[0]) for r in conn.execute(query, params if as_of_date != "All" else None).fetchall()]


@st.cache_data(ttl=20, show_spinner=False)
def _feeder_funds(as_of_date: str) -> list[str]:
    params = {"as_of_date": as_of_date}
    where = "" if as_of_date == "All" else "WHERE as_of_date = :as_of_date"
    query = text(f"SELECT DISTINCT feeder_fund_id FROM stg_fund_links {where} ORDER BY feeder_fund_id")
    with _staging_engine().connect() as conn:
        return [str(r[0]) for r in conn.execute(query, params if as_of_date != "All" else None).fetchall()]


@st.cache_data(ttl=20, show_spinner=False)
def _asset_catalog(as_of_date: str) -> pd.DataFrame:
    params = {"as_of_date": as_of_date}
    where = "" if as_of_date == "All" else "WHERE as_of_date = :as_of_date"

    query = text(
        f"""
        SELECT asset_id, asset_name, asset_type
        FROM stg_holdings
        {where}
        """
    )

    with _staging_engine().connect() as conn:
        df = pd.read_sql(query, conn, params=params if as_of_date != "All" else None)

    if df.empty:
        return pd.DataFrame(columns=["asset_id", "asset_name", "asset_type"])

    df["asset_id"] = df["asset_id"].fillna("").astype(str).str.strip()
    df["asset_name"] = df["asset_name"].fillna("").astype(str).str.strip()
    df["asset_type"] = df["asset_type"].fillna("").astype(str).str.strip().str.lower()

    def pick_name(values: pd.Series) -> str:
        for v in values:
            if v:
                return v
        return ""

    def pick_type(values: pd.Series) -> str:
        cleaned = [v for v in values if v]
        if not cleaned:
            return ""
        return Counter(cleaned).most_common(1)[0][0]

    grouped = (
        df.groupby("asset_id", as_index=False)
        .agg(asset_name=("asset_name", pick_name), asset_type=("asset_type", pick_type))
        .sort_values(by=["asset_id"])
        .reset_index(drop=True)
    )

    return grouped


def _build_where(spec: dict[str, Any], as_of: str, source: str, keyword: str) -> tuple[str, dict[str, Any]]:
    conditions: list[str] = []
    params: dict[str, Any] = {}

    if as_of != "All":
        conditions.append(f"{spec['as_of_expr']} = :as_of_date")
        params["as_of_date"] = as_of

    if source != "All" and spec.get("source_expr"):
        conditions.append(f"{spec['source_expr']} = :source")
        params["source"] = source

    cleaned = keyword.strip()
    if cleaned:
        groups: list[str] = []
        for idx, token in enumerate(cleaned.split()):
            key = f"kw{idx}"
            params[key] = f"%{token}%"
            inner = " OR ".join(f"CAST({expr} AS CHAR) LIKE :{key}" for expr in spec["search_exprs"])
            groups.append(f"({inner})")
        conditions.append("(" + " AND ".join(groups) + ")")

    if not conditions:
        return "", params

    return "WHERE " + " AND ".join(conditions), params


def _sanitize_page_size(raw: int) -> int:
    return 25 if raw < 1 else min(raw, 500)


@st.cache_data(ttl=10, show_spinner=False)
def _run_explorer_query(
    dataset: str,
    as_of: str,
    source: str,
    keyword: str,
    sort_by: str,
    sort_desc: bool,
    page: int,
    page_size: int,
) -> tuple[pd.DataFrame, int, int]:
    spec = DATASET_SPECS[dataset]
    valid_sort = {c[0] for c in spec["columns"]}
    sort_field = sort_by if sort_by in valid_sort else spec["default_sort"]

    where_sql, params = _build_where(spec, as_of, source, keyword)
    select_sql = ", ".join(f"{expr} AS {name}" for name, expr, _ in spec["columns"])

    count_q = f"SELECT COUNT(*) {spec['from_sql']} {where_sql}"
    direction = "DESC" if sort_desc else "ASC"
    data_q = (
        f"SELECT {select_sql} {spec['from_sql']} {where_sql} "
        f"ORDER BY {sort_field} {direction} "
        "LIMIT :limit_rows OFFSET :offset_rows"
    )

    with _staging_engine().connect() as conn:
        total_rows = int(conn.execute(text(count_q), params).scalar_one())
        total_pages = max(1, math.ceil(total_rows / page_size)) if total_rows else 1

        actual_page = max(1, min(page, total_pages))
        run_params = dict(params)
        run_params["limit_rows"] = page_size
        run_params["offset_rows"] = (actual_page - 1) * page_size

        rows = conn.execute(text(data_q), run_params).mappings().all()
        df = pd.DataFrame([dict(r) for r in rows])

    if not df.empty:
        for c in df.columns:
            if "weight" in c or "confidence" in c:
                df[c] = pd.to_numeric(df[c], errors="ignore")

    return df, total_rows, total_pages


@st.cache_data(ttl=10, show_spinner=False)
def _direct_holdings(fund_id: str, as_of_date: str) -> pd.DataFrame:
    params = {"as_of_date": as_of_date, "fund_id": fund_id}
    where = "WHERE fund_id = :fund_id"
    if as_of_date != "All":
        where += " AND as_of_date = :as_of_date"

    query = text(
        f"""
        SELECT fund_id, asset_id, asset_name, asset_type, weight
        FROM stg_holdings
        {where}
        ORDER BY weight DESC
        """
    )
    with _staging_engine().connect() as conn:
        return pd.read_sql(query, conn, params=params if as_of_date != "All" else {"fund_id": fund_id})


@st.cache_data(ttl=10, show_spinner=False)
def _true_exposure_for_fund(fund_id: str, as_of_date: str, min_weight: float) -> pd.DataFrame:
    params = {"as_of_date": as_of_date, "fund_id": fund_id, "min_weight": min_weight}
    where = "WHERE root_fund_id = :fund_id AND effective_weight >= :min_weight"
    if as_of_date != "All":
        where += " AND as_of_date = :as_of_date"

    query = text(
        f"""
        SELECT root_fund_id, final_asset_id, effective_weight, path_depth
        FROM mart_true_exposure
        {where}
        ORDER BY effective_weight DESC
        """
    )

    with _mart_engine().connect() as conn:
        df = pd.read_sql(query, conn, params=params if as_of_date != "All" else {"fund_id": fund_id, "min_weight": min_weight})

    if df.empty:
        return df

    df["effective_weight"] = pd.to_numeric(df["effective_weight"], errors="coerce")
    df["path_depth"] = pd.to_numeric(df["path_depth"], errors="coerce")
    return df


@st.cache_data(ttl=10, show_spinner=False)
def _funds_exposed_to_asset(asset_id: str, as_of_date: str, min_weight: float) -> pd.DataFrame:
    params = {"as_of_date": as_of_date, "asset_id": asset_id, "min_weight": min_weight}
    where = "WHERE final_asset_id = :asset_id AND effective_weight >= :min_weight"
    if as_of_date != "All":
        where += " AND as_of_date = :as_of_date"

    query = text(
        f"""
        SELECT root_fund_id, effective_weight, path_depth
        FROM mart_true_exposure
        {where}
        ORDER BY effective_weight DESC
        """
    )

    with _mart_engine().connect() as conn:
        df = pd.read_sql(query, conn, params=params if as_of_date != "All" else {"asset_id": asset_id, "min_weight": min_weight})

    if df.empty:
        return df

    df["effective_weight"] = pd.to_numeric(df["effective_weight"], errors="coerce")
    df["path_depth"] = pd.to_numeric(df["path_depth"], errors="coerce")
    return df


@st.cache_data(ttl=10, show_spinner=False)
def _direct_holders_of_asset(asset_id: str, as_of_date: str) -> pd.DataFrame:
    params = {"as_of_date": as_of_date, "asset_id": asset_id}
    where = "WHERE asset_id = :asset_id"
    if as_of_date != "All":
        where += " AND as_of_date = :as_of_date"

    query = text(
        f"""
        SELECT fund_id, asset_id, asset_name, asset_type, weight
        FROM stg_holdings
        {where}
        ORDER BY weight DESC
        """
    )

    with _staging_engine().connect() as conn:
        return pd.read_sql(query, conn, params=params if as_of_date != "All" else {"asset_id": asset_id})


@st.cache_data(ttl=10, show_spinner=False)
def _top_assets(
    as_of_date: str,
    asset_types: list[str],
    top_n: int,
    thai_only: bool,
    contains: str,
    aum_map: dict[str, float] | None,
    root_fund_ids: set[str] | None,
) -> pd.DataFrame:
    params = {"as_of_date": as_of_date}
    where = "" if as_of_date == "All" else "WHERE e.as_of_date = :as_of_date"

    query = text(
        f"""
        SELECT e.root_fund_id, e.final_asset_id, e.effective_weight
        FROM mart_true_exposure e
        {where}
        """
    )

    with _mart_engine().connect() as conn:
        exposures = pd.read_sql(query, conn, params=params if as_of_date != "All" else None)

    if exposures.empty:
        return pd.DataFrame(columns=["asset_id", "asset_name", "asset_type", "total_weight", "total_value"])

    exposures["effective_weight"] = pd.to_numeric(exposures["effective_weight"], errors="coerce").fillna(0.0)
    if root_fund_ids:
        exposures = exposures[exposures["root_fund_id"].astype(str).isin(root_fund_ids)].copy()

    assets = _asset_catalog(as_of_date)
    merged = exposures.merge(assets, left_on="final_asset_id", right_on="asset_id", how="left")

    if asset_types:
        merged = merged[merged["asset_type"].isin(asset_types)]

    if thai_only:
        pattern = re.compile(r"(\\.BK$|\\bBK\\b|\\-BK$)", re.IGNORECASE)
        merged = merged[
            merged["final_asset_id"].astype(str).str.contains(pattern)
            | merged["asset_name"].astype(str).str.contains(pattern)
        ]

    if contains.strip():
        merged = merged[merged["final_asset_id"].astype(str).str.contains(contains.strip(), case=False, regex=False)]

    merged["total_value"] = 0.0
    if aum_map:
        merged["aum"] = merged["root_fund_id"].map(aum_map).fillna(0.0)
        merged["total_value"] = merged["effective_weight"] * merged["aum"]

    grouped = (
        merged.groupby(["final_asset_id", "asset_name", "asset_type"], as_index=False)
        .agg(total_weight=("effective_weight", "sum"), total_value=("total_value", "sum"))
        .rename(columns={"final_asset_id": "asset_id"})
        .sort_values(by=["total_value" if aum_map else "total_weight"], ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )

    return grouped


@st.cache_data(ttl=10, show_spinner=False)
def _top_master_funds(as_of_date: str, top_n: int, aum_map: dict[str, float] | None) -> pd.DataFrame:
    params = {"as_of_date": as_of_date}
    where = "" if as_of_date == "All" else "WHERE l.as_of_date = :as_of_date"

    query = text(
        f"""
        SELECT l.feeder_fund_id, l.master_fund_id, l.confidence,
               mf.fund_name AS master_name, mf.source AS master_source
        FROM stg_fund_links l
        LEFT JOIN stg_funds mf ON l.master_fund_id = mf.fund_id AND l.as_of_date = mf.as_of_date
        {where}
        """
    )

    with _staging_engine().connect() as conn:
        df = pd.read_sql(query, conn, params=params if as_of_date != "All" else None)

    if df.empty:
        return pd.DataFrame(columns=["master_fund_id", "master_name", "master_source", "score", "total_value", "feeder_count"])

    df["confidence"] = pd.to_numeric(df["confidence"], errors="coerce").fillna(0.0)
    df["score"] = df["confidence"].clip(lower=0.0, upper=1.0)

    df["total_value"] = 0.0
    if aum_map:
        df["aum"] = df["feeder_fund_id"].map(aum_map).fillna(0.0)
        df["total_value"] = df["score"] * df["aum"]

    grouped = (
        df.groupby(["master_fund_id", "master_name", "master_source"], as_index=False)
        .agg(score=("score", "sum"), total_value=("total_value", "sum"), feeder_count=("feeder_fund_id", "nunique"))
        .sort_values(by=["total_value" if aum_map else "score"], ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )

    return grouped


def _load_aum_mapping(uploaded: Any) -> dict[str, float] | None:
    if uploaded is None:
        return None

    try:
        df = pd.read_csv(uploaded)
    except Exception as exc:
        st.warning(f"Could not read AUM CSV: {exc}")
        return None

    if not {"fund_id", "aum"}.issubset(df.columns):
        st.warning("AUM CSV must have columns: fund_id,aum")
        return None

    df = df.copy()
    df["fund_id"] = df["fund_id"].astype(str).str.strip()
    df["aum"] = pd.to_numeric(df["aum"], errors="coerce").fillna(0.0)

    mapping = {row.fund_id: float(row.aum) for row in df.itertuples(index=False)}
    return mapping


@st.cache_data(ttl=10, show_spinner=False)
def _graph_edges(as_of_date: str) -> dict[str, list[tuple[str, float, bool, str]]]:
    """Build adjacency: fund -> [(child_id, weight, child_is_fund, edge_kind)]."""

    params = {"as_of_date": as_of_date}

    h_where = "" if as_of_date == "All" else "WHERE as_of_date = :as_of_date"
    l_where = "" if as_of_date == "All" else "WHERE as_of_date = :as_of_date"

    with _staging_engine().connect() as conn:
        holdings = pd.read_sql(
            text(
                f"""
                SELECT fund_id, asset_id, asset_type, weight
                FROM stg_holdings
                {h_where}
                """
            ),
            conn,
            params=params if as_of_date != "All" else None,
        )
        links = pd.read_sql(
            text(
                f"""
                SELECT feeder_fund_id, master_fund_id, confidence
                FROM stg_fund_links
                {l_where}
                """
            ),
            conn,
            params=params if as_of_date != "All" else None,
        )

    edges: dict[str, list[tuple[str, float, bool, str]]] = {}

    if not holdings.empty:
        holdings["fund_id"] = holdings["fund_id"].fillna("").astype(str).str.strip()
        holdings["asset_id"] = holdings["asset_id"].fillna("").astype(str).str.strip()
        holdings["asset_type"] = holdings["asset_type"].fillna("").astype(str).str.strip().str.lower()
        holdings["weight"] = pd.to_numeric(holdings["weight"], errors="coerce").fillna(0.0)

        for row in holdings.itertuples(index=False):
            if not row.fund_id or not row.asset_id:
                continue
            child_is_fund = row.asset_type in FUND_LIKE_TYPES
            edges.setdefault(row.fund_id, []).append((row.asset_id, float(row.weight), bool(child_is_fund), "holding"))

    if not links.empty:
        links["feeder_fund_id"] = links["feeder_fund_id"].fillna("").astype(str).str.strip()
        links["master_fund_id"] = links["master_fund_id"].fillna("").astype(str).str.strip()
        links["confidence"] = pd.to_numeric(links["confidence"], errors="coerce").fillna(1.0).clip(lower=0.0, upper=1.0)

        for row in links.itertuples(index=False):
            if not row.feeder_fund_id or not row.master_fund_id:
                continue
            edges.setdefault(row.feeder_fund_id, []).append((row.master_fund_id, float(row.confidence), True, "link"))

    return edges


def _find_trace_path(
    root_fund_id: str,
    target_asset_id: str,
    edges: dict[str, list[tuple[str, float, bool, str]]],
    max_depth: int,
) -> tuple[list[dict[str, Any]], float] | None:
    """Return one shortest path to target + cumulative weight product."""

    if root_fund_id == target_asset_id:
        return ([{"from": root_fund_id, "to": target_asset_id, "edge_kind": "self", "edge_weight": 1.0, "cum_weight": 1.0}], 1.0)

    queue: deque[tuple[str, list[dict[str, Any]], float, int]] = deque()
    queue.append((root_fund_id, [], 1.0, 0))
    visited: set[str] = {root_fund_id}

    while queue:
        node, path, cum, depth = queue.popleft()
        if depth >= max_depth:
            continue

        for child, w, child_is_fund, edge_kind in edges.get(node, []):
            next_cum = cum * w
            step = {
                "from": node,
                "to": child,
                "edge_kind": edge_kind,
                "edge_weight": w,
                "cum_weight": next_cum,
            }
            next_path = path + [step]

            if child == target_asset_id:
                return next_path, next_cum

            if child_is_fund and child not in visited:
                visited.add(child)
                queue.append((child, next_path, next_cum, depth + 1))

    return None


def _render_cards(cards: dict[str, int]) -> None:
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.markdown(
            f"<div class='top-card'><div class='value'>{cards.get('stg_funds', 0):,}</div><div class='label'>stg_funds</div></div>",
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            f"<div class='top-card'><div class='value'>{cards.get('stg_holdings', 0):,}</div><div class='label'>stg_holdings</div></div>",
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            f"<div class='top-card'><div class='value'>{cards.get('stg_fund_links', 0):,}</div><div class='label'>stg_fund_links</div></div>",
            unsafe_allow_html=True,
        )
    with c4:
        st.markdown(
            f"<div class='top-card'><div class='value'>{cards.get('mart_true_exposure', 0):,}</div><div class='label'>mart_true_exposure</div></div>",
            unsafe_allow_html=True,
        )


def _render_dashboard(as_of_date: str) -> None:
    st.subheader("Dashboard")

    cards = _count_cards(as_of_date)
    _render_cards(cards)

    st.markdown("<div class='small-note'>Tip: Use the bidirectional search pages to debug real exposure outcomes.</div>", unsafe_allow_html=True)

    left, right = st.columns([1, 1])

    with left:
        st.markdown("#### Freshness")
        st.dataframe(_freshness(as_of_date), use_container_width=True, hide_index=True)

    with right:
        st.markdown("#### Source Distribution")
        funds = _fund_catalog(as_of_date)
        if funds.empty or "source" not in funds.columns:
            st.info("No fund metadata available.")
        else:
            src = funds.groupby("source", as_index=False).agg(funds=("fund_id", "nunique")).sort_values("funds", ascending=False)
            chart = alt.Chart(src).mark_bar(color="#2563eb").encode(
                x=alt.X("funds:Q", title="# Funds"),
                y=alt.Y("source:N", sort="-x", title="Source"),
                tooltip=["source:N", "funds:Q"],
            )
            st.altair_chart(chart, use_container_width=True)

    st.markdown("#### Top Funds by Holdings Count")
    if as_of_date == "All":
        where = ""
        params = None
    else:
        where = "WHERE as_of_date = :as_of_date"
        params = {"as_of_date": as_of_date}

    query = text(
        f"""
        SELECT fund_id, COUNT(*) AS holdings_count, SUM(weight) AS weight_sum
        FROM stg_holdings
        {where}
        GROUP BY fund_id
        ORDER BY holdings_count DESC
        LIMIT 20
        """
    )
    with _staging_engine().connect() as conn:
        top = pd.read_sql(query, conn, params=params)

    if top.empty:
        st.info("No holdings found.")
        return

    st.dataframe(top, use_container_width=True, hide_index=True)


def _render_explorer(as_of_date: str, source: str) -> None:
    st.subheader("Explorer")

    cols = st.columns([1.2, 1.2, 1.2, 1.0])
    with cols[0]:
        dataset = st.selectbox(
            "Dataset",
            options=list(DATASET_SPECS.keys()),
            format_func=lambda k: DATASET_SPECS[k]["label"],
            index=1,
        )
    with cols[1]:
        keyword = st.text_input("Keyword", placeholder="fund id, fund name, asset id, ...")
    with cols[2]:
        page_size = _sanitize_page_size(int(st.selectbox("Page Size", options=[25, 50, 100, 250], index=1)))
    with cols[3]:
        sort_desc = st.toggle("Sort desc", value=False)

    spec = DATASET_SPECS[dataset]
    sort_options = [c[0] for c in spec["columns"]]
    sort_by = st.selectbox("Sort By", options=sort_options, index=0)

    page = int(st.number_input("Page", min_value=1, value=1, step=1))

    df, total_rows, total_pages = _run_explorer_query(
        dataset=dataset,
        as_of=as_of_date,
        source=source,
        keyword=keyword,
        sort_by=sort_by,
        sort_desc=sort_desc,
        page=page,
        page_size=page_size,
    )

    st.caption(
        f"Dataset: {spec['label']} | Total rows: {total_rows:,} | Page: {min(max(page, 1), total_pages)}/{total_pages}"
    )

    st.dataframe(df, use_container_width=True, height=440)

    if not df.empty:
        st.download_button(
            label="Download current page CSV",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name=f"explorer_{dataset}_page_{min(max(page, 1), total_pages)}.csv",
            mime="text/csv",
        )


def _render_fund_search(as_of_date: str, aum_map: dict[str, float] | None) -> None:
    st.subheader("Fund -> Assets")

    feeder_only = st.toggle("Feeder funds only (from stg_fund_links)", value=True)
    roots = _feeder_funds(as_of_date) if feeder_only else _root_funds(as_of_date)
    funds = _fund_catalog(as_of_date)

    search = st.text_input("Search fund (root fund id)", placeholder="TH_..., feeder id, ...")
    candidates = roots
    if search.strip():
        s = search.strip().lower()
        candidates = [f for f in roots if s in f.lower()][:200]

    if not candidates:
        st.info("No matching root funds for this as-of date.")
        return

    selected = st.selectbox("Select root fund", options=candidates, index=0)

    meta = None
    if not funds.empty:
        meta_rows = funds[funds["fund_id"] == selected]
        if not meta_rows.empty:
            meta = meta_rows.iloc[0].to_dict()

    if meta:
        st.markdown(
            f"**{meta.get('fund_name','')}**  \\n            fund_id=`{meta.get('fund_id')}` | source=`{meta.get('source')}` | currency=`{meta.get('currency')}`"
        )
    else:
        st.markdown(f"fund_id=`{selected}` (not found in `stg_funds` for this snapshot)")

    min_weight = float(st.slider("Min effective weight", min_value=0.0, max_value=1.0, value=0.0, step=0.01))

    tabs = st.tabs(["True Exposure (Indirect)", "Direct Holdings", "Trace Path"])

    with tabs[0]:
        try:
            exposure = _true_exposure_for_fund(selected, as_of_date, min_weight)
        except Exception as exc:
            st.error(f"Failed to load mart_true_exposure: {exc}")
            st.stop()

        if exposure.empty:
            st.info("No exposure rows found. Run `pipelines/run_build_mart.py` for this as-of date.")
            return

        assets = _asset_catalog(as_of_date)
        enriched = exposure.merge(assets, left_on="final_asset_id", right_on="asset_id", how="left")
        enriched = enriched.drop(columns=["asset_id"], errors="ignore")

        show_types = sorted([t for t in assets["asset_type"].unique().tolist() if t])
        types = st.multiselect("Asset type filter", options=show_types, default=[])
        if types:
            enriched = enriched[enriched["asset_type"].isin(types)]

        enriched = enriched.sort_values(by=["effective_weight"], ascending=False).reset_index(drop=True)

        if aum_map:
            aum = float(aum_map.get(selected, 0.0))
            enriched["notional_value"] = enriched["effective_weight"] * aum
            st.caption(f"AUM mapping found for this fund: {aum:,.2f} (notional_value = effective_weight * AUM)")

        st.dataframe(enriched, use_container_width=True, height=420)

        top_n = min(20, len(enriched))
        chart_df = enriched.head(top_n).copy()
        chart_df["label"] = chart_df["final_asset_id"].astype(str)
        if "asset_name" in chart_df.columns:
            chart_df["label"] = chart_df["label"] + " | " + chart_df["asset_name"].fillna("").astype(str)

        chart = alt.Chart(chart_df).mark_bar(color="#10b981").encode(
            x=alt.X("effective_weight:Q", title="Effective Weight"),
            y=alt.Y("label:N", sort="-x", title="Top Assets"),
            tooltip=["final_asset_id:N", "asset_name:N", "asset_type:N", "effective_weight:Q", "path_depth:Q"],
        )
        st.altair_chart(chart, use_container_width=True)

        st.download_button(
            "Download exposure CSV",
            data=enriched.to_csv(index=False).encode("utf-8"),
            file_name=f"true_exposure_{selected}_{as_of_date}.csv",
            mime="text/csv",
        )

    with tabs[1]:
        direct = _direct_holdings(selected, as_of_date)
        if direct.empty:
            st.info("No direct holdings for this fund in staging (maybe feeder-only fund).")
        else:
            st.dataframe(direct, use_container_width=True, height=420)

    with tabs[2]:
        st.markdown("Find one trace path from root fund to a target asset using staging graph edges.")
        max_depth = int(st.slider("Max depth", min_value=1, max_value=10, value=6, step=1))

        target = st.text_input("Target asset_id", placeholder="EQ_..., ticker, ...")
        if st.button("Find Path"):
            if not target.strip():
                st.warning("Provide a target asset_id first.")
            else:
                edges = _graph_edges(as_of_date)
                found = _find_trace_path(selected, target.strip(), edges, max_depth=max_depth)
                if not found:
                    st.info("No path found (within max depth).")
                else:
                    path_rows, final_weight = found
                    st.success(f"Found path. cumulative_weight={final_weight:.10f}")
                    st.dataframe(pd.DataFrame(path_rows), use_container_width=True, hide_index=True)


def _render_asset_search(as_of_date: str, aum_map: dict[str, float] | None) -> None:
    st.subheader("Asset -> Funds")

    assets = _asset_catalog(as_of_date)
    if assets.empty:
        st.info("No assets in staging holdings.")
        return

    feeder_only = st.toggle("Only feeder funds (from stg_fund_links)", value=True)
    feeder_set = set(_feeder_funds(as_of_date)) if feeder_only else set()

    keyword = st.text_input("Search asset", placeholder="ticker, asset name, ...")
    filtered = assets
    if keyword.strip():
        s = keyword.strip().lower()
        filtered = assets[
            assets["asset_id"].astype(str).str.lower().str.contains(s)
            | assets["asset_name"].astype(str).str.lower().str.contains(s)
        ].head(300)

    options = filtered["asset_id"].tolist() if not filtered.empty else []
    if not options:
        st.info("No matching assets.")
        return

    display = filtered.set_index("asset_id")["asset_name"].to_dict()
    selected = st.selectbox("Select asset", options=options, format_func=lambda x: f"{x} | {display.get(x,'')}")

    asset_row = assets[assets["asset_id"] == selected]
    if not asset_row.empty:
        asset_meta = asset_row.iloc[0].to_dict()
        st.markdown(
            f"asset_id=`{asset_meta.get('asset_id')}` | type=`{asset_meta.get('asset_type')}`  \\n            {asset_meta.get('asset_name','')}"
        )

    min_weight = float(st.slider("Min effective weight", min_value=0.0, max_value=1.0, value=0.0, step=0.01))

    tabs = st.tabs(["Funds With Exposure", "Direct Holders"])

    with tabs[0]:
        try:
            funds = _funds_exposed_to_asset(selected, as_of_date, min_weight)
        except Exception as exc:
            st.error(f"Failed to query mart_true_exposure: {exc}")
            st.stop()

        if funds.empty:
            st.info("No funds exposed to this asset for the selected as-of date.")
            return

        if feeder_set:
            funds = funds[funds["root_fund_id"].astype(str).isin(feeder_set)].copy()
            if funds.empty:
                st.info("No feeder funds exposed to this asset for the selected as-of date.")
                return

        # Enrich root fund names when available.
        fund_meta = _fund_catalog(as_of_date)
        if not fund_meta.empty:
            funds = funds.merge(
                fund_meta[["fund_id", "fund_name", "source"]].rename(columns={"fund_id": "root_fund_id"}),
                on="root_fund_id",
                how="left",
            )

        if aum_map:
            funds["aum"] = funds["root_fund_id"].map(aum_map).fillna(0.0)
            funds["notional_value"] = funds["effective_weight"] * funds["aum"]

        st.dataframe(funds, use_container_width=True, height=420)

        top_n = min(20, len(funds))
        chart_df = funds.head(top_n).copy()
        chart_df["label"] = chart_df["root_fund_id"].astype(str)
        if "fund_name" in chart_df.columns:
            chart_df["label"] = chart_df["label"] + " | " + chart_df["fund_name"].fillna("").astype(str)

        chart = alt.Chart(chart_df).mark_bar(color="#f59e0b").encode(
            x=alt.X("effective_weight:Q", title="Effective Weight"),
            y=alt.Y("label:N", sort="-x", title="Top Funds"),
            tooltip=["root_fund_id:N", "fund_name:N", "effective_weight:Q", "path_depth:Q"],
        )
        st.altair_chart(chart, use_container_width=True)

    with tabs[1]:
        direct = _direct_holders_of_asset(selected, as_of_date)
        if direct.empty:
            st.info("No direct holders found in staging holdings.")
        else:
            st.dataframe(direct, use_container_width=True, height=420)


def _render_top10(as_of_date: str, aum_map: dict[str, float] | None) -> None:
    st.subheader("Top 10")

    tab1, tab2 = st.tabs(["Top Assets", "Top Foreign Master Funds (Links)"])

    with tab1:
        assets = _asset_catalog(as_of_date)
        types = sorted([t for t in assets["asset_type"].unique().tolist() if t])
        asset_types = st.multiselect("Asset types", options=types, default=["equity"] if "equity" in types else [])
        top_n = int(st.slider("Top N", min_value=5, max_value=50, value=10, step=1))
        feeder_only = st.toggle("Only feeder funds (from stg_fund_links)", value=True)
        thai_only = st.toggle("Thai-only heuristic (.BK)", value=False)
        contains = st.text_input("asset_id contains", value="")

        root_filter = set(_feeder_funds(as_of_date)) if feeder_only else None
        top = _top_assets(
            as_of_date,
            asset_types=asset_types,
            top_n=top_n,
            thai_only=thai_only,
            contains=contains,
            aum_map=aum_map,
            root_fund_ids=root_filter,
        )

        if top.empty:
            st.info("No results.")
        else:
            metric = "total_value" if aum_map else "total_weight"
            chart = alt.Chart(top).mark_bar(color="#2563eb").encode(
                x=alt.X(f"{metric}:Q", title=metric),
                y=alt.Y("asset_id:N", sort="-x", title="Asset"),
                tooltip=["asset_id:N", "asset_name:N", "asset_type:N", "total_weight:Q", "total_value:Q"],
            )
            st.altair_chart(chart, use_container_width=True)
            st.dataframe(top, use_container_width=True, hide_index=True)

    with tab2:
        top_n = int(st.slider("Top N (masters)", min_value=5, max_value=50, value=10, step=1))
        masters = _top_master_funds(as_of_date, top_n=top_n, aum_map=aum_map)

        st.markdown(
            "<div class='small-note'>Note: `confidence` in stg_fund_links is a match score, not allocation. "
            "If you have allocation %, add it to the link table and we can use it here.</div>",
            unsafe_allow_html=True,
        )

        if masters.empty:
            st.info("No links found.")
        else:
            metric = "total_value" if aum_map else "score"
            chart = alt.Chart(masters).mark_bar(color="#7c3aed").encode(
                x=alt.X(f"{metric}:Q", title=metric),
                y=alt.Y("master_fund_id:N", sort="-x", title="Master Fund"),
                tooltip=["master_fund_id:N", "master_name:N", "master_source:N", "score:Q", "feeder_count:Q", "total_value:Q"],
            )
            st.altair_chart(chart, use_container_width=True)
            st.dataframe(masters, use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(page_title="Fund Traceability Debugger", layout="wide")
    _inject_css()

    st.markdown(
        """
        <div class="hero">
          <h1>Fund Traceability Debugger</h1>
          <p>
            Dashboard + bidirectional search for fund holdings and true exposure.
            Designed as a local developer tool for debugging staging/mart data.
          </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    try:
        as_of_dates = _list_as_of_dates()
    except Exception as exc:
        st.error(f"Failed to connect staging database: {exc}")
        st.stop()

    if "as_of_date" not in st.session_state:
        st.session_state["as_of_date"] = _default_as_of(as_of_dates)

    with st.sidebar:
        st.subheader("Navigation")
        page = st.radio(
            "Page",
            options=[
                "Dashboard",
                "Explorer",
                "Fund -> Assets",
                "Asset -> Funds",
                "Top 10",
            ],
            index=0,
        )

        st.divider()
        st.subheader("Global Filters")
        as_of_options = ["All"] + as_of_dates
        default_as_of = st.session_state["as_of_date"] if st.session_state["as_of_date"] in as_of_options else "All"
        as_of_date = st.selectbox("As Of Date", options=as_of_options, index=as_of_options.index(default_as_of))
        st.session_state["as_of_date"] = as_of_date

        sources = _list_sources(as_of_date)
        source = st.selectbox("Source", options=sources, index=0)

        st.divider()
        st.subheader("Optional AUM")
        uploaded = st.file_uploader("Upload fund AUM CSV (fund_id,aum)", type=["csv"], accept_multiple_files=False)
        aum_map = _load_aum_mapping(uploaded)

        if st.button("Refresh (clear cache)", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    if page == "Dashboard":
        _render_dashboard(as_of_date)
    elif page == "Explorer":
        _render_explorer(as_of_date, source)
    elif page == "Fund -> Assets":
        _render_fund_search(as_of_date, aum_map)
    elif page == "Asset -> Funds":
        _render_asset_search(as_of_date, aum_map)
    elif page == "Top 10":
        _render_top10(as_of_date, aum_map)


if __name__ == "__main__":
    main()
