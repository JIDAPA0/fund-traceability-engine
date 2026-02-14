"""Local web search UI (Streamlit) for debugging staging data."""

from __future__ import annotations

import math
from pathlib import Path
import sys
from typing import Any

import pandas as pd
import streamlit as st
from sqlalchemy import text

# Allow running directly from repo root without package installation.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from db.connections import create_traceability_staging_engine  # noqa: E402


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
            ("feeder_source", "ff.source", "Feeder Source"),
            ("master_fund_id", "l.master_fund_id", "Master Fund ID"),
            ("master_name", "mf.fund_name", "Master Name"),
            ("master_source", "mf.source", "Master Source"),
            ("confidence", "l.confidence", "Confidence"),
            ("as_of_date", "l.as_of_date", "As Of"),
        ],
        "search_exprs": [
            "l.feeder_fund_id",
            "ff.fund_name",
            "ff.source",
            "l.master_fund_id",
            "mf.fund_name",
            "mf.source",
        ],
        "as_of_expr": "l.as_of_date",
        "source_expr": "COALESCE(ff.source, mf.source)",
        "default_sort": "feeder_fund_id",
    },
}


def _inject_css() -> None:
    st.markdown(
        """
        <style>
          .stApp {
            background: linear-gradient(150deg, #f8fbff 0%, #eef4ff 45%, #f4fbf8 100%);
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
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_resource(show_spinner=False)
def _engine():
    return create_traceability_staging_engine()


@st.cache_data(ttl=20, show_spinner=False)
def _count_cards() -> dict[str, int]:
    qmap = {
        "stg_funds": "SELECT COUNT(*) FROM stg_funds",
        "stg_holdings": "SELECT COUNT(*) FROM stg_holdings",
        "stg_fund_links": "SELECT COUNT(*) FROM stg_fund_links",
    }
    out: dict[str, int] = {}
    with _engine().connect() as conn:
        for key, q in qmap.items():
            out[key] = int(conn.execute(text(q)).scalar_one())
    return out


def _build_where(spec: dict[str, Any], as_of: str, source: str, keyword: str) -> tuple[str, dict[str, Any]]:
    conditions: list[str] = []
    params: dict[str, Any] = {}

    if as_of != "All":
        conditions.append(f"{spec['as_of_expr']} = :as_of_date")
        params["as_of_date"] = as_of

    if source != "All":
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


@st.cache_data(ttl=20, show_spinner=False)
def _filter_values(dataset: str) -> tuple[list[str], list[str]]:
    spec = DATASET_SPECS[dataset]
    as_of_q = f"SELECT DISTINCT {spec['as_of_expr']} AS value {spec['from_sql']} WHERE {spec['as_of_expr']} IS NOT NULL ORDER BY value DESC"
    source_q = (
        f"SELECT DISTINCT {spec['source_expr']} AS value {spec['from_sql']} "
        f"WHERE {spec['source_expr']} IS NOT NULL AND TRIM({spec['source_expr']}) <> '' ORDER BY value"
    )

    with _engine().connect() as conn:
        as_of_values = ["All"] + [str(r[0]) for r in conn.execute(text(as_of_q)).fetchall()]
        source_values = ["All"] + [str(r[0]) for r in conn.execute(text(source_q)).fetchall()]
    return as_of_values, source_values


def _sanitize_page_size(raw: int) -> int:
    return 25 if raw < 1 else min(raw, 500)


@st.cache_data(ttl=10, show_spinner=False)
def _run_query(
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

    with _engine().connect() as conn:
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


def _render_cards(cards: dict[str, int]) -> None:
    c1, c2, c3 = st.columns(3)

    with c1:
        st.markdown(
            f"<div class='top-card'><div class='value'>{cards.get('stg_funds', 0):,}</div><div class='label'>stg_funds rows</div></div>",
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            f"<div class='top-card'><div class='value'>{cards.get('stg_holdings', 0):,}</div><div class='label'>stg_holdings rows</div></div>",
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            f"<div class='top-card'><div class='value'>{cards.get('stg_fund_links', 0):,}</div><div class='label'>stg_fund_links rows</div></div>",
            unsafe_allow_html=True,
        )


def main() -> None:
    st.set_page_config(page_title="Fund Traceability - Local Search Web UI", layout="wide")
    _inject_css()

    st.markdown(
        """
        <div class="hero">
          <h1>Fund Traceability Local Search</h1>
          <p>
            Interactive debug console for staging data. Search by fund, asset, or link fields, filter by as-of date/source,
            inspect row-level details, and export the current page instantly.
          </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    try:
        cards = _count_cards()
    except Exception as exc:  # intentionally broad for local diagnostics
        st.error(f"Failed to connect staging database: {exc}")
        st.stop()

    _render_cards(cards)

    with st.sidebar:
        st.subheader("Controls")
        dataset = st.selectbox(
            "Dataset",
            options=list(DATASET_SPECS.keys()),
            format_func=lambda k: DATASET_SPECS[k]["label"],
            index=0,
        )

        as_of_values, source_values = _filter_values(dataset)
        as_of_date = st.selectbox("As Of Date", options=as_of_values, index=0)
        source = st.selectbox("Source", options=source_values, index=0)

        keyword = st.text_input("Keyword", placeholder="fund id, fund name, asset id, ...")

        spec = DATASET_SPECS[dataset]
        sort_options = [c[0] for c in spec["columns"]]
        default_sort_idx = sort_options.index(spec["default_sort"]) if spec["default_sort"] in sort_options else 0
        sort_by = st.selectbox("Sort By", options=sort_options, index=default_sort_idx)
        sort_desc = st.toggle("Sort Descending", value=False)

        page_size = _sanitize_page_size(int(st.selectbox("Page Size", options=[25, 50, 100, 250], index=1)))

        if st.button("Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    left, right = st.columns([1, 1])
    with left:
        page = int(st.number_input("Page", min_value=1, value=1, step=1))
    with right:
        st.write("")
        st.write("")

    try:
        df, total_rows, total_pages = _run_query(
            dataset=dataset,
            as_of=as_of_date,
            source=source,
            keyword=keyword,
            sort_by=sort_by,
            sort_desc=sort_desc,
            page=page,
            page_size=page_size,
        )
    except Exception as exc:  # intentionally broad for local diagnostics
        st.error(f"Query failed: {exc}")
        st.stop()

    st.caption(
        f"Dataset: {spec['label']} | Total rows: {total_rows:,} | Page: {min(max(page, 1), total_pages)}/{total_pages} | Page size: {page_size}"
    )

    st.dataframe(df, use_container_width=True, height=430)

    if not df.empty:
        inspect_idx = int(
            st.number_input(
                "Inspect row index on current page",
                min_value=0,
                max_value=len(df) - 1,
                value=0,
                step=1,
            )
        )
        row = df.iloc[inspect_idx].to_dict()
        st.markdown("**Row Detail**")
        st.json(row)

        csv_data = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Download Current Page CSV",
            data=csv_data,
            file_name=f"{dataset}_page_{min(max(page, 1), total_pages)}.csv",
            mime="text/csv",
        )
    else:
        st.info("No rows matched current filters.")


if __name__ == "__main__":
    main()
