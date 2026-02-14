"""Local Tkinter search UI for debugging fund traceability staging data."""

from __future__ import annotations

import math
from pathlib import Path
import sys
import time
from typing import Any

from sqlalchemy import text

try:
    import tkinter as tk
    from tkinter import filedialog, messagebox, ttk
except Exception as exc:  # pragma: no cover - depends on local Python build
    tk = None  # type: ignore[assignment]
    filedialog = None  # type: ignore[assignment]
    messagebox = None  # type: ignore[assignment]
    ttk = None  # type: ignore[assignment]
    TK_IMPORT_ERROR: Exception | None = exc
else:
    TK_IMPORT_ERROR = None

# Allow running directly from repo root without package installation.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from db.connections import create_traceability_staging_engine  # noqa: E402


def _format_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.8f}".rstrip("0").rstrip(".") if value != 0 else "0"
    return str(value)


DATASET_SPECS: dict[str, dict[str, Any]] = {
    "funds": {
        "label": "Funds",
        "from_sql": "FROM stg_funds t",
        "columns": [
            ("fund_id", "t.fund_id", "Fund ID", 180, "w"),
            ("fund_name", "t.fund_name", "Fund Name", 320, "w"),
            ("source", "t.source", "Source", 120, "center"),
            ("currency", "t.currency", "Currency", 90, "center"),
            ("as_of_date", "t.as_of_date", "As Of", 110, "center"),
            ("loaded_at", "t.loaded_at", "Loaded At", 180, "center"),
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
            ("fund_id", "h.fund_id", "Fund ID", 170, "w"),
            ("fund_name", "f.fund_name", "Fund Name", 260, "w"),
            ("source", "f.source", "Source", 120, "center"),
            ("asset_id", "h.asset_id", "Asset ID", 170, "w"),
            ("asset_name", "h.asset_name", "Asset Name", 260, "w"),
            ("asset_type", "h.asset_type", "Asset Type", 110, "center"),
            ("weight", "h.weight", "Weight", 100, "e"),
            ("as_of_date", "h.as_of_date", "As Of", 110, "center"),
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
            ("feeder_fund_id", "l.feeder_fund_id", "Feeder Fund ID", 170, "w"),
            ("feeder_name", "ff.fund_name", "Feeder Name", 240, "w"),
            ("feeder_source", "ff.source", "Feeder Source", 120, "center"),
            ("master_fund_id", "l.master_fund_id", "Master Fund ID", 170, "w"),
            ("master_name", "mf.fund_name", "Master Name", 240, "w"),
            ("master_source", "mf.source", "Master Source", 120, "center"),
            ("confidence", "l.confidence", "Confidence", 100, "e"),
            ("as_of_date", "l.as_of_date", "As Of", 110, "center"),
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


class LocalSearchUI:
    def __init__(self) -> None:
        if TK_IMPORT_ERROR is not None:
            raise RuntimeError(
                "Tkinter is not available in this Python environment. "
                "Install a Python build with Tk support, then run this UI again."
            ) from TK_IMPORT_ERROR

        self.engine = create_traceability_staging_engine()

        self.root = tk.Tk()
        self.root.title("Fund Traceability - Local Search UI")
        self.root.geometry("1460x860")
        self.root.minsize(1200, 720)

        self.dataset_var = tk.StringVar(value="funds")
        self.as_of_var = tk.StringVar(value="All")
        self.source_var = tk.StringVar(value="All")
        self.search_var = tk.StringVar(value="")
        self.page_size_var = tk.StringVar(value="50")

        self.current_page = 1
        self.total_rows = 0
        self.total_pages = 1
        self.sort_column = "fund_id"
        self.sort_desc = False

        self._search_after_id: str | None = None
        self._item_rows: dict[str, dict[str, Any]] = {}
        self._last_rows: list[dict[str, Any]] = []

        self._build_style()
        self._build_layout()
        self._reload_filter_options()
        self._configure_tree_for_dataset()
        self._run_query(reset_page=True)

    def _build_style(self) -> None:
        self.root.configure(bg="#f3f6fb")
        style = ttk.Style(self.root)
        style.theme_use("clam")

        style.configure("App.TFrame", background="#f3f6fb")
        style.configure("Card.TFrame", background="#ffffff")

        style.configure(
            "AppHeader.TLabel",
            background="#f3f6fb",
            foreground="#0f172a",
            font=("Segoe UI Semibold", 22),
        )
        style.configure(
            "AppSubheader.TLabel",
            background="#f3f6fb",
            foreground="#475569",
            font=("Segoe UI", 11),
        )
        style.configure(
            "FieldLabel.TLabel",
            background="#ffffff",
            foreground="#334155",
            font=("Segoe UI Semibold", 9),
        )
        style.configure(
            "Status.TLabel",
            background="#f3f6fb",
            foreground="#334155",
            font=("Segoe UI", 10),
        )

        style.configure(
            "TButton",
            font=("Segoe UI Semibold", 10),
            padding=(10, 6),
        )

        style.configure(
            "Treeview",
            background="#ffffff",
            fieldbackground="#ffffff",
            foreground="#0f172a",
            rowheight=28,
            bordercolor="#dbe4f0",
            borderwidth=1,
            font=("Segoe UI", 10),
        )
        style.configure(
            "Treeview.Heading",
            background="#eaf0fb",
            foreground="#0f172a",
            bordercolor="#dbe4f0",
            borderwidth=1,
            font=("Segoe UI Semibold", 10),
        )
        style.map("Treeview", background=[("selected", "#dbeafe")], foreground=[("selected", "#1e3a8a")])

    def _build_layout(self) -> None:
        outer = ttk.Frame(self.root, style="App.TFrame", padding=(18, 16, 18, 14))
        outer.pack(fill="both", expand=True)

        ttk.Label(outer, text="Local Search UI", style="AppHeader.TLabel").pack(anchor="w")
        ttk.Label(
            outer,
            text="Debug staging data from multi-source ingestion with live search, filters, and table explorer.",
            style="AppSubheader.TLabel",
        ).pack(anchor="w", pady=(2, 12))

        controls_card = ttk.Frame(outer, style="Card.TFrame", padding=(14, 12, 14, 10))
        controls_card.pack(fill="x")

        self._build_controls(controls_card)

        grid_card = ttk.Frame(outer, style="Card.TFrame", padding=(10, 10, 10, 8))
        grid_card.pack(fill="both", expand=True, pady=(10, 0))

        self._build_grid(grid_card)

        self.status_label = ttk.Label(outer, text="Ready", style="Status.TLabel")
        self.status_label.pack(anchor="w", pady=(8, 0))

    def _build_controls(self, parent: ttk.Frame) -> None:
        for idx in range(10):
            parent.columnconfigure(idx, weight=0)
        parent.columnconfigure(3, weight=1)
        parent.columnconfigure(8, weight=1)

        ttk.Label(parent, text="Dataset", style="FieldLabel.TLabel").grid(row=0, column=0, sticky="w", padx=(0, 6))
        self.dataset_combo = ttk.Combobox(
            parent,
            textvariable=self.dataset_var,
            values=["funds", "holdings", "links"],
            state="readonly",
            width=12,
        )
        self.dataset_combo.grid(row=1, column=0, sticky="w", padx=(0, 12), pady=(2, 0))
        self.dataset_combo.bind("<<ComboboxSelected>>", self._on_dataset_changed)

        ttk.Label(parent, text="As Of Date", style="FieldLabel.TLabel").grid(row=0, column=1, sticky="w", padx=(0, 6))
        self.as_of_combo = ttk.Combobox(parent, textvariable=self.as_of_var, values=["All"], state="readonly", width=14)
        self.as_of_combo.grid(row=1, column=1, sticky="w", padx=(0, 12), pady=(2, 0))
        self.as_of_combo.bind("<<ComboboxSelected>>", lambda _e: self._run_query(reset_page=True))

        ttk.Label(parent, text="Source", style="FieldLabel.TLabel").grid(row=0, column=2, sticky="w", padx=(0, 6))
        self.source_combo = ttk.Combobox(
            parent,
            textvariable=self.source_var,
            values=["All"],
            state="readonly",
            width=18,
        )
        self.source_combo.grid(row=1, column=2, sticky="w", padx=(0, 12), pady=(2, 0))
        self.source_combo.bind("<<ComboboxSelected>>", lambda _e: self._run_query(reset_page=True))

        ttk.Label(parent, text="Keyword Search", style="FieldLabel.TLabel").grid(row=0, column=3, sticky="w")
        self.search_entry = ttk.Entry(parent, textvariable=self.search_var)
        self.search_entry.grid(row=1, column=3, sticky="ew", padx=(0, 8), pady=(2, 0))
        self.search_entry.bind("<KeyRelease>", self._on_search_keyup)

        clear_btn = ttk.Button(parent, text="Clear", command=self._clear_search)
        clear_btn.grid(row=1, column=4, sticky="w", padx=(0, 10), pady=(2, 0))

        ttk.Label(parent, text="Page Size", style="FieldLabel.TLabel").grid(row=0, column=5, sticky="w", padx=(0, 6))
        self.page_size_combo = ttk.Combobox(
            parent,
            textvariable=self.page_size_var,
            values=["25", "50", "100", "250"],
            state="readonly",
            width=8,
        )
        self.page_size_combo.grid(row=1, column=5, sticky="w", padx=(0, 12), pady=(2, 0))
        self.page_size_combo.bind("<<ComboboxSelected>>", lambda _e: self._run_query(reset_page=True))

        refresh_btn = ttk.Button(parent, text="Refresh", command=self._refresh_all)
        refresh_btn.grid(row=1, column=6, sticky="w", padx=(0, 8), pady=(2, 0))

        export_btn = ttk.Button(parent, text="Export CSV", command=self._export_current_page)
        export_btn.grid(row=1, column=7, sticky="w", pady=(2, 0))

    def _build_grid(self, parent: ttk.Frame) -> None:
        parent.rowconfigure(0, weight=1)
        parent.columnconfigure(0, weight=1)

        table_frame = ttk.Frame(parent, style="Card.TFrame")
        table_frame.grid(row=0, column=0, sticky="nsew")
        table_frame.rowconfigure(0, weight=1)
        table_frame.columnconfigure(0, weight=1)

        self.tree = ttk.Treeview(table_frame, show="headings")
        self.tree.grid(row=0, column=0, sticky="nsew")
        self.tree.bind("<<TreeviewSelect>>", self._on_row_selected)

        y_scroll = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll = ttk.Scrollbar(table_frame, orient="horizontal", command=self.tree.xview)
        x_scroll.grid(row=1, column=0, sticky="ew")
        self.tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)

        paging_frame = ttk.Frame(parent, style="Card.TFrame", padding=(0, 8, 0, 6))
        paging_frame.grid(row=1, column=0, sticky="ew")
        paging_frame.columnconfigure(1, weight=1)

        self.prev_btn = ttk.Button(paging_frame, text="Previous", command=self._go_previous)
        self.prev_btn.grid(row=0, column=0, sticky="w")

        self.page_label = ttk.Label(paging_frame, text="Page 1/1", style="Status.TLabel")
        self.page_label.grid(row=0, column=1, sticky="ew")

        self.next_btn = ttk.Button(paging_frame, text="Next", command=self._go_next)
        self.next_btn.grid(row=0, column=2, sticky="e")

        details_frame = ttk.Frame(parent, style="Card.TFrame", padding=(0, 4, 0, 0))
        details_frame.grid(row=2, column=0, sticky="ew")
        details_frame.columnconfigure(0, weight=1)

        ttk.Label(details_frame, text="Row Details", style="FieldLabel.TLabel").grid(row=0, column=0, sticky="w")
        self.details_text = tk.Text(
            details_frame,
            height=7,
            wrap="none",
            font=("Consolas", 10),
            bg="#f8fafc",
            fg="#0f172a",
            relief="solid",
            borderwidth=1,
        )
        self.details_text.grid(row=1, column=0, sticky="ew", pady=(4, 0))
        self.details_text.configure(state="disabled")

    def _reload_filter_options(self) -> None:
        as_of_values = ["All"]
        source_values = ["All"]

        try:
            with self.engine.connect() as conn:
                as_of_rows = conn.execute(
                    text(
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
                ).fetchall()
                as_of_values.extend(_format_value(row[0]) for row in as_of_rows)

                source_rows = conn.execute(
                    text(
                        """
                        SELECT DISTINCT source
                        FROM stg_funds
                        WHERE source IS NOT NULL AND TRIM(source) <> ''
                        ORDER BY source
                        """
                    )
                ).fetchall()
                source_values.extend(_format_value(row[0]) for row in source_rows)
        except Exception as exc:  # intentionally broad for interactive UI
            messagebox.showwarning("Staging Lookup Warning", f"Could not load filter options: {exc}")

        self.as_of_combo["values"] = as_of_values
        self.source_combo["values"] = source_values

        if self.as_of_var.get() not in as_of_values:
            self.as_of_var.set("All")
        if self.source_var.get() not in source_values:
            self.source_var.set("All")

    def _dataset_spec(self) -> dict[str, Any]:
        return DATASET_SPECS[self.dataset_var.get()]

    def _configure_tree_for_dataset(self) -> None:
        spec = self._dataset_spec()
        columns = [col[0] for col in spec["columns"]]

        self.tree.delete(*self.tree.get_children())
        self.tree["columns"] = columns

        for key, _expr, heading, width, anchor in spec["columns"]:
            self.tree.heading(key, text=heading, command=lambda column=key: self._on_sort(column))
            self.tree.column(key, width=width, minwidth=80, anchor=anchor, stretch=True)

        self.sort_column = spec["default_sort"]
        self.sort_desc = False

    def _on_dataset_changed(self, _event: tk.Event[tk.Misc] | None = None) -> None:
        self._configure_tree_for_dataset()
        self._run_query(reset_page=True)

    def _on_search_keyup(self, _event: tk.Event[tk.Misc]) -> None:
        if self._search_after_id is not None:
            self.root.after_cancel(self._search_after_id)
        self._search_after_id = self.root.after(280, lambda: self._run_query(reset_page=True))

    def _clear_search(self) -> None:
        self.search_var.set("")
        self._run_query(reset_page=True)

    def _refresh_all(self) -> None:
        self._reload_filter_options()
        self._run_query(reset_page=True)

    def _build_where_clause(self, spec: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        conditions: list[str] = []
        params: dict[str, Any] = {}

        as_of_date = self.as_of_var.get().strip()
        if as_of_date and as_of_date != "All":
            conditions.append(f"{spec['as_of_expr']} = :as_of_date")
            params["as_of_date"] = as_of_date

        source = self.source_var.get().strip()
        if source and source != "All" and spec.get("source_expr"):
            conditions.append(f"{spec['source_expr']} = :source")
            params["source"] = source

        keyword = self.search_var.get().strip()
        if keyword:
            token_groups: list[str] = []
            for idx, token in enumerate(keyword.split()):
                key = f"kw{idx}"
                params[key] = f"%{token}%"
                per_token_exprs = [f"CAST({expr} AS CHAR) LIKE :{key}" for expr in spec["search_exprs"]]
                token_groups.append("(" + " OR ".join(per_token_exprs) + ")")
            conditions.append("(" + " AND ".join(token_groups) + ")")

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        return where_clause, params

    def _run_query(self, reset_page: bool) -> None:
        if reset_page:
            self.current_page = 1

        spec = self._dataset_spec()
        allowed_sort = {col[0] for col in spec["columns"]}
        if self.sort_column not in allowed_sort:
            self.sort_column = spec["default_sort"]
            self.sort_desc = False

        try:
            page_size = max(1, int(self.page_size_var.get()))
        except ValueError:
            page_size = 50
            self.page_size_var.set("50")

        where_clause, params = self._build_where_clause(spec)
        sort_direction = "DESC" if self.sort_desc else "ASC"

        select_cols = ", ".join(f"{expr} AS {name}" for name, expr, *_ in spec["columns"])
        count_sql = f"SELECT COUNT(*) {spec['from_sql']} {where_clause}"
        data_sql = (
            f"SELECT {select_cols} {spec['from_sql']} {where_clause} "
            f"ORDER BY {self.sort_column} {sort_direction} "
            "LIMIT :limit_rows OFFSET :offset_rows"
        )

        t0 = time.perf_counter()
        try:
            with self.engine.connect() as conn:
                total_rows = int(conn.execute(text(count_sql), params).scalar_one())

                total_pages = max(1, math.ceil(total_rows / page_size)) if total_rows else 1
                if self.current_page > total_pages:
                    self.current_page = total_pages

                query_params = dict(params)
                query_params["limit_rows"] = page_size
                query_params["offset_rows"] = (self.current_page - 1) * page_size

                row_mappings = conn.execute(text(data_sql), query_params).mappings().all()
                rows = [dict(row) for row in row_mappings]
        except Exception as exc:  # intentionally broad for interactive UI
            messagebox.showerror("Query Error", str(exc))
            return

        elapsed_ms = (time.perf_counter() - t0) * 1000.0

        self.total_rows = total_rows
        self.total_pages = max(1, math.ceil(total_rows / page_size)) if total_rows else 1
        self._last_rows = rows

        self._render_rows(rows)
        self._update_page_controls()
        dataset_label = spec["label"]
        self.status_label.configure(
            text=(
                f"Dataset: {dataset_label} | Rows: {len(rows)} on page {self.current_page}/{self.total_pages} "
                f"(total {self.total_rows}) | Query: {elapsed_ms:.1f} ms"
            )
        )

    def _render_rows(self, rows: list[dict[str, Any]]) -> None:
        self.tree.delete(*self.tree.get_children())
        self._item_rows.clear()

        spec = self._dataset_spec()
        keys = [col[0] for col in spec["columns"]]

        for row in rows:
            item_values = [_format_value(row.get(key)) for key in keys]
            item_id = self.tree.insert("", "end", values=item_values)
            self._item_rows[item_id] = row

        self._set_details({})

    def _update_page_controls(self) -> None:
        self.page_label.configure(text=f"Page {self.current_page} / {self.total_pages}")

        if self.current_page <= 1:
            self.prev_btn.state(["disabled"])
        else:
            self.prev_btn.state(["!disabled"])

        if self.current_page >= self.total_pages:
            self.next_btn.state(["disabled"])
        else:
            self.next_btn.state(["!disabled"])

    def _go_previous(self) -> None:
        if self.current_page > 1:
            self.current_page -= 1
            self._run_query(reset_page=False)

    def _go_next(self) -> None:
        if self.current_page < self.total_pages:
            self.current_page += 1
            self._run_query(reset_page=False)

    def _on_sort(self, column: str) -> None:
        if self.sort_column == column:
            self.sort_desc = not self.sort_desc
        else:
            self.sort_column = column
            self.sort_desc = False
        self._run_query(reset_page=False)

    def _on_row_selected(self, _event: tk.Event[tk.Misc]) -> None:
        selected = self.tree.selection()
        if not selected:
            self._set_details({})
            return

        row = self._item_rows.get(selected[0], {})
        self._set_details(row)

    def _set_details(self, row: dict[str, Any]) -> None:
        lines: list[str] = []
        if row:
            max_key = max(len(key) for key in row)
            for key, value in row.items():
                lines.append(f"{key.ljust(max_key)} : {_format_value(value)}")
        else:
            lines.append("Select a row to inspect raw values.")

        self.details_text.configure(state="normal")
        self.details_text.delete("1.0", "end")
        self.details_text.insert("1.0", "\n".join(lines))
        self.details_text.configure(state="disabled")

    def _export_current_page(self) -> None:
        if not self._last_rows:
            messagebox.showinfo("Export CSV", "No rows on current page to export.")
            return

        default_name = f"local_search_{self.dataset_var.get()}_page{self.current_page}.csv"
        target = filedialog.asksaveasfilename(
            title="Export Current Page",
            defaultextension=".csv",
            initialfile=default_name,
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if not target:
            return

        import pandas as pd

        pd.DataFrame(self._last_rows).to_csv(target, index=False)
        messagebox.showinfo("Export CSV", f"Saved: {target}")

    def run(self) -> None:
        self.root.mainloop()


def main() -> int:
    try:
        app = LocalSearchUI()
        app.run()
        return 0
    except RuntimeError as exc:
        print(f"run_local_search_ui failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
