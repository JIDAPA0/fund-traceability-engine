"""Write normalized datasets to staging schema."""

from __future__ import annotations

import pandas as pd
from sqlalchemy.engine import Engine


def write_staging_table(df: pd.DataFrame, table_name: str, engine: Engine) -> None:
    df.to_sql(table_name, engine, if_exists="replace", index=False)
