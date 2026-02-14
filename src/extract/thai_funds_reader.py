"""Read source rows from Thai funds source database."""

from __future__ import annotations

import pandas as pd
from sqlalchemy.engine import Engine


def read_thai_funds(engine: Engine) -> pd.DataFrame:
    query = "SELECT * FROM thai_funds"
    return pd.read_sql(query, engine)
