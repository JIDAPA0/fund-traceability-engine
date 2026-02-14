"""Read source rows from global funds database."""

from __future__ import annotations

import pandas as pd
from sqlalchemy.engine import Engine


def read_global_funds(engine: Engine) -> pd.DataFrame:
    query = "SELECT * FROM global_funds"
    return pd.read_sql(query, engine)
