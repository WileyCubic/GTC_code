"""Data access for the dashboard (reads Postgres through gtc_common.db)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from gtc_common.db import read_sql_file

SQL_DIR = Path(__file__).parent / "sql"


def load_lineitem_analysis() -> pd.DataFrame:
    """Every analysed line item. Columns: see the lineitem_analysis table in docs/DATABASE.md."""
    df = read_sql_file(SQL_DIR / "lineitem_analysis_all.sql")
    df["order_date"] = pd.to_datetime(df["order_date"])
    return df
