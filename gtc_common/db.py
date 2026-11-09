"""Postgres access for every GTC project.

Connection settings come from the root ``.env``:
``PG_HOST``, ``PG_PORT``, ``PG_USER``, ``PG_PASSWORD``, ``PG_DATABASE``.

Tables are created by ``gtc-db-migrate`` from ``gtc_common/schema/*.sql``;
the load helpers here only ever write into tables that already exist, so the
column types in the schema are the contract.
"""
from __future__ import annotations

import logging
import re
from functools import lru_cache
from pathlib import Path

import pandas as pd
from sqlalchemy import URL, create_engine, text
from sqlalchemy.engine import Engine

from gtc_common import env

logger = logging.getLogger(__name__)

_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


def _check_identifier(name: str) -> str:
    if not _IDENTIFIER.match(name):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return name


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    url = URL.create(
        "postgresql+psycopg",
        username=env.require("PG_USER"),
        password=env.get("PG_PASSWORD"),
        host=env.get("PG_HOST", "localhost"),
        port=int(env.get("PG_PORT", "5432")),
        database=env.get("PG_DATABASE", "GTC"),
    )
    return create_engine(url, pool_pre_ping=True)


def to_snake(column: str) -> str:
    """'Lineitem compare at price' -> 'lineitem_compare_at_price', 'City/State' -> 'city_state'."""
    snake = re.sub(r"[^0-9a-zA-Z]+", "_", column).strip("_").lower()
    return snake


def snake_columns(df: pd.DataFrame, overrides: dict[str, str] | None = None) -> pd.DataFrame:
    overrides = overrides or {}
    return df.rename(columns={c: overrides.get(c, to_snake(c)) for c in df.columns})


def table_columns(table: str) -> list[str]:
    _check_identifier(table)
    with get_engine().connect() as conn:
        rows = conn.execute(
            text(
                "select column_name from information_schema.columns "
                "where table_schema = 'public' and table_name = :t order by ordinal_position"
            ),
            {"t": table},
        )
        return [r[0] for r in rows]


def _align_to_table(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """Keep only columns the table has; fail loudly on unexpected ones."""
    columns = table_columns(table)
    if not columns:
        raise RuntimeError(f"Table '{table}' does not exist. Run `gtc-db-migrate` first.")
    unknown = [c for c in df.columns if c not in columns]
    if unknown:
        raise RuntimeError(
            f"Columns {unknown} are not in table '{table}'. The CSV export format may "
            f"have changed; update gtc_common/schema and re-run `gtc-db-migrate`."
        )
    return df


def replace_table(df: pd.DataFrame, table: str) -> int:
    """Empty ``table`` and load ``df`` into it, in one transaction."""
    _check_identifier(table)
    df = _align_to_table(df, table)
    with get_engine().begin() as conn:
        conn.execute(text(f"truncate table {table} restart identity"))
        df.to_sql(table, conn, if_exists="append", index=False, method="multi", chunksize=500)
    logger.info("Replaced contents of %s with %s rows", table, len(df))
    return len(df)


def upsert_orders(df: pd.DataFrame, table: str, key: str) -> int:
    """Replace every order present in ``df`` and leave all other orders untouched.

    Re-loading an export that overlaps an earlier one therefore never creates
    duplicates, and deleting old CSV files never deletes history.
    """
    _check_identifier(table)
    _check_identifier(key)
    df = _align_to_table(df, table)
    keys = df[key].dropna().astype(str).unique().tolist()
    with get_engine().begin() as conn:
        deleted = conn.execute(
            text(f"delete from {table} where {key} = any(:keys)"), {"keys": keys}
        ).rowcount
        df.to_sql(table, conn, if_exists="append", index=False, method="multi", chunksize=500)
    logger.info(
        "%s: replaced %s existing rows with %s rows covering %s orders",
        table, deleted, len(df), len(keys),
    )
    return len(df)


def read_sql(sql: str, params: dict | None = None) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params or {})


def read_sql_file(path: Path, params: dict | None = None) -> pd.DataFrame:
    logger.info("Running query %s", path.name)
    df = read_sql(Path(path).read_text(encoding="utf-8"), params)
    logger.info("Query %s returned %s rows", path.name, len(df))
    return df
