"""One-time: fill the new Postgres database.   gtc-db-seed [--sqlite GTC_DB.db]

Why not just re-run the ETLs? The Square exports in SQUARE_ORDERS_CSV_DIR only
go back to Aug-Nov 2024 (older files were removed after loading), but the old
SQLite database still holds every Square order. So:

* square_raw        <- old SQLite ``square_raw`` table, cleaned with today's transform
                       (then any CSVs in SQUARE_ORDERS_CSV_DIR are upserted on top)
* shopify_raw       <- Shopify CSV export (covers every order in the old table)
* bp/ab_expences    <- AmEx CSVs
* lineitem_analysis <- rebuilt from the two raw tables

Prints old-vs-new row counts at the end. Safe to re-run.
"""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import pandas as pd

from gtc_common.env import REPO_ROOT
from gtc_common.log import setup_logging


def _sqlite_count(conn: sqlite3.Connection, table: str) -> int | None:
    try:
        return conn.execute(f'select count(*) from "{table}"').fetchone()[0]
    except sqlite3.Error:
        return None


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sqlite", type=Path, default=REPO_ROOT / "GTC_DB.db",
                        help="old SQLite database holding square_raw (default: GTC_DB.db at repo root)")
    args = parser.parse_args(argv)
    logger = setup_logging("db_seed")

    from expenses_etl.pipeline import main as run_expenses
    from gtc_common.db import read_sql, upsert_orders
    from Sales_ETL.Pipelines.line_item_analysis.pipeline import run_pipeline as run_lineitem
    from Sales_ETL.Pipelines.raw_orders.pipeline import load_shopify, load_square
    from Sales_ETL.Pipelines.raw_orders.transform import transform_square

    with sqlite3.connect(args.sqlite) as conn:
        logger.info("Seeding square_raw from %s", args.sqlite)
        old_square = pd.read_sql('select * from square_raw', conn, dtype=str)
        old_counts = {t: _sqlite_count(conn, t) for t in ("square_raw", "shopify_raw", "lineitem_analysis")}
    upsert_orders(transform_square(old_square), "square_raw", key="order_id")

    load_square()
    load_shopify()
    run_lineitem()
    run_expenses()
    setup_logging("db_seed")  # run_expenses re-points logging

    print("\nRow counts (old SQLite -> new Postgres):")
    for table in ("square_raw", "shopify_raw", "lineitem_analysis", "bp_expences", "ab_expences"):
        new = int(read_sql(f"select count(*) as n from {table}")["n"].iloc[0])
        print(f"  {table:<18} {str(old_counts.get(table, '-')):>6} -> {new}")


if __name__ == "__main__":
    main()
