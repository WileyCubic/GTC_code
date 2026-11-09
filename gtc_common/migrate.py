"""Create / update the Postgres tables.  Run:  gtc-db-migrate

Applies every ``gtc_common/schema/NNN_*.sql`` file in order. Each file must be
safe to re-run (``create table if not exists`` etc.).
"""
from __future__ import annotations

from pathlib import Path

from sqlalchemy import text

from gtc_common.db import get_engine
from gtc_common.log import setup_logging

SCHEMA_DIR = Path(__file__).parent / "schema"


def main() -> None:
    logger = setup_logging("db_migrate")
    files = sorted(SCHEMA_DIR.glob("*.sql"))
    engine = get_engine()
    with engine.begin() as conn:
        for sql_file in files:
            logger.info("Applying %s", sql_file.name)
            conn.execute(text(sql_file.read_text(encoding="utf-8")))
        tables = conn.execute(
            text("select table_name from information_schema.tables "
                 "where table_schema = 'public' order by table_name")
        ).scalars().all()
    logger.info("Done. Tables in %s: %s", engine.url.database, ", ".join(tables))


if __name__ == "__main__":
    main()
