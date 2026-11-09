"""Quarterly royalty report from the Postgres database (fed by the Sales ETL).

    gtc-sales-etl                       # load the latest exports first
    gtc-royalties-db [--year 2026 --quarter 3]

Writes ``Royalties_report_<year>_Q<quarter>_from_db.csv`` to ROYALTY_OUTPUT_DIR.
Defaults to the last completed quarter. Query: scripts/src/sql/royalties.sql.

Note: this report differs from gtc-royalties-csv on purpose-unclear points
inherited from the two original scripts (Square NCL-only filter, Shopify
customer = Billing Name vs Shipping Name). See docs/AUDIT.md.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from gtc_common import env, orders
from gtc_common.db import read_sql_file
from gtc_common.log import setup_logging

logger = logging.getLogger(__name__)

QUERY_FILE = Path(__file__).parent / "sql" / "royalties.sql"


def main(argv: list[str] | None = None) -> None:
    default_year, default_quarter = orders.last_completed_quarter()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--year", type=int, default=default_year)
    parser.add_argument("--quarter", type=int, choices=[1, 2, 3, 4], default=default_quarter)
    args = parser.parse_args(argv)

    setup_logging("royalties_db")
    start, end = orders.quarter_bounds(args.year, args.quarter)
    logger.info("Royalty report for %s Q%s (%s to %s)", args.year, args.quarter, start, end)
    report = read_sql_file(QUERY_FILE, {"start": start, "end": end})

    out_dir = env.path("ROYALTY_OUTPUT_DIR")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"Royalties_report_{args.year}_Q{args.quarter}_from_db.csv"
    report.to_csv(out_file, index=False)
    logger.info("Wrote %s rows to %s", len(report), out_file)


if __name__ == "__main__":
    main()
