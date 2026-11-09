"""Run the Sales ETL.   gtc-sales-etl  [--only raw|lineitem]

1. raw_orders          Square + Shopify CSV exports  -> square_raw, shopify_raw
2. line_item_analysis  square_raw + shopify_raw      -> lineitem_analysis
"""
from __future__ import annotations

import argparse

from gtc_common.log import setup_logging


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--only", choices=["raw", "lineitem"], help="run just one of the two steps")
    args = parser.parse_args(argv)

    if args.only in (None, "raw"):
        setup_logging("sales_raw_orders", summary_log="sales_master.log")
        from Sales_ETL.Pipelines.raw_orders.pipeline import run_pipeline as run_raw
        run_raw()

    if args.only in (None, "lineitem"):
        setup_logging("sales_lineitem_analysis", summary_log="sales_master.log")
        from Sales_ETL.Pipelines.line_item_analysis.pipeline import run_pipeline as run_lineitem
        run_lineitem()


if __name__ == "__main__":
    main()
