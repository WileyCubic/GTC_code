"""Quarterly royalty report straight from the store exports (no database).

    gtc-royalties-csv --input-dir "data/royalty report copys/Affinity_Q3_2026" [--year 2026 --quarter 3]

The input folder must contain exactly one Square export (``orders-*.csv``) and
one Shopify export (``orders_export*.csv``) covering the quarter. The report is
written to ROYALTY_OUTPUT_DIR (and also to ROYALTY_COPY_DIR when that is set)
as ``Royalties_report_<year>_Q<quarter>.csv``.

Year/quarter only name the output file; they default to the last completed
quarter. Every row in the two files is included (no date filter), so export
exactly the quarter you are reporting.

Output columns: Date, Invoice Id, Item Description, Customer, Shipping Country,
Shipping Postal Code, Number of Units, Price per Unit.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from gtc_common import env, orders
from gtc_common.log import setup_logging

logger = logging.getLogger(__name__)

OUT_COLUMNS = [
    "Date", "Invoice Id", "Item Description", "Customer", "Shipping Country",
    "Shipping Postal Code", "Number of Units", "Price per Unit",
]
SQUARE_COLUMNS = [
    "Order Date", "Order", "Item Name", "Order Name", "Recipient Country",
    "Recipient Postal Code", "Item Quantity", "Item Price",
]
SHOPIFY_COLUMNS = [
    "Paid at", "Name", "Lineitem name", "Shipping Name", "Shipping Country",
    "Shipping Zip", "Lineitem quantity", "Lineitem price",
]


def transform_square(df: pd.DataFrame) -> pd.DataFrame:
    df = df[SQUARE_COLUMNS].copy()
    df["Order Date"] = pd.to_datetime(df["Order Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["Item Price"] = orders.to_money(df["Item Price"])
    df.columns = OUT_COLUMNS
    return df


def transform_shopify(df: pd.DataFrame) -> pd.DataFrame:
    # Fill order-level fields per order first, then drop cancelled orders, so
    # every line of a cancelled order is removed (previously only its first line was).
    df = orders.fill_within_order(df, "Name", ["Paid at", "Created at", "Shipping Name",
                                               "Shipping Zip", "Shipping Country", "Cancelled at"])
    df = orders.drop_cancelled_shopify(df)
    # Some paid orders have no "Paid at" (no payment method, e.g. free / paid
    # offline). Use the order's creation date for those. (The old code
    # back-filled them with the *next* order's date.)
    df["Paid at"] = df["Paid at"].fillna(df["Created at"])
    df = df[SHOPIFY_COLUMNS].copy()
    # Dates are kept in the store's local time zone (as written in the export).
    df["Paid at"] = df["Paid at"].astype("string").str.slice(0, 10)
    df["Shipping Zip"] = orders.strip_apostrophes(df["Shipping Zip"])
    df["Lineitem price"] = orders.to_money(df["Lineitem price"])
    df.columns = OUT_COLUMNS
    return df


def build_report(input_dir: Path) -> pd.DataFrame:
    square_file = orders.find_one(input_dir, "orders-*.csv")
    shopify_file = orders.find_one(input_dir, "orders_export*.csv")
    logger.info("Square file:  %s", square_file.name)
    logger.info("Shopify file: %s", shopify_file.name)
    square = transform_square(pd.read_csv(square_file, dtype=str))
    shopify = transform_shopify(pd.read_csv(shopify_file, dtype=str))
    logger.info("Square rows: %s, Shopify rows: %s", len(square), len(shopify))
    report = pd.concat([square, shopify], ignore_index=True)
    return report.sort_values("Date", kind="stable").reset_index(drop=True)


def output_dirs() -> list[Path]:
    dirs = [env.path("ROYALTY_OUTPUT_DIR")]
    copy_dir = env.optional_path("ROYALTY_COPY_DIR")
    if copy_dir:
        dirs.append(copy_dir)
    return dirs


def main(argv: list[str] | None = None) -> None:
    default_year, default_quarter = orders.last_completed_quarter()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input-dir", type=Path, default=env.optional_path("ROYALTY_INPUT_DIR"),
                        help="folder with this quarter's Square + Shopify exports (default: ROYALTY_INPUT_DIR)")
    parser.add_argument("--year", type=int, default=default_year)
    parser.add_argument("--quarter", type=int, choices=[1, 2, 3, 4], default=default_quarter)
    args = parser.parse_args(argv)
    if args.input_dir is None:
        parser.error("give --input-dir or set ROYALTY_INPUT_DIR in .env")

    setup_logging("royalties_csv")
    report = build_report(args.input_dir)
    file_name = f"Royalties_report_{args.year}_Q{args.quarter}.csv"
    for directory in output_dirs():
        directory.mkdir(parents=True, exist_ok=True)
        report.to_csv(directory / file_name, index=False)
        logger.info("Wrote %s rows to %s", len(report), directory / file_name)


if __name__ == "__main__":
    main()
