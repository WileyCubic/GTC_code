"""Yearly sales-tax report: one row per order from Shopify, Affinity and Square.

    gtc-tax-report [--input-dir "data/yearly tax reporting"] [--output "scripts/scripts output/Yearly Tax Reporting.csv"]

The input folder (default TAX_INPUT_DIR) must contain:
  * one Shopify export          ``orders_export*.csv``    (only "paid" orders are used)
  * one Affinity royalty report ``*royalty_report*.csv``  (marketplace orders)
  * one sub-folder ``orders-*`` holding the Square export CSV(s) for the year

Output (default TAX_OUTPUT_FILE) columns:
OrderId, Customer_Name, Subtotal, Shipping, Taxes, Total, State, Country, City, Zip

There is no date filter: export exactly the tax year. Affinity rows have no
state/city in the report, so they are looked up from the US zip code
(pgeocode; downloads its data the first time it runs).
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from gtc_common import env, orders
from gtc_common.log import setup_logging

logger = logging.getLogger(__name__)

OUT_COLUMNS = ["OrderId", "Customer_Name", "Subtotal", "Shipping", "Taxes", "Total",
               "State", "Country", "City", "Zip"]


def add_location_from_zip(df: pd.DataFrame, zip_col: str) -> pd.DataFrame:
    import pgeocode

    nomi = pgeocode.Nominatim("us")

    def lookup(zip_code):
        try:
            result = nomi.query_postal_code(str(zip_code).split("-")[0].zfill(5))
            state = result["state_code"] if pd.notna(result["state_code"]) else None
            city = result["place_name"] if pd.notna(result["place_name"]) else None
            return pd.Series([state, city])
        except Exception:
            return pd.Series([None, None])

    df = df.copy()
    df[["State", "Shipping City"]] = df[zip_col].apply(lookup)
    return df


def shopify_orders(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    # Subtotal is only on the first row of each order, so this keeps one row per order.
    df = df[df["Subtotal"].notna() & (df["Financial Status"] == "paid")]
    df = df[["Name", "Shipping Name", "Subtotal", "Shipping", "Taxes", "Total",
             "Shipping Province", "Shipping Country", "Shipping City", "Shipping Zip"]].copy()
    df.columns = OUT_COLUMNS[:6] + ["State", "Country", "City", "Zip"]
    return df


def affinity_orders(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = df[df["Sub-total (before shipping and taxes)"].notna()]
    df = df[["Invoice Id", "Customer", "Sub-total (before shipping and taxes)", "Customer Total",
             "Shipping Fees", "Shipping Country", "Shipping Postal Code"]]
    df = add_location_from_zip(df, "Shipping Postal Code")
    df["tax total"] = (df["Customer Total"] - df["Sub-total (before shipping and taxes)"]
                       - df["Shipping Fees"]).round(2)
    df = df[["Invoice Id", "Customer", "Sub-total (before shipping and taxes)", "Shipping Fees",
             "tax total", "Customer Total", "State", "Shipping Country", "Shipping City",
             "Shipping Postal Code"]].copy()
    df.columns = OUT_COLUMNS
    return df


def square_orders(folder: Path) -> pd.DataFrame:
    df = orders.read_csvs(orders.list_csvs(folder))
    df = df[df["Order Subtotal"].notna()]
    df = df[["Order", "Recipient Name", "Order Subtotal", "Order Shipping Price", "Order Tax Total",
             "Order Total", "Recipient Region", "Recipient Country", "Recipient City",
             "Recipient Postal Code"]]
    df = df.groupby("Order").first().reset_index()  # one row per order
    for col in ["Order Subtotal", "Order Shipping Price", "Order Tax Total", "Order Total"]:
        df[col] = orders.to_money(df[col])
    df.columns = OUT_COLUMNS
    return df


def _square_folder(input_dir: Path) -> Path:
    folders = [p for p in input_dir.glob("orders-*") if p.is_dir()]
    if len(folders) != 1:
        raise FileNotFoundError(f"Expected one Square sub-folder 'orders-*' in {input_dir}, found {len(folders)}")
    return folders[0]


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input-dir", type=Path, default=env.optional_path("TAX_INPUT_DIR"))
    parser.add_argument("--output", type=Path, default=env.optional_path("TAX_OUTPUT_FILE"))
    args = parser.parse_args(argv)
    if args.input_dir is None or args.output is None:
        parser.error("give --input-dir/--output or set TAX_INPUT_DIR/TAX_OUTPUT_FILE in .env")

    setup_logging("yearly_tax_report")
    parts = {
        "Shopify": shopify_orders(orders.find_one(args.input_dir, "orders_export*.csv")),
        "Affinity": affinity_orders(orders.find_one(args.input_dir, "*royalty_report*.csv")),
        "Square": square_orders(_square_folder(args.input_dir)),
    }
    for name, part in parts.items():
        logger.info("%s orders: %s", name, len(part))

    all_orders = pd.concat(parts.values(), ignore_index=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    all_orders.to_csv(args.output, index=False)
    logger.info("Wrote %s orders to %s", len(all_orders), args.output)


if __name__ == "__main__":
    main()
