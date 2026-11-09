"""Clean Square and Shopify order exports into the shape of the Postgres tables."""
from __future__ import annotations

import logging

import pandas as pd

from gtc_common import orders
from gtc_common.db import snake_columns

logger = logging.getLogger(__name__)

SQUARE_MONEY = [
    "Order Subtotal", "Order Shipping Price", "Order Tax Total", "Order Total",
    "Order Refunded Amount", "Item Price", "Item Options Total Price", "Item Total Price",
]
SHOPIFY_MONEY = [
    "Subtotal", "Shipping", "Taxes", "Total", "Discount Amount", "Lineitem price",
    "Lineitem compare at price", "Refunded Amount", "Outstanding Balance",
    "Lineitem discount", "Tax 1 Value", "Tax 2 Value", "Tax 3 Value", "Tax 4 Value", "Tax 5 Value",
]
SHOPIFY_TIMESTAMPS = ["Paid at", "Fulfilled at", "Created at", "Cancelled at"]
SHOPIFY_DEFAULTS = {
    "Notes": "No Notes Given",
    "Note Attributes": "No Note Attributes Given",
    "Billing Company": "No Company Given",
    "Shipping Company": "No Company Given",
    "Email": "No Email Given",
    **{f"Tax {i} Name": "No Name Given" for i in range(1, 6)},
}


def _require(df: pd.DataFrame, columns: list[str], source: str) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"{source} export is missing expected columns: {missing}")


def transform_square(df: pd.DataFrame) -> pd.DataFrame:
    _require(df, ["Order", "Order Date", "Item Name"], "Square")
    df = df.copy()

    # Older loads added a made-up "Order ID" (row number). The real id is "Order".
    df = df.drop(columns=["Order ID"], errors="ignore")

    for col in SQUARE_MONEY:
        if col in df.columns:
            df[col] = orders.to_money(df[col])
    for col in ["Order Shipping Price", "Order Tax Total", "Order Refunded Amount"]:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    df["Order Date"] = orders.to_timestamp(df["Order Date"])
    if "Fulfillment Date" in df.columns:
        df["Fulfillment Date"] = orders.to_timestamp(df["Fulfillment Date"])
    if "Recipient Phone" in df.columns:
        df["Recipient Phone"] = df["Recipient Phone"].map(orders.format_phone_number)
    if "Item Quantity" in df.columns:
        df["Item Quantity"] = pd.to_numeric(df["Item Quantity"], errors="coerce").astype("Int64")

    bad_dates = df["Order Date"].isna().sum()
    if bad_dates:
        logger.warning("%s Square row(s) have an unreadable Order Date", bad_dates)

    df = df.sort_values("Order Date", kind="stable").reset_index(drop=True)
    logger.info("Square: %s rows, %s orders", len(df), df["Order"].nunique())
    return snake_columns(df, overrides={"Order": "order_id"})


def transform_shopify(df: pd.DataFrame) -> pd.DataFrame:
    _require(df, ["Name", "Paid at", "Lineitem name"], "Shopify")

    # Copy order-level fields onto every line item of the same order *first*,
    # so that every line of a cancelled order carries "Cancelled at" and is dropped.
    df = orders.fill_within_order(df, "Name", orders.SHOPIFY_ORDER_LEVEL_COLUMNS)
    df = orders.drop_cancelled_shopify(df)

    for col in ["Shipping Zip", "Billing Zip"]:
        if col in df.columns:
            df[col] = orders.strip_apostrophes(df[col])

    df = df.fillna({k: v for k, v in SHOPIFY_DEFAULTS.items() if k in df.columns})

    for col in SHOPIFY_TIMESTAMPS:
        if col in df.columns:
            df[col] = orders.to_timestamp(df[col], utc=True)
    for col in ["Billing Phone", "Shipping Phone", "Phone"]:
        if col in df.columns:
            df[col] = df[col].map(orders.format_phone_number)
    for col in SHOPIFY_MONEY:
        if col in df.columns:
            df[col] = orders.to_money(df[col])
    for i in range(1, 6):
        col = f"Tax {i} Value"
        if col in df.columns:
            df[col] = df[col].fillna(0)
    df["Lineitem quantity"] = pd.to_numeric(df["Lineitem quantity"], errors="coerce").astype("Int64")

    df = df.sort_values("Name", kind="stable").reset_index(drop=True)
    logger.info("Shopify: %s rows, %s orders", len(df), df["Name"].nunique())
    return snake_columns(df)
