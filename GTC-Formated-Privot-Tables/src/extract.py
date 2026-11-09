"""Read the daily order CSVs and keep only the columns the pivot tables need."""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from gtc_common import orders

logger = logging.getLogger(__name__)

SQUARE = "square"
SHOPIFY = "shopify"


def detect_source(df: pd.DataFrame) -> str:
    """Identify the export by its columns (the old code relied on exact column counts)."""
    if {"Order Name", "Item Name", "Item Variation"} <= set(df.columns):
        return SQUARE
    if {"Name", "Lineitem name", "Shipping Name"} <= set(df.columns):
        return SHOPIFY
    raise ValueError(f"Unrecognised CSV format; columns were: {list(df.columns)[:10]}...")


def load_input(folder: Path) -> tuple[str, pd.DataFrame]:
    """Read every CSV in ``folder``. All files must come from the same store."""
    files = orders.list_csvs(folder)
    if not files:
        raise FileNotFoundError(f"No CSV files in {folder}")
    frames = [pd.read_csv(f) for f in files]
    sources = {detect_source(df) for df in frames}
    if len(sources) > 1:
        raise ValueError(
            "The input folder mixes Square and Shopify exports. Run the pivot tables "
            "for one store at a time."
        )
    return sources.pop(), pd.concat(frames, ignore_index=True)


def clean(source: str, df: pd.DataFrame) -> pd.DataFrame:
    if source == SQUARE:
        df = df[["Order Name", "Item Quantity", "Item Name", "Item Variation",
                 "Item Modifiers", "Item Price"]].copy()
        df["Item Quantity"] = pd.to_numeric(df["Item Quantity"], errors="coerce").fillna(0).astype(int)
        df[["Order Name", "Item Name", "Item Variation", "Item Modifiers"]] = (
            df[["Order Name", "Item Name", "Item Variation", "Item Modifiers"]].fillna("None")
        )
    else:
        # Customer name is only on the first line of each order; copy it per order.
        df = orders.fill_within_order(df, "Name", ["Shipping Name"])
        df = df[["Lineitem quantity", "Lineitem name", "Lineitem price", "Shipping Name"]].copy()
        df[["Lineitem name", "Shipping Name"]] = df[["Lineitem name", "Shipping Name"]].fillna("None")
    logger.info("%s input cleaned: %s rows", source, len(df))
    return df
