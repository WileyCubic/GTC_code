"""Square / Shopify order-export helpers shared by the Sales ETL and the reports.

Before this module existed the same cleaning code was copy-pasted into four
places (raw_transform.py, temp_pipes(quarterly).py, the royalty script and the
yearly tax script), each slightly different.
"""
from __future__ import annotations

import datetime as dt
import logging
import re
from pathlib import Path
from typing import Iterable

import pandas as pd

logger = logging.getLogger(__name__)

# Shopify exports one row per line item; order-level fields are only filled on
# the first row of each order. These descriptive fields are copied onto the
# order's other rows. Money totals (Subtotal, Shipping, Taxes, Total) are
# deliberately NOT copied: they stay on the first row only, so summing them
# never double-counts an order.
SHOPIFY_ORDER_LEVEL_COLUMNS = [
    "Financial Status", "Paid at", "Fulfillment Status", "Fulfilled at",
    "Cancelled at",
    "Billing Name", "Billing Street", "Billing Address1", "Billing City",
    "Billing Zip", "Billing Province", "Billing Country",
    "Shipping Name", "Shipping Street", "Shipping Address1", "Shipping City",
    "Shipping Zip", "Shipping Province", "Shipping Country",
    "Payment Reference", "Id", "Payment ID",
]


# ----------------------------------------------------------------------------
# Reading files
# ----------------------------------------------------------------------------

def list_csvs(folder: Path, pattern: str = "*.csv") -> list[Path]:
    folder = Path(folder)
    if not folder.exists():
        logger.error("Input folder does not exist: %s", folder)
        return []
    files = sorted(folder.glob(pattern))
    logger.info("Found %s CSV file(s) matching %s in %s", len(files), pattern, folder)
    return files


def read_csvs(files: Iterable[Path]) -> pd.DataFrame:
    files = list(files)
    if not files:
        return pd.DataFrame()
    return pd.concat((pd.read_csv(f, dtype=str, keep_default_na=True) for f in files), ignore_index=True)


def find_one(folder: Path, pattern: str) -> Path:
    """Return the single file in ``folder`` matching ``pattern`` (error if 0 or >1)."""
    matches = sorted(Path(folder).glob(pattern))
    if len(matches) != 1:
        raise FileNotFoundError(
            f"Expected exactly one file matching '{pattern}' in {folder}, found {len(matches)}: "
            f"{[m.name for m in matches]}"
        )
    return matches[0]


# ----------------------------------------------------------------------------
# Cleaning helpers
# ----------------------------------------------------------------------------

def format_phone_number(value) -> str | None:
    """Normalise a phone number to the formats the business has always used.

    Accepts raw numbers ("3109901637", "13109901637.0") and already formatted
    values ("1-(310) 990-1637"), so it is safe to run twice. Anything that is
    not a recognised length is returned unchanged rather than dropped.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)) or value is pd.NA:
        return None
    raw = str(value).strip()
    if raw == "" or raw.lower() == "nan":
        return None
    if re.fullmatch(r"\d+\.0", raw):
        raw = raw[:-2]
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 10:
        return f"({digits[:3]})-{digits[3:6]}-{digits[6:]}"
    if len(digits) == 11:
        return f"{digits[0]}-({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    if len(digits) == 9:
        return f"({digits[:2]}) {digits[2:5]}-{digits[5:]}"
    if len(digits) == 12:
        return f"{digits[0:2]}-({digits[2:5]}) {digits[5:8]}-{digits[8:]}"
    return raw


def fill_within_order(df: pd.DataFrame, order_col: str, columns: Iterable[str]) -> pd.DataFrame:
    """Copy order-level values onto every line-item row of the same order.

    Uses a per-order forward *and* backward fill. The old code did
    ``groupby(order).ffill().bfill()`` which back-filled across the whole
    column and could copy one customer's details onto a different order.
    """
    df = df.copy()
    grouped = df.groupby(order_col, sort=False)
    for col in columns:
        if col in df.columns:
            df[col] = grouped[col].transform(lambda s: s.ffill().bfill())
    return df


def strip_apostrophes(series: pd.Series) -> pd.Series:
    """Shopify prefixes zips with ' to keep leading zeros in Excel."""
    return series.astype("string").str.replace("'", "", regex=False)


def to_money(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").round(2)


def to_timestamp(series: pd.Series, utc: bool = False) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=utc)


def drop_cancelled_shopify(df: pd.DataFrame) -> pd.DataFrame:
    """Remove Shopify orders that were cancelled (checked after the order-level fill)."""
    if "Cancelled at" not in df.columns:
        return df
    keep = df["Cancelled at"].isna()
    dropped = df.loc[~keep, "Name"].nunique() if "Name" in df.columns else int((~keep).sum())
    if dropped:
        logger.info("Dropped %s cancelled Shopify order(s)", dropped)
    return df.loc[keep].copy()


# ----------------------------------------------------------------------------
# Reporting periods
# ----------------------------------------------------------------------------

def last_completed_quarter(today: dt.date | None = None) -> tuple[int, int]:
    """(year, quarter) of the most recent quarter that has fully ended.

    Run in January 2027 this returns (2026, 4). The old royalty script computed
    ``(month - 1) // 3`` with the current year, which produced "Q0" in Q1.
    """
    today = today or dt.date.today()
    current_q = (today.month - 1) // 3 + 1
    if current_q == 1:
        return today.year - 1, 4
    return today.year, current_q - 1


def quarter_bounds(year: int, quarter: int) -> tuple[dt.date, dt.date]:
    """First day of the quarter and first day of the *next* quarter (exclusive end)."""
    if quarter not in (1, 2, 3, 4):
        raise ValueError(f"quarter must be 1-4, got {quarter}")
    start = dt.date(year, 3 * (quarter - 1) + 1, 1)
    end = dt.date(year + 1, 1, 1) if quarter == 4 else dt.date(year, 3 * quarter + 1, 1)
    return start, end
