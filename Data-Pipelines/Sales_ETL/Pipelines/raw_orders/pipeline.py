"""Raw orders pipeline: Square + Shopify CSV exports -> Postgres square_raw / shopify_raw.

Reads every CSV in SQUARE_ORDERS_CSV_DIR and SHOPIFY_ORDERS_CSV_DIR, cleans
them, and *upserts by order*: orders in the files replace the same orders in the
table, all other orders are kept. So you can drop a new export in the folder
(even one that overlaps the last one) and re-run safely.
"""
from __future__ import annotations

import logging

from gtc_common import env, orders
from gtc_common.db import upsert_orders

from .transform import transform_shopify, transform_square

logger = logging.getLogger(__name__)

SQUARE_TABLE = "square_raw"
SHOPIFY_TABLE = "shopify_raw"


def load_square() -> int:
    files = orders.list_csvs(env.path("SQUARE_ORDERS_CSV_DIR"))
    if not files:
        logger.warning("No Square CSV files found; square_raw left unchanged")
        return 0
    df = transform_square(orders.read_csvs(files))
    return upsert_orders(df, SQUARE_TABLE, key="order_id")


def load_shopify() -> int:
    files = orders.list_csvs(env.path("SHOPIFY_ORDERS_CSV_DIR"))
    if not files:
        logger.warning("No Shopify CSV files found; shopify_raw left unchanged")
        return 0
    df = transform_shopify(orders.read_csvs(files))
    return upsert_orders(df, SHOPIFY_TABLE, key="name")


def run_pipeline() -> dict[str, int]:
    logger.info("[Starting] raw orders pipeline")
    result = {SQUARE_TABLE: load_square(), SHOPIFY_TABLE: load_shopify()}
    logger.info("[Completed] raw orders pipeline: %s", result)
    return result
