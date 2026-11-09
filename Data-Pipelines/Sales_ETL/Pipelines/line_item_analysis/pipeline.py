"""Line-item analysis pipeline: square_raw + shopify_raw -> lineitem_analysis.

Runs ``sql/lineitem_analysis_query.sql`` (one row per line item from both
stores), tags each item with size / color / organization / garment, and
rebuilds the ``lineitem_analysis`` table.
"""
from __future__ import annotations

import logging
from pathlib import Path

from gtc_common.db import read_sql_file, replace_table

from .transform import transform_lineitem_analysis

logger = logging.getLogger(__name__)

TABLE = "lineitem_analysis"
QUERY_FILE = Path(__file__).resolve().parents[2] / "sql" / "lineitem_analysis_query.sql"


def run_pipeline() -> int:
    logger.info("[Starting] line item analysis pipeline")
    df = transform_lineitem_analysis(read_sql_file(QUERY_FILE))
    rows = replace_table(df, TABLE)
    logger.info("[Completed] line item analysis pipeline: %s rows", rows)
    return rows
