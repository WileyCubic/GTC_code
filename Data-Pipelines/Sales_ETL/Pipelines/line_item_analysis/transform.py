"""Extract size / color / organization / garment from line-item names."""
from __future__ import annotations

import json
import logging
import re
from functools import lru_cache
from pathlib import Path

import pandas as pd

from gtc_common.patterns import partial_pattern, word_pattern

logger = logging.getLogger(__name__)

PATTERN_FILE = Path(__file__).resolve().parents[2] / "assets" / "lineitem_patterns.json"


@lru_cache(maxsize=1)
def patterns() -> dict[str, re.Pattern]:
    lists = json.loads(PATTERN_FILE.read_text(encoding="utf-8"))
    return {
        "size": word_pattern(lists["sizes"]),
        "color": word_pattern(lists["colors"]),
        "organization": word_pattern(lists["organization_names"]),
        "garment": partial_pattern(lists["garments"]),
    }


def transform_lineitem_analysis(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce").dt.date
    df["shipping_location"] = df["shipping_location"].fillna("InPerson")

    names = df["item_name"].astype("string")
    p = patterns()
    df["size"] = names.str.extract(p["size"], expand=False).str.upper()
    df["color"] = names.str.extract(p["color"], expand=False)
    df["organization_name"] = names.str.extract(p["organization"], expand=False)
    df["garment_type"] = names.str.extract(p["garment"], expand=False)
    logger.info(
        "Unmatched counts - size:%s color:%s org:%s garment:%s (of %s rows)",
        df["size"].isna().sum(), df["color"].isna().sum(),
        df["organization_name"].isna().sum(), df["garment_type"].isna().sum(), len(df),
    )

    df["item_quantity"] = pd.to_numeric(df["item_quantity"], errors="coerce").astype("Int64")
    df["item_price"] = pd.to_numeric(df["item_price"], errors="coerce")
    df["money_value"] = (df["item_quantity"].astype("Float64") * df["item_price"]).round(2)
    df["rolling_total"] = df["money_value"].fillna(0).cumsum().round(2)
    return df
