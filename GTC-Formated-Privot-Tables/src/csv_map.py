"""Split size / color / organization out of item names and variations."""
from __future__ import annotations

import pandas as pd

from gtc_pivot_tables.config import patterns


def square_split_color_size(df: pd.DataFrame) -> pd.DataFrame:
    """Square: size comes from "Item Variation"; color is what is left of the
    variation after removing the size, falling back to a color in the item name."""
    p = patterns()
    df = df.copy()
    variation = df["Item Variation"].astype(str)
    size = variation.str.extract(p.size, expand=False)
    color = (
        variation.str.replace(p.size, "", regex=True)
        .str.replace(r"\s*,\s*", ", ", regex=True)
        .str.strip(" ,")
    )
    color = color.mask(color.eq("") | color.eq("None"))
    color_from_name = df["Item Name"].astype(str).str.extract(p.color, expand=False)
    df["Color"] = color.combine_first(color_from_name)
    df["Size"] = size.str.upper()
    return df.drop(columns=["Item Variation"])


def shopify_split_color_size(df: pd.DataFrame) -> pd.DataFrame:
    p = patterns()
    df = df.copy()
    name = df["Lineitem name"].astype(str)
    df["Greek Name"] = name.str.extract(p.organization, expand=False)
    df["Size"] = name.str.extract(p.size, expand=False).str.upper()
    df["Color"] = name.str.extract(p.color, expand=False)
    return df
