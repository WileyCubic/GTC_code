"""The three pivot tables in the daily workbook, with subtotals and grand totals."""
from __future__ import annotations

import pandas as pd

from gtc_pivot_tables.csv_map import shopify_split_color_size, square_split_color_size
from gtc_pivot_tables.extract import SQUARE

# Column names per store: (quantity, price, item, customer, extra item levels)
_COLUMNS = {
    SQUARE: dict(qty="Item Quantity", price="Item Price", item="Item Name",
                 customer="Order Name", extra=["Item Modifiers", "Item Variation"]),
    "shopify": dict(qty="Lineitem quantity", price="Lineitem price", item="Lineitem name",
                    customer="Shipping Name", extra=[]),
}


def garment_counts(source: str, df: pd.DataFrame) -> pd.DataFrame:
    """Units sold per item / size / color (Square) or item / size / organization (Shopify)."""
    if source == SQUARE:
        df = square_split_color_size(df)
        index, qty = ["Item Name", "Size", "Color"], "Item Quantity"
    else:
        df = shopify_split_color_size(df)
        index, qty = ["Lineitem name", "Size", "Greek Name"], "Lineitem quantity"
    # Items with no recognised size/color/org are kept under a blank label.
    # (pivot_table silently drops rows with a missing index value; the Shopify
    # table used to lose them.)
    df[index] = df[index].fillna("")
    table = df.pivot_table(index=index, values=[qty], aggfunc={qty: "sum"}).sort_index()
    table.loc[("Grand Total",) + ("",) * (len(index) - 1), qty] = table[qty].sum()
    return table


def _pivot(df: pd.DataFrame, index: list[str], qty: str, price: str) -> pd.DataFrame:
    return df.pivot_table(index=index, values=[qty, price],
                          aggfunc={qty: "sum", price: "first"}).sort_index()


def add_totals(table: pd.DataFrame, qty: str, price: str) -> pd.DataFrame:
    """Add a quantity subtotal under each value of the first index level, plus a
    grand-total row. The grand total's revenue (sum of price x quantity) goes in
    its own "Total Revenue" column; it used to be written into the unit-price column.
    """
    names = list(table.index.names)
    group, marker = names[0], names[1]
    total_qty = table[qty].sum()
    total_revenue = (table[price] * table[qty]).sum()

    sub = table.groupby(level=group)[[qty]].sum()
    sub_index = sub.index.to_frame(index=False)
    for name in names[1:]:
        sub_index[name] = "SubTotal" if name == marker else ""
    sub.index = pd.MultiIndex.from_frame(sub_index[names])

    out = pd.concat([table, sub])
    keys = out.index.to_frame(index=False).astype(str)
    keys["__is_sub__"] = keys[marker] == "SubTotal"
    out = out.iloc[keys.sort_values([group, "__is_sub__"] + names[1:], kind="stable").index]

    grand = pd.DataFrame(
        {qty: [total_qty], price: [float("nan")], "Total Revenue": [round(total_revenue, 2)]},
        index=pd.MultiIndex.from_tuples([("Grand Total",) + ("",) * (len(names) - 1)], names=names),
    )
    return pd.concat([out, grand])


def by_item(source: str, df: pd.DataFrame) -> pd.DataFrame:
    c = _COLUMNS[source]
    index = [c["item"], *c["extra"], c["customer"]]
    return add_totals(_pivot(df, index, c["qty"], c["price"]), c["qty"], c["price"])


def by_customer(source: str, df: pd.DataFrame) -> pd.DataFrame:
    c = _COLUMNS[source]
    index = [c["customer"], c["item"], *c["extra"]]
    return add_totals(_pivot(df, index, c["qty"], c["price"]), c["qty"], c["price"])
