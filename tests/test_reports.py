import pandas as pd

from gtc_pivot_tables import pivots
from gtc_pivot_tables.extract import SHOPIFY, SQUARE, clean, detect_source
from gtc_reports.royalties_from_csv import OUT_COLUMNS, build_report


def test_royalty_report_from_csv(tmp_path):
    pd.DataFrame({
        "Order Date": ["2026-07-05 00:00:00"], "Order": ["Square Online 1"], "Item Name": ["NCL Tee"],
        "Order Name": ["Ann"], "Recipient Country": ["US"], "Recipient Postal Code": ["02139"],
        "Item Quantity": ["1"], "Item Price": ["20"],
    }).to_csv(tmp_path / "orders-2026-07-01-2026-09-30.csv", index=False)
    pd.DataFrame({
        "Name": ["#1", "#1", "#2"],
        "Paid at": [None, None, "2026-07-03 10:00:00 -0700"],
        "Created at": ["2026-07-24 10:41:17 -0700", None, "2026-07-03 09:00:00 -0700"],
        "Cancelled at": [None, None, None],
        "Lineitem name": ["Hoodie", "Short", "Tee"],
        "Shipping Name": ["Sam", None, "Bo"],
        "Shipping Country": ["US", None, "US"],
        "Shipping Zip": ["'33896", None, "'10001"],
        "Lineitem quantity": ["1", "1", "2"],
        "Lineitem price": ["65", "38", "10"],
    }).to_csv(tmp_path / "orders_export_1.csv", index=False)

    report = build_report(tmp_path)
    assert list(report.columns) == OUT_COLUMNS
    assert len(report) == 4
    order_1 = report[report["Invoice Id"] == "#1"]
    # No "Paid at" anywhere in #1 -> falls back to its own creation date.
    assert order_1["Date"].tolist() == ["2026-07-24", "2026-07-24"]
    assert order_1["Customer"].tolist() == ["Sam", "Sam"]
    assert report["Date"].is_monotonic_increasing


def test_detect_source():
    assert detect_source(pd.DataFrame(columns=["Order Name", "Item Name", "Item Variation"])) == SQUARE
    assert detect_source(pd.DataFrame(columns=["Name", "Lineitem name", "Shipping Name"])) == SHOPIFY


def _square_daily():
    return clean(SQUARE, pd.DataFrame({
        "Order Name": ["Ann", "Ann", "Bob"],
        "Item Quantity": [1, 2, 3],
        "Item Name": ["NCL Tee", "NCL Tee", "Mystery Item"],
        "Item Variation": ["Navy, S", "Navy, M", None],
        "Item Modifiers": [None, None, None],
        "Item Price": [26.0, 26.0, 10.0],
    }))


def test_garment_counts_keep_unmatched_items():
    table = pivots.garment_counts(SQUARE, _square_daily())
    assert table.loc[("Grand Total", "", ""), "Item Quantity"] == 6


def test_totals_put_revenue_in_its_own_column():
    table = pivots.by_customer(SQUARE, _square_daily())
    grand = table.loc[("Grand Total", "", "", "")]
    assert grand["Item Quantity"] == 6
    assert grand["Total Revenue"] == 26 * 3 + 10 * 3
    assert pd.isna(grand["Item Price"])
    subtotals = table[table.index.get_level_values(1) == "SubTotal"]
    assert subtotals["Item Quantity"].tolist() == [3, 3]
