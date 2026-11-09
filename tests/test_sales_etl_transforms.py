import pandas as pd

from Sales_ETL.Pipelines.line_item_analysis.transform import transform_lineitem_analysis
from Sales_ETL.Pipelines.raw_orders.transform import transform_shopify, transform_square


def _square_rows():
    return pd.DataFrame({
        "Order": ["Square Online 1", "Square Online 1", "Square Online 2"],
        "Order Name": ["Ann", "Ann", "Bob"],
        "Order Date": ["2026-07-02 00:00:00", "2026-07-02 00:00:00", "2026-07-01 00:00:00"],
        "Order Subtotal": ["50", "50", "20.556"],
        "Order Shipping Price": [None, None, "5"],
        "Recipient Phone": ["3109901637", "3109901637", None],
        "Item Quantity": ["1", "2", "1"],
        "Item Name": ["NCL Tee", "NCL Hat", "Hoodie"],
        "Item Price": ["20", "15", "20.555"],
        "Order ID": ["9", "8", "7"],   # fake row-number column from old loads
    })


def test_transform_square_shapes_rows_for_postgres():
    out = transform_square(_square_rows())
    assert "order_id" in out.columns and "order" not in out.columns   # "order" is an SQL keyword
    assert "order_id" in out.columns and len(out.columns) == 9        # fake "Order ID" dropped
    assert out["order_id"].tolist()[0] == "Square Online 2"        # sorted by date
    assert out["order_shipping_price"].tolist() == [5.0, 0.0, 0.0]
    assert out["order_subtotal"].tolist() == [20.56, 50.0, 50.0]
    assert out["recipient_phone"].iloc[1] == "(310)-990-1637"
    assert str(out["item_quantity"].dtype) == "Int64"


def _shopify_rows():
    return pd.DataFrame({
        "Name": ["#10", "#10", "#11", "#11"],
        "Paid at": ["2026-07-01 10:00:00 -0700", None, "2026-07-02 10:00:00 -0700", None],
        "Created at": ["2026-07-01 09:59:00 -0700", None, "2026-07-02 09:59:00 -0700", None],
        "Cancelled at": [None, None, "2026-07-03 10:00:00 -0700", None],
        "Shipping Name": ["Ann", None, "Bob", None],
        "Shipping Zip": ["'02139", None, "'90210", None],
        "Subtotal": ["40", None, "10", None],
        "Lineitem quantity": ["1", "1", "1", "1"],
        "Lineitem name": ["Tee - S", "Hat", "Tee - M", "Hat"],
        "Lineitem price": ["20", "20", "10", "0"],
    })


def test_transform_shopify_fills_per_order_and_drops_whole_cancelled_order():
    out = transform_shopify(_shopify_rows())
    assert out["name"].unique().tolist() == ["#10"]       # both lines of #11 removed
    assert out["shipping_name"].tolist() == ["Ann", "Ann"]
    assert out["shipping_zip"].tolist() == ["02139", "02139"]
    assert out["paid_at"].notna().all()
    # Money totals stay on the first line only, so sums don't double count.
    assert out["subtotal"].notna().sum() == 1


def test_lineitem_analysis_tags_and_totals():
    df = pd.DataFrame({
        "order_date": ["2026-07-01", "2026-07-02"],
        "item_quantity": [2, 1],
        "item_name": ["Men's Tee - Black", "Hoodie - XL"],
        "item_price": [10.0, 30.0],
        "shipping_location": [None, "1 Main St"],
        "source": ["Square", "Shopify"],
    })
    out = transform_lineitem_analysis(df)
    assert out["shipping_location"].tolist() == ["InPerson", "1 Main St"]
    assert pd.isna(out.loc[0, "size"])                    # "Men's" is not size S
    assert out.loc[1, "size"] == "XL"
    assert out["money_value"].tolist() == [20.0, 30.0]
    assert out["rolling_total"].tolist() == [20.0, 50.0]
