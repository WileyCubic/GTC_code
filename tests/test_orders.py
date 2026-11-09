import datetime as dt

import pandas as pd
import pytest

from gtc_common import orders


@pytest.mark.parametrize("raw, expected", [
    ("3109901637", "(310)-990-1637"),
    ("13109901637.0", "1-(310) 990-1637"),
    ("1-(310) 990-1637", "1-(310) 990-1637"),   # already formatted -> unchanged
    ("+44 20 7946 0958", "44-(207) 946-0958"),
    ("12345", "12345"),                         # unknown length -> kept as-is
    (None, None),
    (float("nan"), None),
])
def test_format_phone_number(raw, expected):
    assert orders.format_phone_number(raw) == expected


def test_fill_within_order_does_not_leak_between_orders():
    df = pd.DataFrame({
        "Name": ["#1", "#1", "#2", "#3", "#3"],
        "Shipping Name": ["Ann", None, None, None, "Cat"],
    })
    out = orders.fill_within_order(df, "Name", ["Shipping Name"])
    assert out["Shipping Name"].tolist()[:2] == ["Ann", "Ann"]
    assert pd.isna(out.loc[2, "Shipping Name"])      # order #2 has no name; must NOT get "Cat"
    assert out["Shipping Name"].tolist()[3:] == ["Cat", "Cat"]   # back-fill within #3


def test_drop_cancelled_removes_every_line_after_fill():
    df = pd.DataFrame({"Name": ["#1", "#1", "#2"], "Cancelled at": ["2026-01-01", None, None]})
    df = orders.fill_within_order(df, "Name", ["Cancelled at"])
    assert orders.drop_cancelled_shopify(df)["Name"].tolist() == ["#2"]


@pytest.mark.parametrize("today, expected", [
    (dt.date(2027, 1, 15), (2026, 4)),
    (dt.date(2026, 10, 2), (2026, 3)),
    (dt.date(2026, 4, 1), (2026, 1)),
])
def test_last_completed_quarter(today, expected):
    assert orders.last_completed_quarter(today) == expected


def test_quarter_bounds():
    assert orders.quarter_bounds(2026, 3) == (dt.date(2026, 7, 1), dt.date(2026, 10, 1))
    assert orders.quarter_bounds(2026, 4) == (dt.date(2026, 10, 1), dt.date(2027, 1, 1))
    with pytest.raises(ValueError):
        orders.quarter_bounds(2026, 5)


def test_find_one(tmp_path):
    (tmp_path / "orders-2026.csv").write_text("a\n1\n")
    assert orders.find_one(tmp_path, "orders-*.csv").name == "orders-2026.csv"
    with pytest.raises(FileNotFoundError):
        orders.find_one(tmp_path, "orders_export*.csv")
