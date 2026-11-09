"""Scratch analysis: money in (sales) vs money out (AmEx expenses) over time.

    python Analysis-Bashboard/notebooks/money_in_out.py

Exploratory only (not part of the dashboard). Prints totals and opens two
Plotly charts in the browser. Reads Postgres via gtc_common.db.
"""
import plotly.graph_objects as go

from gtc_common.db import read_sql


def running_total(sql: str, date_col: str, value_col: str):
    df = read_sql(sql).sort_values(date_col)
    df["running_total"] = df[value_col].astype(float).cumsum()
    return df


def main() -> None:
    square = running_total(
        "select order_date, item_quantity * item_price as value from square_raw", "order_date", "value")
    shopify = running_total(
        "select paid_at, lineitem_quantity * lineitem_price as value from shopify_raw", "paid_at", "value")
    bp = running_total("select date, amount from bp_expences", "date", "amount")
    ab = running_total("select date, amount from ab_expences", "date", "amount")

    sales = square["value"].sum() + shopify["value"].sum()
    expenses = bp["amount"].sum() + ab["amount"].sum()
    print(f"Total sales (line items): {sales:,.2f}")
    print(f"Total AmEx expenses:      {expenses:,.2f}")

    fig = go.Figure()
    fig.add_scatter(x=square["order_date"], y=square["running_total"], name="Square Sales")
    fig.add_scatter(x=shopify["paid_at"], y=shopify["running_total"], name="Shopify Sales")
    fig.update_layout(title="Money In Over Time", xaxis_title="Date", yaxis_title="Amount")
    fig.show()

    fig = go.Figure()
    fig.add_scatter(x=bp["date"], y=bp["running_total"], name="Business Platinum")
    fig.add_scatter(x=ab["date"], y=ab["running_total"], name="Amazon Business")
    fig.update_layout(title="Money Out Over Time", xaxis_title="Date", yaxis_title="Amount")
    fig.show()


if __name__ == "__main__":
    main()
