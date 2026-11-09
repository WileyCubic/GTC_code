# Data-Pipelines

Two ETL jobs that load exports into the Postgres database `GTC`.
Overview: [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md). Tables: [docs/DATABASE.md](../docs/DATABASE.md).

## Sales ETL: `gtc-sales-etl`

```
Square CSVs  (SQUARE_ORDERS_CSV_DIR)  ─┐
                                       ├─ step 1 raw_orders ─▶ square_raw, shopify_raw
Shopify CSVs (SHOPIFY_ORDERS_CSV_DIR) ─┘                              │
                                        step 2 line_item_analysis ◀───┘ ─▶ lineitem_analysis
```

* **Step 1, `Sales_ETL/Pipelines/raw_orders/`**: reads every CSV in the two
  folders and cleans them (`transform.py`):
  * Shopify order fields are filled per order, and every line of a cancelled order is dropped.
  * Phones are formatted; money is rounded; dates are parsed.
  * Columns become snake_case.

  The rows are then **upserted by order** (`pipeline.py`): orders in the files
  replace the same orders in the table, and every other order is kept.
* **Step 2, `Sales_ETL/Pipelines/line_item_analysis/`**: runs
  `sql/lineitem_analysis_query.sql` (every line item from both stores), extracts
  size / color / organization / garment with the regex lists in
  `assets/lineitem_patterns.json`, adds `money_value` and `rolling_total`, and
  rebuilds `lineitem_analysis`.

Run one step only with `gtc-sales-etl --only raw` or `--only lineitem`.
Logs: `logs/sales_raw_orders.log`, `logs/sales_lineitem_analysis.log`, and `logs/sales_master.log` (both steps).

`Sales_ETL/OverView.md` and `images/` sketch a normalized schema that was
planned but **not built**.

## Expenses ETL: `gtc-expenses-etl`

`Expences_ETL/pipeline.py` reads every AmEx statement CSV in `AMEX_BP_CSV_DIR`
(Business Platinum) and `AMEX_AB_CSV_DIR` (Amazon Business Prime). It:

* drops the card members in `AMEX_EXCLUDE_CARD_MEMBERS`;
* drops the payment lines in `AMEX_EXCLUDE_DESCRIPTIONS`;
* fills blanks with "Not Provided";
* rebuilds `bp_expences` and `ab_expences`.

Keep every month's statement in those folders, since the tables are rebuilt
from scratch each run.
