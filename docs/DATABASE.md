# Database

**Local Postgres, database `GTC`.** Connection settings are the `PG_*`
variables in `.env`. MySQL and the SQLite files (`GTC_DB.db`) are retired;
they are left on disk (git-ignored) but nothing reads them except the one-time
`gtc-db-seed`.

## Tables

The full column list with types is in
[`gtc_common/schema/001_init.sql`](../gtc_common/schema/001_init.sql).
Columns are the CSV headers in snake_case (`Lineitem compare at price` →
`lineitem_compare_at_price`); the only rename is Square `Order` → `order_id`
because `order` is an SQL keyword. Every table has a `row_id` primary key.

| Table | One row per | Written by | Load style | Read by |
|---|---|---|---|---|
| `square_raw` | Square line item | `gtc-sales-etl` | **Upsert by `order_id`**: orders in the new files replace the same orders; other orders are kept | line-item step, `gtc-royalties-db`, notebook |
| `shopify_raw` | Shopify line item (cancelled orders removed) | `gtc-sales-etl` | **Upsert by `name`** (e.g. `#1621`) | line-item step, `gtc-royalties-db`, notebook |
| `lineitem_analysis` | line item from either store, tagged with size / color / organization / garment | `gtc-sales-etl` (step 2) | Rebuilt in full each run | dashboard |
| `bp_expences` | Business Platinum card charge | `gtc-expenses-etl` | Rebuilt in full from every statement CSV | notebook |
| `ab_expences` | Amazon Business Prime card charge | `gtc-expenses-etl` | Rebuilt in full | notebook |

Key columns worth knowing:

* `square_raw`: `order_id`, `order_name` (customer), `order_date`, `item_name`,
  `item_variation`, `item_quantity`, `item_price`, `order_total`,
  `order_refunded_amount`, `recipient_*` (shipping address).
* `shopify_raw`: `name` (order number), `financial_status`, `paid_at`,
  `created_at` (timestamps with time zone, stored in UTC), `lineitem_name`,
  `lineitem_quantity`, `lineitem_price`, `billing_*`, `shipping_*`.
  **Order totals (`subtotal`, `shipping`, `taxes`, `total`) are only on the
  first line of each order**, so `sum(total)` counts each order once.
  A few paid orders have no `paid_at`; queries use `coalesce(paid_at, created_at)`.
* `lineitem_analysis`: `order_date`, `source` (`Square`/`Shopify`), `item_name`,
  `item_quantity`, `item_price`, `size`, `color`, `organization_name`,
  `garment_type`, `money_value` (= quantity × price), `rolling_total`.

Shopify dates are converted to the store's time zone (`America/Los_Angeles`)
in queries so a late-evening order lands on the right day.

## Setting up / changing the schema

```bash
gtc-db-migrate     # creates any missing tables (safe to re-run)
gtc-db-seed        # one time only: fills the new database (see below)
```

To change the schema, add a new file `gtc_common/schema/002_<what>.sql` with
statements that are safe to re-run (`alter table … add column if not exists …`)
and run `gtc-db-migrate`. If Square or Shopify add a column to their export,
the ETL stops with a message naming the new column; add it to the schema the
same way.

## One-time seed (2026-11-08)

The Square exports in `data/import dump (square)` only go back to Aug–Nov 2024
(older files were deleted after loading), but the old SQLite `GTC_DB.db` held
every Square order since Aug 2024. `gtc-db-seed` therefore:

1. copies `square_raw` from SQLite, re-cleaned with today's transform;
2. loads Shopify from the CSV export (it covers every order in the old table);
3. loads the AmEx CSVs;
4. rebuilds `lineitem_analysis`;
5. prints old-vs-new row counts.

## Useful queries

```sql
-- Sales by month and store
select date_trunc('month', order_date) as month, source, sum(money_value)
from lineitem_analysis group by 1, 2 order by 1, 2;

-- Shopify orders per financial status
select financial_status, count(distinct name) from shopify_raw group by 1;

-- Most recent order loaded from each store (is the data up to date?)
select 'square', max(order_date)::date from square_raw
union all
select 'shopify', max(coalesce(paid_at, created_at))::date from shopify_raw;
```
