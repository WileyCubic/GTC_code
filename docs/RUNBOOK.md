# Runbook: how to do each job

All commands are run from a terminal in the repo folder with the virtual
environment active:

```bash
cd "C:/Users/wiley/Desktop/D3 Design Inc/GTC_code"
source .venv/Scripts/activate        # Git Bash   (PowerShell: .venv\Scripts\Activate.ps1)
```

Every command prints its progress and writes `logs/<command>.log`.
Add `--help` to any `gtc-*` command to see its options.

## First-time setup (new computer)

1. Install Python 3.11+ and PostgreSQL. Create a database named `GTC`.
2. `python -m venv .venv`, activate it, then `pip install -e ".[test]"`.
3. Copy `.env.example` to `.env` and fill it in ([ENV.md](ENV.md)).
4. `gtc-db-migrate` to create the tables.
5. `pytest` should pass (the database test runs only when `PG_PASSWORD` is set).

## Update the sales database (Sales ETL)

When: whenever you want the database current, and before `gtc-royalties-db`.

1. Export orders from Square (online store → Orders → Export) and Shopify
   (Orders → Export → CSV, "all orders" or a date range).
2. Put the files in `SQUARE_ORDERS_CSV_DIR` / `SHOPIFY_ORDERS_CSV_DIR`.
   Overlapping date ranges are fine; old files can stay or be removed.
3. `gtc-sales-etl` (or `gtc-sales-etl --only raw` / `--only lineitem`).

## Quarterly royalty report

**From the export files (the proven route):**

1. Make a folder for the quarter, e.g. `data/royalty report copys/Affinity_Q4_2026/`.
2. Export that quarter's orders from Square (`orders-YYYY-MM-DD-YYYY-MM-DD.csv`)
   and Shopify (`orders_export_1.csv`) into it, one file each.
3. `gtc-royalties-csv --input-dir "data/royalty report copys/Affinity_Q4_2026"`
   (year/quarter default to the last completed quarter; pass `--year 2026 --quarter 4` to be explicit).
4. The report is written to `ROYALTY_OUTPUT_DIR` and, when set, `ROYALTY_COPY_DIR`
   as `Royalties_report_2026_Q4.csv`.
5. Manual steps still needed (not automated, see [AUDIT.md](AUDIT.md#open-questions)):
   add the Affinity marketplace orders, and remove duplicate invoices (e.g. #1621/#1622).

**From the database:** run `gtc-sales-etl` with current exports, then
`gtc-royalties-db [--year 2026 --quarter 4]` → `Royalties_report_2026_Q4_from_db.csv`.

## Yearly sales-tax report

1. In `TAX_INPUT_DIR` (default `data/yearly tax reporting/`) put:
   * the Shopify export for the year: `orders_export*.csv`
   * the Affinity marketplace royalty report: a file with `royalty_report` in its name
   * a sub-folder `orders-<start>-<end>/` with the Square export(s) for the year
2. `gtc-tax-report` → `TAX_OUTPUT_FILE`.
3. Only Shopify orders with status `paid` are included; there is no date filter,
   so export exactly the tax year.

## Daily formatted pivot tables

1. Put today's export (Square **or** Shopify, not both) in `PIVOT_INPUT_CSV_DIR`.
2. `gtc-pivot-tables` → `PIVOT_OUTPUT_DIR/Formated Table MM-DD-YYYY.xlsx`.
3. Clear the input folder afterwards (the folder name says so; the tool does not delete files).

## Expenses (AmEx)

1. Download each month's statement CSV into `AMEX_BP_CSV_DIR` or `AMEX_AB_CSV_DIR`.
   Keep all months in the folder: the tables are rebuilt from every file.
2. `gtc-expenses-etl`.

## Affinity helpers

* `gtc-upi-names [file.csv …]` (default `AFFINITY_REPORT_FILES`): UPI → product names table.
* `gtc-affinity-images`: download every URL in `IMAGE_URLS_FILE` into `IMAGE_OUTPUT_DIR`.

## Dashboard and analysis

* `gtc-dashboard` then open http://127.0.0.1:8050. Layout prototype only; no data yet.
* `python Analysis-Bashboard/notebooks/money_in_out.py`: sales vs expenses charts.

## Shopify API scripts (Node)

```bash
cd shopify-graphql-retrieve-orders/src
npm install
npm run check     # verifies SHOPIFY_SHOP + SHOPIFY_ADMIN_ACCESS_TOKEN against the API
npm run shop      # prints shop info
npm run orders    # prints the first 10 order ids
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Setting 'X' is missing` | Add `X` to `.env` (see [ENV.md](ENV.md)). |
| `Table '…' does not exist. Run gtc-db-migrate first.` | Run `gtc-db-migrate`. |
| `Columns [...] are not in table` | The export format changed; add the column via a new schema file ([DATABASE.md](DATABASE.md)). |
| `Expected exactly one file matching …` | The input folder has zero or several exports of that kind; keep one. |
| `password authentication failed` | Check `PG_USER` / `PG_PASSWORD`. |
| `gtc-…: command not found` | Activate `.venv`, or run `pip install -e .` again. |
| `The input folder mixes Square and Shopify exports` | Run the pivot tables for one store at a time. |
