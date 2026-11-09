# Settings (`.env`)

All settings live in **one file: `.env` at the repo root.** It is git-ignored;
`.env.example` is the committed template. Python reads it through
`gtc_common/env.py`, Node through `load-root-env.cjs`, so it is found no matter
which folder you start a command from.

Rules:

* Paths may be absolute or **relative to the repo root** (e.g. `data/amex/Business Platinum`).
* Wrap values containing spaces, `\`, `#` or parentheses in single quotes.
* Lists use `|` between items.
* A missing required setting stops the command with a message naming it.

## Reference

| Variable | Required by | Meaning |
|---|---|---|
| `PG_HOST` | all DB commands | Postgres host (default `localhost`) |
| `PG_PORT` | all DB commands | Postgres port (default `5432`) |
| `PG_DATABASE` | all DB commands | Database name (default `GTC`) |
| `PG_USER` | all DB commands | Postgres user |
| `PG_PASSWORD` | all DB commands | Postgres password |
| `LOG_DIR` | every command | Folder for log files (default `logs`) |
| `SQUARE_ORDERS_CSV_DIR` | `gtc-sales-etl` | Folder of Square order exports; every `*.csv` is loaded |
| `SHOPIFY_ORDERS_CSV_DIR` | `gtc-sales-etl` | Folder of Shopify order exports; every `*.csv` is loaded |
| `AMEX_BP_CSV_DIR` | `gtc-expenses-etl` | Business Platinum statement CSVs |
| `AMEX_AB_CSV_DIR` | `gtc-expenses-etl` | Amazon Business Prime statement CSVs |
| `AMEX_EXCLUDE_CARD_MEMBERS` | `gtc-expenses-etl` | `\|`-list of Business Platinum card members whose charges are not business expenses |
| `AMEX_EXCLUDE_DESCRIPTIONS` | `gtc-expenses-etl` | `\|`-list of exact descriptions to drop (card payments) |
| `PIVOT_INPUT_CSV_DIR` | `gtc-pivot-tables` | Folder holding today's order CSV(s), all from one store |
| `PIVOT_OUTPUT_DIR` | `gtc-pivot-tables` | Where the workbook is written |
| `ROYALTY_INPUT_DIR` | `gtc-royalties-csv` | Default folder with the quarter's `orders-*.csv` + `orders_export*.csv` (override with `--input-dir`) |
| `ROYALTY_OUTPUT_DIR` | royalty reports, `gtc-upi-names` | Where royalty reports are written |
| `ROYALTY_COPY_DIR` | `gtc-royalties-csv` | Optional second copy of the royalty report |
| `AFFINITY_REPORT_FILES` | `gtc-upi-names` | `\|`-list of Affinity royalty report CSVs |
| `TAX_INPUT_DIR` | `gtc-tax-report` | Folder with the year's exports (see RUNBOOK) |
| `TAX_OUTPUT_FILE` | `gtc-tax-report` | Output CSV path |
| `IMAGE_URLS_FILE` | `gtc-affinity-images` | Text file, one image URL per line |
| `IMAGE_OUTPUT_DIR` | `gtc-affinity-images` | Where images are saved |
| `SHOPIFY_API_KEY`, `SHOPIFY_API_SECRET`, `SCOPES`, `SHOPIFY_APP_URL`, `SHOP_CUSTOM_DOMAIN`, `NODE_ENV`, `PORT`, `FRONTEND_PORT`, `HOST` | `gtc-graphql-api` | Shopify CLI app settings (OAuth app) |
| `SHOPIFY_SHOP` | `shopify-graphql-retrieve-orders` | `yourstore.myshopify.com` |
| `SHOPIFY_ADMIN_ACCESS_TOKEN` | `shopify-graphql-retrieve-orders` | Admin API token (`shpat_…`) |
| `SHOPIFY_API_VERSION` | `shopify-graphql-retrieve-orders` | Admin API version, e.g. `2025-10` |

Things that used to be settings but are now fixed in code because they never
change: database table names, the line-item SQL file, both regex pattern JSON
files, and the dashboard query file (all live inside their package).

## Old name → new name (migration of 2026-11-08)

Your previous `.env` was converted automatically; the original is saved as
`.env.backup-2026-11-08` (git-ignored; delete it once you're happy).

| Old | New |
|---|---|
| `Mysql_host`, `Mysql_user`, `Mysql_password`, `Mysql_database`, `SQLite_database` | removed: `PG_*` (MySQL and SQLite are retired) |
| `square_raw`, `shopify_raw`, `lineitem_analysis` | removed (table names are constants) |
| `raw_orders_log`, `lineitem_analysis_log`, `Master_sales_log`, `Master_expences_log`, `Backup_log`, `Primary_log`, `Secondary_log`, `ETL_Expences_to_DB_log_file`, `Dashboard_log_file`, `Query_log_file`, `ETL_to_table_daily_log_file` | `LOG_DIR` |
| `ELT_Sales_to_DB_square_CSV_input` | `SQUARE_ORDERS_CSV_DIR` |
| `ETL_Sales_to_DB_shopify_CSV_input` | `SHOPIFY_ORDERS_CSV_DIR` |
| `lineitem_analysis_query`, `Sales_ETL_lineitem_patterns`, `GTC_formated_tables_patterns`, `Dashboard_query_all`, `aquery1`–`aquery10` | removed (package files) |
| `square_raw_data`, `shopify_raw_data` | removed (only used by commented-out test code) |
| `ETL_Expences_to_DB_BP_CSV_input` / `_AB_` | `AMEX_BP_CSV_DIR` / `AMEX_AB_CSV_DIR` |
| `Card_Member_1`, `Card_Member_4` | `AMEX_EXCLUDE_CARD_MEMBERS` (`Card_Member_2`/`3` were never used) |
| `PMT_Line_1`, `PMT_Line_2` | `AMEX_EXCLUDE_DESCRIPTIONS` |
| `ETL_to_table_daily_csv_input` / `_output` | `PIVOT_INPUT_CSV_DIR` / `PIVOT_OUTPUT_DIR` |
| `Royalty_report_out_dir` | `ROYALTY_OUTPUT_DIR` |
| hardcoded paths in the royalty script | `ROYALTY_INPUT_DIR`, `ROYALTY_COPY_DIR` |
| `q1_2025` … `q4_2025` | `AFFINITY_REPORT_FILES` |
| hardcoded paths in the tax script | `TAX_INPUT_DIR`, `TAX_OUTPUT_FILE` |
| `image_url_file` / `Images_urls_output` | `IMAGE_URLS_FILE` / `IMAGE_OUTPUT_DIR` |
| `shopify-graphql-order-query` | removed (never read) |
