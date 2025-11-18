# Formated Pivot Tables

Pipeline for turning daily Square/Shopify order exports into cleaned, formatted pivot tables in a single Excel workbook.

## What the pipeline does
- Loads environment variables and configures logging via `logging_utils.logging_config.setup_logging` (see `Utils.py`).
- Detects input files from `ETL_to_table_daily_csv_input`, merges them, and normalizes columns depending on source (`ETL_for_CSV_data.py`).
- Splits item names into size/color fields (`CSV_map.py`).
- Builds three pivot tables: size-color counts, by item, and by customer name, adding subtotals and grand totals where applicable (`Pivot_Table_by_SizeColor.py`, `Pivot_Table_by_Item.py`, `Pivot_Table_by_Name.py`).
- Writes a dated Excel workbook with all tables to `ETL_to_table_daily_output` (`Export_to_Excel.py`).

## Project layout
- `main.py` orchestrates ETL, pivot creation, and export.
- `ETL_for_CSV_data.py` extracts/cleans CSV input (Square or Shopify) and standardizes columns.
- `CSV_map.py` regex helpers to split size/color (and sorority name) from product text.
- `Pivot_Table_by_SizeColor.py` builds garment-count table with grand total.
- `Pivot_Table_by_Item.py` builds item pivot with subtotals per item and grand totals.
- `Pivot_Table_by_Name.py` builds customer-name pivot with subtotals per order/customer and grand totals.
- `Export_to_Excel.py` writes all pivot tables into one workbook using `xlsxwriter`.
- `ETL_from_query.py` (not working) will be used in the future to extract stat data from MySQL.
- `Delete ETL to table files.py` utility for clearing input/output folders (not working).

## Environment variables
Create a `.env` with at least:
- `ETL_to_table_daily_csv_input` — folder containing input `.csv` files.
- `ETL_to_table_daily_output` — folder where the Excel workbook is written.
- `ETL_to_table_daily_log_file` — path to the log file used by `Utils.py`.
- `Delete_log_file` — log file path for the delete utility (if used).
- `ETL_from_query.py`: 
    - `Mysql_host`, `Mysql_user`, `Mysql_password`, `Mysql_database`.

## Notes
- Source detection relies on column counts (Square -> 6 cleaned columns, Shopify -> 4); mismatched schemas return a warning.
- Update regex patterns in `CSV_map.py` if new sizes/colors/affiliations appear in product names.
