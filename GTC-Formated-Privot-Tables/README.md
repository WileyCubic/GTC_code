# Formatted pivot tables: `gtc-pivot-tables` (`gtc_pivot_tables` package)

Turns the day's order export into an Excel workbook used for production and
packing: `PIVOT_OUTPUT_DIR/Formated Table MM-DD-YYYY.xlsx`.

1. `extract.py` reads every CSV in `PIVOT_INPUT_CSV_DIR`. It detects Square
   vs Shopify by the column names (all files must be from one store) and keeps
   only the needed columns. For Shopify, the customer name is copied onto each
   line of its order.
2. `csv_map.py` pulls size, color and Greek organization out of item
   names and variations, using `assets/regex_patterns.json` and
   `gtc_common.patterns`.
3. `pivots.py` builds three sheets:
   * **Garment Counts**: units per item / size / color (Square) or item /
     size / organization (Shopify), plus a grand total. Unrecognised values
     show as blank.
   * **By Item Name**: units and unit price per item and customer, with a
     subtotal per item.
   * **By Customer Name**: the same data grouped by customer, with a
     subtotal per customer.

   Both "By" sheets end with a grand total, where `Total Revenue` = Σ price × quantity.
4. `export_to_excel.py` writes the workbook. Running twice on one day overwrites it.

To add a size, color or organization, edit `src/assets/regex_patterns.json`.
Longer names automatically win over shorter ones, so "Penn State Theta x Sigma Pi" beats "Penn State".
