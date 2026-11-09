# scripts: reports (`gtc_reports` package)

The code is in `src/`, input lists are in `scripts input/`, and generated files go to `scripts output/` (git-ignored).
Step-by-step instructions: [docs/RUNBOOK.md](../docs/RUNBOOK.md).

| Command | File | Inputs | Output |
|---|---|---|---|
| `gtc-royalties-csv` | `royalties_from_csv.py` | `--input-dir` (or `ROYALTY_INPUT_DIR`) holding one `orders-*.csv` (Square) and one `orders_export*.csv` (Shopify) for the quarter | `ROYALTY_OUTPUT_DIR/Royalties_report_<year>_Q<n>.csv` (+ copy in `ROYALTY_COPY_DIR`) |
| `gtc-royalties-db` | `royalties_from_db.py` + `sql/royalties.sql` | Postgres `square_raw`, `shopify_raw` (run `gtc-sales-etl` first) | `ROYALTY_OUTPUT_DIR/Royalties_report_<year>_Q<n>_from_db.csv` |
| `gtc-tax-report` | `yearly_tax_report.py` | `TAX_INPUT_DIR`: `orders_export*.csv`, `*royalty_report*.csv`, `orders-*/` Square folder | `TAX_OUTPUT_FILE` |
| `gtc-upi-names` | `upi_to_names.py` | Affinity royalty reports (`AFFINITY_REPORT_FILES` or file arguments) | `ROYALTY_OUTPUT_DIR/UPI_to_names_output.csv` |
| `gtc-affinity-images` | `affinity_image_scraper.py` | `IMAGE_URLS_FILE` (one URL per line) | images in `IMAGE_OUTPUT_DIR` |

Royalty report columns: Date, Invoice Id, Item Description, Customer, Shipping
Country, Shipping Postal Code, Number of Units, Price per Unit. `--year` and
`--quarter` default to the last completed quarter.

Tax report columns: OrderId, Customer_Name, Subtotal, Shipping, Taxes, Total,
State, Country, City, Zip (one row per order; Shopify `paid` orders only).

Known gaps you still handle by hand: Affinity marketplace rows in the royalty
report, duplicate invoices, refunds. See [docs/AUDIT.md](../docs/AUDIT.md#open-questions).
