"""Formatted pivot tables: daily order CSVs -> 'Formated Table MM-DD-YYYY.xlsx'.

    gtc-pivot-tables

Reads every CSV in PIVOT_INPUT_CSV_DIR (all from Square *or* all from Shopify)
and writes a workbook to PIVOT_OUTPUT_DIR with three sheets:
  * Garment Counts    units per item / size / color (or organization)
  * By Item Name      units per item and customer, with subtotals per item
  * By Customer Name  units per customer and item, with subtotals per customer
"""
from __future__ import annotations

import logging

from gtc_common import env
from gtc_common.log import setup_logging
from gtc_pivot_tables import pivots
from gtc_pivot_tables.export_to_excel import excel_export
from gtc_pivot_tables.extract import clean, load_input

logger = logging.getLogger(__name__)


def main() -> None:
    setup_logging("pivot_tables")
    source, raw = load_input(env.path("PIVOT_INPUT_CSV_DIR"))
    df = clean(source, raw)
    excel_export(
        pivots.garment_counts(source, df),
        pivots.by_item(source, df),
        pivots.by_customer(source, df),
        env.path("PIVOT_OUTPUT_DIR"),
    )


if __name__ == "__main__":
    main()
