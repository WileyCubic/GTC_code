"""Write the three pivot tables to one Excel workbook."""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def excel_export(garments: pd.DataFrame, items: pd.DataFrame, customers: pd.DataFrame,
                 output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"Formated Table {datetime.now():%m-%d-%Y}.xlsx"
    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        garments.to_excel(writer, sheet_name="Garment Counts")
        items.to_excel(writer, sheet_name="By Item Name")
        customers.to_excel(writer, sheet_name="By Customer Name")
    logger.info("Workbook written to %s", path)
    return path
