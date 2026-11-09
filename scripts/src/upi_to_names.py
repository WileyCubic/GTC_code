"""Map Affinity UPI codes to the product names sold under them.

    gtc-upi-names [report.csv ...]

Reads Affinity royalty report CSVs (default: AFFINITY_REPORT_FILES, a
``|``-separated list in .env) and writes ``UPI_to_names_output.csv`` to
ROYALTY_OUTPUT_DIR with one row per UPI: product names, product category and
the number of distinct names.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from gtc_common import env
from gtc_common.log import setup_logging

logger = logging.getLogger(__name__)


def build_table(files: list[Path]) -> pd.DataFrame:
    data = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    data = data[["Affinity UPI", "Item Description", "Affinity Product Category"]]
    table = (
        data.groupby("Affinity UPI")
        .agg(**{
            "Product Names": ("Item Description", lambda s: "; ".join(sorted(s.dropna().unique()))),
            "Product Categories": ("Affinity Product Category", "first"),
            "Unique Product Names Count": ("Item Description", "nunique"),
        })
        .sort_index()
        .reset_index()
    )
    return table[["Affinity UPI", "Product Categories", "Product Names", "Unique Product Names Count"]]


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("files", nargs="*", type=Path)
    args = parser.parse_args(argv)
    setup_logging("upi_to_names")
    files = args.files or env.path_list("AFFINITY_REPORT_FILES")
    table = build_table(files)
    out_dir = env.path("ROYALTY_OUTPUT_DIR")
    out_dir.mkdir(parents=True, exist_ok=True)
    table.to_csv(out_dir / "UPI_to_names_output.csv", index=False)
    logger.info("Wrote %s UPIs from %s file(s) to %s", len(table), len(files), out_dir / "UPI_to_names_output.csv")


if __name__ == "__main__":
    main()
