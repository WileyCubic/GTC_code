"""Expenses ETL: AmEx statement CSVs -> Postgres bp_expences / ab_expences.

    gtc-expenses-etl

* Business Platinum statements (AMEX_BP_CSV_DIR) -> bp_expences
* Amazon Business Prime statements (AMEX_AB_CSV_DIR) -> ab_expences

Rows for the card members in AMEX_EXCLUDE_CARD_MEMBERS (Business Platinum only)
and rows whose Description is in AMEX_EXCLUDE_DESCRIPTIONS (card payments, so
they are not counted as expenses) are removed. Both tables are rebuilt in full
from every CSV in the folders, so keep all statements in those folders.
"""
from __future__ import annotations

import logging

import pandas as pd

from gtc_common import env, orders
from gtc_common.db import replace_table, snake_columns
from gtc_common.log import setup_logging

logger = logging.getLogger(__name__)

FILL_NOT_PROVIDED = ["Address", "City/State", "Zip Code", "Country", "Reference", "Category"]


def _clean_common(df: pd.DataFrame) -> pd.DataFrame:
    df = df.fillna({c: "Not Provided" for c in FILL_NOT_PROVIDED if c in df.columns})
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    df["Amount"] = orders.to_money(df["Amount"])
    for col in ["Extended Details", "City/State"]:
        if col in df.columns:
            df[col] = df[col].astype("string").str.replace(r"\s*\n\s*", ", ", regex=True)
    return df.sort_values("Date", kind="stable").reset_index(drop=True)


def _drop_payment_lines(df: pd.DataFrame) -> pd.DataFrame:
    exclude = env.str_list("AMEX_EXCLUDE_DESCRIPTIONS")
    before = len(df)
    df = df.loc[~df["Description"].isin(exclude)].copy()
    logger.info("Removed %s payment line(s)", before - len(df))
    return df


def transform_bp(df: pd.DataFrame) -> pd.DataFrame:
    exclude_members = env.str_list("AMEX_EXCLUDE_CARD_MEMBERS")
    before = len(df)
    df = df.loc[~df["Card Member"].isin(exclude_members)].copy()
    logger.info("Removed %s row(s) for excluded card members", before - len(df))
    df = _drop_payment_lines(df)
    df["Account #"] = df["Account #"].astype("string").str.replace("-", "", regex=False)
    return snake_columns(_clean_common(df), overrides={"Account #": "account"})


def transform_ab(df: pd.DataFrame) -> pd.DataFrame:
    return snake_columns(_clean_common(_drop_payment_lines(df)))


def _load(folder_setting: str, transform, table: str) -> int:
    files = orders.list_csvs(env.path(folder_setting))
    if not files:
        logger.warning("No CSV files in %s; %s left unchanged", folder_setting, table)
        return 0
    # Transform completely before touching the table, so a bad file never
    # leaves the table empty (the old script dropped the tables first).
    df = transform(orders.read_csvs(files))
    return replace_table(df, table)


def main() -> None:
    setup_logging("expenses_etl")
    logger.info("[Starting] expenses ETL")
    bp = _load("AMEX_BP_CSV_DIR", transform_bp, "bp_expences")
    ab = _load("AMEX_AB_CSV_DIR", transform_ab, "ab_expences")
    logger.info("[Completed] expenses ETL: bp_expences=%s ab_expences=%s", bp, ab)


if __name__ == "__main__":
    main()
