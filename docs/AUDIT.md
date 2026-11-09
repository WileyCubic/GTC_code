# Code audit: November 2026

Audit of the whole repo (branch `db_restructure` plus the unmerged
`chore/env-and-dependency-unification`), followed by fixes on branch
`audit/consolidation`. Every finding below has an outcome:

* **Fixed** — changed in this audit.
* **Fixed earlier** — already fixed on the `chore/…` branch this work is built on.
* **Open** — needs a business decision; not changed. See [Open questions](#open-questions).

Findings marked ⚠ **can change report numbers** compared with older outputs.

## Summary

| Before | After |
|---|---|
| Only the royalty CSV script and the yearly tax script worked. The Sales ETL failed on import (SyntaxError), as did the dashboard. | Every tool runs from one install with a `gtc-*` command. |
| ~60 env vars, some read under wrong names, sub-folder `.env.example` files all empty, root example out of date. | One `.env`, 30 vars, documented in [ENV.md](ENV.md); missing values give a clear error. |
| MySQL + SQLite written side by side; no Postgres. | Postgres only, with an explicit schema and a migration command. |
| 3–4 copies of config, logging, DB-connection and CSV-cleaning code; `sys.path` hacks; a stale editable install doing the real work. | `gtc_common` shared package; real `pyproject.toml`; no path hacks. |
| Raw sales tables were replaced on every run, so deleting old exports deleted history. | Upsert by order: history is kept, re-loads don't duplicate. |
| Test suite only tested pandas and mocks; "89% coverage" in the README was not real. | 26 tests of the real transforms and reports. |

## How the existing outputs compare

Re-running the new code on the inputs behind your latest outputs:

| Output | Result |
|---|---|
| `Yearly Tax Reporting.csv` | **Identical** (840 orders). |
| `Royalties_report_2026_Q3.csv` | Script rows identical except **3 rows with corrected dates** (#1532 now 2026-07-24, was 07-20; #1687 now 09-30, was 09-29; see D1/D13). The saved file also contains **99 Affinity marketplace rows** (numeric invoice ids) that no script produces; they were added after the script ran. |
| `UPI_to_names_output.csv` | Same 49 UPIs and columns; product names are now plain text (`A; B`) instead of Python list text (`['A' 'B']`). |
| `Formated Table *.xlsx` | Runs; garment counts now include items with no recognised size/org (D8); grand total revenue moved to its own column (D9). |

## Findings

### Blockers (things that crashed)

| # | Where | Finding | Outcome |
|---|---|---|---|
| B1 | `Sales_ETL/Pipelines/raw_orders/pipeline.py:2` | `import logging import sys`: SyntaxError; `import Sales_ETL` failed, so every Sales ETL entry point was dead. | Fixed earlier |
| B2 | Sales ETL | Mixed import styles (`from config import…`, relative `.utils`, `Sales_ETL.…`) plus `sys.path` edits; the two pipelines could not both import from one entry point. `Sales_ETL/__init__.py` re-exported names that didn't exist. | Fixed |
| B3 | Sales ETL `pipeline.py` | `logger = setup_logging(...)` returned `None` → `AttributeError` on first log call. | Fixed |
| B4 | every `config.py` | `Path(os.getenv('X'))` at import time: one missing var crashed the whole module with `TypeError: expected str… not NoneType`. | Fixed (`gtc_common.env.require`) |
| B5 | `Analysis-Bashboard/src/config.py`, `figure.py` | Read env `all` (absent) and `lineitem_analysis_test` (absent); `figure.py` ran test charts at import on an undefined `data`. | Fixed |
| B6 | `Analysis-Bashboard/src/Utils.py` | `logging_utils` only importable through a stale editable install. | Fixed |
| B7 | `analysis 1.py`, `Query to CSV.py`, Expenses ETL | `Query_log_file` empty → `open('')` crash; MySQL connection failure → `NameError` later. | Fixed (rewritten on Postgres) |
| B8 | `GTC-Formated-Privot-Tables/src/Formate-pipe.py` | Half the imports were bare (`from ptable_by_customer import…`) and failed under the package. | Fixed |
| B9 | `shopify-graphql-retrieve-orders` | Read `Shopify_access_token`; `.env` had `SHOPIFY_ADMIN_ACCESS_TOKEN`. `package.json` `main` pointed at a missing file. API version never set. | Env name fixed earlier; rest fixed |
| B10 | `Data-Pipelines/requirements.txt` | Listed stdlib modules (`sqlite3`, `pathlib`, `dataclasses`) and the Py2-era `logging` package; pinned `SQLAlchemy<2` while pandas needs 2.x. Root `requirments.txt` empty. | Fixed (`pyproject.toml`) |
| B11 | `line_item_analysis/extract.py:27` | `df` undefined when the query failed (`UnboundLocalError`). Same in `sql_helpers.py`. | Fixed |
| B12 | `roalty_reports/last_quarty_roalties.py:193` | If either CSV was missing, setting `.columns` on an empty frame crashed. | Fixed (clear error naming the missing file) |

### Data correctness

| # | Where | Finding | Outcome |
|---|---|---|---|
| D1 ⚠ | raw_transform, temp_pipes, royalty script, pivot ETL | `df.groupby('Name')[col].ffill().bfill()`: the back-fill ran across the **whole column**, so an order missing a value took the **next order's** value (dates, names, addresses). | Fixed: `gtc_common.orders.fill_within_order` |
| D2 ⚠ | `raw_transform.py:149` | `.apply(datetime.datetime.date)` on strings raised; a broad `except` swallowed it, so **phone, money rounding and tax cleaning never ran** for Shopify. (Old Shopify rows had raw timestamp strings.) | Fixed |
| D3 ⚠ | raw ETL, royalty CSV | Cancelled Shopify orders were filtered *before* filling order fields, so only the first line of a cancelled order was removed; its other lines stayed. | Fixed (fill, then drop) |
| D4 | raw ETL | Square `Order ID` was the row number, not an order id. | Fixed (dropped; real id `order_id`) |
| D5 | raw ETL | `format_phone_number` crashed on non-numeric input and returned the `ValueError` *class* as a cell value for odd lengths. | Fixed |
| D6 ⚠ | raw ETL | Tables were replaced with whatever CSVs were in the folder; old exports had been deleted, so a re-run would have wiped all Square history before Aug 2024's files. | Fixed: upsert by order |
| D7 ⚠ | both regex pattern sets | `\bS\b` matched the "s" in "Men's" (size S). Alternation order meant longer names could never match: `White` hid `White/Blue`; `Penn State` hid `Penn State Theta x Sigma Pi`; `NCL` hid `NCL California Love`; `Mira Costa` hid its team names. | Fixed: `gtc_common.patterns` |
| D8 ⚠ | pivot garment table (Shopify) | `pivot_table` silently drops rows with a missing index value: items with no recognised size/org vanished from the counts. | Fixed (blank label) |
| D9 ⚠ | pivot totals | Grand-total row put total revenue (Σ price × qty) in the unit-price column. | Fixed (`Total Revenue` column) |
| D10 | pivot `ETL_from_csv.py:52` | Returned the `ImportError` *class* instead of raising; the "both sources" branch was unreachable (Square silently won). | Fixed (clear errors) |
| D11 ⚠ | Expenses ETL | `fillna(...)` and `replace(...)` results never assigned (no-ops); `Series.replace('-')` only matched whole values; tables dropped **before** reading CSVs (a bad file left them empty). | Fixed |
| D12 | royalty CSV script | File named with `(month-1)//3`: a January run produced `Q0` of the *current* year. | Fixed: `--year/--quarter`, default last completed quarter |
| D13 ⚠ | Shopify exports | Some **paid** orders have no `Paid at` (no payment method). They were dated with the next order's date via D1. | Fixed: fall back to `Created at` (CSV report, DB report, line items) |
| D14 | line-item SQL | MySQL `CONCAT_WS` of all-NULL returns `''`, so the `InPerson` default for in-person sales never applied. | Fixed (`nullif`) |
| D15 | `figure.py:124` | Horizontal bar "top N" sorted descending then took `.tail` → bottom N. | Fixed |
| D16 | `UPI_to_names.py:37` | `pd.Series.unique` as aggregation wrote Python list text into the CSV. | Fixed |
| D17 | Shopify cleaning | Order totals (`Subtotal`, `Total`, …) are only on an order's first line. | Kept that way on purpose, and documented, so sums count each order once |

### Structure and hygiene

| # | Finding | Outcome |
|---|---|---|
| S1 | 1,834 `node_modules` files committed. | Fixed earlier |
| S2 | Duplicates: `config.py` ×4, `utils.py` ×3, `logging_config.py` ×3, DB connection ×3, `load.py` ×3, CSV cleaning ×4 (incl. `temp_pipes(quarterly).py`, a single-file copy of the raw pipeline). | Fixed (merged into `gtc_common`; duplicates deleted) |
| S3 | Dead/scratch files: `ETL_from_query.py` (empty), `test.py` (scratch), `common/utils.py` (empty), `sql_helpers.py` (unused), `Sales_ETL/run_raw_orders.py`, commented-out code blocks. | Fixed (deleted) |
| S4 | `scripts` modules ran everything at import (no `main()`), cosmetic `sleep()` calls. | Fixed |
| S5 | Working royalty script and tax script untracked / hardcoded `C:\Users\wiley\…` paths. | Fixed (in `gtc_reports`, paths from `.env`/CLI) |
| S6 | No secrets found in git history (checked `.env`, tokens, passwords). | OK |
| S7 | `.gitignore` missed `*.egg-info`, `.pytest_cache`, `.benchmarks`, `.claude`, env backups, `.xlsx` outputs. | Fixed |
| S8 | README claimed "89% coverage", "real-time analytics", "scheduled processing". None existed. | Fixed (docs rewritten) |
| S9 | `pytest.ini` used the `[tool:pytest]` header (setup.cfg syntax) inside `tests/`, so none of its settings applied. | Fixed (config in `pyproject.toml`) |

## Open questions

These need your decision. Nothing was changed for them.

1. **Duplicate invoice #1621 / #1622.** `RoyaltyQ32026Notes.md` says money was
   received on only one, but no report removes the duplicate. Remove it in code
   (which one?), or keep doing it by hand?
2. **The two royalty reports disagree on purpose-unclear points** inherited
   from the original scripts:
   * Square rows: the CSV report includes **every** Square item; the DB report
     only items whose name contains "ncl".
   * Shopify customer: CSV report uses **Shipping Name**; DB report uses **Billing Name**.

   Which is right for Affinity? Once decided, both can share one rule.
3. **Affinity marketplace orders** (99 rows in 2026 Q3) are added to the
   royalty report by hand. Automate from the Affinity report CSV?
4. **Refunds.** Neither royalty report subtracts refunds (`order_refunded_amount`,
   Shopify `refunded`/`partially_refunded`). The tax report keeps only Shopify
   orders with status exactly `paid`, so partially refunded orders are left out
   entirely. Intended?
5. **Tax report zip lookup** for Affinity orders assumes US zip codes.
6. **`gtc-graphql-api` requests nearly every Shopify scope** (read/write
   customers, orders, payments, themes…). It's an unused template. Cut the
   scopes to `read_orders` or delete the app?
7. **Exports are still manual.** `shopify-graphql-retrieve-orders` proves the
   API token works but nothing pulls orders automatically. Worth building?
8. **Two regex pattern files** (`Sales_ETL/assets/lineitem_patterns.json` and
   `GTC-Formated-Privot-Tables/src/assets/regex_patterns.json`) have different
   organization lists. Merge into one?
9. **Dashboard** is a layout stub; `dashboard-brief.md` is half filled in.
10. **`Data-Pipelines/Sales_ETL/OverView.md`** describes a normalized schema
    (separate order / customer / item tables) that was never built.
11. **Folder name typos** (`Analysis-Bashboard`, `GTC-Formated-Privot-Tables`,
    `Expences_ETL`, `Royalties Roports`, `Affinitu_Q3_2026`) were kept so paths
    keep working. Rename later?

## Housekeeping for you

* **Set `PG_PASSWORD` in `.env`**, then run `gtc-db-migrate` and `gtc-db-seed`.
* The global Python 3.14 still has an old editable install named `gtc-code`
  pointing at files that no longer exist. Remove it with
  `C:\Python314\python -m pip uninstall gtc-code` and use `.venv` from now on.
* `.vscode/launch.json` (git-ignored) still targets the old
  `Sales_ETL.Pipelines.raw_orders.pipeline` module; point it at `Sales_ETL.run`.
* Delete `.env.backup-2026-11-08` once the new `.env` is confirmed.
* `shopify-graphql-retrieve-orders/src/package.json` lists `dotenv` and
  `csv-writer`, which nothing uses (left so `package-lock.json` stays in sync).
