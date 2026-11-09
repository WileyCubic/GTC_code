# GTC Code

Back-office tools for **Game Time Couture / GTC Greek**: they turn Square,
Shopify and Affinity order exports and AmEx statements into royalty reports,
sales-tax reports, daily pivot tables and a Postgres database for analysis.

Every tool is a command you run by hand after putting the latest export files
in a folder under `data/`. Nothing runs on a schedule.

## What's here

| Command | What it does | Code |
|---|---|---|
| `gtc-royalties-csv` | Quarterly royalty report from one quarter's Square + Shopify exports | `scripts/src/royalties_from_csv.py` |
| `gtc-royalties-db` | Same report from the Postgres database | `scripts/src/royalties_from_db.py` |
| `gtc-tax-report` | Yearly sales-tax report (Shopify + Affinity + Square, one row per order) | `scripts/src/yearly_tax_report.py` |
| `gtc-sales-etl` | Square + Shopify exports → `square_raw`, `shopify_raw` → `lineitem_analysis` | `Data-Pipelines/Sales_ETL/` |
| `gtc-expenses-etl` | AmEx statements → `bp_expences`, `ab_expences` | `Data-Pipelines/Expences_ETL/` |
| `gtc-pivot-tables` | Daily order CSV → `Formated Table MM-DD-YYYY.xlsx` | `GTC-Formated-Privot-Tables/src/` |
| `gtc-upi-names` | Affinity UPI code → product names table | `scripts/src/upi_to_names.py` |
| `gtc-affinity-images` | Download product images from a URL list | `scripts/src/affinity_image_scraper.py` |
| `gtc-dashboard` | Dash dashboard (layout prototype, no data yet) | `Analysis-Bashboard/src/` |
| `gtc-db-migrate` / `gtc-db-seed` | Create the Postgres tables / one-time initial load | `gtc_common/` |

Two Node projects sit alongside: `shopify-graphql-retrieve-orders/` (Shopify
Admin API test scripts) and `gtc-graphql-api/` (an unused Shopify app template).

## Quick start

```bash
python -m venv .venv
source .venv/Scripts/activate          # PowerShell: .venv\Scripts\Activate.ps1
pip install -e ".[test]"
cp .env.example .env                   # then fill it in, see docs/ENV.md
gtc-db-migrate                         # create the Postgres tables
pytest                                 # run the tests
```

## Documentation

| Doc | Read it for |
|---|---|
| [docs/RUNBOOK.md](docs/RUNBOOK.md) | Step-by-step: quarterly royalties, yearly tax, daily pivots, expenses, troubleshooting |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | How the projects connect, with a data-flow diagram |
| [docs/ENV.md](docs/ENV.md) | Every `.env` setting, and the old → new name mapping |
| [docs/DATABASE.md](docs/DATABASE.md) | Postgres tables, who writes/reads them, schema changes |
| [docs/AUDIT.md](docs/AUDIT.md) | The November 2026 audit: what was broken, what changed, open questions |

## Conventions

* **One `.env`** at the repo root for every project. Never commit it.
* **One install** (`pip install -e .`) provides every Python package and command.
* Shared code goes in `gtc_common/`, not copied into each project.
* Inputs live in `data/`, outputs in `scripts/scripts output/`, logs in `logs/`
  (all git-ignored).
* Database changes go in a new `gtc_common/schema/NNN_*.sql` file.

## License

MIT, see [LICENSE](LICENSE).

---

<p align='center'>
    <img src=https://8d6f392e60e520ff48d4.cdn6.editmysite.com/uploads/b/8d6f392e60e520ff48d435335921b21eff5fef11c4c62075ccbb7fc0b9906752/8.8.2024_66b4e208d312b8.66067049.png?width=2400&optimize=medium alt='Game Time Couture' width='200'>
    <img src=https://cdn.shopify.com/s/files/1/0710/3145/5940/files/GTC_LOGO-02.png?v=1749586626 alt='Game Time Couture' width='500'>
</p>
