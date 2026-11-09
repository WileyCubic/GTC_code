"""Postgres smoke test: skipped unless the database in .env is reachable."""
import pytest

from gtc_common import env

EXPECTED_TABLES = {"square_raw", "shopify_raw", "lineitem_analysis", "bp_expences", "ab_expences"}


@pytest.fixture(scope="module")
def engine():
    if not env.get("PG_PASSWORD"):
        pytest.skip("PG_PASSWORD not set in .env")
    from gtc_common.db import get_engine
    eng = get_engine()
    try:
        with eng.connect():
            pass
    except Exception as exc:  # pragma: no cover - depends on local setup
        pytest.skip(f"Postgres not reachable: {exc}")
    return eng


def test_schema_tables_exist(engine):
    from gtc_common.db import table_columns
    missing = {t for t in EXPECTED_TABLES if not table_columns(t)}
    assert not missing, f"run gtc-db-migrate; missing tables: {missing}"
