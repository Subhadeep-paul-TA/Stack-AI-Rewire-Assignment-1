"""features/ingest.py: one loader per source, pure I/O plus type coercion."""

import sqlite3

import pandas as pd
import pytest

from features import ingest
from features.build import DEFAULT_SOURCE_PATHS
from features.transforms import parse_as_of


def test_real_sources_load_with_expected_shapes_and_dtypes():
    customers = ingest.load_customers(DEFAULT_SOURCE_PATHS["customers"])
    transactions = ingest.load_transactions(DEFAULT_SOURCE_PATHS["transactions"])
    monthly = ingest.load_monthly_agg(DEFAULT_SOURCE_PATHS["monthly_agg"])
    assert customers.shape == (1500, 5) and customers["customer_id"].is_unique
    assert transactions.shape == (11742, 6)
    assert monthly.shape == (8086, 5)
    assert customers["signup_date"].dtype == "datetime64[ns]"
    assert transactions["txn_timestamp"].dtype == "datetime64[ns]"
    assert monthly["txn_month"].dtype == "datetime64[ns]"
    assert set(transactions["is_returned"].unique()) == {0, 1}


def test_transaction_timestamps_keep_nanoseconds():
    transactions = ingest.load_transactions(DEFAULT_SOURCE_PATHS["transactions"])
    row = transactions.loc[transactions["customer_id"] == 697, "txn_timestamp"].max()
    assert row == pd.Timestamp("2026-05-02 17:12:19.254026276")


def test_sql_buyer_query_matches_the_parsed_timestamps():
    as_of = parse_as_of("2026-05-02")
    transactions = ingest.load_transactions(DEFAULT_SOURCE_PATHS["transactions"])
    parsed = set(transactions.loc[transactions["txn_timestamp"] < as_of, "customer_id"])
    assert ingest.load_customer_ids_with_txn_before(DEFAULT_SOURCE_PATHS["transactions"], as_of) == parsed


def test_sql_buyer_query_excludes_a_row_at_exactly_as_of(tmp_path):
    db = tmp_path / "t.db"
    with sqlite3.connect(db) as con:
        con.execute("CREATE TABLE transactions (customer_id INTEGER, txn_timestamp TEXT)")
        con.executemany("INSERT INTO transactions VALUES (?, ?)", [
            (1, "2026-05-02 00:00:00.000000000"), (2, "2026-05-01 23:59:59.999999999"),
        ])
    assert ingest.load_customer_ids_with_txn_before(db, parse_as_of("2026-05-02")) == {2}


def test_missing_database_fails_without_creating_a_file(tmp_path):
    path = tmp_path / "missing.db"
    with pytest.raises(sqlite3.OperationalError):
        ingest.load_transactions(path)
    assert not path.exists()


def test_monthly_loader_rejects_a_non_month_start(tmp_path):
    path = tmp_path / "m.parquet"
    pd.DataFrame({
        "customer_id": [1], "txn_month": pd.to_datetime(["2026-04-02"]), "txn_count": [1],
        "total_amount": [5.0], "return_count": [0],
    }).to_parquet(path)
    with pytest.raises(ValueError, match="not a month start"):
        ingest.load_monthly_agg(path)


def test_customers_loader_requires_its_columns(tmp_path):
    path = tmp_path / "c.csv"
    pd.DataFrame({"customer_id": [1], "signup_date": ["2026-01-01"]}).to_csv(path, index=False)
    with pytest.raises(ValueError, match="missing columns"):
        ingest.load_customers(path)
