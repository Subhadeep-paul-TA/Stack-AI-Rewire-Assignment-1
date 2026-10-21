"""
Pure I/O loaders for the three heterogeneous raw sources described in
`data_dictionary.md`. No feature logic belongs here — only loading and basic
type coercion, so ingestion can be unit-tested and swapped independently of
`transforms.py`.

Fill in each function per your confirmed `feature_spec.md`.
"""

from pathlib import Path

import pandas as pd


def load_customers(path: str = "data/customers.csv") -> pd.DataFrame:
    """Load customers.csv.

    TODO:
    - Read the CSV with pandas.
    - Parse `signup_date` as a date.
    - Return one row per customer_id, unmodified otherwise.
    """
    raise NotImplementedError


def load_transactions(path: str = "data/transactions.db") -> pd.DataFrame:
    """Load the row-level `transactions` table from the SQLite database.

    TODO:
    - Connect with sqlite3 (or pandas.read_sql) against `path`.
    - Cast `txn_timestamp` to a real datetime (SQLite stores it as ISO-8601 text).
    - Cast `is_returned` to a clean 0/1 int.
    - Return the full row-level frame — do not aggregate here.
    """
    raise NotImplementedError


def load_monthly_agg(path: str = "data/transactions_monthly_agg.parquet") -> pd.DataFrame:
    """Load the pre-aggregated customer x month parquet extract.

    TODO:
    - Read the parquet file with pandas.
    - Parse `txn_month` as a date (first-of-month).
    - Return as-is — remember customers/months with zero activity have no row.
    """
    raise NotImplementedError
