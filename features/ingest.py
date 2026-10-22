"""
Pure I/O loaders for the three heterogeneous raw sources described in
`data_dictionary.md`. No feature logic belongs here — only loading and basic
type coercion, so ingestion can be unit-tested and swapped independently of
`transforms.py`.

Every timestamp is coerced to datetime64[ns]: pandas 3 parses plain dates to [us], while
`txn_timestamp` carries nanoseconds (data_dictionary.md DQ10) that must not be truncated.
The SQLite database is always opened read-only, so a wrong path fails loudly instead of
creating an empty database.
"""

from contextlib import closing
from pathlib import Path
import sqlite3

import pandas as pd

CUSTOMER_COLUMNS = ["customer_id", "signup_date", "acquisition_channel", "region", "churned"]
TRANSACTION_COLUMNS = ["customer_id", "txn_timestamp", "category", "channel", "amount", "is_returned"]
MONTHLY_COLUMNS = ["customer_id", "txn_month", "txn_count", "total_amount", "return_count"]


def _require_columns(df: pd.DataFrame, expected: list[str], source: str) -> None:
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"{source} is missing columns {missing}")


def _connect_read_only(path: str | Path) -> sqlite3.Connection:
    uri = Path(path).resolve().as_uri() + "?mode=ro"
    return sqlite3.connect(uri, uri=True)


def load_customers(path: str | Path) -> pd.DataFrame:
    """Load customers.csv, with `signup_date` parsed as a datetime64[ns] date."""
    df = pd.read_csv(path)
    _require_columns(df, CUSTOMER_COLUMNS, str(path))
    df["customer_id"] = df["customer_id"].astype("int64")
    df["signup_date"] = pd.to_datetime(df["signup_date"], format="%Y-%m-%d").astype("datetime64[ns]")
    return df


def load_transactions(path: str | Path) -> pd.DataFrame:
    """Load the full row-level `transactions` table, with `txn_timestamp` parsed from its
    ISO-8601 text and `is_returned` as a validated 0/1 int64. Not aggregated or filtered."""
    with closing(_connect_read_only(path)) as con:
        df = pd.read_sql_query(f"SELECT {', '.join(TRANSACTION_COLUMNS)} FROM transactions", con)
    df["customer_id"] = df["customer_id"].astype("int64")
    df["txn_timestamp"] = pd.to_datetime(df["txn_timestamp"], format="ISO8601").astype("datetime64[ns]")
    df["amount"] = df["amount"].astype("float64")
    bad = ~df["is_returned"].isin([0, 1])
    if bad.any():
        raise ValueError(f"{path}: is_returned has values other than 0/1 in {int(bad.sum())} rows")
    df["is_returned"] = df["is_returned"].astype("int64")
    return df


def load_customer_ids_with_txn_before(path: str | Path, as_of: pd.Timestamp) -> set[int]:
    """Customer ids with at least one transaction strictly before `as_of`, queried directly
    in SQL on the raw text column.

    This is QC3's independent view of who is zero-activity: it does not go through
    `load_transactions`' datetime parsing or any transform. The fixed-width
    `YYYY-MM-DD HH:MM:SS.fffffffff` text (DQ10) sorts chronologically, and a row at exactly
    `as_of` 00:00:00 compares greater than the bare date, so `<` excludes it.
    """
    cutoff = pd.Timestamp(as_of).strftime("%Y-%m-%d")
    with closing(_connect_read_only(path)) as con:
        rows = con.execute(
            "SELECT DISTINCT customer_id FROM transactions WHERE txn_timestamp < ?", (cutoff,)
        ).fetchall()
    return {int(r[0]) for r in rows}


def load_monthly_agg(path: str | Path) -> pd.DataFrame:
    """Load the customer x month parquet extract, `txn_month` as datetime64[ns].

    Returned as-is: months with zero activity have no row (DQ3).
    """
    df = pd.read_parquet(path)
    _require_columns(df, MONTHLY_COLUMNS, str(path))
    df["customer_id"] = df["customer_id"].astype("int64")
    df["txn_month"] = pd.to_datetime(df["txn_month"]).astype("datetime64[ns]")
    not_month_start = df["txn_month"] != df["txn_month"].dt.to_period("M").dt.to_timestamp()
    if not_month_start.any():
        raise ValueError(f"{path}: {int(not_month_start.sum())} txn_month values are not a month start")
    return df
