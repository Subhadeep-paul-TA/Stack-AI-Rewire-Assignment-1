"""
Local feature store API: a SQLite metadata/version registry (`registry.db`, table
`feature_registry`) plus one parquet value snapshot per registered version
(`values/<feature_name>__v<version>.parquet`), both under one store root.

The store root defaults to this module's own directory (`feature_store/`), resolved from
the module path rather than the working directory. Every function takes a keyword-only
`store_dir` to override it; tests always pass a temporary one.

Versions are append-only, enforced in the database as well as in this module:
- a new version always gets a new registry row and a new parquet file. The file is opened
  with exclusive create, so an existing file is never overwritten;
- triggers reject any UPDATE of a row's content, any DELETE, and any attempt to set
  is_latest on an existing row, so is_latest can only move to a newly inserted version;
- a partial unique index allows at most one is_latest row per feature, and the old latest
  row is cleared in the same transaction as the insert.

Every stored snapshot in one store has the same as_of_date (feature_spec.md, "One as_of per
store"). The registry has no as_of column, so the date is read from the snapshots.

Never hardcode a parquet filename outside this module — all reads resolve the path via the
registry.
"""

from __future__ import annotations

from contextlib import closing
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sqlite3

import pandas as pd

DEFAULT_STORE_DIR = Path(__file__).resolve().parent
REGISTRY_FILENAME = "registry.db"
VALUES_DIRNAME = "values"
_FEATURE_NAME = re.compile(r"[a-z][a-z0-9_]*")

_DDL = """
CREATE TABLE IF NOT EXISTS feature_registry (
    feature_name       TEXT    NOT NULL,
    version            INTEGER NOT NULL CHECK (version >= 1),
    created_at         TEXT    NOT NULL,
    description        TEXT    NOT NULL,
    source_columns     TEXT    NOT NULL,
    transform_summary  TEXT    NOT NULL,
    business_rationale TEXT    NOT NULL,
    parameters         TEXT    NOT NULL,
    parquet_path       TEXT    NOT NULL UNIQUE,
    is_latest          INTEGER NOT NULL CHECK (is_latest IN (0, 1)),
    PRIMARY KEY (feature_name, version)
);
CREATE UNIQUE INDEX IF NOT EXISTS feature_registry_one_latest
    ON feature_registry (feature_name) WHERE is_latest = 1;
CREATE TRIGGER IF NOT EXISTS feature_registry_rows_immutable
    BEFORE UPDATE OF feature_name, version, created_at, description, source_columns,
                     transform_summary, business_rationale, parameters, parquet_path
    ON feature_registry
BEGIN
    SELECT RAISE(ABORT, 'feature_registry rows are immutable; register a new version instead');
END;
CREATE TRIGGER IF NOT EXISTS feature_registry_latest_only_moves_forward
    BEFORE UPDATE OF is_latest ON feature_registry
    WHEN NEW.is_latest = 1 AND OLD.is_latest = 0
BEGIN
    SELECT RAISE(ABORT, 'is_latest is set only by registering a new version');
END;
CREATE TRIGGER IF NOT EXISTS feature_registry_rows_never_deleted
    BEFORE DELETE ON feature_registry
BEGIN
    SELECT RAISE(ABORT, 'feature_registry rows are never deleted');
END;
"""

REGISTRY_COLUMNS = [
    "feature_name", "version", "created_at", "description", "source_columns",
    "transform_summary", "business_rationale", "parameters", "parquet_path", "is_latest",
]


class StoreError(Exception):
    """A registration the store refuses: it would break one of the store's invariants."""


def _store_dir(store_dir) -> Path:
    return DEFAULT_STORE_DIR if store_dir is None else Path(store_dir).resolve()


def store_root(*, store_dir=None) -> Path:
    """The resolved store root: `store_dir`, or feature_store/ by default."""
    return _store_dir(store_dir)


def registry_path(*, store_dir=None) -> Path:
    return _store_dir(store_dir) / REGISTRY_FILENAME


def _connect_write(root: Path) -> sqlite3.Connection:
    (root / VALUES_DIRNAME).mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(root / REGISTRY_FILENAME, isolation_level=None)
    con.executescript(_DDL)
    return con


def _connect_read(root: Path) -> sqlite3.Connection | None:
    """Read-only connection, or None when the store has no registry yet. Reads never create
    files."""
    path = root / REGISTRY_FILENAME
    if not path.exists():
        return None
    return sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)


def _query(root: Path, sql: str, args: tuple = ()) -> pd.DataFrame:
    con = _connect_read(root)
    if con is None:
        return pd.DataFrame(columns=REGISTRY_COLUMNS)
    with closing(con):
        return pd.read_sql_query(sql, con, params=args)


def _resolve(root: Path, parquet_path: str) -> Path:
    """parquet_path is stored relative to the store root's parent, e.g.
    'feature_store/values/x__v1.parquet'. Refuse anything outside the values directory."""
    path = (root.parent / parquet_path).resolve()
    if path.parent != (root / VALUES_DIRNAME).resolve():
        raise StoreError(f"registry parquet_path {parquet_path!r} is outside {root / VALUES_DIRNAME}")
    return path


def _snapshot_as_of(path: Path) -> set[pd.Timestamp]:
    return set(pd.to_datetime(pd.read_parquet(path, columns=["as_of_date"])["as_of_date"]).unique())


def _validate_values(feature_name: str, values_df: pd.DataFrame) -> pd.DataFrame:
    expected = ["customer_id", feature_name, "as_of_date"]
    if sorted(values_df.columns) != sorted(expected):
        raise StoreError(f"values_df must have exactly the columns {expected}, got {list(values_df.columns)}")
    df = values_df[expected].reset_index(drop=True)
    if df.empty:
        raise StoreError(f"values_df for {feature_name} is empty")
    if df["customer_id"].isna().any() or df["customer_id"].duplicated().any():
        raise StoreError(f"values_df for {feature_name} has null or duplicated customer_id")
    if df[feature_name].isna().any():
        raise StoreError(f"values_df for {feature_name} has null values")
    dates = pd.to_datetime(df["as_of_date"]).unique()
    if len(dates) != 1 or pd.isna(dates[0]):
        raise StoreError(f"values_df for {feature_name} must have exactly one as_of_date, got {list(dates)}")
    df["as_of_date"] = pd.to_datetime(df["as_of_date"]).astype("datetime64[ns]")
    return df.sort_values("customer_id", ignore_index=True)


def register_version(
    feature_name: str,
    values_df: pd.DataFrame,
    description: str,
    source_columns: str,
    transform_summary: str,
    business_rationale: str,
    parameters: dict,
    *,
    store_dir=None,
) -> int:
    """Write a new parquet snapshot, insert the registry row, flip is_latest, and return the
    new version number (1 for a new feature, then max + 1).

    Raises StoreError if the snapshot's as_of_date differs from the store's, or if the
    version's parquet file already exists (it is never overwritten).
    """
    if not _FEATURE_NAME.fullmatch(feature_name):
        raise StoreError(f"feature_name {feature_name!r} must match {_FEATURE_NAME.pattern}")
    if not isinstance(parameters, dict):
        raise StoreError("parameters must be a dict")
    params_json = json.dumps(parameters, sort_keys=True)
    df = _validate_values(feature_name, values_df)
    new_as_of = pd.Timestamp(df["as_of_date"].iloc[0])

    root = _store_dir(store_dir)
    created_file: Path | None = None
    with closing(_connect_write(root)) as con:
        con.execute("BEGIN IMMEDIATE")
        try:
            stored = [r[0] for r in con.execute("SELECT parquet_path FROM feature_registry")]
            store_dates = set().union(*(_snapshot_as_of(_resolve(root, p)) for p in stored)) if stored else set()
            if store_dates and store_dates != {new_as_of}:
                raise StoreError(
                    f"store holds snapshots at {sorted(d.date() for d in store_dates)}; refusing to "
                    f"register {feature_name} at {new_as_of.date()} (one as_of per store)"
                )
            version = con.execute(
                "SELECT COALESCE(MAX(version), 0) + 1 FROM feature_registry WHERE feature_name = ?",
                (feature_name,),
            ).fetchone()[0]
            relative = f"{root.name}/{VALUES_DIRNAME}/{feature_name}__v{version}.parquet"
            path = _resolve(root, relative)
            try:
                handle = open(path, "xb")
            except FileExistsError:
                raise StoreError(
                    f"{relative} already exists but is not registered; refusing to overwrite it"
                ) from None
            created_file = path
            with handle:
                df.to_parquet(handle, index=False)
            con.execute(
                "UPDATE feature_registry SET is_latest = 0 WHERE feature_name = ? AND is_latest = 1",
                (feature_name,),
            )
            con.execute(
                f"INSERT INTO feature_registry ({', '.join(REGISTRY_COLUMNS)}) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)",
                (
                    feature_name, version, datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    description, source_columns, transform_summary, business_rationale,
                    params_json, relative,
                ),
            )
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            if created_file is not None:
                created_file.unlink(missing_ok=True)
            raise
    return int(version)


def list_versions(feature_name: str, *, store_dir=None) -> pd.DataFrame:
    """Every registered version's registry row for feature_name, most recent first. Empty
    (with the registry columns) if the feature has none."""
    return _query(
        _store_dir(store_dir),
        f"SELECT {', '.join(REGISTRY_COLUMNS)} FROM feature_registry WHERE feature_name = ? "
        "ORDER BY version DESC",
        (feature_name,),
    )


def list_features(*, store_dir=None) -> list[str]:
    df = _query(_store_dir(store_dir), "SELECT DISTINCT feature_name FROM feature_registry ORDER BY feature_name")
    return df["feature_name"].tolist()


def get_version(feature_name: str, version: int, *, store_dir=None) -> pd.DataFrame:
    """The snapshot of (feature_name, version), resolved through the registry."""
    root = _store_dir(store_dir)
    row = _query(
        root,
        "SELECT parquet_path FROM feature_registry WHERE feature_name = ? AND version = ?",
        (feature_name, int(version)),
    )
    if row.empty:
        raise KeyError(f"{feature_name} v{version} is not registered in {root}")
    return pd.read_parquet(_resolve(root, row["parquet_path"].iloc[0]))


def latest_version(feature_name: str, *, store_dir=None) -> int:
    """The version flagged is_latest, checked against the highest registered version."""
    root = _store_dir(store_dir)
    rows = _query(
        root,
        "SELECT (SELECT version FROM feature_registry WHERE feature_name = ? AND is_latest = 1) AS latest, "
        "(SELECT MAX(version) FROM feature_registry WHERE feature_name = ?) AS highest",
        (feature_name, feature_name),
    )
    if rows.empty or pd.isna(rows["highest"].iloc[0]):
        raise KeyError(f"{feature_name} has no registered versions in {root}")
    latest, highest = rows["latest"].iloc[0], int(rows["highest"].iloc[0])
    if pd.isna(latest) or int(latest) != highest:
        raise StoreError(f"{feature_name}: is_latest points to v{latest} but the highest version is v{highest}")
    return highest


def get_latest(feature_name: str, *, store_dir=None) -> pd.DataFrame:
    """The snapshot of the version flagged is_latest = 1 (always the highest version)."""
    return get_version(feature_name, latest_version(feature_name, store_dir=store_dir), store_dir=store_dir)


def store_as_of(*, store_dir=None) -> pd.Timestamp | None:
    """The single as_of_date of every snapshot in the store, or None if the store is empty."""
    root = _store_dir(store_dir)
    paths = _query(root, "SELECT parquet_path FROM feature_registry")["parquet_path"].tolist()
    dates = set().union(*(_snapshot_as_of(_resolve(root, p)) for p in paths)) if paths else set()
    if len(dates) > 1:
        raise StoreError(f"store mixes as_of dates {sorted(dates)}")
    return pd.Timestamp(next(iter(dates))) if dates else None
