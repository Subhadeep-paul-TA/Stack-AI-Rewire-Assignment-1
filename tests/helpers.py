"""Constants and helpers shared by the test modules (fixtures live in conftest.py)."""

from pathlib import Path
import sqlite3

from feature_store import store
from features import schema

AS_OF = "2026-05-02"


def version_params(k: int) -> dict[str, dict]:
    """Each feature's k-th schema version (or its last, if it has fewer)."""
    return {f.name: f.versions[min(k, len(f.versions)) - 1].parameters for f in schema.FEATURE_SCHEMA}


def registry_rows(root: Path) -> list[tuple]:
    path = root / store.REGISTRY_FILENAME
    if not path.exists():
        return []
    with sqlite3.connect(path) as con:
        return con.execute("SELECT * FROM feature_registry ORDER BY feature_name, version").fetchall()


def store_files(root: Path) -> list[str]:
    values = root / store.VALUES_DIRNAME
    return sorted(p.name for p in values.glob("*")) if values.exists() else []
