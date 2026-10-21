"""
Local feature store API: a SQLite metadata/version registry (`feature_store/registry.db`,
table `feature_registry`) plus one parquet value snapshot per registered version
(`feature_store/values/<feature_name>__v<version>.parquet`).

Do not add mlflow, feast, or any other feature-store library — this store is SQLite +
parquet only. See skill.md's Workflow section for the exact registry schema this module
must implement (columns: feature_name, version, created_at, description, source_columns,
transform_summary, business_rationale, parameters, parquet_path, is_latest).

Never hardcode a parquet filename outside this module — all reads must resolve the path
via the registry.
"""

from pathlib import Path

import pandas as pd

REGISTRY_DB = "feature_store/registry.db"
VALUES_DIR = "feature_store/values"


def register_version(
    feature_name: str,
    values_df: pd.DataFrame,
    description: str,
    source_columns: str,
    transform_summary: str,
    business_rationale: str,
    parameters: dict,
) -> int:
    """Write a new parquet snapshot, insert the registry row, flip is_latest, return the
    new version number.

    TODO:
    - Determine the next version number for feature_name (starts at 1, increments).
    - Write values_df to feature_store/values/<feature_name>__v<version>.parquet —
      never overwrite an existing version's file.
    - Insert a row into feature_registry with all metadata columns, JSON-encoding
      `parameters`.
    - In the same transaction, flip the prior is_latest row (if any) to 0 and set the
      new row's is_latest to 1.
    - Return the new version number.
    """
    raise NotImplementedError


def get_latest(feature_name: str) -> pd.DataFrame:
    """Return the DataFrame for the version currently flagged is_latest = 1.

    TODO: look up the latest row in feature_registry, resolve parquet_path, load it.
    """
    raise NotImplementedError


def get_version(feature_name: str, version: int) -> pd.DataFrame:
    """Return the DataFrame for a specific (feature_name, version).

    TODO: look up the exact row in feature_registry, resolve parquet_path, load it.
    """
    raise NotImplementedError


def list_versions(feature_name: str) -> pd.DataFrame:
    """Return every registered version's metadata row for feature_name, most recent first.

    TODO: query feature_registry, order by version descending.
    """
    raise NotImplementedError
