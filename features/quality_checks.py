"""
Runnable-standalone data-quality checks (drift / null / outlier) against a built
features DataFrame, implementing every check listed in your confirmed
`feature_spec.md`'s data-quality section.

Each check function should return a clear pass/fail plus which rows/features
triggered a failure — never silently pass on bad input.
"""

import pandas as pd


def check_no_unexpected_nulls(features: pd.DataFrame) -> dict:
    """TODO: fail if any feature column that should never be null (per schema.py) is null.

    Return something like {"passed": bool, "failing_features": [...], "failing_rows": [...]}.
    """
    raise NotImplementedError


def check_outliers(features: pd.DataFrame) -> dict:
    """TODO: fail if a monetary/aggregate feature has values outside a sane range per
    feature_spec.md's stated thresholds.
    """
    raise NotImplementedError


def check_drift(features: pd.DataFrame, reference: pd.DataFrame | None = None) -> dict:
    """TODO: compare the current feature distribution against a reference snapshot
    (e.g. a prior build) and fail if the shift exceeds what feature_spec.md allows.
    """
    raise NotImplementedError


def run_all_checks(features: pd.DataFrame) -> dict:
    """TODO: run every check above and return a combined pass/fail summary."""
    raise NotImplementedError
