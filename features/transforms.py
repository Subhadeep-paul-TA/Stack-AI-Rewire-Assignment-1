"""
One pure function per candidate feature named in your confirmed `feature_spec.md`.

Each function should:
- Be independent of any I/O (take already-loaded DataFrames in, return a Series/DataFrame out).
- Handle the zero-activity case explicitly, per the default your feature_spec.md states —
  not with a bare try/except that swallows the real problem.
- Only touch the source(s) feature_spec.md names for that feature. If a feature could
  plausibly come from either `transactions.db` or `transactions_monthly_agg.parquet`,
  use the one your spec chose and justified — do not silently blend both.

This file intentionally contains no implementations. Add one function per feature as you
build against your confirmed spec.
"""

import pandas as pd


def example_feature_placeholder(
    customers: pd.DataFrame,
    transactions: pd.DataFrame,
    monthly_agg: pd.DataFrame,
    as_of: str,
) -> pd.Series:
    """TODO: replace with your first real feature function.

    Rename this function (and this file's contents) to match the candidate features
    named in your confirmed feature_spec.md. Keep one function per feature.
    """
    raise NotImplementedError
