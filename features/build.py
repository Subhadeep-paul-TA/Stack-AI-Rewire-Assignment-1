"""
The ONE shared feature-build entry point. Both `scripts/build_training_features.py`
(training path: a historical as-of date, batch of many customers) and
`scripts/score_customer.py` (scoring path: single customer_id, same as-of date) must call
this same function — that is the entire train/score parity guarantee for this pipeline.

There must be exactly one code path here. If you find yourself writing separate logic for
"training mode" vs. "scoring mode" beyond which rows they select at the end, that is a
parity bug — go back and unify it rather than special-casing either path.
"""

import pandas as pd

from features import ingest, transforms  # noqa: F401 (import when transforms are implemented)


def build_features(
    as_of: str,
    source_paths: dict | None = None,
    customer_ids: list[int] | None = None,
) -> pd.DataFrame:
    """Build the full feature set as of `as_of` for either the whole customer base
    (training path, `customer_ids=None`) or a specific subset (scoring path, one or more
    `customer_ids`).

    TODO:
    - Default `source_paths` to the standard data/ locations
      (customers.csv, transactions.db, transactions_monthly_agg.parquet) if not provided.
    - Load all three sources via features/ingest.py — no direct file I/O here.
    - Filter to `customer_ids` if provided; otherwise use the full customer base. This
      filtering is the ONLY difference allowed between the training and scoring call
      sites — every transform must run identically either way.
    - Compute every candidate feature named in your confirmed feature_spec.md via
      features/transforms.py, applying the explicit zero-activity default per feature.
    - Return a DataFrame with one row per requested customer_id, one column per feature,
      plus `customer_id` and `as_of_date`.
    """
    raise NotImplementedError
