"""
The ONE shared feature-build entry point. Both `scripts/build_training_features.py`
(training path: a historical as-of date, batch of many customers) and
`scripts/score_customer.py` (scoring path: single customer_id, same as-of date) must call
this same function — that is the entire train/score parity guarantee for this pipeline.

There is exactly one code path. Features are always computed, and checked, for the full
population at `as_of`; the only training/scoring difference is which rows the last step
returns (`customer_ids`).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from features import ingest, schema, transforms
from features import quality_checks as qc

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_PATHS = {
    "customers": REPO_ROOT / "data" / "customers.csv",
    "transactions": REPO_ROOT / "data" / "transactions.db",
    "monthly_agg": REPO_ROOT / "data" / "transactions_monthly_agg.parquet",
}


def resolve_source_paths(source_paths: dict | None) -> dict[str, Path]:
    paths = dict(DEFAULT_SOURCE_PATHS)
    unknown = set(source_paths or {}) - set(paths)
    if unknown:
        raise ValueError(f"unknown source_paths keys {sorted(unknown)}; expected {sorted(paths)}")
    paths.update({k: Path(v) for k, v in (source_paths or {}).items()})
    return paths


def _compute_features(
    population: pd.DataFrame,
    transactions: pd.DataFrame,
    monthly: pd.DataFrame,
    as_of: pd.Timestamp,
    params: dict[str, dict],
) -> pd.DataFrame:
    def kw(name: str) -> dict:
        return schema.get_spec(name).transform_kwargs(params[name])

    columns = {
        "tenure_days": transforms.tenure_days(population, as_of),
        "days_since_last_txn": transforms.days_since_last_txn(
            population, transactions, as_of, **kw("days_since_last_txn")
        ),
        "has_ever_purchased": transforms.has_ever_purchased(population, transactions, as_of),
        "txn_count_90d": transforms.txn_count_90d(population, transactions, as_of, **kw("txn_count_90d")),
        "avg_spend_per_active_month_6m": transforms.avg_spend_per_active_month_6m(
            population, monthly, as_of, **kw("avg_spend_per_active_month_6m")
        ),
        "spend_trend_3m_vs_prior_3m": transforms.spend_trend_3m_vs_prior_3m(
            population, monthly, as_of, **kw("spend_trend_3m_vs_prior_3m")
        ),
        "active_month_share": transforms.active_month_share(
            population, monthly, as_of, **kw("active_month_share")
        ),
    }
    if list(columns) != schema.FEATURE_NAMES:
        raise RuntimeError(f"build computes {list(columns)} but schema.py lists {schema.FEATURE_NAMES}")
    return pd.DataFrame(columns).rename_axis("customer_id").reset_index()


def _requested_ids(customer_ids, customers: pd.DataFrame, as_of: pd.Timestamp) -> list[int]:
    """Validate scoring ids: each must exist and have signed up before as_of. Raises a
    ValueError naming the offending id; never lets an empty selection through."""
    ids = list(customer_ids)
    if not ids:
        raise ValueError("customer_ids is empty; pass None for the full population")
    signup = customers.set_index("customer_id")["signup_date"]
    seen = set()
    for raw in ids:
        try:
            cid = int(raw)
        except (TypeError, ValueError):
            cid = None
        if isinstance(raw, bool) or cid is None or cid != raw:
            raise ValueError(f"customer_id {raw!r} is not an integer")
        if cid in seen:
            raise ValueError(f"customer_id {cid} is requested more than once")
        seen.add(cid)
        if cid not in signup.index:
            raise ValueError(f"customer_id {cid} does not exist in customers.csv")
        if signup[cid] >= as_of:
            raise ValueError(
                f"customer_id {cid} signed up on {signup[cid].date()}, not before as_of {as_of.date()}"
            )
    return [int(i) for i in ids]


def build_features(
    as_of,
    source_paths: dict | None = None,
    customer_ids: list[int] | None = None,
    feature_params: dict[str, dict] | None = None,
) -> pd.DataFrame:
    """Build all 7 features at `as_of` for the full population (`customer_ids=None`, the
    training call) or for the requested ids (the scoring call).

    `as_of` is required and parsed once here. `feature_params` maps a feature name to one
    version's parameters (used by the training script to rebuild older versions); features
    it doesn't name use their latest version in `features/schema.py`.

    Returns one row per customer: `customer_id`, the 7 features in schema order, and
    `as_of_date`. The check results are attached as `df.attrs["quality_checks"]`. Raises
    `QualityCheckError` if any of QC1-QC6 or QC8 fails, whichever rows were requested.
    """
    as_of_ts = transforms.parse_as_of(as_of)
    paths = resolve_source_paths(source_paths)
    params = schema.resolve_feature_params(feature_params)

    customers = ingest.load_customers(paths["customers"])
    transactions = ingest.load_transactions(paths["transactions"])
    monthly = ingest.load_monthly_agg(paths["monthly_agg"])
    buyer_ids = ingest.load_customer_ids_with_txn_before(paths["transactions"], as_of_ts)

    # The cutoff: no DB row at or after as_of, and no parquet month at or after as_of's month,
    # reaches a transform (QC5 verifies these exact frames).
    transactions_used = transactions.loc[transactions["txn_timestamp"] < as_of_ts]
    monthly_used = monthly.loc[monthly["txn_month"] < transforms.month_start(as_of_ts)]
    results = qc.run_input_checks(transactions_used, monthly_used, transactions, monthly, as_of_ts, params)
    qc.raise_on_failure(results)

    population = transforms.population(customers, as_of_ts)
    features = _compute_features(population, transactions_used, monthly_used, as_of_ts, params)
    output_results = qc.run_output_checks(
        features, customers=customers, transactions=transactions, monthly=monthly,
        buyer_ids=buyer_ids, as_of=as_of_ts, params=params,
    )
    results += output_results
    qc.raise_on_failure(output_results)

    for spec in schema.FEATURE_SCHEMA:
        if str(features[spec.name].dtype) != spec.dtype:
            raise TypeError(f"{spec.name} built as {features[spec.name].dtype}, schema.py says {spec.dtype}")

    # The only training/scoring difference: which rows are returned.
    requested = population.index.tolist() if customer_ids is None else _requested_ids(customer_ids, customers, as_of_ts)
    selected = features.set_index("customer_id").loc[requested].reset_index()
    selection = qc.check_selection_rows(selected, requested)
    results.append(selection)
    qc.raise_on_failure([selection])

    selected["as_of_date"] = as_of_ts
    selected.attrs["quality_checks"] = [r.as_dict() for r in results]
    selected.attrs["feature_params"] = params
    return selected
