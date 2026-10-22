"""
Data-quality checks QC1-QC8 from `feature_spec.md`'s data-quality section.

Each check is a pure function returning a `CheckResult` with a clear pass/fail, a detail
line, and the features/customer_ids that triggered a failure. `raise_on_failure` turns any
failure into a `QualityCheckError`, so a failing check always stops the caller (a non-zero
exit in both scripts) rather than logging a warning.

Where the checks run:
- QC5 and QC6 check the raw inputs, and QC1-QC4 and QC8 check the full-population output.
  All of them run inside `features.build.build_features` on every call, before the final
  customer_ids selection.
- QC7 needs the feature store. `scripts/build_training_features.py` runs it on every built
  version before registering anything.

Independence: a check never trusts the transform it checks. QC1 and QC3 recompute the
population from customers.csv, QC3 takes the zero-activity set from a direct SQL query, and
QC8 recounts the raw source rows.

Standalone use against a features DataFrame (wide: customer_id + feature columns):
    python -m features.quality_checks                      # the store's latest snapshot
    python -m features.quality_checks --features file.parquet
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
import sys

import numpy as np
import pandas as pd

from features import schema
from features.transforms import day_window, month_window

DRIFT_TOLERANCE = 1e-9
AMOUNT_TOLERANCE = 0.01
_MAX_LISTED = 20


@dataclass(frozen=True)
class CheckResult:
    check_id: str
    name: str
    passed: bool
    detail: str
    failing_features: tuple[str, ...] = field(default_factory=tuple)
    failing_rows: tuple[int, ...] = field(default_factory=tuple)

    def as_dict(self) -> dict:
        return {
            "check_id": self.check_id,
            "name": self.name,
            "passed": self.passed,
            "detail": self.detail,
            "failing_features": list(self.failing_features),
            "failing_rows": list(self.failing_rows),
        }

    def line(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        text = f"{status} {self.check_id} {self.name}: {self.detail}"
        if self.failing_features:
            text += f" | features: {', '.join(self.failing_features)}"
        if self.failing_rows:
            shown = ", ".join(str(r) for r in self.failing_rows[:_MAX_LISTED])
            more = f" (+{len(self.failing_rows) - _MAX_LISTED} more)" if len(self.failing_rows) > _MAX_LISTED else ""
            text += f" | customer_ids: {shown}{more}"
        return text


class QualityCheckError(Exception):
    def __init__(self, failures: list[CheckResult]):
        self.failures = failures
        super().__init__("quality checks failed:\n" + "\n".join(f.line() for f in failures))


def raise_on_failure(results: list[CheckResult]) -> None:
    failures = [r for r in results if not r.passed]
    if failures:
        raise QualityCheckError(failures)


def format_results(results: list[CheckResult]) -> str:
    return "\n".join(r.line() for r in results)


def _ids(values) -> tuple[int, ...]:
    return tuple(sorted(int(v) for v in pd.unique(pd.Series(list(values), dtype="int64"))))


def _population_ids(customers: pd.DataFrame, as_of: pd.Timestamp) -> pd.Series:
    return customers.loc[customers["signup_date"] < as_of, "customer_id"]


# QC1 ------------------------------------------------------------------------------------
def check_population_rows(features: pd.DataFrame, customers: pd.DataFrame, as_of: pd.Timestamp) -> CheckResult:
    """One row per population customer (recomputed from customers.csv), customer_id unique."""
    expected = set(_population_ids(customers, as_of))
    ids = features["customer_id"]
    duplicated = _ids(ids[ids.duplicated()])
    missing = _ids(expected - set(ids))
    extra = _ids(set(ids) - expected)
    passed = len(features) == len(expected) and not (duplicated or missing or extra)
    detail = (
        f"{len(features)} rows for {len(expected)} population customers (signup_date < "
        f"{as_of.date()}); duplicated={len(duplicated)}, missing={len(missing)}, extra={len(extra)}"
    )
    return CheckResult("QC1", "population row count", passed, detail, failing_rows=duplicated + missing + extra)


def check_selection_rows(selected: pd.DataFrame, requested_ids) -> CheckResult:
    """After the customer_ids selection: exactly one row per requested id, in request order."""
    requested = [int(i) for i in requested_ids]
    got = [int(i) for i in selected["customer_id"]]
    passed = got == requested
    bad = _ids(set(got) ^ set(requested)) if not passed else ()
    detail = f"{len(got)} rows selected for {len(requested)} requested ids"
    return CheckResult("QC1", "selected row count", passed, detail, failing_rows=bad)


# QC2 ------------------------------------------------------------------------------------
def check_no_nulls(features: pd.DataFrame, feature_names: list[str] | None = None) -> CheckResult:
    names = feature_names or schema.FEATURE_NAMES
    missing_cols = [n for n in names if n not in features.columns]
    present = [n for n in names if n in features.columns]
    nulls = features[present].isna()
    bad_features = tuple(missing_cols + [n for n in present if nulls[n].any()])
    bad_rows = _ids(features.loc[nulls.any(axis=1), "customer_id"])
    passed = not bad_features
    detail = (
        f"{int(nulls.to_numpy().sum())} null values across {len(present)} feature columns"
        + (f"; missing columns {missing_cols}" if missing_cols else "")
    )
    return CheckResult("QC2", "no nulls after defaults", passed, detail, bad_features, bad_rows)


# QC3 ------------------------------------------------------------------------------------
def _zero_activity_defaults() -> dict[str, float]:
    return {f.name: f.zero_activity_default for f in schema.FEATURE_SCHEMA if f.zero_activity_default is not None}


def check_zero_activity_defaults(
    features: pd.DataFrame, customers: pd.DataFrame, buyer_ids: set[int], as_of: pd.Timestamp
) -> CheckResult:
    """Every population customer with no DB row before as_of (buyer_ids comes from a direct
    SQL query, not from has_ever_purchased) holds exactly each feature's spec default, and
    every other population customer has has_ever_purchased = 1."""
    pop = set(_population_ids(customers, as_of))
    zero_ids = pop - set(buyer_ids)
    by_id = features.set_index("customer_id")
    zero = by_id.loc[by_id.index.isin(zero_ids)]
    bad_features, bad_rows = [], set()
    for name, default in _zero_activity_defaults().items():
        wrong = zero.index[zero[name].ne(default) | zero[name].isna()]
        if len(wrong):
            bad_features.append(name)
            bad_rows.update(wrong)
    buyers = by_id.loc[by_id.index.isin(pop - zero_ids), "has_ever_purchased"]
    unflagged = buyers.index[buyers.ne(1)]
    if len(unflagged):
        bad_features.append("has_ever_purchased (buyers)")
        bad_rows.update(unflagged)
    passed = not bad_features and len(zero) == len(zero_ids)
    detail = (
        f"{len(zero_ids)} zero-activity customers from the DB query, {len(zero)} found in the "
        f"output; defaults {_zero_activity_defaults()}"
    )
    return CheckResult("QC3", "zero-activity defaults", passed, detail, tuple(bad_features), _ids(bad_rows))


# QC4 ------------------------------------------------------------------------------------
def range_bounds(params: dict[str, dict]) -> dict[str, tuple[float, float]]:
    """Inclusive [low, high] per feature at the parameters in effect (feature_spec.md QC4)."""
    cap = params["avg_spend_per_active_month_6m"]["monthly_cap"]
    return {
        "tenure_days": (1, np.inf),
        "days_since_last_txn": (0, params["days_since_last_txn"]["cap_days"]),
        "has_ever_purchased": (0, 1),
        "txn_count_90d": (0, schema.TXN_COUNT_UPPER_BOUND),
        "avg_spend_per_active_month_6m": (0.0, schema.UNCAPPED_MONTHLY_SPEND_UPPER_BOUND if cap is None else cap),
        "spend_trend_3m_vs_prior_3m": (0.0, params["spend_trend_3m_vs_prior_3m"]["ratio_cap"]),
        "active_month_share": (0.0, 1.0),
    }


def check_ranges(features: pd.DataFrame, params: dict[str, dict]) -> CheckResult:
    bad_features, bad_rows, notes = [], set(), []
    for name, (low, high) in range_bounds(params).items():
        values = features[name]
        ok = values.between(low, high)
        if name == "has_ever_purchased":
            ok &= values.isin([0, 1])
        if not ok.all():
            bad_features.append(name)
            bad_rows.update(features.loc[~ok, "customer_id"])
            notes.append(f"{name} outside [{low}, {high}]: {int((~ok).sum())}")
    detail = "; ".join(notes) if notes else "all 7 features within their bounds"
    return CheckResult("QC4", "range limits", not bad_features, detail, tuple(bad_features), _ids(bad_rows))


# QC5 ------------------------------------------------------------------------------------
def check_no_future_data(
    transactions_used: pd.DataFrame, monthly_used: pd.DataFrame, as_of: pd.Timestamp
) -> CheckResult:
    """The frames handed to the transforms hold nothing at or after as_of (DB) or as_of's
    month (parquet)."""
    cutoff_month = as_of.to_period("M").to_timestamp()
    late_txn = transactions_used.loc[transactions_used["txn_timestamp"] >= as_of]
    late_month = monthly_used.loc[monthly_used["txn_month"] >= cutoff_month]
    passed = late_txn.empty and late_month.empty
    detail = (
        f"{len(late_txn)} DB rows at/after {as_of}, {len(late_month)} parquet rows in/after "
        f"{cutoff_month.strftime('%Y-%m')}"
    )
    bad = _ids(pd.concat([late_txn["customer_id"], late_month["customer_id"]]))
    failing = []
    if not late_txn.empty:
        failing += [f.name for f in schema.FEATURE_SCHEMA if f.source == "transactions.db"]
    if not late_month.empty:
        failing += [f.name for f in schema.FEATURE_SCHEMA if f.source.startswith("transactions_monthly_agg")]
    return CheckResult("QC5", "no future data", passed, detail, tuple(failing), bad)


# QC6 ------------------------------------------------------------------------------------
def coverage_months(as_of: pd.Timestamp, params: dict[str, dict], only: str | None = None) -> list[pd.Timestamp]:
    """Month starts used by any month window at these parameters (or by feature `only`)."""
    months = set()
    for w in schema.month_windows_in_effect(params, only=only):
        start, end = month_window(as_of, w.months, w.offset)
        months.update(pd.date_range(start, end, freq="MS", inclusive="left"))
    return sorted(months)


def check_parquet_coverage(
    transactions: pd.DataFrame, monthly: pd.DataFrame, as_of: pd.Timestamp, params: dict[str, dict]
) -> CheckResult:
    """For every month any month-based window uses, the parquet equals the DB's monthly
    group-by for every customer, and no customer-month exists in only one source."""
    months = coverage_months(as_of, params)
    month_set = pd.DatetimeIndex(months)
    txn_month = transactions["txn_timestamp"].dt.to_period("M").dt.to_timestamp().astype("datetime64[ns]")
    in_scope = txn_month.isin(month_set)
    db = (
        transactions.loc[in_scope]
        .assign(txn_month=txn_month[in_scope])
        .groupby(["customer_id", "txn_month"])
        .agg(txn_count=("amount", "size"), total_amount=("amount", "sum"), return_count=("is_returned", "sum"))
        .reset_index()
    )
    pq = monthly.loc[monthly["txn_month"].isin(month_set), ["customer_id", "txn_month", "txn_count", "total_amount", "return_count"]]
    duplicate_keys = pq.loc[pq.duplicated(["customer_id", "txn_month"], keep=False)]
    merged = pq.merge(db, on=["customer_id", "txn_month"], how="outer", suffixes=("_pq", "_db"), indicator=True)
    both = merged["_merge"] == "both"
    mismatch = both & (
        (merged["txn_count_pq"] != merged["txn_count_db"])
        | (merged["return_count_pq"] != merged["return_count_db"])
        | ((merged["total_amount_pq"] - merged["total_amount_db"]).abs() > AMOUNT_TOLERANCE)
    )
    only_one = ~both
    bad = merged.loc[mismatch | only_one]
    bad_months = sorted({m.strftime("%Y-%m") for m in bad["txn_month"]})
    passed = bad.empty and duplicate_keys.empty
    span = f"{months[0].strftime('%Y-%m')} to {months[-1].strftime('%Y-%m')}" if months else "no months"
    detail = (
        f"{len(months)} months checked ({span}), {int(both.sum())} customer-months in both sources; "
        f"{int(mismatch.sum())} differ, {int(only_one.sum())} in one source only, "
        f"{len(duplicate_keys)} duplicate parquet keys"
        + (f"; failing months {bad_months}" if bad_months else "")
    )
    rows = _ids(pd.concat([bad["customer_id"], duplicate_keys["customer_id"]]))
    failing_months = set(bad["txn_month"]) | set(duplicate_keys["txn_month"])
    affected = tuple(
        f.name for f in schema.FEATURE_SCHEMA
        if failing_months & set(coverage_months(as_of, params, only=f.name))
    )
    return CheckResult("QC6", "parquet coverage", passed, detail, affected, rows)


# QC7 ------------------------------------------------------------------------------------
def check_drift_against_store(
    feature_name: str, values: pd.DataFrame, reference: pd.DataFrame | None
) -> CheckResult:
    """Rebuilt values must equal the stored copy of a version with identical parameters at
    the same as_of (to within 1e-9). No stored copy: passes, reporting 'no reference'."""
    if reference is None:
        return CheckResult("QC7", f"drift vs store [{feature_name}]", True, "no reference")
    name = f"drift vs store [{feature_name}]"
    duplicated = _ids(values.loc[values["customer_id"].duplicated(), "customer_id"])
    if duplicated:
        return CheckResult("QC7", name, False, f"{len(duplicated)} duplicated customer_ids in the rebuilt values",
                           (feature_name,), duplicated)
    new = values.set_index("customer_id")
    ref = reference.set_index("customer_id")
    new_dates = set(pd.to_datetime(new["as_of_date"]).unique())
    ref_dates = set(pd.to_datetime(ref["as_of_date"]).unique())
    if new_dates != ref_dates:
        return CheckResult(
            "QC7", name, False,
            f"as_of_date differs: rebuilt {sorted(new_dates)} vs stored {sorted(ref_dates)}", (feature_name,),
        )
    missing, extra = _ids(ref.index.difference(new.index)), _ids(new.index.difference(ref.index))
    common = new.index.intersection(ref.index)
    a = new.loc[common, feature_name].astype("float64")
    b = ref.loc[common, feature_name].astype("float64")
    changed = common[~np.isclose(a, b, rtol=0.0, atol=DRIFT_TOLERANCE)]
    passed = not (missing or extra or len(changed))
    detail = (
        f"{len(changed)} of {len(common)} customers differ from the stored copy "
        f"(stored mean {b.mean():.6g}, rebuilt mean {a.mean():.6g}); missing={len(missing)}, extra={len(extra)}"
    )
    return CheckResult(
        "QC7", name, passed, detail,
        () if passed else (feature_name,), _ids(list(changed)) + missing + extra,
    )


# QC8 ------------------------------------------------------------------------------------
def _eligible_months_by_enumeration(signup_dates: pd.Series, window_months: pd.DatetimeIndex) -> pd.Series:
    """Independent of transforms.eligible_months: count window months whose start is on or
    after the signup month's start."""
    signup_month = signup_dates.dt.to_period("M").dt.to_timestamp().to_numpy()
    grid = window_months.to_numpy()[None, :] >= signup_month[:, None]
    return pd.Series(grid.sum(axis=1), index=signup_dates.index)


def check_totals_reconcile(
    features: pd.DataFrame,
    customers: pd.DataFrame,
    transactions: pd.DataFrame,
    monthly: pd.DataFrame,
    as_of: pd.Timestamp,
    params: dict[str, dict],
) -> CheckResult:
    """Totals behind txn_count_90d and active_month_share equal raw source row counts."""
    pop = customers.loc[customers["signup_date"] < as_of].set_index("customer_id")
    problems, bad_features = [], []

    start, end = day_window(as_of, schema.get_spec("txn_count_90d").fixed["window_days"])
    ts = transactions["txn_timestamp"]
    db_rows = int((transactions["customer_id"].isin(pop.index) & (ts >= start) & (ts < end)).sum())
    feature_total = int(features["txn_count_90d"].sum())
    if feature_total != db_rows:
        bad_features.append("txn_count_90d")
    problems.append(f"txn_count_90d sum {feature_total} vs {db_rows} DB rows in [{start.date()}, {end.date()})")

    lookback = params["active_month_share"]["lookback_months"]
    m_start, m_end = month_window(as_of, lookback)
    window = pd.date_range(m_start, m_end, freq="MS", inclusive="left")
    eligible = _eligible_months_by_enumeration(pop["signup_date"], window).rename("eligible")
    # merge, not reindex: a duplicated customer must be counted twice here, not crash the check
    shares = features[["customer_id", "active_month_share"]].merge(
        eligible.rename_axis("customer_id").reset_index(), on="customer_id", how="left")
    implied = shares["active_month_share"] * shares["eligible"]
    rounded = implied.round()
    pq_rows = int((monthly["customer_id"].isin(pop.index) & monthly["txn_month"].isin(window)).sum())
    non_integer = shares.loc[~((implied - rounded).abs() <= 1e-6), "customer_id"]  # NaN counts as bad
    active_total = int(rounded.sum()) if not implied.isna().any() else None
    if active_total != pq_rows or len(non_integer):
        bad_features.append("active_month_share")
    problems.append(
        f"active months implied by active_month_share {active_total} vs {pq_rows} parquet rows in "
        f"{m_start.strftime('%Y-%m')} to {(m_end - pd.DateOffset(months=1)).strftime('%Y-%m')}"
        + (f"; {len(non_integer)} non-integer share x eligible" if len(non_integer) else "")
    )
    return CheckResult("QC8", "totals reconcile", not bad_features, "; ".join(problems), tuple(bad_features), _ids(non_integer))


# Runners --------------------------------------------------------------------------------
def run_input_checks(
    transactions_used: pd.DataFrame,
    monthly_used: pd.DataFrame,
    transactions: pd.DataFrame,
    monthly: pd.DataFrame,
    as_of: pd.Timestamp,
    params: dict[str, dict],
) -> list[CheckResult]:
    return [
        check_no_future_data(transactions_used, monthly_used, as_of),
        check_parquet_coverage(transactions, monthly, as_of, params),
    ]


def run_output_checks(
    features: pd.DataFrame,
    *,
    customers: pd.DataFrame,
    transactions: pd.DataFrame,
    monthly: pd.DataFrame,
    buyer_ids: set[int],
    as_of: pd.Timestamp,
    params: dict[str, dict],
) -> list[CheckResult]:
    """QC1-QC4 and QC8 against a full-population features frame (customer_id column)."""
    return [
        check_population_rows(features, customers, as_of),
        check_no_nulls(features),
        check_zero_activity_defaults(features, customers, buyer_ids, as_of),
        check_ranges(features, params),
        check_totals_reconcile(features, customers, transactions, monthly, as_of, params),
    ]


# Standalone CLI -------------------------------------------------------------------------
def _store_latest_frame(store_dir) -> tuple[pd.DataFrame, dict[str, dict]]:
    import json

    from feature_store import store

    frame, params = None, {}
    for name in schema.FEATURE_NAMES:
        versions = store.list_versions(name, store_dir=store_dir)
        if versions.empty:
            raise SystemExit(f"feature store has no versions of {name}; run scripts/build_training_features.py")
        params[name] = json.loads(versions.loc[versions["is_latest"] == 1, "parameters"].iloc[0])
        values = store.get_latest(name, store_dir=store_dir)
        frame = values if frame is None else frame.merge(values, on=["customer_id", "as_of_date"], how="outer")
    return frame, params


def main(argv: list[str] | None = None) -> int:
    from features import build, ingest
    from features.transforms import parse_as_of

    parser = argparse.ArgumentParser(description="Run QC1-QC4, QC6 and QC8 against a features frame.")
    parser.add_argument("--features", help="Wide features parquet (customer_id, the 7 features, as_of_date). "
                        "Default: the feature store's latest version of every feature.")
    parser.add_argument("--as-of", help="Required with a --features file that has no as_of_date column.")
    parser.add_argument("--store-dir", default=None, help="Feature store root (default: feature_store/).")
    args = parser.parse_args(argv)

    if args.features:
        frame = pd.read_parquet(args.features)
        params = schema.latest_parameters()
    else:
        frame, params = _store_latest_frame(args.store_dir)
    if "as_of_date" in frame.columns:
        dates = pd.to_datetime(frame["as_of_date"]).unique()
        if len(dates) != 1:
            raise SystemExit(f"features frame mixes as_of dates: {sorted(dates)}")
        as_of = parse_as_of(pd.Timestamp(dates[0]))
        if args.as_of and parse_as_of(args.as_of) != as_of:
            raise SystemExit(f"--as-of {args.as_of} does not match the frame's as_of_date {as_of.date()}")
    elif args.as_of:
        as_of = parse_as_of(args.as_of)
    else:
        raise SystemExit("--as-of is required when the features frame has no as_of_date column")

    paths = build.resolve_source_paths(None)
    customers = ingest.load_customers(paths["customers"])
    transactions = ingest.load_transactions(paths["transactions"])
    monthly = ingest.load_monthly_agg(paths["monthly_agg"])
    buyer_ids = ingest.load_customer_ids_with_txn_before(paths["transactions"], as_of)
    results = [check_parquet_coverage(transactions, monthly, as_of, params)] + run_output_checks(
        frame, customers=customers, transactions=transactions, monthly=monthly,
        buyer_ids=buyer_ids, as_of=as_of, params=params,
    )
    print(f"as_of {as_of.date()}, {len(frame)} rows")
    print(format_results(results))
    return 0 if all(r.passed for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
