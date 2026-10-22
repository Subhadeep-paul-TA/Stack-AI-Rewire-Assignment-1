"""
Each feature function in features/transforms.py in isolation, on small synthetic frames:
a normal case, the zero-activity case, a single-record case, and the window boundary
cases feature_spec.md defines ([as_of - N days, as_of) on full timestamps; N complete
calendar months before as_of's month).
"""

from datetime import date, datetime, timezone
import inspect

import numpy as np
import pandas as pd
import pytest

from features import schema, transforms as T

AS_OF = T.parse_as_of("2026-05-02")


def pop(*rows) -> pd.DataFrame:
    """rows: (customer_id, signup_date)."""
    df = pd.DataFrame(rows, columns=["customer_id", "signup_date"])
    df["signup_date"] = pd.to_datetime(df["signup_date"]).astype("datetime64[ns]")
    return df.set_index("customer_id")


def txns(*rows) -> pd.DataFrame:
    """rows: (customer_id, txn_timestamp)."""
    df = pd.DataFrame(rows, columns=["customer_id", "txn_timestamp"])
    df["txn_timestamp"] = pd.to_datetime(df["txn_timestamp"], format="ISO8601").astype("datetime64[ns]")
    df["amount"] = 10.0
    df["is_returned"] = 0
    return df


def months(*rows) -> pd.DataFrame:
    """rows: (customer_id, 'YYYY-MM', total_amount)."""
    df = pd.DataFrame(rows, columns=["customer_id", "txn_month", "total_amount"])
    df["txn_month"] = pd.to_datetime(df["txn_month"]).astype("datetime64[ns]")
    df["txn_count"] = 1
    df["return_count"] = 0
    return df


POP = pop((1, "2024-01-10"), (2, "2024-01-10"), (3, "2024-01-10"))
NO_TXNS = txns()
NO_MONTHS = months()


def v(series: pd.Series, cid: int):
    return series.loc[cid]


# as_of parsing and windows ---------------------------------------------------------------
@pytest.mark.parametrize("value", ["2026-05-02", date(2026, 5, 2), datetime(2026, 5, 2), pd.Timestamp("2026-05-02")])
def test_parse_as_of_accepts_calendar_dates(value):
    assert T.parse_as_of(value) == pd.Timestamp("2026-05-02")


@pytest.mark.parametrize(
    "value",
    ["2026-05-02 10:00", "2026-05-02T00:00:00", "2026-5-2", "2026-02-30",
     pd.Timestamp("2026-05-02 00:00:01"), datetime(2026, 5, 2, tzinfo=timezone.utc)],
)
def test_parse_as_of_rejects_time_components_and_bad_dates(value):
    with pytest.raises(ValueError):
        T.parse_as_of(value)


def test_parse_as_of_rejects_non_dates():
    with pytest.raises(TypeError):
        T.parse_as_of(20260502)


def test_windows_match_the_spec_dates():
    assert T.day_window(AS_OF, 90) == (pd.Timestamp("2026-02-01"), pd.Timestamp("2026-05-02"))
    assert T.month_window(AS_OF, 6) == (pd.Timestamp("2025-11-01"), pd.Timestamp("2026-05-01"))
    assert T.month_window(AS_OF, 3, offset=3) == (pd.Timestamp("2025-11-01"), pd.Timestamp("2026-02-01"))
    assert T.month_window(AS_OF, 12) == (pd.Timestamp("2025-05-01"), pd.Timestamp("2026-05-01"))
    # as_of on the 1st: its own month is still excluded
    assert T.month_window(T.parse_as_of("2026-05-01"), 1) == (pd.Timestamp("2026-04-01"), pd.Timestamp("2026-05-01"))


def test_population_is_signup_strictly_before_as_of():
    customers = pd.DataFrame({
        "customer_id": [1, 2, 3],
        "signup_date": pd.to_datetime(["2026-05-01", "2026-05-02", "2026-06-01"]).astype("datetime64[ns]"),
    })
    assert T.population(customers, AS_OF).index.tolist() == [1]


def test_no_transform_defaults_as_of_or_its_parameters():
    for name in schema.FEATURE_NAMES:
        params = inspect.signature(getattr(T, name)).parameters
        assert params["as_of"].default is inspect.Parameter.empty, name
        assert all(p.default is inspect.Parameter.empty for p in params.values()), name


def test_each_transform_reads_only_its_spec_source():
    db_features = {"days_since_last_txn", "has_ever_purchased", "txn_count_90d"}
    parquet_features = {"avg_spend_per_active_month_6m", "spend_trend_3m_vs_prior_3m", "active_month_share"}
    for name in schema.FEATURE_NAMES:
        args = set(inspect.signature(getattr(T, name)).parameters)
        assert ("transactions" in args) == (name in db_features), name
        assert ("monthly" in args) == (name in parquet_features), name


# F1 tenure_days --------------------------------------------------------------------------
def test_tenure_days_normal_and_minimum():
    out = T.tenure_days(pop((1, "2025-05-02"), (2, "2026-05-01")), AS_OF)
    assert out.tolist() == [365, 1]
    assert out.dtype == "int64"


# F2 days_since_last_txn ------------------------------------------------------------------
def test_days_since_last_txn_normal_floors_whole_days():
    t = txns((1, "2026-04-01 09:00"), (1, "2026-04-30 23:59"), (2, "2026-04-01 00:00"))
    out = T.days_since_last_txn(POP, t, AS_OF, cap_days=365)
    assert v(out, 1) == 1  # 1 day 1 minute -> 1
    assert v(out, 2) == 31


def test_days_since_last_txn_zero_activity_gets_365():
    out = T.days_since_last_txn(POP, NO_TXNS, AS_OF, cap_days=365)
    assert out.tolist() == [365, 365, 365] and out.dtype == "int64"


def test_days_since_last_txn_single_record():
    out = T.days_since_last_txn(POP, txns((3, "2026-05-01 12:00")), AS_OF, cap_days=365)
    assert v(out, 3) == 0 and v(out, 1) == 365


def test_days_since_last_txn_excludes_rows_at_or_after_as_of():
    # customer 697's case: a purchase on as_of's own day must not count
    t = txns((1, "2026-03-15 08:00"), (1, "2026-05-02 17:12"), (2, "2026-05-02 00:00:00"),
             (3, "2026-05-01 23:59:59.999999999"))
    out = T.days_since_last_txn(POP, t, AS_OF, cap_days=365)
    assert v(out, 1) == 47
    assert v(out, 2) == 365
    assert v(out, 3) == 0


def test_days_since_last_txn_caps_long_gaps():
    out = T.days_since_last_txn(POP, txns((1, "2024-12-01")), AS_OF, cap_days=365)
    assert v(out, 1) == 365


# F3 has_ever_purchased -------------------------------------------------------------------
def test_has_ever_purchased_normal_zero_and_single():
    t = txns((1, "2025-01-01"), (1, "2026-01-01"), (2, "2026-04-30"))
    out = T.has_ever_purchased(POP, t, AS_OF)
    assert out.tolist() == [1, 1, 0] and out.dtype == "int64"


def test_has_ever_purchased_boundary_purchase_at_as_of_does_not_count():
    out = T.has_ever_purchased(POP, txns((1, "2026-05-02 00:00:00"), (2, "2026-05-01 23:59")), AS_OF)
    assert v(out, 1) == 0 and v(out, 2) == 1


# F4 txn_count_90d ------------------------------------------------------------------------
def test_txn_count_90d_normal_counts_returned_purchases():
    t = txns((1, "2026-03-01"), (1, "2026-04-01"), (1, "2026-04-20"))
    t.loc[1, "is_returned"] = 1
    out = T.txn_count_90d(POP, t, AS_OF, window_days=90)
    assert v(out, 1) == 3


def test_txn_count_90d_zero_activity_and_single():
    out = T.txn_count_90d(POP, txns((2, "2026-04-15")), AS_OF, window_days=90)
    assert out.tolist() == [0, 1, 0] and out.dtype == "int64"


def test_txn_count_90d_window_boundaries():
    t = txns(
        (1, "2026-02-01 00:00:00"),            # exactly as_of - 90 days: included
        (1, "2026-01-31 23:59:59.999999999"),  # just before the window: excluded
        (2, "2026-05-01 23:59:59"),            # last moment before as_of: included
        (2, "2026-05-02 00:00:00"),            # at as_of: excluded
    )
    out = T.txn_count_90d(POP, t, AS_OF, window_days=90)
    assert v(out, 1) == 1 and v(out, 2) == 1


# F5 avg_spend_per_active_month_6m --------------------------------------------------------
def test_avg_spend_normal_averages_over_active_months_only():
    m = months((1, "2025-12", 100.0), (1, "2026-03", 50.0))
    out = T.avg_spend_per_active_month_6m(POP, m, AS_OF, window_months=6, monthly_cap=None)
    assert v(out, 1) == pytest.approx(75.0)


def test_avg_spend_cap_applies_per_month():
    m = months((1, "2025-12", 300.0), (1, "2026-03", 100.0))
    v1 = T.avg_spend_per_active_month_6m(POP, m, AS_OF, window_months=6, monthly_cap=None)
    v2 = T.avg_spend_per_active_month_6m(POP, m, AS_OF, window_months=6, monthly_cap=197.10)
    assert v(v1, 1) == pytest.approx(200.0)
    assert v(v2, 1) == pytest.approx((197.10 + 100.0) / 2)


def test_avg_spend_zero_activity_and_bought_only_before_window():
    m = months((2, "2025-06", 80.0))
    out = T.avg_spend_per_active_month_6m(POP, m, AS_OF, window_months=6, monthly_cap=197.10)
    assert out.tolist() == [0.0, 0.0, 0.0] and out.dtype == "float64"


def test_avg_spend_single_record():
    out = T.avg_spend_per_active_month_6m(POP, months((3, "2026-04", 42.5)), AS_OF, window_months=6, monthly_cap=None)
    assert v(out, 3) == pytest.approx(42.5)


def test_avg_spend_month_boundaries():
    m = months((1, "2025-10", 999.0), (1, "2025-11", 10.0), (1, "2026-05", 999.0))
    out = T.avg_spend_per_active_month_6m(POP, m, AS_OF, window_months=6, monthly_cap=None)
    assert v(out, 1) == pytest.approx(10.0)  # 2025-10 is before the window, 2026-05 is as_of's month


# F6 spend_trend_3m_vs_prior_3m -----------------------------------------------------------
def test_spend_trend_four_cases():
    p4 = pop((1, "2024-01-10"), (2, "2024-01-10"), (3, "2024-01-10"), (4, "2024-01-10"))
    m = months(
        (1, "2025-12", 100.0), (1, "2026-03", 50.0),  # both: 0.5
        (2, "2025-11", 80.0),                         # prior only: stopped
        (3, "2026-04", 30.0),                         # recent only
    )
    out = T.spend_trend_3m_vs_prior_3m(p4, m, AS_OF, window_months=3, ratio_cap=3)
    assert out.tolist() == [0.5, 0.0, 1.0, 1.0]  # customer 4: zero activity
    assert out.dtype == "float64"


def test_spend_trend_ratio_cap():
    m = months((1, "2026-01", 10.0), (1, "2026-02", 200.0))
    assert v(T.spend_trend_3m_vs_prior_3m(POP, m, AS_OF, window_months=3, ratio_cap=10), 1) == 10.0
    assert v(T.spend_trend_3m_vs_prior_3m(POP, m, AS_OF, window_months=3, ratio_cap=3), 1) == 3.0


def test_spend_trend_single_record_each_side():
    m = months((1, "2026-02", 25.0), (2, "2026-01", 25.0))
    out = T.spend_trend_3m_vs_prior_3m(POP, m, AS_OF, window_months=3, ratio_cap=3)
    assert v(out, 1) == 1.0 and v(out, 2) == 0.0


def test_spend_trend_month_boundaries():
    # recent = 2026-02..04, prior = 2025-11..2026-01; 2025-10 and 2026-05 are ignored
    m = months((1, "2025-10", 500.0), (1, "2025-11", 40.0), (1, "2026-02", 20.0), (1, "2026-05", 500.0))
    out = T.spend_trend_3m_vs_prior_3m(POP, m, AS_OF, window_months=3, ratio_cap=3)
    assert v(out, 1) == pytest.approx(0.5)


# F7 active_month_share -------------------------------------------------------------------
def test_eligible_months_starts_at_signup_month():
    signup = pd.to_datetime(pd.Series(["2020-01-01", "2026-02-15", "2026-04-30", "2026-05-01"])).astype("datetime64[ns]")
    assert T.eligible_months(signup, AS_OF, 6).tolist() == [6, 3, 1, 0]


def test_active_month_share_normal_and_recent_signup():
    p = pop((1, "2020-01-01"), (2, "2026-02-15"))
    m = months((1, "2025-12", 5.0), (1, "2026-04", 5.0), (2, "2026-02", 5.0), (2, "2026-03", 5.0), (2, "2026-04", 5.0))
    out = T.active_month_share(p, m, AS_OF, lookback_months=6)
    assert v(out, 1) == pytest.approx(2 / 6)
    assert v(out, 2) == pytest.approx(1.0)  # 3 of its 3 eligible months


def test_active_month_share_zero_activity_and_zero_eligible():
    p = pop((1, "2020-01-01"), (2, "2026-05-01"))  # 2 is customer 1022's case
    out = T.active_month_share(p, NO_MONTHS, AS_OF, lookback_months=12)
    assert out.tolist() == [0.0, 0.0] and out.dtype == "float64"
    assert not out.isna().any()


def test_active_month_share_single_record_and_lookback_boundary():
    p = pop((1, "2020-01-01"))
    m = months((1, "2025-05", 5.0))  # first month of the 12-month window, outside the 6-month one
    assert v(T.active_month_share(p, m, AS_OF, lookback_months=12), 1) == pytest.approx(1 / 12)
    assert v(T.active_month_share(p, m, AS_OF, lookback_months=6), 1) == 0.0


# Shared contract -------------------------------------------------------------------------
def test_every_transform_returns_the_population_index_with_schema_dtype_and_no_nulls():
    t = txns((1, "2026-04-01"))
    m = months((1, "2026-04", 10.0))
    for spec in schema.FEATURE_SCHEMA:
        fn = getattr(T, spec.name)
        args = inspect.signature(fn).parameters
        inputs = {"population": POP, "as_of": AS_OF}
        if "transactions" in args:
            inputs["transactions"] = t
        if "monthly" in args:
            inputs["monthly"] = m
        out = fn(**inputs, **spec.transform_kwargs(spec.latest.parameters))
        assert out.index.equals(POP.index), spec.name
        assert str(out.dtype) == spec.dtype, spec.name
        assert out.name == spec.name
        assert not out.isna().any(), spec.name
        assert np.isfinite(out.to_numpy(dtype="float64")).all(), spec.name


def test_avg_spend_is_gross_of_returns():
    """feature_spec.md F5: the parquet has no returned amounts, so return_count is ignored."""
    m = months((1, "2026-03", 60.0))
    m["return_count"] = 1
    out = T.avg_spend_per_active_month_6m(POP, m, AS_OF, window_months=6, monthly_cap=None)
    assert v(out, 1) == pytest.approx(60.0)
