"""
One pure function per candidate feature in `feature_spec.md` (F1-F7), plus the date
conventions they share.

Every feature function:
- takes already-loaded frames, the population, `as_of` (a parsed pd.Timestamp) and its own
  parameters as explicit arguments — nothing reads the clock, a hard-coded date or a
  default parameter;
- filters its own window relative to `as_of`, so it is correct even on unfiltered input;
- returns a Series indexed by the population's customer_id, with the spec's zero-activity
  default filled explicitly by reindexing onto the population (never a bare NaN);
- reads only the source feature_spec.md assigns it (DB for F2-F4, parquet for F5-F7).

Conventions (feature_spec.md, "Scope and conventions"):
- day windows are [as_of - N days, as_of) on full timestamps;
- month windows are N complete calendar months before as_of's month;
- recency is whole days, rounded down.
"""

from datetime import date, datetime
import re

import pandas as pd

_DATE_ONLY = re.compile(r"\d{4}-\d{2}-\d{2}")


def parse_as_of(value) -> pd.Timestamp:
    """Parse the pipeline's single cutoff parameter.

    Accepts a 'YYYY-MM-DD' string, a datetime.date, or a naive datetime/Timestamp at exactly
    midnight. Anything with a time of day (or a timezone) is rejected: as_of is a calendar
    date read as 00:00:00 naive local time.
    """
    if isinstance(value, str):
        if not _DATE_ONLY.fullmatch(value):
            raise ValueError(f"as_of must be a calendar date 'YYYY-MM-DD' with no time component, got {value!r}")
        ts = pd.Timestamp(value)
    elif isinstance(value, datetime):
        ts = pd.Timestamp(value)
        if ts.tzinfo is not None:
            raise ValueError(f"as_of must be naive local time, got timezone-aware {value!r}")
        if ts != ts.normalize():
            raise ValueError(f"as_of must not have a time component, got {value!r}")
    elif isinstance(value, date):
        ts = pd.Timestamp(value)
    else:
        raise TypeError(f"as_of must be a date string, date or Timestamp, got {type(value).__name__}")
    return ts.as_unit("ns")


def month_start(ts: pd.Timestamp) -> pd.Timestamp:
    return ts.to_period("M").to_timestamp().as_unit("ns")


def day_window(as_of: pd.Timestamp, days: int) -> tuple[pd.Timestamp, pd.Timestamp]:
    """[as_of - days, as_of) as (inclusive start, exclusive end)."""
    return as_of - pd.Timedelta(days=days), as_of


def month_window(as_of: pd.Timestamp, months: int, offset: int = 0) -> tuple[pd.Timestamp, pd.Timestamp]:
    """`months` complete calendar months ending `offset` months before as_of's month, as
    (first month start, exclusive end month start)."""
    current = month_start(as_of)
    end = current - pd.DateOffset(months=offset)
    start = current - pd.DateOffset(months=offset + months)
    return start, end


def population(customers: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
    """Customers with signup_date < as_of, indexed by customer_id (sorted), with signup_date."""
    pop = customers.loc[customers["signup_date"] < as_of, ["customer_id", "signup_date"]]
    return pop.set_index("customer_id").sort_index()


def _on_population(values: pd.Series, population: pd.DataFrame, default, dtype: str, name: str) -> pd.Series:
    """Reindex per-customer values onto the population, filling customers absent from the
    source with the feature's explicit default."""
    out = values.reindex(population.index, fill_value=default)
    return out.astype(dtype).rename(name)


def _before(transactions: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
    return transactions.loc[transactions["txn_timestamp"] < as_of]


def _months_in_window(monthly: pd.DataFrame, as_of: pd.Timestamp, months: int, offset: int = 0) -> pd.DataFrame:
    start, end = month_window(as_of, months, offset)
    month = monthly["txn_month"]
    return monthly.loc[(month >= start) & (month < end)]


def _month_index(ts):
    """Months since year 0, for whole-month arithmetic on a Timestamp or datetime Series."""
    if isinstance(ts, pd.Series):
        return ts.dt.year * 12 + ts.dt.month - 1
    return ts.year * 12 + ts.month - 1


# F1 -------------------------------------------------------------------------------------
def tenure_days(population: pd.DataFrame, as_of: pd.Timestamp) -> pd.Series:
    days = (as_of - population["signup_date"]) // pd.Timedelta(days=1)
    return days.astype("int64").rename("tenure_days")


# F2 -------------------------------------------------------------------------------------
def days_since_last_txn(
    population: pd.DataFrame, transactions: pd.DataFrame, as_of: pd.Timestamp, *, cap_days: int
) -> pd.Series:
    last = _before(transactions, as_of).groupby("customer_id")["txn_timestamp"].max()
    days = ((as_of - last) // pd.Timedelta(days=1)).clip(upper=cap_days)
    return _on_population(days, population, default=cap_days, dtype="int64", name="days_since_last_txn")


# F3 -------------------------------------------------------------------------------------
def has_ever_purchased(population: pd.DataFrame, transactions: pd.DataFrame, as_of: pd.Timestamp) -> pd.Series:
    buyers = _before(transactions, as_of)["customer_id"].unique()
    flag = pd.Series(1, index=pd.Index(buyers, name="customer_id"))
    return _on_population(flag, population, default=0, dtype="int64", name="has_ever_purchased")


# F4 -------------------------------------------------------------------------------------
def txn_count_90d(
    population: pd.DataFrame, transactions: pd.DataFrame, as_of: pd.Timestamp, *, window_days: int
) -> pd.Series:
    start, end = day_window(as_of, window_days)
    ts = transactions["txn_timestamp"]
    counts = transactions.loc[(ts >= start) & (ts < end)].groupby("customer_id").size()
    return _on_population(counts, population, default=0, dtype="int64", name="txn_count_90d")


# F5 -------------------------------------------------------------------------------------
def avg_spend_per_active_month_6m(
    population: pd.DataFrame,
    monthly: pd.DataFrame,
    as_of: pd.Timestamp,
    *,
    window_months: int,
    monthly_cap: float | None,
) -> pd.Series:
    rows = _months_in_window(monthly, as_of, window_months)
    amounts = rows["total_amount"] if monthly_cap is None else rows["total_amount"].clip(upper=monthly_cap)
    avg = amounts.groupby(rows["customer_id"]).mean()
    return _on_population(avg, population, default=0.0, dtype="float64", name="avg_spend_per_active_month_6m")


# F6 -------------------------------------------------------------------------------------
SPEND_STOPPED = 0.0  # prior > 0, recent = 0
NO_PRIOR_SPEND = 1.0  # recent > 0, prior = 0: no earlier spend to fall from
NO_SPEND_EITHER = 1.0  # both 0: the zero-activity default, no trend to measure


def spend_trend_3m_vs_prior_3m(
    population: pd.DataFrame,
    monthly: pd.DataFrame,
    as_of: pd.Timestamp,
    *,
    window_months: int,
    ratio_cap: float,
) -> pd.Series:
    def spend(offset: int) -> pd.Series:
        rows = _months_in_window(monthly, as_of, window_months, offset)
        return rows.groupby("customer_id")["total_amount"].sum().reindex(population.index, fill_value=0.0)

    recent, prior = spend(0), spend(window_months)
    value = pd.Series(NO_SPEND_EITHER, index=population.index, dtype="float64")
    both = (recent > 0) & (prior > 0)
    value.loc[both] = (recent[both] / prior[both]).clip(upper=ratio_cap)
    value.loc[(prior > 0) & (recent == 0)] = SPEND_STOPPED
    value.loc[(recent > 0) & (prior == 0)] = NO_PRIOR_SPEND
    return value.rename("spend_trend_3m_vs_prior_3m")


# F7 -------------------------------------------------------------------------------------
def eligible_months(signup_dates: pd.Series, as_of: pd.Timestamp, lookback_months: int) -> pd.Series:
    """Number of the lookback window's months on or after each customer's signup month."""
    start, end = month_window(as_of, lookback_months)
    first_eligible = _month_index(signup_dates).clip(lower=_month_index(start))
    return (_month_index(end) - first_eligible).clip(lower=0, upper=lookback_months).astype("int64")


def active_month_share(
    population: pd.DataFrame, monthly: pd.DataFrame, as_of: pd.Timestamp, *, lookback_months: int
) -> pd.Series:
    rows = _months_in_window(monthly, as_of, lookback_months)
    active = rows.groupby("customer_id").size().reindex(population.index, fill_value=0)
    eligible = eligible_months(population["signup_date"], as_of, lookback_months)
    share = (active / eligible).where(eligible > 0, 0.0)
    return share.astype("float64").rename("active_month_share")
