"""
Feed each check in features/quality_checks.py a deliberately bad input (an injected
null, an out-of-range amount, a shifted distribution) and confirm it fires; feed it
clean input and confirm it doesn't false-positive. Bad inputs are realistic mutations of
the real 2026-05-02 build, so each check is shown catching the failure mode feature_spec.md
names for it.
"""

import pandas as pd
import pytest

from features import ingest, schema, transforms
from features import quality_checks as qc
from features.build import DEFAULT_SOURCE_PATHS, build_features
from tests.helpers import AS_OF, version_params

AS_OF_TS = transforms.parse_as_of(AS_OF)
LATEST = schema.latest_parameters()
V1 = schema.resolve_feature_params(version_params(1))


@pytest.fixture(scope="module")
def sources():
    transactions = ingest.load_transactions(DEFAULT_SOURCE_PATHS["transactions"])
    monthly = ingest.load_monthly_agg(DEFAULT_SOURCE_PATHS["monthly_agg"])
    return {
        "customers": ingest.load_customers(DEFAULT_SOURCE_PATHS["customers"]),
        "transactions": transactions,
        "monthly": monthly,
        "buyer_ids": ingest.load_customer_ids_with_txn_before(DEFAULT_SOURCE_PATHS["transactions"], AS_OF_TS),
        "transactions_used": transactions.loc[transactions["txn_timestamp"] < AS_OF_TS],
        "monthly_used": monthly.loc[monthly["txn_month"] < transforms.month_start(AS_OF_TS)],
    }


@pytest.fixture
def clean(full_latest):
    return full_latest.drop(columns="as_of_date").copy()


def output_checks(features, sources, params=LATEST):
    return {r.check_id: r for r in qc.run_output_checks(
        features, customers=sources["customers"], transactions=sources["transactions"],
        monthly=sources["monthly"], buyer_ids=sources["buyer_ids"], as_of=AS_OF_TS, params=params)}


def set_value(df, cid, column, value):
    df.loc[df["customer_id"] == cid, column] = value
    return df


# Clean input: no false positives ---------------------------------------------------------
def test_checks_do_not_false_positive_on_clean_input(clean, sources, full_v1):
    for params, frame in ((LATEST, clean), (V1, full_v1.drop(columns="as_of_date"))):
        results = list(output_checks(frame, sources, params).values()) + qc.run_input_checks(
            sources["transactions_used"], sources["monthly_used"], sources["transactions"],
            sources["monthly"], AS_OF_TS, params)
        assert all(r.passed for r in results), qc.format_results(results)
        qc.raise_on_failure(results)  # does not raise


def test_spec_expected_counts_at_2026_05_02(full_latest, sources):
    """feature_spec.md: 1,399 population customers, 232 of them zero-activity."""
    assert len(full_latest) == 1399
    population = sources["customers"].loc[sources["customers"]["signup_date"] < AS_OF_TS, "customer_id"]
    assert len(set(population) - sources["buyer_ids"]) == 232
    assert int((full_latest["has_ever_purchased"] == 0).sum()) == 232


# QC1 -------------------------------------------------------------------------------------
def test_qc1_fires_when_a_zero_activity_customer_is_dropped(clean, sources):
    result = output_checks(clean[clean["customer_id"] != 27], sources)["QC1"]
    assert not result.passed and 27 in result.failing_rows


def test_qc1_fires_on_a_duplicated_customer(clean, sources):
    result = output_checks(pd.concat([clean, clean[clean["customer_id"] == 3]]), sources)["QC1"]
    assert not result.passed and 3 in result.failing_rows


def test_qc1_selection_fires_when_rows_do_not_match_the_request(clean):
    assert qc.check_selection_rows(clean[clean["customer_id"].isin([3, 5])], [3, 5]).passed
    assert not qc.check_selection_rows(clean[clean["customer_id"].isin([3])], [3, 5]).passed


# QC2 -------------------------------------------------------------------------------------
def test_qc2_null_check_fires_on_injected_null(clean, sources):
    result = output_checks(set_value(clean, 27, "avg_spend_per_active_month_6m", float("nan")), sources)["QC2"]
    assert not result.passed
    assert result.failing_features == ("avg_spend_per_active_month_6m",) and result.failing_rows == (27,)


# QC3 -------------------------------------------------------------------------------------
@pytest.mark.parametrize("column, wrong", [
    ("days_since_last_txn", 0),            # 0 instead of 365
    ("spend_trend_3m_vs_prior_3m", 0.0),   # "stopped" instead of the 1.0 default
    ("has_ever_purchased", 1),             # F3 disagrees with the DB query
    ("active_month_share", 0.5),
])
def test_qc3_fires_on_a_wrong_zero_activity_default(clean, sources, column, wrong):
    result = output_checks(set_value(clean, 27, column, wrong), sources)["QC3"]
    assert not result.passed and column in result.failing_features and result.failing_rows == (27,)


def test_qc3_fires_when_a_buyer_is_flagged_as_never_purchased(clean, sources):
    result = output_checks(set_value(clean, 5, "has_ever_purchased", 0), sources)["QC3"]
    assert not result.passed and 5 in result.failing_rows


# QC4 -------------------------------------------------------------------------------------
@pytest.mark.parametrize("column, bad", [
    ("tenure_days", 0), ("days_since_last_txn", 366), ("has_ever_purchased", 2),
    ("txn_count_90d", 101), ("spend_trend_3m_vs_prior_3m", 3.5), ("active_month_share", 1.2),
    ("avg_spend_per_active_month_6m", -1.0),
])
def test_qc4_outlier_check_fires_on_out_of_range_value(clean, sources, column, bad):
    result = output_checks(set_value(clean, 3, column, bad), sources)["QC4"]
    assert not result.passed and result.failing_features == (column,) and result.failing_rows == (3,)


def test_qc4_monetary_bound_follows_the_version_cap(clean, sources):
    above_cap = set_value(clean, 3, "avg_spend_per_active_month_6m", 501.94)
    assert not output_checks(above_cap, sources, LATEST)["QC4"].passed  # v2: cap 197.10 not applied
    assert qc.check_ranges(above_cap, V1).passed                         # v1: uncapped, bound 2,000
    assert not qc.check_ranges(set_value(clean, 3, "avg_spend_per_active_month_6m", 2500.0), V1).passed


# QC5 -------------------------------------------------------------------------------------
def test_qc5_fires_when_a_row_at_or_after_as_of_reaches_the_transforms(sources):
    leaky = sources["transactions"].loc[sources["transactions"]["txn_timestamp"] < AS_OF_TS + pd.Timedelta(days=1)]
    result = qc.check_no_future_data(leaky, sources["monthly_used"], AS_OF_TS)
    assert not result.passed and 697 in result.failing_rows
    assert "days_since_last_txn" in result.failing_features


def test_qc5_fires_when_as_of_month_reaches_the_monthly_transforms(sources):
    leaky = sources["monthly"].loc[sources["monthly"]["txn_month"] <= AS_OF_TS]  # includes 2026-05
    result = qc.check_no_future_data(sources["transactions_used"], leaky, AS_OF_TS)
    assert not result.passed and "avg_spend_per_active_month_6m" in result.failing_features


# QC6 -------------------------------------------------------------------------------------
def test_qc6_fires_on_the_real_late_july_etl_gap(sources):
    as_of = transforms.parse_as_of("2026-08-01")
    result = qc.check_parquet_coverage(sources["transactions"], sources["monthly"], as_of, LATEST)
    assert not result.passed
    assert "110 differ, 102 in one source only" in result.detail and "2026-07" in result.detail
    assert set(result.failing_features) == {
        "avg_spend_per_active_month_6m", "spend_trend_3m_vs_prior_3m", "active_month_share"}


def test_build_stops_on_the_etl_gap_for_training_and_scoring_alike():
    with pytest.raises(qc.QualityCheckError, match="QC6"):
        build_features("2026-08-01")
    with pytest.raises(qc.QualityCheckError, match="QC6"):
        build_features("2026-08-01", customer_ids=[3])


def _shifted_monthly(sources, change):
    monthly = sources["monthly"].copy()
    row = monthly.index[(monthly["customer_id"] == 3) & (monthly["txn_month"] == "2026-02-01")][0]
    return change(monthly, row)


@pytest.mark.parametrize("delta, fires", [(0.02, True), (0.005, False)])
def test_qc6_amount_tolerance_is_one_cent(sources, delta, fires):
    monthly = _shifted_monthly(sources, lambda m, r: m.assign(total_amount=m["total_amount"].where(m.index != r, m.at[r, "total_amount"] + delta)))
    result = qc.check_parquet_coverage(sources["transactions"], monthly, AS_OF_TS, LATEST)
    assert result.passed is not fires


def test_qc6_fires_on_a_missing_or_duplicated_parquet_row(sources):
    missing = _shifted_monthly(sources, lambda m, r: m.drop(index=r))
    duplicated = _shifted_monthly(sources, lambda m, r: pd.concat([m, m.loc[[r]]]))
    for monthly in (missing, duplicated):
        result = qc.check_parquet_coverage(sources["transactions"], monthly, AS_OF_TS, LATEST)
        assert not result.passed and 3 in result.failing_rows


def test_qc6_scope_follows_the_parameters_in_effect(sources):
    """v1's 12-month lookback widens the months QC6 must cover."""
    assert len(qc.coverage_months(AS_OF_TS, LATEST)) == 6
    assert len(qc.coverage_months(AS_OF_TS, V1)) == 12


# QC7 -------------------------------------------------------------------------------------
def _snapshot(full, name="avg_spend_per_active_month_6m"):
    return full[["customer_id", name, "as_of_date"]].reset_index(drop=True)


def test_qc7_reports_no_reference_and_passes_on_identical_values(full_latest):
    snap = _snapshot(full_latest)
    assert qc.check_drift_against_store("avg_spend_per_active_month_6m", snap, None).detail == "no reference"
    assert qc.check_drift_against_store("avg_spend_per_active_month_6m", snap, snap.copy()).passed


def test_qc7_drift_check_fires_on_shifted_distribution(full_latest):
    ref = _snapshot(full_latest)
    shifted = ref.assign(avg_spend_per_active_month_6m=ref["avg_spend_per_active_month_6m"] * 1.05)
    result = qc.check_drift_against_store("avg_spend_per_active_month_6m", shifted, ref)
    assert not result.passed and result.failing_features == ("avg_spend_per_active_month_6m",)


@pytest.mark.parametrize("delta, fires", [(1e-6, True), (1e-12, False)])
def test_qc7_tolerance_is_1e_9(full_latest, delta, fires):
    ref = _snapshot(full_latest)
    new = set_value(ref.copy(), 3, "avg_spend_per_active_month_6m",
                    ref.loc[ref["customer_id"] == 3, "avg_spend_per_active_month_6m"].iloc[0] + delta)
    result = qc.check_drift_against_store("avg_spend_per_active_month_6m", new, ref)
    assert result.passed is not fires
    if fires:
        assert result.failing_rows == (3,)


def test_qc7_fires_on_missing_customers_or_a_different_as_of(full_latest):
    ref = _snapshot(full_latest)
    assert not qc.check_drift_against_store("avg_spend_per_active_month_6m", ref[ref["customer_id"] != 3], ref).passed
    moved = ref.assign(as_of_date=pd.Timestamp("2026-04-01"))
    assert "as_of_date differs" in qc.check_drift_against_store("avg_spend_per_active_month_6m", moved, ref).detail


# QC8 -------------------------------------------------------------------------------------
def test_qc8_fires_on_a_double_counted_transaction(clean, sources):
    doubled = set_value(clean, 3, "txn_count_90d", int(clean.loc[clean["customer_id"] == 3, "txn_count_90d"].iloc[0]) * 2)
    result = output_checks(doubled, sources)["QC8"]
    assert output_checks(doubled, sources)["QC4"].passed  # QC4's bound of 100 cannot see it
    assert not result.passed and result.failing_features == ("txn_count_90d",)


def test_qc8_fires_on_a_double_counted_month(clean, sources):
    # customer 3 has 3 active of 6 eligible months; counting one twice gives 4/6
    doubled = set_value(clean, 3, "active_month_share", 4 / 6)
    result = output_checks(doubled, sources)["QC8"]
    assert output_checks(doubled, sources)["QC4"].passed
    assert not result.passed and result.failing_features == ("active_month_share",)


# Wiring: a failing check raises and stops the build --------------------------------------
def test_raise_on_failure_lists_every_failure(clean, sources):
    bad = set_value(set_value(clean, 27, "days_since_last_txn", 0), 3, "active_month_share", 1.5)
    with pytest.raises(qc.QualityCheckError) as err:
        qc.raise_on_failure(list(output_checks(bad, sources).values()))
    assert {f.check_id for f in err.value.failures} == {"QC3", "QC4", "QC8"}


def test_build_raises_when_a_transform_leaves_zero_activity_customers_null(monkeypatch):
    """The easy off-by-one: computing on buyers only and forgetting the default fill."""

    def no_fill(population, transactions, as_of, *, cap_days):
        last = transactions.groupby("customer_id")["txn_timestamp"].max()
        return ((as_of - last) // pd.Timedelta(days=1)).reindex(population.index).rename("days_since_last_txn")

    monkeypatch.setattr(transforms, "days_since_last_txn", no_fill)
    with pytest.raises(qc.QualityCheckError) as err:
        build_features(AS_OF)
    assert {"QC2", "QC3"} <= {f.check_id for f in err.value.failures}


def test_build_attaches_every_check_result(full_latest):
    ids = [r["check_id"] for r in full_latest.attrs["quality_checks"]]
    assert ids == ["QC5", "QC6", "QC1", "QC2", "QC3", "QC4", "QC8", "QC1"]
    assert all(r["passed"] for r in full_latest.attrs["quality_checks"])


# Standalone runner -----------------------------------------------------------------------
def test_standalone_runner_passes_on_the_built_store(built_store, capsys):
    assert qc.main(["--store-dir", str(built_store)]) == 0
    assert "FAIL" not in capsys.readouterr().out


def test_standalone_runner_fails_on_a_bad_features_file(full_latest, tmp_path, capsys):
    path = tmp_path / "features.parquet"
    set_value(full_latest.copy(), 27, "spend_trend_3m_vs_prior_3m", 0.0).to_parquet(path)
    assert qc.main(["--features", str(path)]) == 1
    assert "FAIL QC3" in capsys.readouterr().out


def test_spec_expected_feature_figures_at_2026_05_02(full_latest):
    """Figures feature_spec.md quotes for its features at 2026-05-02 (coverage-of-intent check)."""
    f = full_latest
    buyers = f["has_ever_purchased"] == 1
    assert f.loc[buyers, "days_since_last_txn"].max() == 289            # F2: only never-buyers hold 365
    assert int((f["txn_count_90d"] > 0).sum()) == 1049                  # F4
    assert int((f["tenure_days"] < 182).sum()) == 302                   # F1: under six months
    assert int((f["avg_spend_per_active_month_6m"] == 0.0).sum()) == 250  # F5: 232 never + 18 stopped earlier
    assert int((f["spend_trend_3m_vs_prior_3m"] == 0.0).sum()) == 108   # F6: spending stopped
    assert int((f["spend_trend_3m_vs_prior_3m"] == 1.0).sum()) == 312 + 250  # F6: recent-only + neither
    assert f["avg_spend_per_active_month_6m"].max() == pytest.approx(197.10)
