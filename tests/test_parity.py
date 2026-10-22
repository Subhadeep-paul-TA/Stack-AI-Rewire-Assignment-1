"""
The parity test that matters most for this pipeline: build features for the same
customer via the "training" path (historical as-of date, batch of many customers) and
via the "scoring" path (single customer, same as-of date), and assert the resulting
feature values are identical.

If these two paths can't be driven through the same test with the same expected output,
the parity guarantee in feature_spec.md isn't real — fix features/build.py, not this test.

Runs with the latest parameters and with every versioned feature's v1 parameters, over the
customers feature_spec.md requires.
"""

import inspect
import json
import random

import pandas as pd
import pytest

from feature_store import store
from features import schema, transforms
from features.build import build_features
from features.quality_checks import QualityCheckError
from scripts import score_customer
from tests.helpers import AS_OF, version_params

PARITY_CUSTOMERS = {
    27: "zero-activity: no transaction before as_of, 289 days of tenure",
    350: "first purchase on 2026-05-01: counts in txn_count_90d, outside every month window",
    697: "purchase at 2026-05-02 17:12, at/after as_of, must be excluded",
    1022: "signed up 2026-05-01: 0 eligible months for active_month_share",
    5: "spend_trend 'recent spend only' case",
    3: "a month above monthly_cap, so avg_spend v1 and v2 differ",
}
PARAM_SETS = {"latest": None, "v1": version_params(1)}


@pytest.fixture(scope="module")
def training(full_latest, full_v1):
    return {"latest": full_latest, "v1": full_v1}


def _row(frame: pd.DataFrame, cid: int) -> pd.DataFrame:
    return frame.loc[frame["customer_id"] == cid].reset_index(drop=True)


def test_parity_customers_really_cover_their_cases(training):
    latest = training["latest"].set_index("customer_id")
    v1 = training["v1"].set_index("customer_id")
    assert latest.loc[27, "has_ever_purchased"] == 0 and latest.loc[27, "days_since_last_txn"] == 365
    assert latest.loc[350, "txn_count_90d"] >= 1 and latest.loc[350, "active_month_share"] == 0.0
    assert latest.loc[697, "days_since_last_txn"] == 54  # not 0: the 2026-05-02 17:12 purchase is excluded
    assert latest.loc[1022, "tenure_days"] == 1 and latest.loc[1022, "active_month_share"] == 0.0
    assert latest.loc[5, "spend_trend_3m_vs_prior_3m"] == 1.0 and latest.loc[5, "txn_count_90d"] > 0
    assert v1.loc[3, "avg_spend_per_active_month_6m"] != latest.loc[3, "avg_spend_per_active_month_6m"]


@pytest.mark.parametrize("param_set", PARAM_SETS)
@pytest.mark.parametrize("cid", PARITY_CUSTOMERS)
def test_training_and_scoring_paths_agree_for_same_customer(training, param_set, cid):
    scored = build_features(AS_OF, customer_ids=[cid], feature_params=PARAM_SETS[param_set])
    pd.testing.assert_frame_equal(scored, _row(training[param_set], cid), check_exact=True)


@pytest.mark.parametrize("param_set", PARAM_SETS)
def test_scoring_every_customer_in_shuffled_order_matches_training(training, param_set):
    """Catches a scoring-only divergence that would affect any customer, not just the six above."""
    full = training[param_set]
    ids = full["customer_id"].tolist()
    random.Random(0).shuffle(ids)
    scored = build_features(AS_OF, customer_ids=ids, feature_params=PARAM_SETS[param_set])
    expected = full.set_index("customer_id").loc[ids].reset_index()
    pd.testing.assert_frame_equal(scored, expected, check_exact=True)


def test_scoring_script_matches_the_training_scripts_stored_latest_values(built_store, capsys):
    ids = list(PARITY_CUSTOMERS)
    argv = [a for cid in ids for a in ("--customer-id", str(cid))] + ["--format", "json"]
    assert score_customer.main(argv) == 0
    scored = pd.DataFrame(json.loads(capsys.readouterr().out)["customers"]).set_index("customer_id")
    for name in schema.FEATURE_NAMES:
        stored = store.get_latest(name, store_dir=built_store).set_index("customer_id")[name]
        assert scored[name].tolist() == stored.loc[ids].tolist(), name


@pytest.mark.parametrize(
    "ids, message",
    [([99999], "99999 does not exist"), ([899], "899 signed up on 2026-05-02"), ([5, 5], "more than once"),
     ([], "empty"), (["5"], "not an integer")],
)
def test_invalid_scoring_requests_raise_naming_the_id(ids, message):
    with pytest.raises(ValueError, match=message):
        build_features(AS_OF, customer_ids=ids)


def test_scoring_script_exits_non_zero_on_unknown_id(capsys):
    assert score_customer.main(["--customer-id", "99999"]) == 1
    captured = capsys.readouterr()
    assert captured.out == "" and "99999" in captured.err


def test_checks_run_on_the_full_population_even_when_scoring(monkeypatch):
    """A defect for customer 27 must stop a scoring call for customer 5: QC1-QC8 sit on the
    shared path before the selection, not behind an 'if training' branch."""
    real = transforms.days_since_last_txn

    def broken(population, transactions, as_of, *, cap_days):
        out = real(population, transactions, as_of, cap_days=cap_days)
        out.loc[27] = 0
        return out

    monkeypatch.setattr(transforms, "days_since_last_txn", broken)
    with pytest.raises(QualityCheckError, match="QC3"):
        build_features(AS_OF, customer_ids=[5])


def test_as_of_is_a_required_argument_with_no_default():
    params = inspect.signature(build_features).parameters
    assert params["as_of"].default is inspect.Parameter.empty
    with pytest.raises(ValueError):
        build_features("2026-05-02 00:00")


def test_build_never_returns_the_label(full_latest):
    assert list(full_latest.columns) == ["customer_id", *schema.FEATURE_NAMES, "as_of_date"]
    assert "churned" not in full_latest.columns
    assert (full_latest["as_of_date"] == pd.Timestamp(AS_OF)).all()


def test_scoring_script_exits_non_zero_and_prints_no_features_on_a_failed_check(capsys):
    assert score_customer.main(["--customer-id", "3", "--as-of", "2026-08-01"]) == 1
    captured = capsys.readouterr()
    assert captured.out == "" and "QC6" in captured.err


@pytest.mark.parametrize("feature_params, message", [
    ({"active_month_share": {"lookback_months": 6, "definition": 2}}, "implements definition 1"),
    ({"active_month_share": {"lookback": 6, "definition": 1}}, "exactly the keys"),
    ({"no_such_feature": {"definition": 1}}, "unknown feature"),
])
def test_feature_params_are_validated_not_silently_defaulted(feature_params, message):
    with pytest.raises((ValueError, KeyError), match=message):
        build_features(AS_OF, feature_params=feature_params)
