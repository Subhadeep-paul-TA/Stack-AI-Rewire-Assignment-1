# PARITY_CHECK.md — train/score parity verification

Claude ran both checks below during the build session on 2026-10-22, in the
`churn-pipeline` conda env (Python 3.11, pandas 3.0.6). The results are real, not predicted.

**Result: parity holds.** `tests/test_parity.py` (29 tests) passes on the real code. It also
caught each of three deliberately injected training/scoring splits, so no strengthening was
needed.

## What is under test

- **One code path.** Both scripts call
  `features.build.build_features(as_of, source_paths=None, customer_ids=None, feature_params=None)`.
- **One allowed difference.** Features are always computed, and QC1–QC6 and QC8 always run,
  for the full population at `as_of`. The only difference between the two calls is one line
  at the end, which picks the rows to return:
  `requested = population.index.tolist() if customer_ids is None else _requested_ids(...)`.
- **No batch statistics.** Every cap (365, 197.10, 10, 3) is a constant in `features/schema.py`,
  so nothing depends on which or how many customers are in the call.

What `tests/test_parity.py` checks, at `as_of = 2026-05-02`, once with the latest
parameters and once with every versioned feature's v1 parameters:

| Test | What it proves |
|---|---|
| `test_training_and_scoring_paths_agree_for_same_customer` (6 customers × 2 parameter sets) | A single-customer scoring call equals that customer's row of the full training build, exactly (`check_exact=True`) |
| `test_parity_customers_really_cover_their_cases` | The six customers really are the spec's cases, so the parity test can't pass on the wrong ones |
| `test_scoring_every_customer_in_shuffled_order_matches_training` (× 2) | Scoring all 1,399 ids in shuffled order equals training: a backstop for a split that affects customers outside the six |
| `test_scoring_script_matches_the_training_scripts_stored_latest_values` | `scripts/score_customer.py` output (JSON) equals the values `scripts/build_training_features.py` stored in a tmp feature store. This tests the two real entry points end to end |
| `test_checks_run_on_the_full_population_even_when_scoring` | A defect for customer 27 stops a scoring call for customer 5, so the checks aren't behind an "if training" branch |
| `test_as_of_is_a_required_argument_with_no_default`, `test_invalid_scoring_requests_raise_naming_the_id`, `test_feature_params_are_validated_not_silently_defaulted`, two script exit-code tests | as_of threading, and the spec's ValueError and exit rules |

The six customers feature_spec.md requires:

| Customer | Case |
|---|---|
| 27 | Zero-activity: no transaction before as_of, 289 days of tenure |
| 350 | First purchase on 2026-05-01: counts in `txn_count_90d`, outside every month window |
| 697 | Purchase at 2026-05-02 17:12, at or after as_of, so it must be excluded |
| 1022 | Signed up 2026-05-01: 0 eligible months for `active_month_share` |
| 5 | `spend_trend_3m_vs_prior_3m` "recent spend only" case |
| 3 | A month above `monthly_cap`, so `avg_spend_per_active_month_6m` v1 and v2 differ |

## Check 1 — parity mutation check

**Method.** Each mutation went into a scratch copy of the repo, outside the working tree:
`features/`, `scripts/`, `tests/`, `data/`, `pytest.ini` and `feature_store/store.py`. The
mutation added a scoring-only branch to the copy's `features/build.py`, and
`tests/test_parity.py` then ran in that copy. Each mutation is inserted **after** the quality
checks and the selection, so QC1–QC8 can't see it and only the parity tests can catch it.
The real `features/build.py` was never edited: its SHA-256 matched before and after. An
unmutated control copy passed 29/29.

| Mutation (scoring path only) | Realistic bug it models | Parity tests failed | Caught? |
|---|---|---|---|
| **M1**: `days_since_last_txn` rounded instead of floored | A second implementation of recency that "looks the same" | 9 of 29: customers 350, 5 and 3 (× 2 parameter sets), both shuffled-all tests, the script test | Yes |
| **M2**: `txn_count_90d` counted on the raw DB frame through the end of as_of's day (`ts.dt.normalize() <= as_of`) | The scoring path bypasses the cutoff and leaks customer 697's 2026-05-02 17:12 purchase | 5 of 29: customer 697 (× 2), both shuffled-all tests, the script test | Yes |
| **M3**: `monthly_cap` recomputed as the 95th percentile of the scored batch's own months | The batch-statistic bug the spec warns about: a percentile over 1,399 customers in training vs one customer in scoring | 7 of 29: customers 5 and 3 (× 2), both shuffled-all tests, the script test | Yes |

What the failures show:

- **Each mutation was caught by more than one independent test.** The single-customer tests,
  the all-customers test and the end-to-end script test all fired.
- **M2 depends on the spec's choice of customer 697.** Among the six named customers, 697 is
  the only one with a purchase on as_of's own day, so it is the only one that sees M2. The
  shuffled all-customers test catches it as well, and is the backstop if that list ever
  changes.
- **The customers a mutation missed were missed for a real reason, not by luck:**
  - M1 missed 27 and 1022, who have never bought, so both paths give the default 365. It
    also missed 697, whose last gap is 54.x days with a fraction below .5, so rounding and
    flooring agree.
  - M3 missed 27, 1022 and 350, who have no month in the window (0.0 on both paths). It also
    missed 697, who has a single month, so its own 95th percentile is its own value.

  This is why the suite has the shuffled all-customers test, and not just a fixed list.

Each mutation was inserted directly below
`selected = features.set_index("customer_id").loc[requested].reset_index()` in `build_features`,
so it can be reproduced:

```python
# M1
if customer_ids is not None:
    _last = transactions_used.groupby("customer_id")["txn_timestamp"].max()
    _days = ((as_of_ts - _last).dt.total_seconds() / 86400).round().clip(upper=365)
    selected["days_since_last_txn"] = _days.reindex(selected["customer_id"]).fillna(365).astype("int64").to_numpy()

# M2
if customer_ids is not None:
    _ts = transactions["txn_timestamp"]
    _rows = transactions.loc[(_ts >= as_of_ts - pd.Timedelta(days=90)) & (_ts.dt.normalize() <= as_of_ts)]
    selected["txn_count_90d"] = _rows.groupby("customer_id").size().reindex(selected["customer_id"], fill_value=0).to_numpy()

# M3
if customer_ids is not None:
    _start, _end = transforms.month_window(as_of_ts, 6)
    _m = monthly_used.loc[(monthly_used["txn_month"] >= _start) & monthly_used["customer_id"].isin(selected["customer_id"])]
    _cap = _m["total_amount"].quantile(0.95)
    _avg = _m["total_amount"].clip(upper=_cap).groupby(_m["customer_id"]).mean()
    selected["avg_spend_per_active_month_6m"] = _avg.reindex(selected["customer_id"], fill_value=0.0).to_numpy()
```

## Check 2 — coverage of intent

I diffed the spec's candidate features, data-quality checks and stated rules against what
the suite exercises.

| Spec item | Covered by |
|---|---|
| F1–F7: normal, zero-activity, single-record and window-boundary cases | `test_transforms.py` (41 tests; F1 has no single-record case because it doesn't read transactions) |
| Day windows `[as_of − N, as_of)`, month windows, whole-day floor, as_of with a time component rejected | `test_transforms.py` (window, boundary and `parse_as_of` tests) |
| Each feature reads only its spec source (DB for F2–F4, parquet for F5–F7) | `test_each_transform_reads_only_its_spec_source` |
| QC1–QC8 each fire on bad input and stay quiet on clean input | `test_quality_checks.py` (40 tests, all on mutations of the real 2026-05-02 build) |
| A failing check raises; training writes nothing, scoring prints nothing | `test_build_raises_when_…`, `test_training_script_writes_nothing_when_a_build_check_fails`, `test_qc7_stops_a_silent_definition_change_…`, `test_scoring_script_exits_non_zero_…` |
| The spec's expected counts (1,399 and 232) "a test asserts" | `test_spec_expected_counts_at_2026_05_02` |
| Versioning floor; every version retrievable; `is_latest` = highest; distinct, existing `parquet_path`s | `test_feature_store.py` (spec versions, plus a 4-version case so nothing assumes v1/v2) |
| Never overwrite; one as_of per store; "unchanged — skipped" rerun; cosmetic bump refused | `test_feature_store.py` (append-only and training-script tests) |
| VERSION_COMPARISON expected figures (25/25/140; 162 tied, 458 unchanged; 54/46, 168/176) | `test_version_comparison_matches_the_spec_expectations` |
| Lineage: a separate entry per stored version, with concrete window dates | `test_lineage_has_a_separate_entry_for_every_stored_version` |
| Tests never touch the real store | Session guard in `tests/conftest.py`: it hashes `feature_store/registry.db` and `values/` before and after the run |

The check found these gaps; each now has a test:

1. F5 is "gross of returns": `test_avg_spend_is_gross_of_returns`.
2. `score_customer` exits non-zero and prints no features when a **quality check** fails (only
   the unknown-id case had a test): `test_scoring_script_exits_non_zero_and_prints_no_features_on_a_failed_check`.
3. `feature_params` with a `definition` other than 1, wrong keys, or an unknown feature must
   raise rather than silently fall back to a default: `test_feature_params_are_validated_not_silently_defaulted`.
4. `is_latest` is flipped in the same transaction as the insert, so a write that fails part-way
   must leave the old latest and no orphan file: `test_a_failed_write_rolls_back_and_removes_its_file`.
5. The figures the spec quotes for its features (buyer recency maximum 289, 1,049 customers
   with a 90-day purchase, 302 under six months, 250 zeros in F5, 108 zeros and 562 ones in
   F6): `test_spec_expected_feature_figures_at_2026_05_02`.

Final suite: **156 tests, all passing**. The full output is in `outputs/artifacts/pytest_output.txt`.
