# ARTIFACT_NOTES.md — self-critique log for the build

Written by Claude, which generated the build stage on 2026-10-22: `features/`,
`feature_store/store.py`, `scripts/`, `tests/` and the reports in `outputs/artifacts/`.
It implements the learner's confirmed `feature_spec.md`. No input was edited: not
`feature_spec.md`, `data_dictionary.md`, `requirements.txt`, `data/`, nor the learner's notes.

## 1. What I got wrong or reconsidered, and how each was resolved

| # | Where | What was wrong or reconsidered | Resolution |
|---|---|---|---|
| 1 | `features/quality_checks.py` QC7, QC8 | Both reindexed on `customer_id`. Given a frame with a duplicated customer, which is exactly what QC1 exists to catch, they crashed with a pandas "duplicate labels" error instead of reporting a failure. I found this while writing the QC1 duplicate-row test. | QC8 now merges, so a duplicate is counted twice and fails the reconciliation. QC7 checks for duplicates first. Every check now returns a result instead of crashing. |
| 2 | QC5, QC6 `failing_features` | QC5 reported source names and QC6 reported month strings ("2026-07") in the field meant for feature names. | Both now name the features fed by the failing input. At 2026-08-01, QC6 names F5, F6 and F7. |
| 3 | `scripts/build_training_features.py` | Any error printed "Nothing was registered by this run", including one during registration, where earlier versions had already committed (each registration is its own transaction). The message would have been false. | Planning errors and registration errors are now separate. A registration failure lists what was committed and regenerates the lineage, and a rerun skips those versions. Test: `test_a_registration_failure_part_way_reports_what_was_committed`. |
| 4 | `scripts/compare_feature_versions.py` | The first interpretation text had hard-coded claims: "the median stays at …", "both versions rank the at-risk customers identically", and an unmeasured reason why customers enter or leave F7's bottom decile. | Each claim was made conditional on the computed values. **Corrected after the learner's review (#9):** I called the F7 reason "measured" on the strength of "54 of the 54 entering customers fell and 46 of the 46 leaving rose". That is true by construction, because both versions' bottom-decile cutoff is 1/6: entering means going from above 1/6 to at most 1/6, so the share must fall, and leaving is the reverse. The figure measured nothing and could not support the reason I gave. |
| 5 | `features/schema.py` | I first copied the spec's expected effects ("157 customers change …") into the schema as strings, a second copy that could drift from the real numbers. | Removed. The effects are computed in `VERSION_COMPARISON.md` and asserted by tests. |
| 6 | Registry `parameters` | I first planned to store fixed windows (90 days, 6 and 3 months) in `parameters`. The spec says `parameters` "holds only tunable values and the definition revision". | Windows live in `FeatureSpec.fixed` and appear in the lineage under "Fixed constants". `parameters` holds the caps, the versioned value and `definition`. |
| 7 | `tests/test_feature_store.py` QC7 test | The first injected "definition change" (a 91-day window for F4) was caught by **QC8**, not QC7, so the test didn't exercise QC7. That was good news for QC8 but a wrong test. | The test now uses an off-by-one in buyers' recency, which no build-time check can see. QC7 alone catches it, for that feature only. |
| 8 | `features/quality_checks.py` | It imported `features.transforms` lazily inside functions, without needing to (there is no circular import). | Moved to a top-level import. |
| 9 | `scripts/compare_feature_versions.py`, `VERSION_COMPARISON.md` | **Pushback from the learner's review.** Every figure matched the spec, but three interpretation claims did not hold. (1) F7's "54 of 54 / 46 of 46" is true by construction (see #4). (2) The F7 text explained the 222 customers who fell but not the 668 who rose: 75% of the 890 changes, and the reason the mean goes up. (3) F5's "a 'top spenders' segment moves only at its edge" was never measured, and a leaver can come from deep inside the decile. Holding the rest of the text to the same test, I found three more weak spots. F6's "because the long right tail is cut" and F5's "lose most of their leverage" were unmeasured. F6's "all values below 1 unchanged" was presented as a finding, although a cap guarantees it. My own first draft of the fix also overstated the F7 recount: 350 of the 496 risers it explains are explained by construction. | Every sentence now reads off a computed number that could have come out otherwise, and anything definitional is labelled "by construction". The measurements: (1) the replacement for the tautology is recency from other stored features: 26 of the 54 entering customers (48%) have no purchase in the last 90 days, against 10% of all buyers. All 46 leavers made their first purchase inside the 12-month window, against 73% of buyers. (2) The 668 risers are split by whether they had 12 eligible months in v1 and whether their first purchase falls inside v1's window, and the shares are recomputed with eligible months counted from the first purchase. 350 risers first bought inside the 6-month window, so they rise by construction. Of the other 318, 146 stop rising under the recount and 172 still rise, so 496 of 668 (74%) rise only because of idle months between signup and first purchase. The 316 buyers who bought before the 12-month window fell more often than they rose (146 against 106). Measured from first purchase, the mean would change by −0.020 instead of +0.054. (3) Each mover's v1 rank is reported: the 25 entering customers ranked 144th–180th, just below the 140 cutoff, but 6 of the 25 leavers ranked in the upper half of v1's decile, one as high as 5th. So the claim was false and now says the opposite. F5 and F6 weight claims are now squared-deviation shares: F5's 157 changed customers hold 52.4% in v1 and 30.8% in v2, and F6's 162 hold 83.9% and 52.7%. Tests pin these figures and show the conditional wording flips with the data, including the learner's 500/10/10 example. `FEATURE_LINEAGE.md`, `registry.db` and `values/` are byte-identical. |

## 2. The three re-checks the skill asks for

- **Zero-activity default.** At 2026-05-02, QC3 gets the 232 zero-activity customers from a
  direct SQL query on the raw DB text, not from `has_ever_purchased`. All 232 hold exactly
  365 / 0 / 0 / 0.0 / 1.0 / 0.0. `test_build_raises_when_a_transform_leaves_zero_activity_customers_null`
  injects the classic slip (compute on buyers, forget the fill) and the build stops with QC2 and QC3.
- **as_of threading.**
  - `as_of` is a required argument of `build_features` and is parsed once.
  - No transform has a default for any argument (`test_no_transform_defaults_as_of_or_its_parameters`).
  - A grep finds no clock read or date literal in `features/` outside lineage prose. The only
    `now()` is the registry's `created_at`.
  - Both scripts default `--as-of` to 2026-05-02. Customer 697's 2026-05-02 17:12 purchase is excluded.
- **`get_latest` returns the highest version.**
  - `get_latest` resolves through `latest_version`, which cross-checks the `is_latest` row
    against `MAX(version)` and raises if they disagree.
  - A database trigger stops `is_latest` from being set on an existing row, so it can only
    move to a newly inserted version.
  - In the real store, all three versioned features resolve to v2, and a 4-version test case
    resolves to v4.

## 3. Notes on the inputs (not edited; noted here as the task asks)

1. **Skill scaffold vs. spec, `--as-of` default.** The scaffold's scripts defaulted to
   2026-08-01, the label date. Both scripts default to the spec's 2026-05-02, as instructed.
   Building at 2026-08-01 fails QC6 anyway, because of the late-July ETL gap.
2. **Skill vs. spec, empty `parameters`.** The skill says `parameters` is `{}` when there is
   nothing to tune. The spec says every version carries `"definition": 1`, so no row has `{}`.
   I followed the spec.
3. **Report location.** The skill puts the reports at the repo root. Per the submission rule,
   they are in `outputs/artifacts/`.
4. **feature_spec.md quotes two "median gap" figures.** F2 says 26 days, F4 and F7 say 25.
   Both are correct, but they are different statistics: 26.0 is the median of each repeat
   buyer's own median gap (1,068 buyers), and 25.0 is the pooled median of all gaps. Unlabelled,
   they read as a contradiction.
5. **QC1's after-selection half.** The spec puts QC1–QC6 and QC8 "before the final
   customer_ids selection", but QC1 also requires that "after selection, the row count equals
   the number of requested ids". That part necessarily runs after the selection, as a separate
   "QC1 selected row count" result.
6. **QC3 is slightly stricter than the spec's wording, on purpose.** Besides the zero-activity
   defaults, it requires `has_ever_purchased = 1` for every population customer the SQL query
   shows as a buyer, so F3 is checked in both directions.
7. **Data defects.** I found none beyond the documented ETL gap (data_dictionary.md DQ2), which
   QC6 enforces. Every figure I re-derived from the raw sources matches the spec and the
   dictionary: Step 0 checks, all version-comparison numbers, the F6 case split
   729/108/312/250, F5's 82/75 split, the tenure correlations 0.71/0.51/0.45, the churn rates
   14.7% vs 10.4%, and the 99 single-purchase buyers.
8. **F7 v2's stated reason explains only part of its effect.** The spec justifies v2's 6-month
   lookback as reflecting "the current habit" instead of crediting "a habit that ended months
   ago". The measurement in #9 shows something else drives v2's change. Most of it comes from
   the limitation the spec already accepts for F7: eligible months start at signup, and the
   shorter window drops idle months before the first purchase. Measured from first purchase,
   the mean would fall slightly (−0.020) instead of rising (+0.054). The spec's quoted effect
   (890 change, mean 0.333 → 0.387) is still correct. Whether v2 should still be the latest
   version is the learner's decision; the build implements the spec as confirmed.

## 4. Known limitations left in place

The reasoning behind each is in `review_log.md`.

- **Only definition 1 is implemented.** The training script rebuilds every schema version for
  QC7. So when a computation changes (definition 2), the old definition's code must be kept,
  or its versions marked as not rebuildable. Today `build_features` rejects `definition ≠ 1`
  with a clear error rather than computing the wrong thing.
- **Scoring uses `schema.py`'s latest parameters, not the store's.** Suppose a v3 is added to
  the schema but `build_training_features.py` isn't rerun. Scoring would then serve v3 while
  the store's latest training snapshot is v2. The pipeline doesn't detect this. It is a
  deployment-process risk, not a code-path split.
- **Orphan files block registration.** A hard crash (e.g. `kill -9`) between the parquet write
  and the registry commit leaves an unregistered file. The next registration of that version
  refuses to overwrite it (`StoreError`), so someone has to delete it by hand. This fails safe:
  nothing is ever overwritten.
