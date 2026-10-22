# review_log.md — Step 3 self-review

Claude reviewed the full build diff on 2026-10-22 as a first-time reviewer would: ingestion,
transforms, build, schema, feature store, quality checks, scripts, lineage and version
comparison generators, and tests. The review focused on three questions:

- (a) Could training and scoring silently diverge?
- (b) Does any feature's null or default handling differ from the spec?
- (c) Could the store silently overwrite a version?

Every finding is either fixed or left, with the reason.

| # | Area | Finding | Status |
|---|---|---|---|
| 1 | (a) `features/build.py` | Searched for any branch on `customer_ids` besides the row selection. There is exactly one, on the `requested = …` line, after every check has run. The parity mutation check (`PARITY_CHECK.md`) shows a second branch would be caught. | No issue |
| 2 | (a) scoring vs. store | `score_customer.py` uses `schema.py`'s latest parameters, while models train on the store's snapshot. If the schema gains a version and training isn't rerun, the two disagree, and nothing detects it. | **Left.** The spec defines scoring as a `build_features` call that doesn't read the store. Making scoring depend on the store is a design change for the learner to decide. Noted in ARTIFACT_NOTES §4. |
| 3 | (a) batch statistics | Checked that no parameter is derived from the batch: every cap is a literal in `schema.py`, and `quantile` appears only in the comparison script, which reads stored values. | No issue |
| 4 | (b) defaults | Compared each transform's fill value with the spec table. F2's default is tied to `cap_days` (the default *is* the cap). QC3 checks against `schema.zero_activity_default` (365), which doesn't depend on parameters, so changing the cap without the default would fail QC3. | No issue |
| 5 | (b) F7 eligible = 0 | `active / eligible` is 0/0 for customer 1022, and `.where(eligible > 0, 0.0)` replaces it. If a customer with 0 eligible months ever had activity, the mask would hide it. QC8 would catch that, because the implied active months would no longer match the parquet rows. | No issue |
| 6 | (b) float bounds | Could F5's mean over capped months land 1 ulp above 197.10 and make QC4 fire falsely? Checked numerically: the mean of 1 to 6 capped months (the window holds at most 6) is exactly 197.1. | No issue, no tolerance added |
| 7 | (b) QC robustness | QC7 and QC8 crashed on duplicated `customer_id`s instead of failing. | **Fixed** (ARTIFACT_NOTES §1 #1) |
| 8 | (c) store writes | Reviewed the append-only path: `INSERT` (never `REPLACE`), primary key `(feature_name, version)`, parquet opened with `xb` (exclusive create), triggers blocking UPDATE, DELETE and backwards `is_latest` moves, a partial unique index allowing one latest row per feature, and `BEGIN IMMEDIATE` so concurrent writers can't take the same version number. Tests try each forbidden statement directly in SQL. | No issue |
| 9 | (c) failed writes | A failure after the file is created rolls back the transaction and deletes that file, which is only ever the one this call just created. Tested with a write that fails partway. | No issue |
| 10 | (c) hard crash | A process killed between the parquet write and `COMMIT` leaves an unregistered file, and the next registration of that version stops with `StoreError`. | **Left.** It fails safe (never overwrites). An automatic cleanup could delete a file another process is writing. The error message names the file. |
| 11 | Training script | "Nothing was registered by this run" could be false after a part-way registration failure, and the lineage wasn't regenerated in that case. | **Fixed**, with a recovery test (ARTIFACT_NOTES §1 #3) |
| 12 | Store path layout | `parquet_path` is stored relative to the store root's parent (`feature_store/values/…`, as the skill specifies). Renaming the store directory breaks resolution. | **Left.** It fails loudly: `_resolve` refuses any path outside `<store>/values/`, which also blocks traversal through a tampered registry. |
| 13 | `register_version` cost | It reads every stored snapshot's `as_of_date` to enforce "one as_of per store", O(versions) per registration. | **Left.** There are 10 snapshots of 1,399 rows. Worth caching if the store grows. |
| 14 | Definition revisions | Only `definition = 1` is implemented. A definition-2 version would need the old code kept for QC7 to rebuild v1. | **Left**, rejected with a clear error (ARTIFACT_NOTES §4). Building the mechanism for a revision nobody has specified would be speculative. |
| 15 | `_requested_ids` | It accepts an integral float such as `5.0` as id 5, and rejects bools, non-integers, duplicates, unknown ids, post-as_of signups and empty lists. | **Left.** `5.0` is unambiguous, and argparse already passes ints. |
| 16 | Maintainability | `quality_checks.py` imported `transforms` lazily for no reason. | **Fixed** |
| 17 | Test isolation | The tests could have touched the real `feature_store/`. | **Guarded.** A session fixture hashes `registry.db` and `values/` before and after the run and fails if anything changed. Every store test uses `tmp_path`. |
| 18 | Warnings | Ran the suite with `-W error`: no pandas or numpy deprecation warnings. | No issue |

All findings are resolved: 3 fixed, 1 guard added and 6 left with the reasons above. Final suite: 151 passed (`outputs/artifacts/pytest_output.txt`).
