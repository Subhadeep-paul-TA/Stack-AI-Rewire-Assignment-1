# Learner verification log

This log is the evidence for the churn feature pipeline assignment, with one section per checkpoint in `PLAN.md`.
The rubric cells point to these sections.


| § | Checkpoint | PLAN.md step | Rubric row | Status |
|---|---|---|---|---|
| 1 | Environment setup | 1 | — (supports the README) | Done |
| 2 | Stage 0 spot-check | 2 | Stage 0 review; Engagement (a) | Done (first run by Claude; re-run and confirmed by the learner) |
| 3 | Spec reconciliation | 3 | Engagement (a) | Done (review by a separate Claude session; decisions by the learner) |
| 4 | Build output spot-checks | 6 | Engagement (c) | Done (run by the learner; commands suggested and results written up by a separate Claude session). |
| 5 | `ARTIFACT_NOTES.md` pushback | 6 | Engagement (b) | Done (pushback drafted by a separate Claude session at the learner's request; the build session agreed and fixed all three points). The learner kept F7 v2 as the latest version. |
| 6 | Versioning verification | 7 | Feature Store & Versioning | Figures verified (run by the learner; commands suggested and results written up by a separate Claude session). Interpretations read by that Claude session and accepted by the learner; 3 points fixed by the build session (§5). |
| 7 | Tests and parity mutation check | 8 | Testing; Engagement (c) | Done (run by the learner: 156 passed; the parity tests caught the learner's own mutation, 7 of 29 failing; commands suggested and results written up by a separate Claude session) |
| 8 | `review_log.md` decisions | 9 | Engagement (d) | Done (Claude suggested a decision for each item; the learner accepted all seven). Schema check and lineage reading pending. |

"Engagement" is the rubric row "AI-Native Skill Engagement", items (a)–(d).

---

## 1. Environment setup (2026-10-22)

**Setup (Learner).** The learner created the conda env `churn-pipeline` and installed `requirements.txt` with pip.
They saved the output of `conda list` to `outputs/artifacts/environment.txt`. That file shows Python 3.11.17 from
conda-forge and pandas, numpy, pyarrow and pytest from PyPI. The learner also ran `pip check`.

**Verification (Claude).** Claude ran these checks from its own shell through `conda run -n churn-pipeline`, from the
repo root. All checks were read-only: SQLite was opened with `mode=ro`, and pytest ran with cache and bytecode
writing turned off. `git status` was unchanged afterwards.

| # | Check | Result | Status |
|---|---|---|---|
| V1 | The env exists | `churn-pipeline`, at `/home/subhadeep/miniconda3/envs/churn-pipeline` | Pass |
| V2 | A new VS Code terminal uses the env | `which python` printed `/home/subhadeep/miniconda3/envs/churn-pipeline/bin/python` (reported by the learner) | Pass |
| V3 | Python version | 3.11.17 | Pass |
| V4 | `python -m pip check` | `No broken requirements found.` | Pass |
| — | Package versions | pandas 3.0.6, numpy 2.4.6, pyarrow 26.0.0, pytest 9.1.1. All meet `requirements.txt`. | Pass |
| V5 | `data/customers.csv` loads | 1,500 rows | Pass |
| V6 | `data/transactions.db` loads (read-only) | 11,742 rows | Pass |
| V7 | `data/transactions_monthly_agg.parquet` loads | 8,086 rows | Pass |
| V8 | `python -m pytest -q` | `12 skipped`: the scaffold's placeholder tests | Pass |
| V9 | Claude's shell can reach the env | `/home/subhadeep/miniconda3/envs/churn-pipeline/bin/python` | Pass |

The V5–V7 row counts match `data_dictionary.md` §2.

**Notes**
- Claude's shell starts in conda base, because `~/.bashrc` auto-activates it (`auto_activate_base: True`), and
  `conda env list` there marks base as active. That is why V9 used `conda run`. It is also why the build kickoff
  in PLAN.md Step 5 tells the build session to run all Python through `conda run -n churn-pipeline`.
- Claude investigated V2 on 2026-10-22:
  - VS Code runs through WSL, with the Python extension 2026.10.0 and Python Environments 1.40.0.
  - Terminal auto-activation is at its defaults (`python-envs.terminal.autoActivationType = command`,
    `python.terminal.activateEnvironment = true`).
  - Neither the WSL machine settings nor the Windows user settings override those keys, and the repo has no
    `.vscode/` folder.
  - The likely cause is therefore that `churn-pipeline` is not yet the selected environment for this workspace.
    PLAN.md Step 1 has the fix steps.
- **V2 resolution (Learner, 2026-10-22).** At first, a new VS Code terminal showed `(base)`. The learner then
  reported that `which python` in a VS Code terminal printed
  `/home/subhadeep/miniconda3/envs/churn-pipeline/bin/python`.

---

## 2. Stage 0 spot-check (2026-10-22)

**Claims chosen**
- `data_dictionary.md` §5, "How the three sources relate", with the matching counts in §2. Task 2 of the
  Assignment Doc asks specifically for a check of how the sources relate and whether the same transaction signal
  exists at two grains. The spec's source rule and check QC6 (named DQ6 until §3) also depend on this section.
- `data_dictionary.md` §6: customer 697 and the label's day-counting rule. The spec's recency convention and one of
  the required parity-test customers depend on it.

**Decided before running.** The §5 claims are confirmed if:
- DB rows dated before 2026-07-26 00:00, grouped by customer and calendar month, reproduce exactly the parquet's
  8,086 customer-months, with equal counts, equal return counts and amounts within 0.01;
- the stated edge figures (237, 102, 110 and 10) appear.

They are refuted if any customer-month is in only one source, any count disagrees, any amount differs by more than
0.01, or the parquet's July rows match the full July totals in the DB (which would mean there is no cutoff).

**Method (first run: Claude, at the learner's request).** Python in the `churn-pipeline` env, read-only: SQLite was opened with `mode=ro`, and nothing was
written to the repo. All three sources were loaded with pandas. DB rows dated before 2026-07-26 00:00 were grouped by
customer and calendar month (row count, sum of `amount`, sum of `is_returned`) and outer-joined to the parquet on
customer and month. For the edge, each customer's July parquet row was compared with their full July DB totals. For
the label, `churned` was recomputed at 2026-08-01 with a strict 90-day rule and with a whole-day floor rule.

**Results**

| Claim (section) | Stated | Observed | Held |
|---|---|---|---|
| Row and customer counts (§2) | DB: 11,742 rows, 1,419 customers. Parquet: 8,086 rows, 1,409 customers, months 2024-02 → 2026-07. | Same | Yes |
| No orphan ids (§5) | None in either source | 0 in the DB, 0 in the parquet | Yes |
| Same signal at two grains (§5) | All 8,086 parquet rows match | 8,086 customer-months in both sources, 0 in only one. Counts and return counts: 0 mismatches. Largest amount difference: 0.0. | Yes |
| Last row in the parquet, first one excluded (§5) | 2026-07-25 23:10:04, 2026-07-26 00:01:19 | 2026-07-25 23:10:04.05, 2026-07-26 00:01:19.35 | Yes |
| DB transactions missing from the parquet (§5, DQ2) | 237, 07-26 → 07-31 | 237, 07-26 → 07-31 | Yes |
| July 2026 (§5) | 685 parquet rows vs 787 active customers; 102 absent, 110 undercounted | 685 vs 787; 102 absent, 110 undercounted, 0 overcounted | Yes |
| Customers absent from the parquet (§5) | 10, one purchase each, 07-26 → 07-30 | 10, one purchase each, 07-26 → 07-30 | Yes |
| Months before July 2026 agree exactly (§5) | Yes | 7,401 customer-months, 0 differences | Yes |
| Float noise (§5, DQ11) | 583 totals not exactly 2-decimal | 583 | Yes |
| Customer 697 (§6) | Last purchase 2026-05-02 17:12, 90.3 days before 2026-08-01, `churned = 0` | 17:12:19, 90.28 days (90 whole days), `churned = 0` | Yes |
| Strict 90-day label rule (§6) | Reproduces 1,499 of 1,500 | 1,499; the only miss is customer 697 | Yes |

**Independent re-run (Learner, 2026-10-22).** The learner re-ran the core claims in their own terminal, with the
`churn-pipeline` env active, from the repo root. They used a short read-only script that Claude provided: SQLite
opened with `mode=ro`, nothing written. The script rebuilds the monthly totals from DB rows dated before
2026-07-26, outer-joins them to the parquet, counts the rows the parquet is missing, and computes customer 697's
last purchase. Output:

```
customer-months: 8086 | in one source only: 0
count/return mismatches: 0 | max amount diff: 0.0
DB rows on/after 2026-07-26: 237
customers in DB but not in parquet: 10
customer 697 last purchase: 2026-05-02 17:12:19.254026276 | whole days before 2026-08-01: 90
```

| Claim (section) | Learner's result | Matches the first run |
|---|---|---|
| Same signal at two grains (§5) | 8,086 customer-months, 0 in only one source, 0 count or return mismatches, largest amount difference 0.0 | Yes |
| DB transactions missing from the parquet (§5, DQ2) | 237 rows on or after 2026-07-26 | Yes |
| Customers absent from the parquet (§5) | 10 | Yes |
| Customer 697 (§6) | Last purchase 2026-05-02 17:12:19, 90 whole days before 2026-08-01 | Yes |

The re-run did not repeat the July 2026 breakdown (685 / 787 / 102 / 110), the float-noise count (583) or the
label-rule recomputation. Those rest on the first run.

**Findings**
1. Every number checked holds, so §5's [High] tags are justified.
2. The amounts matched *exactly*: a pandas group-by sum reproduces all 8,086 parquet totals bit for bit, float
   noise included. §5 says reconciliation "must" use a tolerance "not … equality". That is stronger than the
   evidence. A tolerance is still a sensible guard, because a different summation order can round differently, so
   "should" fits better. The spec's QC6 keeps its 0.01 tolerance.
3. A whole-day floor rule reproduces the label for all 1,500 customers. Customer 697 is the only one that tells it
   apart from the strict rule, so §6's [Medium] tag ("inferred from a single boundary case") is still right.
4. A side check for the dictionary review: 101 customers signed up on or after 2026-05-02, and 1,399 before, which
   matches the spec's population. So "Checked and *not* found: … signups after the as-of date" (lines 159–160) holds
   only for the label date, 2026-08-01, not for the spec's `as_of`.

**Consequence.** `feature_spec.md` needs no change. The wording fixes for `data_dictionary.md` are listed under
PLAN.md Step 2.

## 3. Spec reconciliation (2026-10-22)

**Setup (Learner).** The learner opened a new Claude session, separate from the planning session and from the
build, and pasted a review prompt drafted in the planning session. The prompt gave the reviewer
`data_dictionary.md`, `outputs/artifacts/stage0_feature_proposal.md`, `feature_spec.md` and only lines 108–196 of
`skill.md` (Domain Context). It asked for:
- a verdict per standard;
- pushback on every row of the spec's corrections table;
- answers to six specific questions;
- proposed edits, with no files changed.

The reviewer did not edit any file or invoke the skill. It disclosed that it also read the scaffold code and ran
read-only recomputations. Its full reply is saved as `outputs/artifacts/spec_review_response.md`.

**Verdict per standard**

| Standard | Verdict | Main finding |
|---|---|---|
| Rationale quality | Satisfies | The F2, F3, F4 and F7 rationales were weaker than they read. "3 to 12 eligible months" should be 4 to 12. |
| Train/score parity | Satisfies | The first check defined a scoring behaviour, but checks only ran before registration. The time of day of `as_of` was unstated. |
| Versioning and lineage | Satisfies, but two business reasons needed correcting | F5's cap acts per month, not per order. F6's version change only touches the growth side. |
| Multi-source judgment | Satisfies | The stated reasons for F3 and F5–F7 weren't the ones that matter at 2026-05-02. |
| Data-quality checks | Partly satisfies | The checks only protected the training path. |

It also found that the spec's checks DQ1–DQ7 reused the data dictionary's DQ labels for different things.

**Cross-check of the review (Claude, planning session).** At the learner's request, Claude recomputed the figures
behind the review's proposed edits from the raw data, read-only. All of them reproduced except three, and none of
the three changes a conclusion:
1. E6: among the 1,149 customers active in the window, the Stage 0 figure is 0.60 (0.6046), not 0.61.
2. E15: 0.51 (0.30 among buyers) is the share over 12 months, which is v1. The latest share, v2, gives 0.45 (0.20).
3. C10: "the groups at 1.0 churn at 9.9%" holds only for the 312 customers with recent spend and no prior spend.
   The 18 who stopped earlier churn at 16.7%.

**Decisions (Learner, informed by the cross-check)**

*Agreed*
- **E5, the reason for F6's both-zero default.** The reviewer wrote: *"'Only 34 of them churn' is 14.7%. That's
  above the population's 11.1%, and above the 13.9% for the 108 customers whose spending stopped."* The spec's
  argument implied that never-buyers are low-risk, and they aren't. The default of 1.0 stays, now justified by
  meaning alone: no trend exists, 0.0 keeps one meaning, and F3 carries "never bought".
- **E1, checks on both paths.** The reviewer wrote: *"DQ6's own stated failure, 'silently undercounting at a later
  cutoff', would happen on the path most likely to use a later cutoff: scoring."* The checks now run inside
  `build_features` on every call.

*Pushed back*
- **E2's `.gitignore` suggestion.** The reviewer wrote: *"Separately, decide whether the real store gets committed.
  If it doesn't, add both paths to .gitignore."* The store is a required deliverable (Assignment Doc §8 and the
  submission tree), so it is committed, and the spec now says so. The configurable store location was accepted.

*Redirected*
- **E4, where as_of is stored.** The reviewer proposed putting as_of in each version's `parameters` JSON. But
  `skill.md` defines `parameters` as the tunable values, and every snapshot already has an `as_of_date` column. So
  the spec reads as_of from the snapshot instead.
- **Two findings without edits.** The review raised these in Part 1 §4 but proposed no edit, so they were added:
  - F3's choice of the DB now rests on the 5 customers whose first purchase was on 2026-05-01 (verified by
    Claude). The July gap doesn't matter at 2026-05-02.
  - For F5–F7, the spec now says the parquet is the designated monthly source and QC6 checks it against the DB.
    This replaces "the DB would only rebuild the same totals".
- **Number fixes.** E6 uses 0.60, and E15 gives v1 and v2 separately.

**What changed in `feature_spec.md`.** All 19 proposed edits (E1–E19) were applied, with the changes above.
Claude applied them in the planning session, at the learner's direction. The version from before the review is kept
as `outputs/artifacts/feature_spec_pre_review.md`. The changes:
- Checks now run inside `build_features` on every call, unconditionally, before the final `customer_ids`
  selection, so training and scoring get them alike. The store check (QC7) stays in the training script. (E1)
- The checks were renamed from DQ1–DQ7 to QC1–QC7, and QC8 was added: totals must reconcile with the sources. QC4 no
  longer claims to catch a month counted twice. (E9, E17)
- QC1 and QC3 derive their expected counts at run time. The figures 1,399 and 232 are now test expectations. (E3)
- The store location is configurable through a keyword-only `store_dir`. Tests use a temporary store, and the real
  store is committed. (E2)
- The as_of of each version is read from its snapshot (E4, redirected). The format of `as_of` is defined (E19). A
  computation change is recorded as a `definition` revision, which creates a new version. (E18)
- Rank correlation is defined as `a.rank().corr(b.rank())`, so no SciPy is needed. (E8)
- `VERSION_COMPARISON.md` gets one threshold metric per versioned feature, with expected values. (E11)
- Rationales and figures were corrected:
  - the reason for F6's both-zero default (E5);
  - 0.80 replaced by 0.78, with the identity behind it (E6);
  - the F5 cap's reason, and how 197.10 was derived (E7);
  - 4 to 12 eligible months (E10);
  - F4 and the P6 row (E12);
  - F3 (E13);
  - F2 (E14);
  - a new accepted limitation for F7 (E15);
  - the P2, P3, P10 and P11 rows, now with 2026-05-02 figures (E16);
  - a note on the P8 cap row about how little the version change moves the ranking (C11).
- The parity test gains customers 1022, 5 and 3. (E19)
- Lineage entries must now record how each fixed parameter was derived.
- The Authorship note now records this review.

In `data_dictionary.md` §8, the references to the spec's checks now use the QC names.

## 4. Build output spot-checks (2026-10-22)

**Build run (Learner).** PLAN.md Step 5, following `STEP5_BUILD_README.md`.
- Date: 2026-10-22. One new Claude Code session, started with `/churn-feature-pipeline` and the README's kickoff.
- Python: `/home/subhadeep/miniconda3/envs/churn-pipeline/bin/python` (3.11.17), from the header of the build's
  own final pytest run (`pytest_output.txt`).
- Commit: `8b3815a`, "Stage 2: pipeline, feature store and tests built via the churn-feature-pipeline skill",
  pushed to `origin/assignment-1-feature-pipelines`. The message has no `Co-Authored-By` line. `ARTIFACT_NOTES.md`
  and the transcript record that Claude generated the build.
- Transcript: `outputs/artifacts/Claude_churn_output.md`, exported from the build session. The export collapses
  command output, so it shows the conversation and the file edits but not results such as Step 0's checks.
- _Decisions during the run (to add): what you approved or declined, any question you answered, any restart._

**Checks before the commit (Claude, a separate advisory session).** A Claude session separate from the build ran
`STEP5_BUILD_README.md` §4's checks, read-only. All passed:
- changes only in `features/`, `feature_store/`, `scripts/`, `tests/` and `outputs/artifacts/`;
- inputs untouched, and no reports at the repo root;
- 10 parquet files;
- no `NotImplementedError`, no `pytest.skip` and no `.venv`.

It also confirmed two more things. No file changed after the build's final pytest run. A dry run of the `git add`
staged exactly the build's 34 files.

**Method (Learner, with commands from Claude).** After the commit, the learner ran checks A–G in their own terminal,
from the repo root, with `churn-pipeline` active. The same advisory Claude session suggested the commands. It ran
each snippet once with the output discarded, to confirm that it worked and left `git status` clean, so the results
below are the learner's. SQLite was opened with `mode=ro`. Claude wrote this section from the learner's pasted
output. Where it also read code to interpret a result, the row says so.

**Results**

| # | Check | Expected | Observed | Pass |
|---|---|---|---|---|
| A | Re-run `python -m scripts.build_training_features` on the filled store | Every check passes; all 10 versions skipped; nothing written to the store | QC1–QC8 pass (details below); 10 of 10 "unchanged — skipped"; `git status` clean | Pass |
| B | Parity by hand: `score_customer --as-of 2026-05-02` against the stored snapshot (`get_latest`), customers 1, 3, 5, 57, 697 and 1022 | All 42 values equal | 42 of 42 equal as printed (table below) | Pass |
| C | One value from raw data: customer 697's `txn_count_90d`, by SQL over `txn_timestamp` in [2026-02-01, 2026-05-02) | Equals the store | 1, the same as the store. Counting 2026-05-02 as well gives 2, so the as-of day is excluded. | Pass |
| D | Registry query (the Helper Guide's, plus the columns, row counts and as-of), saved to `registry_query.txt` | 10 rows for 7 features; one `is_latest` per feature, on v2 for the three versioned ones; `skill.md`'s 10 columns; 1,399 rows at 2026-05-02 per file | All as expected. The 10 columns match `skill.md`'s registry table in name and order, and every `parquet_path` opened. | Pass |
| E | The spec's numbers, counted in the store | 232 zero-activity; 1,049 with `txn_count_90d ≥ 1`; 108 in F6's "spending stopped" case | 232; 1,049; 108 | Pass |
| F | Code checks | See below | See below | Pass |
| G | Optional: `build_training_features --as-of 2026-08-01` | QC6 fails, exit 1, store unchanged | Refused before building: "the store holds snapshots at 2026-05-02; refusing to build at 2026-08-01 (one as_of per store)". "Nothing was registered by this run", exit 1, `git status feature_store/` clean. QC6 wasn't reached, because the store's one-as_of guard runs first. | Pass (blocked by a different guard) |

**A in detail**
- The script built the features twice, once per parameter set. The v1 set needs 12 months because of
  `active_month_share` v1, so its QC6 covered 2025-05 → 2026-04: 4,968 customer-months in both sources, 0 differing
  (the spec's figure). The latest set covered 2025-11 → 2026-04: 3,150, 0 differing.
- These passed in both builds:
  - QC5: no data at or after the cutoff;
  - QC1: 1,399 rows for the 1,399 customers who signed up before 2026-05-02;
  - QC2: 0 nulls;
  - QC3: 232 zero-activity customers, all with the spec's defaults;
  - QC4: every value in range;
  - QC8: `txn_count_90d` sums to 2,537, the DB's row count in [2026-02-01, 2026-05-02).
- QC7 rebuilt all 10 stored versions from the current code. In each, 0 of the 1,399 customers differ.
- All 10 versions were "unchanged — skipped". The script rewrote `FEATURE_LINEAGE.md` with identical content,
  `registry.db` kept its build timestamp, and `git status` was clean.

**B in detail.** `score_customer` and the stored snapshot printed the same values (to the 6 decimals printed):

| Feature | 1 | 3 | 5 | 57 | 697 | 1022 |
|---|---|---|---|---|---|---|
| `tenure_days` | 16 | 508 | 315 | 414 | 567 | 1 |
| `days_since_last_txn` | 365 | 79 | 20 | 0 | 54 | 365 |
| `has_ever_purchased` | 0 | 1 | 1 | 1 | 1 | 0 |
| `txn_count_90d` | 0 | 3 | 3 | 1 | 1 | 0 |
| `avg_spend_per_active_month_6m` | 0.0 | 103.796667 | 36.71 | 75.39 | 47.54 | 0.0 |
| `spend_trend_3m_vs_prior_3m` | 1.0 | 2.069472 | 1.0 | 0.0 | 1.0 | 1.0 |
| `active_month_share` | 0.0 | 0.5 | 0.333333 | 0.166667 | 0.166667 | 0.0 |

Why each customer was chosen, and what the values show:
- **1**: no purchases. All six activity features hold the spec's defaults (365, 0, 0, 0.0, 1.0, 0.0).
- **3**: a month above the 197.10 cap. `avg_spend_per_active_month_6m` is 116.94 in v1 and 103.80 in v2, and
  scoring serves v2.
- **5**: spend in the recent 3 months only, so `spend_trend_3m_vs_prior_3m` is 1.0.
- **57**: bought on 2026-05-01. The day-window features count it (`days_since_last_txn` 0, `txn_count_90d` 1). The
  month windows don't (see Findings).
- **697**: bought at 2026-05-02 17:12, after the cutoff. `days_since_last_txn` is 54 and `txn_count_90d` is 1, which
  C confirms against the raw data, so that purchase isn't counted.
- **1022**: signed up on 2026-05-01, so tenure 1, 0 eligible months and `active_month_share` 0.0.

**F in detail**

| Code check (PLAN.md Step 6) | How | Result |
|---|---|---|
| Both scripts default `--as-of` to 2026-05-02 | `grep -n 'SPEC_AS_OF =' scripts/*.py` | `build_training_features.py:47` and `score_customer.py:24`: `"2026-05-02"` |
| `build_features` has no default `as_of` | `grep -n -A4 'def build_features' features/build.py` | `as_of` is the first parameter, with no default |
| The output has no `churned` column | `grep -rn churned features/`; `build_features("2026-05-02").columns` | Only `ingest.py:19` mentions it: the list of columns read from `customers.csv`. (A `.pyc` match is the compiled `ingest.py`.) The output columns are `customer_id`, the 7 features and `as_of_date`. |
| QC1–QC8 exist and raise `QualityCheckError` | A's output; `grep -rn QualityCheckError features scripts` | All eight ran in A. `quality_checks.py:82` raises `QualityCheckError(failures)`, and both scripts catch it, print the error and return 1. |
| QC1–QC6 and QC8 run inside `build_features`, with no branch on `customer_ids`; QC7 only in the training script | `grep -n customer_ids features/build.py`; `grep -niE 'qc7\|drift' features/build.py`; A's output | The one branch on `customer_ids` is `build.py:144`, `requested = …`. Claude read `build.py` lines 127–148 to confirm that it comes after every check. The other matches are the parameter, docstrings, the id validator `_requested_ids` and QC3's buyer query. `build.py` never mentions QC7, and in A it ran in the training script after both builds. |
| Every store function takes a keyword-only `store_dir` | `inspect.signature` over `feature_store.store` | All 9 public functions: `KEYWORD_ONLY` |
| Tests use a `tmp_path` store | `grep -n tmp_path tests/test_feature_store.py` | Every store test passes `tmp_path`, or `built_store`. Claude read `tests/conftest.py`: `built_store` is made with `tmp_path_factory`, and a session fixture, `real_store_is_never_touched`, hashes the real store before and after the run. |
| No `method="spearman"` | `grep -rn spearman features feature_store scripts tests` | No matches |

**Findings**
1. Training and scoring agree on every value for all six edge-case customers. The stored snapshots reproduce
   exactly from the current code (QC7: 0 differences in all 10 versions).
2. **Customer 57 shows a limit of the month windows.** The 2026-05-01 purchase counts in the day windows but not in
   the month windows, which end at April. So `spend_trend_3m_vs_prior_3m` is 0.0 ("spending stopped") and
   `active_month_share` is 1/6, for a customer who bought the day before the cutoff. This is by design:
   `feature_spec.md` (Month windows) says the 39 purchases on 2026-05-01 count in day windows but not in month
   windows. Both paths agree, so it isn't a parity bug, and F2 and F4 still carry the recent purchase.
3. **QC7's output doesn't name the version.** Each of the three versioned features prints two QC7 lines, told apart
   only by their means (v1 first). This is cosmetic: raise it with the build session or accept it at Step 9.
4. **A preview of Step 7.** QC7's means already match the spec's version effects: `spend_trend_3m_vs_prior_3m`
   1.51 → 1.14 and `active_month_share` 0.333 → 0.387. `avg_spend_per_active_month_6m` goes from 58.29 to 54.34.
5. B compares the values as printed, to 6 decimals. Exact equality is `test_parity.py`'s job (Step 8).
6. **G shows two layers of protection against the label date.** On the training path, the store refuses any as_of
   other than the one it already holds (`build_training_features.py` lines 149–153), before any build or check
   runs. QC6, the check that would catch July's incomplete data, is the second layer. On this path it's never
   reached.

**Consequence.** No fix is needed. Finding 3 is cosmetic and goes to Step 9. The `ARTIFACT_NOTES.md` pushback is in
§5.

## 5. `ARTIFACT_NOTES.md` pushback (2026-10-22)

**What it claimed.** `ARTIFACT_NOTES.md` §1 #4 says the F7 interpretation's reason "is measured: 54 of the 54
entering customers fell and 46 of the 46 leaving rose". PLAN.md Step 6's fallback pushback didn't apply. The notes
don't claim that nothing went wrong: §1 lists 8 corrections, and §2 already re-checks the zero-activity defaults,
the as_of threading and `get_latest`.

**Pushback (drafted by Claude, advisory session, at the learner's request).** This raises points 1–3 from the
reading in §6. The "measured" reason in §1 #4 holds by construction. The interpretation leaves the 668 customers
who rise unexplained. F5's "moves only at its edge" isn't measured. The message asks the build session to:
- fix the interpretation text in `scripts/compare_feature_versions.py`, so every claim rests on a computed number
  that could have come out otherwise, and regenerate `VERSION_COMPARISON.md`;
- measure what drives the rise in F7;
- correct `ARTIFACT_NOTES.md` §1 #4 and record the pushback there;
- re-run the full test suite, saving `pytest_output.txt`, and the lineage generator.

**Answer (build session).** It agreed with all three points, and found that the F5 claim was false, not just
unmeasured. What it measured:
- **Why F7 values rise.** It split the 1,167 buyers by whether they had 12 eligible months in v1 and whether their
  first purchase fell inside v1's window.
  - 350 of the 668 risers made their first purchase inside the 6-month window, so they rise by construction.
  - It then recomputed both versions with eligible months counted from the first purchase instead of signup. 146
    of the other 318 risers stop rising.
  - So 496 of the 668 (74%) rise only because the shorter window drops idle months before the first purchase. That
    is the limitation the spec already accepts for F7.
  - Counted from the first purchase, the mean would change by −0.020 instead of +0.054.
  - Buyers whose first purchase predates the 12-month window have no idle months in either window. They fell more
    often than they rose: 146 against 106.
- **Who crosses F7's decile.** The tautology is replaced by recency from other stored features:
  - 48% of the 54 entering customers have no purchase in the last 90 days, against 10% of all buyers;
  - all 46 leavers made their first purchase inside the 12-month window, against 73% of buyers.
- **Who crosses F5's decile.** The 25 entering customers ranked 144th–180th in v1, just below the cutoff. But 6 of
  the 25 leavers came from the upper half of v1's top decile, one of them from 5th place. The report now says the
  cap changes the segment from inside.
- **The same standard, applied elsewhere.** It made three more fixes, and also caught its own first draft counting
  the 350 guaranteed risers as measured:
  - Two more unmeasured claims are replaced with squared-deviation shares: F5's "lose most of their leverage" and
    F6's "because the tail is cut".
  - F6's "values below 1 unchanged" is relabelled as a confirmation, because the cap guarantees it.

**What changed**
- **`scripts/compare_feature_versions.py`:** the new measurements and the rewritten interpretations.
- **`VERSION_COMPARISON.md`:** regenerated.
- **`features/schema.py`:** two entries added to F7's `comparison` settings.
- **`tests/test_feature_store.py`:** five new tests. They pin the figures, and toy data, including the 500/10/10
  example from §6, shows that the wording flips when the numbers do.
- **`ARTIFACT_NOTES.md`:** §1 #4 corrected, the pushback added as §1 #9, and the open decision below as §3 #8.
- **`PARITY_CHECK.md` and `review_log.md`:** the test total only, 151 → 156.

**Verification of the fix (Claude, advisory session, read-only).**
- **Scope:** the diff against `8b3815a` touches only the files listed above, plus the learner's own notes.
- **`schema.py`:** the change adds settings for the comparison report and changes no feature computation.
- **Unchanged:** `feature_store/`, the inputs and `FEATURE_LINEAGE.md` are identical to the commit.
- **Tests:** `pytest_output.txt` shows 156 passed, with 0 failed or skipped, and no file changed after that run.
- **Figures:** the report's headline figures are unchanged (157 / 162 / 890, 0.997 / 0.999 / 0.849, 25 in and
  25 out, 54 in and 46 out).
- **Not rechecked:** the new measurements were not recomputed independently.

**Decision (Learner, 2026-10-22): F7 v2 stays the latest version.** The spec's reason for F7 v2 is that it
reflects the current habit. The measurement says most of its effect comes from how eligible months are counted. The
learner accepted the reasons Claude (advisory session) gave for keeping v2:
- **The inputs are frozen.** Rewording v2's reason, or counting eligible months from the first purchase, would mean
  editing `feature_spec.md`, which has been frozen since Step 5.
- **The build doesn't support it.** Counting from the first purchase is a definition change, which the build
  doesn't support (`review_log.md` #14).
- **The spec already covers it.** The spec accepts this limitation for F7 (line 169), and its quoted figures are
  still correct.
- **It's a finding worth keeping.** That v2's stated reason explains only part of its effect is a result of the
  versioning exercise, recorded rather than hidden.

It goes into the README and the rubric as a known limitation of F7 v2.

## 6. Versioning verification (2026-10-22)

**Method (Learner, with commands from Claude).** The learner ran two checks in their own terminal, from the repo
root, with `churn-pipeline` active. The advisory Claude session suggested the commands and checked beforehand that
the snippet ran, with its output discarded. Claude wrote this section from the learner's pasted output.
- **A. The build's comparison.** `python -m scripts.compare_feature_versions` printed one line per versioned
  feature and rewrote `VERSION_COMPARISON.md`. `git status` showed no change to the report, so it reproduces exactly
  from the store. The script writes no timestamps.
- **B. An independent recompute.** A short snippet, separate from the comparison script, loaded each versioned
  feature's v1, v2 and latest through `get_version` and `get_latest`. It computed the customers changed, the rank
  correlation (`a.rank().corr(b.rank())`), and each version's mean and maximum.

**`get_version` and `get_latest`.** For all three versioned features:
- `latest_version` is 2;
- `get_latest` equals `get_version(f, 2)`;
- v1 and v2 differ, for 157, 162 and 890 customers respectively.

Both versions of each feature are stored, and each can be retrieved on its own.

**Results against the spec** (expected figures from `feature_spec.md` lines 215–217 and 237–239)

| Feature | Measure | Spec | Report (A) | Recompute (B) | Match |
|---|---|---|---|---|---|
| `avg_spend_per_active_month_6m` | Customers changed | 157 | 157 | 157 of 1,399 | Yes |
| | Rank correlation | 0.997 | 0.997 | 0.997 | Yes |
| | Maximum | 501.94 → 197.10 | 501.9 → 197.1 | 501.94 → 197.10 | Yes |
| | Top decile | 25 in, 25 out; 140 in each version | 25 in, 25 out; 140 and 140 | — | Yes |
| `spend_trend_3m_vs_prior_3m` | Customers changed | 162 | 162 | 162 of 1,399 | Yes |
| | Rank correlation | 0.999 | 0.999 | 0.999 | Yes |
| | Mean | 1.51 → 1.14 | 1.513 → 1.136 | 1.513 → 1.136 | Yes |
| | Ties at the cap; values below 1 | 162 tied at 3.0; all 458 values below 1 unchanged | 162 at 3 (32 at 10 in v1); 458 below 1, all unchanged | — | Yes |
| `active_month_share` | Customers changed | 890 | 890 | 890 of 1,399 | Yes |
| | Rank correlation | 0.849 | 0.849 | 0.849 | Yes |
| | Mean | 0.333 → 0.387 | 0.333 → 0.387 | 0.333 → 0.387 | Yes |
| | Bottom decile among buyers | 54 in, 46 out; 168 and 176 customers (ties) | 54 in, 46 out; 168 and 176 | — | Yes |

Every figure matches the spec. The report and the recompute also agree with each other. B didn't recompute the
threshold metrics, which rest on the report alone.

**Reading the interpretations (Claude, advisory session; accepted by the learner).** Claude read each section's
interpretation against the report's own numbers and raised four points. The learner accepted them and asked for
points 1–3 to go to the build session (§5).
1. **F7, "54 of the 54 entering have a lower share … 46 of the 46 leaving have a higher one", holds by construction.**
   Both versions' bottom-decile cutoff is 1/6 (report lines 96–97). Entering means going from above 1/6 to at most
   1/6, so the share must fall, and leaving means it must rise. So it can't support the stated reason, "the
   shorter window stops crediting a habit that has since faded".
2. **F7 explains the 222 customers who fall, not the 668 who rise.** The 668 are 75% of the 890 changes and the
   reason the mean goes up. The spec's accepted limitation for F7 (`feature_spec.md` line 169) suggests one cause:
   shares count the idle months between signup and first purchase (a median of 162 days), and the 6-month window
   drops some of them. Not measured.
3. **F5, "a 'top spenders' segment moves only at its edge", isn't measured.** The report doesn't show where the 25
   leavers ranked in v1, and a leaver can come from deep inside the decile. A customer with one month of 500 and
   two of 10 scores 173 in v1, in the top 5%, and 72 in v2.
4. **F6's interpretation holds.** All 458 values below 1 are unchanged, and every other value is at least 1 in both
   versions, so the at-risk end ranks identically.

**Resolution.** The build session agreed with points 1–3 and rewrote the interpretations. The figures above are
unchanged. Details and the open decision on F7 v2 are in §5.

## 7. Tests and parity mutation check (2026-10-22)

**Learner's test run (2026-10-22).** The learner ran
`python -m pytest -v 2>&1 | tee outputs/artifacts/pytest_output_learner.txt` in their own terminal, after the Step 7
fix was committed (`e3cbf07`):
- 156 passed in 16.73 s, with 0 skipped or failed;
- Python 3.11.17 from `envs/churn-pipeline`;
- the same count as the build session's final run (`pytest_output.txt`, 156 passed).

**The tests left the store untouched.** `git status --short feature_store/` was clean afterwards (checked by Claude,
advisory session, read-only). `tests/conftest.py` also hashes the real store before and after the session (§4, F).

**`PARITY_CHECK.md` (read by Claude, advisory session, at the learner's request).** It has everything Step 8 asks
for:
- **The mutation-check result.** The build session put three scoring-only mutations into a scratch copy, each after
  the checks and the row selection:
  - M1: recency rounded instead of floored;
  - M2: customer 697's 2026-05-02 purchase leaked in;
  - M3: `monthly_cap` taken as a percentile of the scored batch.

  Each was caught by several independent tests: 9, 5 and 7 of the 29 parity tests. An unmutated control copy
  passed 29/29, and the report gives the code for each mutation.
- **Coverage.** F1–F7 (`test_transforms.py`, 41 tests), QC1–QC8 (`test_quality_checks.py`, 40 tests) and the spec's
  six parity customers (27, 350, 697, 1022, 5 and 3). A further test checks that those customers really are the
  spec's cases.
- **Counts.** `test_parity.py` has 29 tests, matching the learner's run, and the final total is 156.

None of the three mutations touches a zero-activity default. M1 and M3 both missed customers 27 and 1022, because
both paths give them the default. The learner's own mutation, below, targets that case.

**Mutation check (Learner, 2026-10-22, with commands from Claude).** The learner copied the repo to
`/tmp/parity-mutation` and ran everything there, in their own terminal.
1. **Control.** `python -m pytest tests/test_parity.py -q` on the unmodified copy: 29 passed.
2. **Mutation.** A short script, which refused to run outside the scratch copy, added a scoring-only branch to the
   copy's `features/build.py`. It sits after every quality check and the row selection. `diff -u` against the real
   file showed only these three lines:
   ```python
   if customer_ids is not None:  # MUTATION: scoring path only
       selected["spend_trend_3m_vs_prior_3m"] = selected["spend_trend_3m_vs_prior_3m"].where(
           selected["has_ever_purchased"] == 1, 0.0)
   ```
   This models the bug the spec warns about: the scoring path gives never-buyers a different trend default (0.0)
   from training (1.0).
3. **Result.** 7 failed, 22 passed:
   - `test_training_and_scoring_paths_agree_for_same_customer` for customers 27 and 1022, the two never-buyers,
     with both the latest and the v1 parameters (4 failures);
   - `test_scoring_every_customer_in_shuffled_order_matches_training`, latest and v1 (2 failures);
   - `test_scoring_script_matches_the_training_scripts_stored_latest_values`, the end-to-end test (1 failure).

**Why those tests failed, and the others didn't.**
- **The failures are exactly the expected set.** Customers 350, 697, 5 and 3 have all bought, so the mutation
  doesn't touch them.
- **The passing tests check other things:** argument validation, exit codes, checks on the full population, and
  the label. None of them compares feature values.
- **The quality checks couldn't catch it.** They run before the mutation, so only the parity tests could, which is
  the point of the exercise.
- **This bug is new.** It complements the build session's three: none of those touched a zero-activity default
  (see `PARITY_CHECK.md` above).

**The real repo was unchanged** (checked by Claude, advisory session, read-only):
- `git diff features/` is empty, and the real `build.py` doesn't contain the mutation;
- `git status feature_store/` is clean;
- `git status` lists only the learner's notes.

The "registered as v…" lines in the failing tests' captured output come from the tests' own temporary store
(`/tmp/pytest-of-subhadeep/…`), not from `feature_store/`.

**Cleanup.** The learner deleted the scratch copy (`rm -rf /tmp/parity-mutation`). Afterwards `git status` listed
only the learner's notes, and `features/` and `feature_store/` were unchanged.

## 8. `review_log.md` decisions (2026-10-22)

**Decisions (Learner, on Claude's suggestions).** `review_log.md` leaves 6 of its 18 items open. Claude, in the
advisory session, suggested accepting each one, with the reasons below. The learner accepted all six, and the
cosmetic QC7 label from §4 as well.

| # | Open item | Decision | Reason |
|---|---|---|---|
| 2 | Scoring uses `schema.py`'s latest parameters, not the store's. If the schema gains a version and training isn't rerun, nothing detects the mismatch. | Accepted | The spec defines scoring as a `build_features` call that doesn't read the store, and the spec is frozen. A process rule goes in the README instead: rerun `build_training_features` after any schema change. |
| 10 | A hard crash between the parquet write and the registry commit leaves an unregistered file. The next registration of that version stops with `StoreError`. | Accepted | It fails safe: nothing is overwritten, and the error names the file to delete. |
| 12 | `parquet_path` is stored relative to the store's parent, so renaming the store directory breaks resolution. | Accepted | It fails loudly. The same check stops a tampered registry from pointing outside `values/`. |
| 13 | Each registration reads every stored snapshot's `as_of_date`, so its cost grows with the number of versions. | Accepted | The store holds 10 snapshots of 1,399 rows. Caching is worth adding only if it grows. |
| 14 | Only `definition = 1` is implemented. | Accepted | Any other definition is rejected with a clear error, and building support for a revision nobody has specified would be speculative. This is also why F7's eligible months can't be counted from the first purchase as a new version (§5). |
| 15 | `_requested_ids` accepts an integral float such as `5.0` as id 5. | Accepted | It's unambiguous, and argparse already passes ints. |
| §4, Finding 3 | QC7's output lines don't name the version. | Accepted | Cosmetic. QC7 checks each stored version separately, and the means tell v1 from v2. |

No item needed a fix from the build session.

**Other Step 9 checks**
- **Lineage regenerated (Learner).** `python -m scripts.generate_lineage` rewrote `FEATURE_LINEAGE.md`. The result was
  identical to the committed file, so the report matches the final code and the store. This was checked by Claude
  against `HEAD`, read-only.
- **Reports match the final code.** After the Step 7 fix, the build session re-ran the tests, the version comparison
  and the lineage generator (§5).
- _To add: `features/schema.py` checked against the spec (every feature present, `nullable=False`, feature-set
  version 1.0, its parameters), and `FEATURE_LINEAGE.md` read as an outsider (10 entries, each with its source and
  why, window dates, formula, parameters, default, `parquet_path` and row count)._
