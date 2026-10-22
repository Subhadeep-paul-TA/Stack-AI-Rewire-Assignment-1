# Learner verification log

This log is the evidence for the churn feature pipeline assignment, with one section per checkpoint in `PLAN.md`.
The rubric cells point to these sections.


| § | Checkpoint | PLAN.md step | Rubric row | Status |
|---|---|---|---|---|
| 1 | Environment setup | 1 | — (supports the README) | Done |
| 2 | Stage 0 spot-check | 2 | Stage 0 review; Engagement (a) | Done (first run by Claude; re-run and confirmed by the learner) |
| 3 | Spec reconciliation | 3 | Engagement (a) | Done (review by a separate Claude session; decisions by the learner) |
| 4 | Build output spot-checks | 6 | Engagement (c) | Pending |
| 5 | `ARTIFACT_NOTES.md` pushback | 6 | Engagement (b) | Pending |
| 6 | Versioning verification | 7 | Feature Store & Versioning | Pending |
| 7 | Tests and parity mutation check | 8 | Testing; Engagement (c) | Pending |
| 8 | `review_log.md` decisions | 9 | Engagement (d) | Pending |

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

## 4. Build output spot-checks (pending)

_Record:_
- _the parity check by hand: customer IDs, and all 7 values from scoring against the training snapshot;_
- _one value recomputed from raw data;_
- _the registry query, saved to `registry_query.txt`;_
- _the re-run that should skip every feature;_
- _the code checks;_
- _the spec's numbers, cross-checked._

## 5. `ARTIFACT_NOTES.md` pushback (pending)

_Record what it claimed, what you pushed back on, Claude's answer and what changed._

## 6. Versioning verification (pending)

_Record `get_version` and `get_latest` for each versioned feature, and how `VERSION_COMPARISON.md` compares with the
spec's measured effects (157 / 162 / 890 customers changed)._

## 7. Tests and parity mutation check (pending)

_Record:_
- _the pytest result (`pytest_output.txt`);_
- _that the tests left the store untouched;_
- _the mutation you introduced, and `test_parity.py` failing;_
- _that the file was restored and `git status` is clean._

## 8. `review_log.md` decisions (pending)

_For each unresolved item, record either the fix the build session made, or that you accepted it and why._
