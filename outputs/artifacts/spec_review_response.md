I reviewed [feature_spec.md](feature_spec.md) against Domain Context alone (skill.md lines 108–196). I didn't edit any file or invoke the skill. Your questions depend on how the starter code behaves, so I also read the scaffold files: `feature_store/store.py`, `features/*.py`, `scripts/*.py`, .gitignore and .env.example. Every recomputation below ran read-only and wrote no files. I ran them, which is worth noting in your AI-use log.

**Bottom line:** the spec covers every "at minimum" item for all five standards. The problems are in what it says:
- Two stated reasons are contradicted by the data: the P8 default's churn argument, and the monthly cap's "single large order".
- One number doesn't reproduce: the 0.80.
- The checks only protect the training path.
- The check names DQ1–DQ7 reuse the data dictionary's DQ1–DQ11 labels for different things.

## Part 1: Verdict per standard

### 1. Rationale quality: **Satisfies**

| At-minimum item | Status | Evidence |
|---|---|---|
| Name | ✓ all 7 | the `**Feature**:` line in F1–F7 |
| Source(s) | ✓ all 7, with columns | F7: "parquet (columns `customer_id`, `txn_month`), plus `data/customers.csv` (`signup_date`)" |
| Computation | ✓ all 7 | F2: "`min(floor((as_of − latest txn_timestamp before as_of) / 1 day), 365)`"; F6's four-case table |
| Explicit zero-activity default | ✓ all 7 | "No feature may be null: every entry in `features/schema.py` is `nullable=False`". F1's "not needed" is explicit and correct. |
| Behaviour, and the confound it's distinguished from | ✓ F1, F5, F6. Weak: F2, F3, F4, F7 | see below |

- **F2:** "The median gap between purchases is 25 days, so a customer 120 days out is well past their usual rhythm, whatever their history." The confound is a customer who always buys rarely, and a median pooled over everyone can't rule that out. "Whatever their history" states the point instead of showing it. The data does support it: each repeat buyer's own median gap is 26 days at the median and 57 days at p90, and only 15 of 1,068 repeat buyers exceed 120.
- **F3:** "customers who have never bought are an activation problem: they haven't decided to start." 198 of the 232 never-buyers make their first purchase within the next 90 days. They churn at 14.7%, against 10.4% for buyers. The flag is justified because it separates a distinct group and keeps F2's 365 from being read as behaviour. The text suggests a strong risk signal, which it isn't.
- **F4:** "A low count with a high historical share of active months is a slowdown." With v2 as latest, F7's window (2025-11 → 2026-04) contains F4's three months, so the share isn't "historical". F4's rank correlation with F7 v2 is 0.70 (0.52 among buyers), against 0.55 (0.27) with v1.
- **F7:** "the share only counts months since signup, so it isn't just measuring tenure." That holds compared with the raw count: rank correlation with tenure falls from 0.71 to 0.51 (0.61 to 0.30 among buyers). But eligible months start at signup, and first purchases come a median of 162 days later (data dictionary DQ8). F1 acknowledges DQ8; F7 doesn't. One fact is also wrong: "anywhere from 3 to 12 eligible months" is actually **4 to 12**.

### 2. Train/score parity: **Satisfies**

| Item | Status | Evidence |
|---|---|---|
| Name of the one shared function | ✓ | "`features.build.build_features(as_of, source_paths=None, customer_ids=None, feature_params=None)`… It is the only place features are computed." |
| Single allowed difference | ✓ | "Only allowed training/scoring difference: `customer_ids`… Features are always computed for the full population at `as_of`." `feature_params` doesn't branch on training vs scoring, and the parity test covers the v1 parameters. |
| How as_of is threaded | ✓ | "`as_of` is a required argument of `build_features`, with no default anywhere in `features/`… no transform reads the system clock or a hard-coded date" |

The "No statistics computed from the batch" bullet goes beyond the minimum and is right. Two gaps:
- DQ1 defines a scoring behaviour ("or the number of requested ids in scoring"), but the checks are only said to run "before anything is registered". The spec contradicts itself here.
- The spec never says that as_of means 00:00:00 on that date. Only the customer 697 test implies it.

### 3. Versioning and lineage: **Satisfies**, but two business reasons need correcting

| Item | Status | Evidence |
|---|---|---|
| At least 2 features with at least 2 versions each | ✓ | three features, two versions each |
| Parameter and its value per version | ✓ | e.g. "`monthly_cap` · none · 197.10" |
| Business reason per version | ✓ stated; two are weaker than they read | see below |
| What a lineage entry must reconstruct | ✓ | the seven-item list, with "v1 and v2 as separate entries" |

- **F5 cap:** "Without a cap, a single large order makes an occasional buyer look like a big regular." The cap applies to each customer-month, not each order.
  - 161 customer-months are capped. 78 of them contain no single order above 197.10, and 116 have two or more orders.
  - 75 of the 157 customers the cap changes never placed an order above 197.10.
  - So the stated reason describes about half of what the cap does.
- **F6 cap:** 162 customers change, which meets the letter of the standard. But your own reason says "the useful signal is on the decline side (below 1)", and the cap leaves all 458 values below 1 untouched. It ties 162 customers (11.6%) at 3.0, and the rank correlation between versions is 0.9992. The change is real, but it barely moves the ranking. F7's version change is clearly material.
- **Lineage gaps:**
  - The registry columns listed in [store.py](feature_store/store.py) include no as_of date, yet "One as_of per store" and DQ7's "same `as_of`" both depend on one.
  - The derivation of 197.10 isn't recorded: p95 with linear interpolation over the 5,900 parquet customer-months before 2026-05.
- **Bump rule:** "values differ for at least one customer" is weaker than the standard's "differs materially… with a stated business reason". The rule also can't express a change of computation. DQ7 matches on identical parameters, so a code change with unchanged parameters raises an error instead of creating a new version.

### 4. Multi-source judgment: **Satisfies**

| Item | Status | Evidence |
|---|---|---|
| For each feature either source could supply: which was chosen, and why | ✓ | the table rows for F3–F7, plus "Never mix the two sources within one feature" |
| Source-timing gap considered | ✓ | "4,968 customer-months, 0 mismatches" (verified) |

Two of the reasons aren't the ones that matter at 2026-05-02:
- **F3:** "The parquet misses 10 customers whose only purchase falls in its late-July gap." At 2026-05-02 those 10 are zero-activity anyway, so the July gap changes nothing at the training cutoff. What does change the answer is the monthly grain: 5 customers made their first purchase on 2026-05-01. A parquet version would either miss them by excluding May, or leak data, because the May row includes activity from 05-02 onwards.
- **F5–F7:** "The DB would only rebuild the same monthly totals." DQ6 rebuilds those DB totals on every build anyway. The real reason is that the parquet is the designated monthly source and the DB verifies it. The cost is that a later cutoff fails instead of falling back to the DB. That is defensible, but the spec should say so.

### 5. Data-quality checks: **Partly satisfies**

| Item | Status | Evidence |
|---|---|---|
| No unexpected nulls after defaulting | ✓ | DQ2 |
| Zero-activity default actually applied | ✓, but could check itself | DQ3 lists every default value. It doesn't say the zero-activity set comes from the DB rather than from F3. |
| Monetary/aggregate values within a sane bound | ✓, claim overstated | DQ4 says it catches "a month counted twice". It can't for F4 (bound 100, observed max 9) or uncapped F5 (bound 2,000, max 501.94), because a doubled value stays inside the bound. |
| Row counts reconcile with the customer count | ✓ | DQ1 |
| A failing check raises or exits non-zero | ✓ | "raises `QualityCheckError`, `build_training_features` exits with a non-zero code" |

It's only partly satisfied because the checks are tied to registration, so scoring runs with none. DQ6's own stated failure, "silently undercounting at a later cutoff", would happen on the path most likely to use a later cutoff: scoring, since training is pinned to one as_of per store. DQ5 is worded as "every DB row reaching a transform", which only `build_features` can observe.

## Part 2: The corrections table

| Row | Holds? | Why |
|---|---|---|
| **C1** as_of = 2026-05-02 | **Yes** | The whole-day floor reproduces the label for 1,500/1,500 customers. That makes the label window (05-02 00:00, 08-01), and features stop before 05-02 00:00. No transaction is at exactly 00:00:00. A strict 90×24h reading would start the window on 05-03, so the cutoff is safe either way. |
| **C2** drop acquisition_channel | **Yes; fix the wording** | It fails "why, specifically". But 10.2% is a full-data figure: at 2026-05-02, paid_search churns 7.9% against 12.9% for organic (z ≈ −2.6 against the rest). The data contradicts the generic story; it doesn't show the channel has no signal. Say the drop is for lack of a mechanism. |
| **C3** drop region | **Decision yes; first sentence no** | "Nothing in the data links region to churn" is false. At 2026-05-02 east churns 14.7% against 9.6–10.0% elsewhere (z ≈ 2.35). Your second sentence is the right argument: it's a correlation with no mechanism. |
| **C4** drop P6 | **Decision yes; the reason names the wrong feature** | "covered by reading F4 alongside F7": with F7 v2 latest, F7's window contains F4's (rank correlation 0.70). The feature that actually compares a customer with their own earlier quarter is F6, and Stage 0 itself said P8 "overlaps P6 conceptually". The 94 is a 2026-08-01 figure; at 2026-05-02 it's 99 of 1,167 buyers. |
| **C5** drop P10 | **Yes, but under-argued** | The figures are full-data; at 2026-05-02, 829 of 1,167 buyers have never returned anything. The reason only addresses the share. Stage 0 also offered "a count instead of a share", and the spec doesn't answer that (338 buyers have any return). |
| **C6** drop P11 | **Yes** | Its rank correlation with the 180-day purchase count is 0.93 (0.87 among active customers). Put the number in the row. |
| **C7** 365 cap + F3 | **Yes** | 135 of 232 and the median of 24 both check out. At 2026-05-02, F2 = 365 exactly when F3 = 0, because the longest buyer recency is 289. |
| **C8** spend per active month | **Decision yes; the number doesn't reproduce** | I couldn't reproduce 0.80. The Stage 0 average scores 0.78 against F7 v2, 0.81 against the raw 6-month count and 0.67 against F7 v1. There's a stronger argument: Stage 0's P7 equals F5 v1 × active months ÷ 6, exactly (see Q6). |
| **C9** winsorizing → versioned cap | **Partly** | Versioning instead of guessing is right, and the 157 checks out. But the stated reason, "a single large order", fits the per-order cap Stage 0 floated (p99 = 255.66), not the per-month cap you chose. 75 of the 157 never had an order above 197.10. |
| **C10** P8 both-zero → 1.0 | **Decision survives; the stated reason is wrong** | "Only 34 of them churn" is 14.7%. That's above the population's 11.1%, and above the 13.9% for the 108 customers whose spending stopped (the 0.0 group). The groups at 1.0 churn at 9.9%. By churn rate, never-buyers resemble the "stopped" group. 1.0 is still defensible on meaning: no trend exists, 0.0 keeps one meaning, and F3 carries "never bought". Make that the whole argument. Note that 1.0 now covers 562 customers, none with a measured ratio: 312 with no prior spend, 232 never-buyers, and 18 who stopped earlier. |
| **C11** cap P8, 10 → 3 | **Cap yes; version change weak** | The 85.87 checks out, and 32 customers are above 10. The change only touches the growth side, which your own reason calls uninformative. It isn't wrong, but VERSION_COMPARISON.md should say so. |
| **C12** share, 12 → 6 months | **Yes** | The 569 checks out. "Mostly measured tenure" holds: the raw count's rank correlation with tenure is 0.71, against 0.51 for the share. Choosing v2 as latest is also what weakens C4. |
| **C13** DQ6 instead of ending the window at June | **Holds at 2026-05-02** | 4,968 customer-months, 0 mismatches, verified. But "DQ6 raises an error if that ever stops being true" is only true on the training path (Q1). |

## Part 3: Your questions

**Q1. Yes, and DQ5 has to run there anyway.**
- DQ5 says "every DB row reaching a transform", and only `build_features` can see that.
- Both scaffold scripts still default `--as-of` to 2026-08-01. Scoring at that date puts July 2026 in every month window. The 212 July customer-months that the parquet misses or undercounts (data dictionary §5) would then silently corrupt F5–F7.
- It still keeps one code path if the checks run unconditionally inside `build_features`, on the full-population frame, before the final `customer_ids` selection. What must be ruled out is `if customer_ids is None: run_checks()`, which is a second branch.
- Run DQ1–DQ4 there too, since both paths compute the full population anyway. That gives DQ1's scoring clause a real meaning.
- DQ7 stays in the training script, because it reads the store, which is part of registration.
- The trade-off: one bad row blocks scoring for everyone at that as_of. That is what failing loudly means here.

**Q2. Yes, and it's required.**
- `REGISTRY_DB = "feature_store/registry.db"` ([store.py:20](feature_store/store.py#L20)) is relative to the working directory, not the module. Running pytest from the repo root writes into the real store.
- Your own rules make that pollution permanent and harmful:
  - "Existing rows and files are never modified" leaves no way to clean up.
  - "One as_of per store" means a test at another date breaks the real build.
  - DQ7 would compare real rebuilds against copies a test wrote.
- [.gitignore](.gitignore) excludes neither `feature_store/registry.db` nor `feature_store/values/`, so test files could also get committed.

**Q3. Yes, define it.**
- In `churn-pipeline` (pandas 3.0.6, no SciPy), `Series.corr(method="spearman")` raises `ModuleNotFoundError: No module named 'scipy'`.
- `a.rank().corr(b.rank())` gives 0.676467 on a test pair with ties, the same as `DataFrame.corr(method="spearman")`.
- Also state the population it's computed over. Q6 shows that choice moves 0.47 to 0.08.

**Q4. Not clear enough.**
- DQ1's "(1,399 at 2026-05-02)" reads as an example, but nothing forbids hard-coding it. DQ3's 232 appears only in the "What it catches" column. DQ4's bounds, by contrast, are meant to be constants. Say which numbers are which.
- More important, say where the expected values come from: the population count straight from customers.csv, and the zero-activity set straight from the DB, not from F3. Otherwise DQ3 checks F3 against itself.

**Q5. Not enough, but a plain top decile doesn't work for two of the three features.**
Rank order alone will say "nothing changed" for both caps (correlations 0.9965 and 0.9992), even though those are the changes the versions make. Per feature:
- **F5:** the top decile works and tests the cap's claim directly: 25 of 140 customers enter it and 25 leave.
- **F6:** v2 ties 162 customers (11.6%) at 3.0, so its top decile is one block of ties. All 458 values below 1 are unchanged, which is guaranteed because the cap only acts above 3. Report that, and the tie count.
- **F7:** in v2, 329 customers are at or above the 90th percentile because of ties at 1.0, and 250 are at 0. Use the bottom decile among customers with `has_ever_purchased = 1`: 54 enter and 46 leave.

**Q6. State it against F7 v2.**
- Your 0.47 is F5 v2 against F7 v2 (I reproduced 0.471), so the like-for-like Stage 0 figure is 0.78.
- F7 v2 covers the same six months as F5, and it's the version a model would see.
- The 0.81 is against a raw count the spec never produces, so pairing it with 0.47 compares against two different things.
- Add the identity, and the figures for active customers only. The 250 customers sitting at 0 on both features by default inflate both full-population numbers. Among the 1,149 customers active in the window, the figures are 0.61 for Stage 0 and **0.04** for your feature. That supports your correction far better than 0.80 against 0.47.

**Contradictions with data_dictionary.md**
- **D1 (the real one): the DQ labels mean different things in the two files.**
  - Spec F5 says "DQ6 proves that every month used is complete", but the data dictionary's own DQ6 is "Short windows are sparse".
  - Data dictionary §8 says "Q6 … DQ6 enforces it", meaning the spec's DQ6.
  - The spec already has to write "DQ8 in the data dictionary" to tell them apart.
- **D2: reference dates.** The P2, P6 and P10 rows quote the data dictionary's full-data figures (10.2%, 94, 953 of 1,419) as if they described the 2026-05-02 population.
- **D3: unacknowledged dependency.** F7's denominator depends on `signup_date`, whose meaning the data dictionary rates as uncertain (DQ8).
- Everything else is consistent: the cutoff convention (§4), the parquet gap, the 101 excluded customers, the label window, and the 10 customers missing from the parquet. As a cross-check, the 34 churners among the 232 never-buyers are exactly the 81 − 47 lifetime never-buyers inside the population.

## Part 4: Proposed edits, by impact

### Required before the build

**E1. Run the checks inside `build_features`** (fixes Q1, Part 1 §5, C13)
- **Data-quality checks, intro.**
  - Current: "Every check runs on every built version before anything is registered. … it raises `QualityCheckError`, `build_training_features` exits with a non-zero code, and nothing is written to the store."
  - Proposed: "DQ5 and DQ6 check the raw inputs; DQ1–DQ4 check the full-population output. All six run inside `build_features` on every call, unconditionally, before the final `customer_ids` selection, so training and scoring get the same checks through the same code path. DQ7 reads the store, so it runs in `build_training_features` on every built version before anything is registered. A failure raises `QualityCheckError`: `build_training_features` exits non-zero and writes nothing, and `score_customer` exits non-zero and prints no features."
- **Parity design, new bullet.**
  - Proposed: "**Checks**: DQ1–DQ6 run inside `build_features` on every call. They are part of the one code path, not a branch."
- **Multi-source, last bullet.**
  - Current: "At any cutoff whose windows include July 2026, DQ6 stops the build with an error instead of silently undercounting."
  - Proposed: "At any cutoff whose month windows include July 2026 (any as_of from 2026-08-01 on), DQ6 stops training and scoring alike. This is deliberate: the pipeline fails rather than moving F5–F7 to the DB, because a feature never mixes sources."

**E2. Configurable store location** (Q2)
- **Feature store, new bullet after Registration.**
  - Proposed: "**Store location**: the registry and value files live under one store root. By default it is `feature_store/` next to `store.py`, resolved from the module's own path, not the working directory. Every store function accepts an optional keyword-only `store_dir` that overrides it. Tests always use a `tmp_path` store, so pytest never touches `feature_store/registry.db` or `feature_store/values/`."
- Separately, decide whether the real store gets committed. If it doesn't, add both paths to .gitignore.

**E3. Derive the expected counts independently** (Q4, Part 1 §5)
- **DQ1.**
  - Current: "The row count equals the population (1,399 at 2026-05-02) in training, or the number of requested ids in scoring, and `customer_id` is unique"
  - Proposed: "The full-population frame has one row per population customer, and `customer_id` is unique. The expected count is computed at run time directly from customers.csv (`signup_date < as_of`). After selection, the row count equals the number of requested ids."
- **DQ3, prepend.**
  - Proposed: "The zero-activity set is queried at run time directly from the DB (population customers with no row where `txn_timestamp < as_of`), not taken from F3."
- **Section intro, add.**
  - Proposed: "Counts quoted here (1,399, 232) are the expected values at 2026-05-02, and a test asserts them. The DQ4 bounds are fixed constants."

**E4. Say where as_of is stored** (Part 1 §3)
- **"One as_of per store", add.**
  - Proposed: "The registry table has no as_of column, so each version stores `"as_of": "2026-05-02"` in its `parameters` JSON, and its parquet snapshot carries an `as_of_date` column. The one-date rule and DQ7 read it from `parameters`."

**E5. Fix the P8 default's reason** (C10)
- **Corrections, P8 default row, Why.**
  - Current: "232 of those 250 customers never bought, and only 34 of them churn. A value of 0.0 would look like a collapse, so 0.0 now means only "spending stopped"."
  - Proposed: "With no spend in either period there is no trend to measure, and 0.0 is kept for one meaning: spending stopped. 232 of the 250 never bought, and F3 marks them. This is a choice about meaning, not risk: their churn rate (14.7%) is close to the stopped group's (13.9%), so F3, not this feature, carries the never-bought signal."

**E6. Replace the 0.80** (C8, Q6)
- **F5 rationale.**
  - Current: "At 2026-05-02 its rank correlation with F7 is 0.47. The Stage 0 version, which averaged over all six months, scored 0.80, so it was mostly measuring frequency again."
  - Proposed: "The Stage 0 version, which averaged over all six months, equals this feature (uncapped) × active months ÷ 6, so it contained the frequency count by construction. At 2026-05-02, over the 1,399 customers, its rank correlation with F7 v2 was 0.78; this feature's (v2) is 0.47. Most of the 0.47 comes from the 250 customers at 0.0 on both by default: among the 1,149 active in the window, the figures are 0.61 and 0.04."
- **Corrections, P7 row, Why.**
  - Proposed: "The all-months average is spend per active month × active months ÷ 6, so it contained frequency by construction. Rank correlation with F7 v2 at 2026-05-02: 0.78, against 0.47 for spend per active month (0.61 against 0.04 among the 1,149 active customers)."

**E7. Fix the F5 cap's reason** (C9, Part 1 §3)
- **Versioning table, F5 row.**
  - Current: "Without a cap, a single large order makes an occasional buyer look like a big regular."
  - Proposed: "Without a cap, a single unusually large month, from one big order or several in the same month, makes an occasional buyer look like a big regular. Of the 157 customers the cap changes, 82 have a capped month containing an order above 197.10; 75 are capped only through months of several smaller orders. The cap is per month because the parquet holds no order amounts and a feature never mixes sources. 197.10 is the p95 (linear interpolation) of the 5,900 parquet customer-months before 2026-05."

**E8. Define rank correlation** (Q3)
- **Scope and conventions, new row.**
  - Proposed: "| Rank correlation | Spearman's ρ: the Pearson correlation of ranks, with tied values sharing their average rank, computed as `a.rank().corr(b.rank())`. This avoids SciPy, which `Series.corr(method="spearman")` needs and requirements.txt lacks. Computed over the full population at `as_of` unless a subgroup is named. |"

**E9. Rename the spec's checks** (D1)
- **Data-quality checks, and every reference to them:**
  - Leakage controls ("check DQ5")
  - F5, F6 and F7 ("DQ6 …")
  - the Multi-source rule
  - the last corrections row
- Rename DQ1–DQ7 to QC1–QC7. "DQ8 in the data dictionary" then needs no qualifier.
- Data dictionary §8's "DQ6 enforces it" would become "QC6". That edit is to the data dictionary, so it's your call.

**E10. Fix the F7 eligible-month range** (Part 1 §1)
- **F7 rationale.**
  - Current: "with anywhere from 3 to 12 eligible months"
  - Proposed: "with anywhere from 4 to 12 eligible months"

### Optional

**E11. A threshold metric for each versioned feature** (Q5)
- **Feature store, new bullet.**
  - Proposed: "**Version comparison**: for each versioned feature, report the customers changed, summary statistics, the rank correlation between versions, and one threshold metric. F5: customers entering or leaving the top decile, where the cap acts. F6: the count tied at the cap in v2, and confirmation that all values below 1 are unchanged. F7: customers entering or leaving the bottom decile among those with `has_ever_purchased = 1`. A decile includes every customer at or beyond that version's own 90th (or 10th) percentile, ties included, and tie counts are reported. Expected at 2026-05-02: F5 25 in and 25 out; F6 162 tied and 458 unchanged; F7 54 in and 46 out."

**E12. P6 row and F4 rationale** (C4)
- **Corrections, P6 row, Why.**
  - Current: "Its job, telling a slowdown apart from a habitually rare buyer, is covered by reading F4 alongside F7."
  - Proposed: "Its job, comparing recent activity with the customer's own earlier activity, is done by F6 (recent quarter against prior quarter), with F4 and F7 in support. At 2026-05-02, 99 of the 1,167 buyers have a single purchase."
- **F4 rationale, add.**
  - Proposed: "F7 v2's window includes F4's three months (rank correlation 0.70, against 0.55 for v1), so F6 is the direct comparison with the customer's own baseline."

**E13. F3 rationale**
- Current: "This predicts churn because customers who have never bought are an activation problem: they haven't decided to start."
- Proposed: "Customers who have never bought are an activation case, not a lapse: they had no habit to lose. 198 of the 232 at 2026-05-02 make their first purchase within 90 days, which fits the long gap between signup and first purchase (DQ8 in the data dictionary). They still churn more than buyers (14.7% against 10.4%)."

**E14. F2 rationale**
- Current: "The median gap between purchases is 25 days, so a customer 120 days out is well past their usual rhythm, whatever their history."
- Proposed: "Each repeat buyer's own median gap between purchases is 26 days at the median and 57 days at the 90th percentile, and only 15 of 1,068 repeat buyers exceed 120 days. A customer 120 days out is past their own usual rhythm, not just the population's."

**E15. F7 accepted limitation** (D3)
- **F7, new bullet.**
  - Proposed: "**Accepted limitation**: eligible months start at signup, and first purchases come a median of 162 days later (DQ8 in the data dictionary), so newer customers' shares include idle months before their first purchase. The share's rank correlation with tenure is 0.51 (0.30 among buyers), against 0.71 for the raw count."

**E16. P2, P3, P10 and P11 rows** (C2, C3, C5, C6, D2)
- **P3.**
  - Current: "Nothing in the data links region to churn."
  - Proposed: "Nothing in the data explains why region would drive churn; there are no delivery, store or price fields. At 2026-05-02 east churns at 14.7% against 9.6–10.0% elsewhere."
- **P2.** Use "7.9% at 2026-05-02, against 12.9% for organic", and add "Dropped for lack of a mechanism, not lack of signal."
- **P10.** Use "829 of the 1,167 buyers at 2026-05-02", and add "A return count is equally sparse: 338 buyers have any return."
- **P11.** Add "Rank correlation with the 180-day purchase count: 0.93."

**E17. DQ4's claim** (Part 1 §5)
- Remove "a month counted twice" from DQ4's "What it catches" column.
- If you want double-counting caught, add a reconciliation check: the sum of F4 over the population equals the number of DB rows in [as_of − 90 days, as_of), and the total of active months equals the number of parquet rows in the window.

**E18. Version-bump rule** (Part 1 §3)
- Current: "…**and** the values differ for at least one customer."
- Proposed, appended: "…and the business reason is recorded in `business_rationale`, with the effect reported in VERSION_COMPARISON.md. A change of computation is recorded as a new `definition` revision in `parameters`, so it creates a new version instead of failing DQ7."

**E19. as_of format and extra parity cases**
- **`as_of` row, add.**
  - Proposed: "A calendar date, read as 00:00:00 naive local time; a value with a time component is rejected."
- **Parity test, add** these customers: the one with 0 eligible months, one in F6's "recent spend only" case, and one with a capped month.

## Numbers I checked

| Spec claim | What I got |
|---|---|
| Population 1,399; 101 excluded; 155 churn (11.1%) | ✓ |
| 232 zero-activity customers, 34 of whom churn | ✓ (14.7%, against 10.4% for buyers) |
| 39 purchases on 2026-05-01; customer 697 excluded | ✓ |
| Whole-day floor reproduces the label | ✓ 1,500/1,500 |
| F1: 302 customers under six months old | ✓ |
| F2: no buyer above 289 days; median gap 25 days | ✓ (25.0 before as_of) |
| F4: 1,049 customers with at least one purchase; maximum 9 | ✓ |
| F5: cap 197.10; 157 customers change; maximum 501.94 → 197.10; rank correlation 0.997 | ✓ (197.098; 0.9965) |
| F5 against F7: 0.47 | ✓ 0.471 (F5 v2 against F7 v2) |
| Stage 0 average against F7: 0.80 | ✗ 0.782 against F7 v2, 0.808 against the raw 6-month count, 0.671 against F7 v1 |
| F6 cases 729 / 108 / 312 / 250 (232 + 18); maximum ratio 85.9; 162 change; mean 1.51 → 1.14; rank correlation 0.999 | ✓ |
| F7: 1 customer with 0 eligible months; 569 with fewer than 12; 161 with exactly 3 active months; 890 change; mean 0.333 → 0.387; rank correlation 0.849 | ✓ |
| F7: "3 to 12 eligible months" | ✗ 4 to 12 |
| P4: 135 of 232 never-buyers at 90 days or less; median 24 | ✓ |
| Parquet matches the DB for 2025-05 → 2026-04: 4,968 customer-months, 0 mismatches | ✓ |
| P2 10.2%, P6 94, P10 953 of 1,419 | These are full-data figures. At 2026-05-02: 7.9%, 99, and 829 of 1,167 |

The other figures quoted above come from the same runs: churn by F6 case, how the capped months break down, the correlations, the decile movements, the per-customer purchase gaps and the z-scores.