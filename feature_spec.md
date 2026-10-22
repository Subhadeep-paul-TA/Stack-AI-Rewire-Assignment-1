# feature_spec.md — Churn Feature Pipeline

> **Authorship.** Every decision here is mine: the feature set, the as_of date, the defaults, the sources and the
> versioning. I made them by answering Claude's questions on 2026-10-21. Claude drafted the text from my answers,
> with my instructor's approval. I reviewed it. After the spec-review session with Claude on 2026-10-22
> (`outputs/artifacts/spec_review_response.md`), I chose which findings to accept, and Claude applied those
> revisions (verification log §3).

## Scope and conventions

| Term | Definition |
|---|---|
| `as_of` | The feature cutoff, and the pipeline's single parameter. Every feature uses only data from before it. Both scripts default to **2026-05-02**. It is a calendar date, read as 00:00:00 naive local time; a value with a time component is rejected. |
| Label date | 2026-08-01. This belongs to the provided `churned` column, not to the pipeline. `churned = 1` means no purchase in roughly 2026-05-02 → 2026-08-01 (`data_dictionary.md` §6). |
| Why 2026-05-02 | It is the start of the label window, so a snapshot cut here sees none of the activity the label is computed from. At 2026-08-01, `days_since_last_txn` and `txn_count_90d` would reproduce the label almost exactly. |
| Population | Customers with `signup_date < as_of`. At 2026-05-02: **1,399 customers**. The 101 who signed up later are excluded. 155 of the 1,399 churn (11.1%). |
| Zero-activity customer | A customer with no transaction where `txn_timestamp < as_of`. At 2026-05-02 there are **232**, and 34 of them churn. |
| Day windows | `[as_of − N days, as_of)`: the start is included, the end is excluded, and comparisons use full timestamps. |
| Recency | Whole days, rounded down: `floor((as_of − t) / 1 day)`. This matches how the label appears to count days. |
| Month windows | The N complete calendar months before the month containing `as_of`. At 2026-05-02 the last month used is 2026-04. The 39 purchases on 2026-05-01 count in day windows but not in month windows. |
| Leakage controls | `churned` is never an input to a feature, and `build_features` never returns it. No row timestamped at or after `as_of`, and no month at or after `as_of`'s month, reaches a transform (check QC5). |
| Rank correlation | Spearman's ρ: the Pearson correlation of ranks, with tied values sharing their average rank, computed as `a.rank().corr(b.rank())`. This avoids SciPy, which `Series.corr(method="spearman")` needs and `requirements.txt` lacks. It is computed over the full population at `as_of` unless a subgroup is named. |

## Candidate features

| # | Feature | Family | Source | dtype | Zero-activity default | Versioned on |
|---|---|---|---|---|---|---|
| F1 | `tenure_days` | Profile | customers.csv | int64 | not needed (always defined) | — |
| F2 | `days_since_last_txn` | Recency | transactions.db | int64 | 365 | — |
| F3 | `has_ever_purchased` | Activation | transactions.db | int64 (0/1) | 0 | — |
| F4 | `txn_count_90d` | Frequency | transactions.db | int64 | 0 | — |
| F5 | `avg_spend_per_active_month_6m` | Monetary | parquet | float64 | 0.0 | `monthly_cap` |
| F6 | `spend_trend_3m_vs_prior_3m` | Monetary trend | parquet | float64 | 1.0 | `ratio_cap` |
| F7 | `active_month_share` | Engagement consistency | parquet + customers.csv | float64 | 0.0 | `lookback_months` |

No feature may be null: every entry in `features/schema.py` is `nullable=False`. The feature-set version is `1.0`.

### F1 — `tenure_days`
- **Feature**: `tenure_days`
- **Source**: `data/customers.csv`, column `signup_date`
- **Computation**: whole days from `signup_date` to `as_of`. The value is at least 1 for everyone in the population.
- **Zero-activity default**: not needed. Every customer has a signup date, whether or not they have bought anything.
- **If multiple sources could supply this, which and why?**: only `customers.csv` holds signup dates.
- **Rationale**: This predicts churn because customers who joined recently tend to have short, patchy purchase
  histories while they decide whether the store is worth coming back to. That is different from an established
  customer who is slowing down: the same low activity means "habit not formed yet" for one and "habit breaking"
  for the other. Tenure lets a model tell the two apart. At 2026-05-02, 302 customers are under six months old.
- **Accepted limitation**: first purchases come a median of 162 days after signup (DQ8 in the data dictionary),
  so this measures account age, not buying age. Days since first purchase was rejected because it is undefined
  for the 232 zero-activity customers.

### F2 — `days_since_last_txn`
- **Feature**: `days_since_last_txn`
- **Source**: `data/transactions.db`, column `transactions.txn_timestamp`
- **Computation**: `min(floor((as_of − latest txn_timestamp before as_of) / 1 day), 365)`.
- **Zero-activity default**: **365**. At 2026-05-02 no customer who has bought anything exceeds 289 days, so only
  never-buyers hold 365. F3 marks them explicitly, and at later cutoffs it also separates them from customers who
  have been away for more than a year.
- **If multiple sources could supply this, which and why?**: the DB is the only option. The parquet only records
  months, so it can't say how many days ago the last purchase was.
- **Rationale**: This predicts churn because customers who have gone longer since their last purchase have often
  already moved that spending elsewhere. That is different from buying rarely: recency measures where the customer
  stands at the cutoff, not their average pace. Each repeat buyer's own median gap between purchases is 26 days
  at the median and 57 days at the 90th percentile, and only 15 of the 1,068 repeat buyers exceed 120 days. So a
  customer 120 days out is past their own usual rhythm, not just the population's.

### F3 — `has_ever_purchased`
- **Feature**: `has_ever_purchased`
- **Source**: `data/transactions.db`, columns `transactions.customer_id` and `txn_timestamp`
- **Computation**: 1 if the customer has at least one transaction before `as_of`, otherwise 0.
- **Zero-activity default**: 0. This feature is itself the zero-activity indicator, so it is defined for everyone.
- **If multiple sources could supply this, which and why?**: the DB. The parquet could also show whether any purchase
  exists, but only by month. At 2026-05-02, 5 customers made their first purchase on 2026-05-01: a parquet version
  would miss them if it excluded May, or leak post-cutoff activity if it included May's row. The DB is exact at any
  cutoff, and it also covers the 10 customers whose only purchase falls in the parquet's 2026-07-26 → 07-31 gap.
- **Rationale**: This predicts churn because customers who have never bought are an activation case, not a lapse:
  they had no habit to lose. They still churn more than buyers (14.7% against 10.4% at 2026-05-02), although most
  do start: 198 of the 232 make their first purchase before the label date, which fits the long gap between signup
  and first purchase (DQ8 in the data dictionary). The flag also stops F2's 365 and the other defaults from being
  read as real behaviour.

### F4 — `txn_count_90d`
- **Feature**: `txn_count_90d`
- **Source**: `data/transactions.db`, columns `transactions.customer_id` and `txn_timestamp`
- **Computation**: the number of transactions with `as_of − 90 days ≤ txn_timestamp < as_of`. At 2026-05-02 that
  runs from 2026-02-01 00:00 up to, but not including, 2026-05-02 00:00. Returned purchases still count, because
  the purchase happened.
- **Zero-activity default**: 0.
- **If multiple sources could supply this, which and why?**: the DB. The parquet has monthly counts, but 90 days
  doesn't line up with calendar months. Using it would mean either dropping part of the first month or reaching
  past `as_of`.
- **Rationale**: This predicts churn because customers who are buying less often in the months right before the
  cutoff tend to be partway out the door. It is kept separate from customers who have always bought rarely by
  reading it alongside F6, F7 and F1. A low count with a high share of active months is a slowdown. A low count
  with a low share is a habitually infrequent buyer. F7 v2's window includes F4's three months (rank correlation
  0.70, against 0.55 for v1), so F6 is the direct comparison with the customer's own baseline. 90 days covers
  about three typical purchase cycles (median gap 25 days). At 2026-05-02, 1,049 of the 1,399 customers have at
  least one purchase in the window.

### F5 — `avg_spend_per_active_month_6m` (versioned)
- **Feature**: `avg_spend_per_active_month_6m`
- **Source**: `data/transactions_monthly_agg.parquet`, columns `customer_id`, `txn_month` and `total_amount`
- **Computation**:
  1. Take the 6 complete months before `as_of`'s month (at 2026-05-02: 2025-11 → 2026-04).
  2. Cap each month's `total_amount` at `monthly_cap`.
  3. Add up the capped totals and divide by the number of those months that have a parquet row, i.e. months with
     at least one purchase.

  Spend is gross of returns, because the parquet has no returned amounts.
- **Version parameter**: `monthly_cap`. v1 = none (uncapped). v2 = 197.10 (latest).
- **Zero-activity default**: 0.0. Customers who bought before the window but not during it also get 0.0.
- **If multiple sources could supply this, which and why?**: the parquet. The feature is defined on calendar
  months, which is exactly what the parquet stores. The parquet is the designated monthly source, and the DB
  checks it: QC6 rebuilds the DB's monthly totals on every build and fails if any month used is incomplete or
  differs. At a later cutoff whose windows reach July 2026, the build fails rather than falling back to the DB,
  because a feature never mixes sources.
- **Rationale**: This predicts churn because customers whose usual monthly basket here is small probably spend most
  of their money elsewhere, so leaving costs them little. That is different from purchase frequency, because it
  averages only over months the customer actually bought. A customer who buys once a quarter but spends 150 each
  time scores high here and low on F4 and F7. The Stage 0 version, which averaged over all six months, equals
  this feature (uncapped) × active months ÷ 6, so it contained the frequency count by construction. At
  2026-05-02, over the 1,399 customers, its rank correlation with F7 v2 was 0.78; this feature's (v2) is 0.47.
  Most of the 0.47 comes from the 250 customers at 0.0 on both by default: among the 1,149 active in the window,
  the figures are 0.60 and 0.04.

### F6 — `spend_trend_3m_vs_prior_3m` (versioned)
- **Feature**: `spend_trend_3m_vs_prior_3m`
- **Source**: `data/transactions_monthly_agg.parquet`, columns `customer_id`, `txn_month` and `total_amount`
- **Computation**: `recent` is total `total_amount` over the 3 complete months before `as_of`'s month
  (2026-02 → 2026-04). `prior` is the total over the 3 months before those (2025-11 → 2026-01). Then:

  | Case | Value | Customers at 2026-05-02 |
  |---|---|---|
  | `recent > 0` and `prior > 0` | `min(recent / prior, ratio_cap)` | 729 |
  | `prior > 0`, `recent = 0` | 0.0 (spending stopped) | 108 |
  | `recent > 0`, `prior = 0` | 1.0 (no earlier spend to fall from) | 312 |
  | both are 0 | 1.0 (no trend to measure) | 250 (232 never bought, 18 stopped earlier) |
- **Version parameter**: `ratio_cap`. v1 = 10. v2 = 3 (latest).
- **Zero-activity default**: 1.0. A customer who has never bought has no trend. Their inactivity already shows in
  F2, F3, F4 and F7, so this feature doesn't record it as a collapse. A value of 0.0 means only one thing:
  spending stopped.
- **If multiple sources could supply this, which and why?**: the parquet, for the same reasons as F5. Both 3-month
  windows are whole calendar months, and QC6 guarantees they are complete.
- **Rationale**: This predicts churn because customers whose spend is falling compared with their own previous
  quarter tend to be trading down or moving purchases to a competitor. That is different from a customer whose
  spend has always been low, because each customer is compared with their own baseline rather than with other
  customers. A steady low spender sits at 1.0, while a big spender who halves their spend sits at 0.5.

### F7 — `active_month_share` (versioned)
- **Feature**: `active_month_share`, renamed from `active_months_12m` because the lookback length differs between
  versions
- **Source**: `data/transactions_monthly_agg.parquet` (columns `customer_id`, `txn_month`), plus
  `data/customers.csv` (`signup_date`) to count eligible months
- **Computation**: over the `lookback_months` complete months before `as_of`'s month:
  - *active months* are months with a parquet row for the customer;
  - *eligible months* are months in the window on or after the customer's signup month;
  - the value is active ÷ eligible. It stays within [0, 1] because nobody can buy before signing up.
- **Version parameter**: `lookback_months`. v1 = 12 (2025-05 → 2026-04). v2 = 6 (2025-11 → 2026-04, latest).
- **Zero-activity default**: 0.0. Customers with 0 eligible months, i.e. who signed up in `as_of`'s own month, also
  get 0.0. There is 1 such customer at 2026-05-02.
- **If multiple sources could supply this, which and why?**: the parquet. "Did the customer buy in month M?" is
  answered by a single parquet row. As for F5, the parquet is the designated monthly source, and QC6 checks it
  against the DB on every build.
- **Rationale**: This predicts churn because customers who buy in most of their available months have a habit that
  one quiet month won't break, while customers who buy in only a few can drift away unnoticed. Unlike a raw count,
  the share only counts months since signup, so it isn't just measuring tenure. A customer active in all 3 of
  their 3 months scores 1.0, while an established customer active in 3 of 12 scores 0.25. At 2026-05-02, 161
  customers have exactly 3 active months in the 12-month window, with anywhere from 4 to 12 eligible months.
- **Accepted limitation**: eligible months start at signup, and first purchases come a median of 162 days later
  (DQ8 in the data dictionary), so newer customers' shares include idle months before their first purchase. Rank
  correlation with tenure is 0.71 for the raw 12-month count, against 0.51 for the share over 12 months (v1) and
  0.45 over 6 months (v2). Among buyers the figures are 0.61, 0.30 and 0.20.

## Train/score parity design

- **Shared function/code path**:
  `features.build.build_features(as_of, source_paths=None, customer_ids=None, feature_params=None)`.
  - It is the only place features are computed. It loads all three sources through `features/ingest.py` and calls
    every transform in `features/transforms.py`. Neither script contains any feature logic.
  - `feature_params` exists only so the training script can produce older versions for registration. It maps a
    feature name to that version's parameters, and by default uses the latest version of every feature in
    `features/schema.py`.
- **Only allowed training/scoring difference**: `customer_ids`.
  - Features are always computed for the full population at `as_of`.
  - The last step returns every row for training (`customer_ids=None`) or only the requested rows for scoring.
  - If a requested id doesn't exist, or the customer signed up on or after `as_of`, the function raises a
    `ValueError` naming that id. It never returns an empty result.
- **How as_of is threaded (not hardcoded)**: `as_of` is a required argument of `build_features`, with no default
  anywhere in `features/`. It is parsed once and passed explicitly to every transform; no transform reads the
  system clock or a hard-coded date. The only default date lives in the two scripts' `--as-of` flag (2026-05-02).
- **No statistics computed from the batch**: every cap (365, 197.10, 10, 3) is a fixed parameter stored in
  `schema.py` and the registry, and is never recomputed during a build. A percentile taken over 1,399 customers in
  training would differ from one taken over a single customer in scoring, and that parity bug would only appear
  in scoring.
- **Checks**: QC1–QC6 and QC8 run inside `build_features` on every call, unconditionally, before the final
  `customer_ids` selection. They are part of the one code path, not a branch: there is no `if customer_ids is None`
  around them.
- **Parity test**: `test_parity.py` builds the full population, then single customers, at the same `as_of`, and
  checks that all 7 features match exactly. It runs once with the latest parameters and once with each versioned
  feature's v1 parameters. It must include:
  - a zero-activity customer;
  - a customer with a purchase on 2026-05-01, which counts in F4 but falls outside the month windows;
  - customer 697, whose purchase at 2026-05-02 17:12 must be excluded because it is at or after `as_of`;
  - customer 1022, who signed up on 2026-05-01 and so has 0 eligible months for F7;
  - a customer in F6's "recent spend only" case, e.g. customer 5;
  - a customer with a month above `monthly_cap`, so that F5 v1 and v2 differ, e.g. customer 3.

## Feature store, versioning & lineage

- **Features versioned**: three features, with two versions each: `avg_spend_per_active_month_6m`,
  `spend_trend_3m_vs_prior_3m` and `active_month_share`. The other four features are registered once, as v1.

| Feature | Parameter | v1 | v2 (latest) | Why v2 differs from v1 | Effect at 2026-05-02 |
|---|---|---|---|---|---|
| `avg_spend_per_active_month_6m` | `monthly_cap` | none | 197.10 | Monthly spend is right-skewed: one customer-month reaches 1,753.23. Without a cap, a single unusually large month, from one big order or several in the same month, makes an occasional buyer look like a big regular. Of the 157 customers the cap changes, 82 have a capped month containing an order above 197.10; 75 are capped only through months of several smaller orders. The cap is per month because the parquet holds no order amounts and a feature never mixes sources. 197.10 is the 95th percentile (linear interpolation) of the 5,900 parquet customer-months before 2026-05, fixed as a constant, and keeps the feature about typical monthly basket size. | 157 customers change. The maximum falls from 501.94 to 197.10. Rank correlation between versions is 0.997, so expect little effect on rank-based models and more on linear models, which are sensitive to outliers. |
| `spend_trend_3m_vs_prior_3m` | `ratio_cap` | 10 | 3 | Ratios above 3 come from a tiny previous quarter, often a single small order; the largest observed is 85.9. For retention targeting the useful signal is on the decline side (below 1). Growth beyond 3× adds no information but would dominate the feature's scale. | 162 customers change. The mean falls from 1.51 to 1.14. Rank correlation is 0.999. |
| `active_month_share` | `lookback_months` | 12 | 6 | A 12-month window still credits a habit that ended months ago. With a median gap of 25 days between purchases, 6 months still covers about 7 purchase cycles, which is enough to judge consistency while reflecting the customer's current habit. | 890 customers change. The mean rises from 0.333 to 0.387. Rank correlation is 0.849. |

- **Registration**:
  - `build_training_features` builds every version listed in `schema.py` through `build_features`, oldest first.
  - It runs every quality check on every version before registering anything.
  - It then registers the versions oldest first, so `get_latest()` returns v2, the definition this spec prefers.
  - Each new version gets a new registry row and a new parquet file. `is_latest` is updated in the same
    transaction as the insert.
- **Store location**: the registry and value files live under one store root. By default it is `feature_store/`
  next to `store.py`, resolved from the module's own path, not the working directory. Every store function accepts
  an optional keyword-only `store_dir` that overrides it. Tests always use a `tmp_path` store, so pytest never
  touches `feature_store/registry.db` or `feature_store/values/`. The real store is a deliverable: it is committed,
  not git-ignored.
- **Version comparison**: for each versioned feature, `VERSION_COMPARISON.md` reports the customers changed, summary
  statistics, the rank correlation between versions, and one threshold metric:
  - F5: customers entering or leaving the top decile, where the cap acts;
  - F6: the count tied at the cap in v2, and confirmation that all values below 1 are unchanged;
  - F7: customers entering or leaving the bottom decile among those with `has_ever_purchased = 1`.

  A decile includes every customer at or beyond that version's own 90th (or 10th) percentile, ties included, and
  tie counts are reported. Expected at 2026-05-02: F5 25 in and 25 out (140 customers in each version's top
  decile); F6 162 tied at 3.0 and all 458 values below 1 unchanged; F7 54 in and 46 out (168 customers in v1's
  bottom decile and 176 in v2's, because of ties).
- **What forces a version bump vs. not**:
  - A new version is created only when a parameter value, the source or the computation changes **and** the values
    differ for at least one customer. The business reason is recorded in `business_rationale`, and the effect is
    reported in `VERSION_COMPARISON.md`.
  - A change of computation is recorded as a new `definition` revision in `parameters` (every current version has
    `"definition": 1`), so it creates a new version instead of failing QC7.
  - Renaming a feature, editing its description, or re-running an unchanged definition at the same `as_of` does not
    create a version. The re-run case is reported as "unchanged — skipped" and writes nothing.
  - Existing rows and files are never modified.
- **One as_of per store**: every stored snapshot has `as_of_date = 2026-05-02`. Registering at a different `as_of`
  into a store that already holds another date raises an error, so snapshot dates never mix within one version
  history. The registry table has no as_of column, and `parameters` holds only tunable values and the `definition`
  revision, so each version's as_of is read from its parquet snapshot's `as_of_date` column. The one-date rule and
  QC7 use that value.
- **What a lineage entry must let someone reconstruct**: for every feature *and every stored version*, enough to
  recompute the values by hand from the raw files without reading code:
  - the source file, table and columns;
  - the population rule;
  - the exact window bounds relative to `as_of`, and the dates they translate to for this snapshot;
  - the aggregation formula and parameter values, and how each fixed parameter was derived (e.g. 197.10);
  - the zero-activity default and when it applies;
  - the rationale, and why this version differs from the previous one;
  - the registry row, `parquet_path`, row count and `as_of_date`.

  `FEATURE_LINEAGE.md` is generated from `schema.py` and the registry, with v1 and v2 as separate entries.

## Multi-source judgment calls

**Rule:** use the DB for anything measured in days or needing exact timestamps. Use the parquet only for complete
calendar months that end before `as_of`'s month and pass the QC6 coverage check. Never mix the two sources within
one feature. The parquet is the designated monthly source; the DB is used to check it (QC6), never to replace it.

| Feature | Could come from | Chosen | Why |
|---|---|---|---|
| `tenure_days` | customers.csv only | customers.csv | It is the only source of `signup_date`. |
| `days_since_last_txn` | DB only | DB | It needs exact timestamps, and the parquet only records months. |
| `has_ever_purchased` | DB or parquet | DB | The parquet only records months. At 2026-05-02, 5 customers made their first purchase on 2026-05-01, which a monthly source would either miss or leak around. The DB is exact at any cutoff, including for the 10 customers in the parquet's late-July gap. |
| `txn_count_90d` | DB or parquet | DB | 90 days isn't aligned to calendar months, so the parquet would drop part of the first month or reach past `as_of`. |
| `avg_spend_per_active_month_6m` | DB or parquet | parquet | It is defined on whole calendar months, which is how the parquet is stored. |
| `spend_trend_3m_vs_prior_3m` | DB or parquet | parquet | It compares two 3-month blocks of whole calendar months. |
| `active_month_share` | DB or parquet | parquet (+ customers.csv) | "Active in month M" is a single parquet row. |

**Why the parquet is safe at 2026-05-02:**
- Its only gap is 2026-07-26 → 07-31 (`data_dictionary.md` §5).
- All 12 months the month-based features use (2025-05 → 2026-04) match the DB exactly: 4,968 customer-months,
  0 mismatches.
- At any cutoff whose month windows include July 2026 (any as_of from 2026-08-01 on), QC6 stops training and
  scoring alike. This is deliberate: the pipeline fails rather than moving F5–F7 to the DB, because a feature never
  mixes sources.

## Data-quality checks

QC5 and QC6 check the raw inputs. QC1–QC4 and QC8 check the full-population output. All seven run inside
`build_features` on every call, unconditionally, before the final `customer_ids` selection, so training and scoring
get the same checks through the same code path. QC7 reads the store, so it runs in `build_training_features` on
every built version before anything is registered. Each check reports which rows or features failed. A failure
raises `QualityCheckError`: `build_training_features` exits non-zero and writes nothing, and `score_customer` exits
non-zero and prints no features.

The counts quoted here (1,399 and 232) are the expected values at 2026-05-02, and a test asserts them. The checks
themselves derive them at run time from the sources. The QC4 bounds are fixed constants.

| # | Check | What it catches in this dataset | On failure |
|---|---|---|---|
| QC1 | The full-population frame has one row per population customer, and `customer_id` is unique. The expected count is computed at run time directly from customers.csv (`signup_date < as_of`; 1,399 at 2026-05-02). After selection, the row count equals the number of requested ids | A merge against monthly rows that duplicates customers, or an inner join that drops the 232 zero-activity customers | raise / non-zero exit |
| QC2 | No nulls in any of the 7 feature columns after defaults are applied | A transform that returns NaN for customers missing from a source (no DB rows or no parquet rows) | raise / non-zero exit |
| QC3 | The zero-activity set is queried at run time directly from the DB (population customers with no row where `txn_timestamp < as_of`), not taken from F3. Zero-activity defaults are applied: every customer in that set has exactly `days_since_last_txn = 365`, `has_ever_purchased = 0`, `txn_count_90d = 0`, `avg_spend_per_active_month_6m = 0.0`, `spend_trend_3m_vs_prior_3m = 1.0` and `active_month_share = 0.0` | A fill step that uses the wrong default for the 232 zero-activity customers, for example 0 instead of 365, or 0.0 instead of 1.0 for the trend | raise / non-zero exit |
| QC4 | Range limits: `tenure_days ≥ 1`; `days_since_last_txn` in [0, 365]; `has_ever_purchased` in {0, 1}; `txn_count_90d` in [0, 100] (observed maximum 9); `avg_spend_per_active_month_6m` in [0, `monthly_cap`], or [0, 2,000] when uncapped (the largest customer-month in the parquet is 1,753.23); `spend_trend_3m_vs_prior_3m` in [0, `ratio_cap`]; `active_month_share` in [0, 1] | A unit error, or a cap that wasn't applied | raise / non-zero exit |
| QC5 | No future data: every DB row reaching a transform has `txn_timestamp < as_of`, and every parquet month is before `as_of`'s month | An off-by-one filter that lets activity from the label window into the features | raise / non-zero exit |
| QC6 | Parquet coverage: for every month in a month-based window, the parquet's `txn_count`, `return_count` and `total_amount` (to within 0.01) equal the DB totals for every customer, and no customer-month appears in only one source | The late-July ETL gap (237 purchases from 2026-07-26 to 07-31 missing, 10 customers absent) silently undercounting at a later cutoff | raise / non-zero exit |
| QC7 | Drift against the stored copy: when the store already holds a version with identical parameters at the same `as_of`, the rebuilt values must equal the stored ones (to within 1e-9). On the first build there is nothing to compare, and the check reports "no reference" | A code change that alters a feature's values without a version bump, which would leave the store serving stale values under the same version number | raise / non-zero exit |
| QC8 | Totals reconcile with the sources: the population sum of `txn_count_90d` equals the number of DB rows for population customers in [as_of − 90 days, as_of), and the total of active months behind `active_month_share` (share × eligible months) equals the number of parquet rows for population customers in its window | A month or transaction counted twice, which the QC4 range limits cannot see (QC4's bound for `txn_count_90d` is 100 against an observed maximum of 9) | raise / non-zero exit |

## Corrections to the Stage 0 proposal

| Stage 0 said | This spec decides | Why |
|---|---|---|
| Left the as-of date open, although P4 and P5 reproduce the label at 2026-08-01 | The training cutoff is 2026-05-02, the start of the label window | Features cannot see any of the activity the label is computed from. |
| P2 `acquisition_channel` | Dropped | Its rationale, that customers acquired through paid channels are less loyal, is contradicted by the data: at 2026-05-02 paid_search has the *lowest* churn rate (7.9%, against 12.9% for organic). No reason specific to this dataset remains. It is dropped for lack of a mechanism, not lack of signal. |
| P3 `region` | Dropped | Nothing in the data explains why region would drive churn: there are no delivery, store or price fields. At 2026-05-02 east churns at 14.7% against 9.6–10.0% elsewhere, but a gap with no explanation is a correlation, not a rationale. |
| P6 `freq_ratio_recent_vs_baseline` | Dropped | It needed two different defaults and had tiny denominators: at 2026-05-02, 99 of the 1,167 buyers have a single purchase. Its job, comparing recent activity with the customer's own earlier activity, is done by F6 (recent quarter against prior quarter), with F4 and F7 in support. |
| P10 `return_share_12m` | Dropped | Returns are 5% of purchases, and at 2026-05-02, 829 of the 1,167 buyers have never returned anything. The share would mostly be 0 or based on 1–3 purchases. A return count is equally sparse: 338 buyers have any return. |
| P11 `category_breadth_180d` | Dropped | It is mostly a side effect of frequency: more purchases cover more categories by chance. Its rank correlation with the 180-day purchase count is 0.93 at 2026-05-02. |
| P4 default of `tenure_days` | A cap of 365, plus a new feature F3 `has_ever_purchased` | With `tenure_days`, 135 of the 232 never-buyers would get 90 or less and look like recent buyers (the real median is 24 days). |
| P7 as average monthly spend over all 6 months | Spend per active month | The all-months average is spend per active month × active months ÷ 6, so it contained frequency by construction. Rank correlation with F7 v2 at 2026-05-02: 0.78, against 0.47 for spend per active month (0.60 against 0.04 among the 1,149 customers active in the window). |
| P7 "consider winsorizing" | Versioned: v1 uncapped, v2 capped at 197.10 | The effect is measured rather than guessed: 157 customers change. The cap is per customer-month, not per order, because the parquet holds no order amounts: 75 of the 157 never had a capped month containing an order above 197.10. |
| P8 "0.0 when there is no spend in either period" (also this spec's first draft) | 1.0 | With no spend in either period there is no trend to measure, and 0.0 is kept for one meaning: spending stopped. 232 of the 250 never bought, and F3 marks them. This is a choice about meaning, not risk: their churn rate (14.7%) is close to the stopped group's (13.9%), so F3, not this feature, carries the never-bought signal. |
| P8 with an uncapped ratio | Capped, and versioned from 10 to 3 | The largest observed ratio was 85.9, caused by a tiny previous-quarter base. The version change acts only on the growth side (above 3), so it barely moves the ranking (rank correlation 0.999), and `VERSION_COMPARISON.md` must say so. |
| P9 as a raw count of active months over 12 months | Share of eligible months, renamed `active_month_share`, and versioned from 12 to 6 months | 569 of the 1,399 customers have fewer than 12 eligible months, so a raw count mostly measured tenure. |
| P7 and P9: "either end the window at June 2026 or switch to the DB" | No longer needed at 2026-05-02; the QC6 coverage check was added | The parquet is complete for every month used, and QC6 raises an error, on training and scoring alike, if that ever stops being true. |
