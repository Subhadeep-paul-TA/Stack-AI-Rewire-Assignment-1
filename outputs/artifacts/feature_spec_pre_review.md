# feature_spec.md — Churn Feature Pipeline

> **Authorship.** Every decision here is mine: the feature set, the as_of date, the defaults, the sources and the
> versioning. I made them by answering Claude's questions on 2026-10-21. Claude drafted the text from my answers,
> with my instructor's approval. I reviewed it.

## Scope and conventions

| Term | Definition |
|---|---|
| `as_of` | The feature cutoff, and the pipeline's single parameter. Every feature uses only data from before it. Both scripts default to **2026-05-02**. |
| Label date | 2026-08-01. This belongs to the provided `churned` column, not to the pipeline. `churned = 1` means no purchase in roughly 2026-05-02 → 2026-08-01 (`data_dictionary.md` §6). |
| Why 2026-05-02 | It is the start of the label window, so a snapshot cut here sees none of the activity the label is computed from. At 2026-08-01, `days_since_last_txn` and `txn_count_90d` would reproduce the label almost exactly. |
| Population | Customers with `signup_date < as_of`. At 2026-05-02: **1,399 customers**. The 101 who signed up later are excluded. 155 of the 1,399 churn (11.1%). |
| Zero-activity customer | A customer with no transaction where `txn_timestamp < as_of`. At 2026-05-02 there are **232**, and 34 of them churn. |
| Day windows | `[as_of − N days, as_of)`: the start is included, the end is excluded, and comparisons use full timestamps. |
| Recency | Whole days, rounded down: `floor((as_of − t) / 1 day)`. This matches how the label appears to count days. |
| Month windows | The N complete calendar months before the month containing `as_of`. At 2026-05-02 the last month used is 2026-04. The 39 purchases on 2026-05-01 count in day windows but not in month windows. |
| Leakage controls | `churned` is never an input to a feature, and `build_features` never returns it. No row timestamped at or after `as_of`, and no month at or after `as_of`'s month, reaches a transform (check DQ5). |

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
  stands at the cutoff, not their average pace. The median gap between purchases is 25 days, so a customer 120 days
  out is well past their usual rhythm, whatever their history.

### F3 — `has_ever_purchased`
- **Feature**: `has_ever_purchased`
- **Source**: `data/transactions.db`, columns `transactions.customer_id` and `txn_timestamp`
- **Computation**: 1 if the customer has at least one transaction before `as_of`, otherwise 0.
- **Zero-activity default**: 0. This feature is itself the zero-activity indicator, so it is defined for everyone.
- **If multiple sources could supply this, which and why?**: the DB. The parquet could also show whether any purchase
  exists, but it misses the 10 customers whose only purchase falls in its 2026-07-26 → 07-31 gap. The DB is
  complete at any cutoff.
- **Rationale**: This predicts churn because customers who have never bought are an activation problem: they
  haven't decided to start. That is different from a lapsed customer, who had a purchase habit to lose. They form
  a sizeable separate group (232 at 2026-05-02, 34 of whom churn). The flag also stops F2's 365 and the other
  defaults from being read as real behaviour.

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
  reading it alongside F7 and F1. A low count with a high historical share of active months is a slowdown. A low
  count with a low share is a habitually infrequent buyer. 90 days covers about three typical purchase cycles
  (median gap 25 days). At 2026-05-02, 1,049 of the 1,399 customers have at least one purchase in the window.

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
  months, which is exactly what the parquet stores, and DQ6 proves that every month used is complete and matches
  the DB. Rebuilding monthly totals from DB rows would only duplicate what the parquet already holds.
- **Rationale**: This predicts churn because customers whose usual monthly basket here is small probably spend most
  of their money elsewhere, so leaving costs them little. That is different from purchase frequency, because it
  averages only over months the customer actually bought. A customer who buys once a quarter but spends 150 each
  time scores high here and low on F4 and F7. At 2026-05-02 its rank correlation with F7 is 0.47. The Stage 0
  version, which averaged over all six months, scored 0.80, so it was mostly measuring frequency again.

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
  windows are whole calendar months, and DQ6 guarantees they are complete.
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
  answered by a single parquet row, and DQ6 guarantees the months are complete. The DB would only rebuild the same
  monthly totals.
- **Rationale**: This predicts churn because customers who buy in most of their available months have a habit that
  one quiet month won't break, while customers who buy in only a few can drift away unnoticed. Unlike a raw count,
  the share only counts months since signup, so it isn't just measuring tenure. A customer active in all 3 of
  their 3 months scores 1.0, while an established customer active in 3 of 12 scores 0.25. At 2026-05-02, 161
  customers have exactly 3 active months in the 12-month window, with anywhere from 3 to 12 eligible months.

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
- **Parity test**: `test_parity.py` builds the full population, then single customers, at the same `as_of`, and
  checks that all 7 features match exactly. It runs once with the latest parameters and once with each versioned
  feature's v1 parameters. It must include:
  - a zero-activity customer;
  - a customer with a purchase on 2026-05-01, which counts in F4 but falls outside the month windows;
  - customer 697, whose purchase at 2026-05-02 17:12 must be excluded because it is at or after `as_of`.

## Feature store, versioning & lineage

- **Features versioned**: three features, with two versions each: `avg_spend_per_active_month_6m`,
  `spend_trend_3m_vs_prior_3m` and `active_month_share`. The other four features are registered once, as v1.

| Feature | Parameter | v1 | v2 (latest) | Why v2 differs from v1 | Effect at 2026-05-02 |
|---|---|---|---|---|---|
| `avg_spend_per_active_month_6m` | `monthly_cap` | none | 197.10 | Monthly spend is right-skewed: one customer-month reaches 1,753.23. Without a cap, a single large order makes an occasional buyer look like a big regular. The cap is the 95th percentile of customer-month spend before 2026-05, fixed as a constant, and keeps the feature about typical basket size. | 157 customers change. The maximum falls from 501.94 to 197.10. Rank correlation between versions is 0.997, so expect little effect on rank-based models and more on linear models, which are sensitive to outliers. |
| `spend_trend_3m_vs_prior_3m` | `ratio_cap` | 10 | 3 | Ratios above 3 come from a tiny previous quarter, often a single small order; the largest observed is 85.9. For retention targeting the useful signal is on the decline side (below 1). Growth beyond 3× adds no information but would dominate the feature's scale. | 162 customers change. The mean falls from 1.51 to 1.14. Rank correlation is 0.999. |
| `active_month_share` | `lookback_months` | 12 | 6 | A 12-month window still credits a habit that ended months ago. With a median gap of 25 days between purchases, 6 months still covers about 7 purchase cycles, which is enough to judge consistency while reflecting the customer's current habit. | 890 customers change. The mean rises from 0.333 to 0.387. Rank correlation is 0.849. |

- **Registration**:
  - `build_training_features` builds every version listed in `schema.py` through `build_features`, oldest first.
  - It runs every quality check on every version before registering anything.
  - It then registers the versions oldest first, so `get_latest()` returns v2, the definition this spec prefers.
  - Each new version gets a new registry row and a new parquet file. `is_latest` is updated in the same
    transaction as the insert.
- **What forces a version bump vs. not**:
  - A new version is created only when a parameter value, the source or the computation changes **and** the values
    differ for at least one customer.
  - Renaming a feature, editing its description, or re-running an unchanged definition at the same `as_of` does not
    create a version. The re-run case is reported as "unchanged — skipped" and writes nothing.
  - Existing rows and files are never modified.
- **One as_of per store**: every stored snapshot has `as_of_date = 2026-05-02`. Registering at a different `as_of`
  into a store that already holds another date raises an error, so snapshot dates never mix within one version
  history.
- **What a lineage entry must let someone reconstruct**: for every feature *and every stored version*, enough to
  recompute the values by hand from the raw files without reading code:
  - the source file, table and columns;
  - the population rule;
  - the exact window bounds relative to `as_of`, and the dates they translate to for this snapshot;
  - the aggregation formula and parameter values;
  - the zero-activity default and when it applies;
  - the rationale, and why this version differs from the previous one;
  - the registry row, `parquet_path`, row count and `as_of_date`.

  `FEATURE_LINEAGE.md` is generated from `schema.py` and the registry, with v1 and v2 as separate entries.

## Multi-source judgment calls

**Rule:** use the DB for anything measured in days or needing exact timestamps. Use the parquet only for complete
calendar months that end before `as_of`'s month and pass the DQ6 coverage check. Never mix the two sources within
one feature.

| Feature | Could come from | Chosen | Why |
|---|---|---|---|
| `tenure_days` | customers.csv only | customers.csv | It is the only source of `signup_date`. |
| `days_since_last_txn` | DB only | DB | It needs exact timestamps, and the parquet only records months. |
| `has_ever_purchased` | DB or parquet | DB | The parquet misses 10 customers whose only purchase falls in its late-July gap. The DB is complete at any cutoff. |
| `txn_count_90d` | DB or parquet | DB | 90 days isn't aligned to calendar months, so the parquet would drop part of the first month or reach past `as_of`. |
| `avg_spend_per_active_month_6m` | DB or parquet | parquet | It is defined on whole calendar months, which is how the parquet is stored. |
| `spend_trend_3m_vs_prior_3m` | DB or parquet | parquet | It compares two 3-month blocks of whole calendar months. |
| `active_month_share` | DB or parquet | parquet (+ customers.csv) | "Active in month M" is a single parquet row. |

**Why the parquet is safe at 2026-05-02:**
- Its only gap is 2026-07-26 → 07-31 (`data_dictionary.md` §5).
- All 12 months the month-based features use (2025-05 → 2026-04) match the DB exactly: 4,968 customer-months,
  0 mismatches.
- At any cutoff whose windows include July 2026, DQ6 stops the build with an error instead of silently
  undercounting.

## Data-quality checks

Every check runs on every built version before anything is registered. Each check reports which rows or features
failed. If any check fails, it raises `QualityCheckError`, `build_training_features` exits with a non-zero code,
and nothing is written to the store.

| # | Check | What it catches in this dataset | On failure |
|---|---|---|---|
| DQ1 | The row count equals the population (1,399 at 2026-05-02) in training, or the number of requested ids in scoring, and `customer_id` is unique | A merge against monthly rows that duplicates customers, or an inner join that drops the 232 zero-activity customers | raise / non-zero exit |
| DQ2 | No nulls in any of the 7 feature columns after defaults are applied | A transform that returns NaN for customers missing from a source (no DB rows or no parquet rows) | raise / non-zero exit |
| DQ3 | Zero-activity defaults are applied: every customer with no transaction before `as_of` has exactly `days_since_last_txn = 365`, `has_ever_purchased = 0`, `txn_count_90d = 0`, `avg_spend_per_active_month_6m = 0.0`, `spend_trend_3m_vs_prior_3m = 1.0` and `active_month_share = 0.0` | A fill step that uses the wrong default for the 232 zero-activity customers, for example 0 instead of 365, or 0.0 instead of 1.0 for the trend | raise / non-zero exit |
| DQ4 | Range limits: `tenure_days ≥ 1`; `days_since_last_txn` in [0, 365]; `has_ever_purchased` in {0, 1}; `txn_count_90d` in [0, 100] (observed maximum 9); `avg_spend_per_active_month_6m` in [0, `monthly_cap`], or [0, 2,000] when uncapped (the largest customer-month in the parquet is 1,753.23); `spend_trend_3m_vs_prior_3m` in [0, `ratio_cap`]; `active_month_share` in [0, 1] | A unit error, a month counted twice, or a cap that wasn't applied | raise / non-zero exit |
| DQ5 | No future data: every DB row reaching a transform has `txn_timestamp < as_of`, and every parquet month is before `as_of`'s month | An off-by-one filter that lets activity from the label window into the features | raise / non-zero exit |
| DQ6 | Parquet coverage: for every month in a month-based window, the parquet's `txn_count`, `return_count` and `total_amount` (to within 0.01) equal the DB totals for every customer, and no customer-month appears in only one source | The late-July ETL gap (237 purchases from 2026-07-26 to 07-31 missing, 10 customers absent) silently undercounting at a later cutoff | raise / non-zero exit |
| DQ7 | Drift against the stored copy: when the store already holds a version with identical parameters at the same `as_of`, the rebuilt values must equal the stored ones (to within 1e-9). On the first build there is nothing to compare, and the check reports "no reference" | A code change that alters a feature's values without a version bump, which would leave the store serving stale values under the same version number | raise / non-zero exit |

## Corrections to the Stage 0 proposal

| Stage 0 said | This spec decides | Why |
|---|---|---|
| Left the as-of date open, although P4 and P5 reproduce the label at 2026-08-01 | The training cutoff is 2026-05-02, the start of the label window | Features cannot see any of the activity the label is computed from. |
| P2 `acquisition_channel` | Dropped | Its rationale, that customers acquired through paid channels are less loyal, is contradicted by the data: paid_search has the *lowest* churn rate (10.2%). No reason specific to this dataset remains. |
| P3 `region` | Dropped | Nothing in the data links region to churn. A gap of a few points with no explanation is a correlation, not a rationale. |
| P6 `freq_ratio_recent_vs_baseline` | Dropped | It needed two different defaults and had tiny denominators (94 customers have a single purchase). Its job, telling a slowdown apart from a habitually rare buyer, is covered by reading F4 alongside F7. |
| P10 `return_share_12m` | Dropped | Returns are 5% of purchases, and 953 of the 1,419 customers who have bought never returned anything. The share would mostly be 0 or based on 1–3 purchases. |
| P11 `category_breadth_180d` | Dropped | It is mostly a side effect of frequency: more purchases cover more categories by chance. |
| P4 default of `tenure_days` | A cap of 365, plus a new feature F3 `has_ever_purchased` | With `tenure_days`, 135 of the 232 never-buyers would get 90 or less and look like recent buyers (the real median is 24 days). |
| P7 as average monthly spend over all 6 months | Spend per active month | The all-months average had a rank correlation of 0.80 with F7, so it mostly measured frequency. Spend per active month scores 0.47. |
| P7 "consider winsorizing" | Versioned: v1 uncapped, v2 capped at 197.10 | The effect is measured rather than guessed: 157 customers change. |
| P8 "0.0 when there is no spend in either period" (also this spec's first draft) | 1.0 | 232 of those 250 customers never bought, and only 34 of them churn. A value of 0.0 would look like a collapse, so 0.0 now means only "spending stopped". |
| P8 with an uncapped ratio | Capped, and versioned from 10 to 3 | The largest observed ratio was 85.9, caused by a tiny previous-quarter base. |
| P9 as a raw count of active months over 12 months | Share of eligible months, renamed `active_month_share`, and versioned from 12 to 6 months | 569 of the 1,399 customers have fewer than 12 eligible months, so a raw count mostly measured tenure. |
| P7 and P9: "either end the window at June 2026 or switch to the DB" | No longer needed at 2026-05-02; the DQ6 coverage check was added | The parquet is complete for every month used, and DQ6 raises an error if that ever stops being true. |
