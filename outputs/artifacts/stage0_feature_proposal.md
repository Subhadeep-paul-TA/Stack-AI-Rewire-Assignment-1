# Stage 0 — Candidate-Feature Proposal (Claude-generated draft)

> **Status: best-effort raw material, not a spec.** Claude generated this on 2026-10-21 from the Stage 0
> `data_dictionary.md`, following `skills/churn-feature-pipeline/skill.md` Stage 0, step 3. It is meant to be
> reacted to and corrected. It does **not** decide the feature list, the defaults, the versioning or the
> as-of convention. Those belong to the learner's `feature_spec.md`, reviewed against skill.md's *Domain Context*.
> Each candidate carries a confidence tag, and weak spots are flagged on purpose rather than smoothed over.

---

## 0. Read this first: a leakage problem affects several candidates

The label is "no purchase in the 90 days before 2026-08-01" (`data_dictionary.md` §6). Any candidate below
marked **⚠ LABEL-OVERLAP** reproduces the label nearly exactly *if computed at as_of = 2026-08-01*. Some
reproduce it fully, such as `days_since_last_txn > 90`, which is equivalent to `churned = 1`. Others reproduce it partially, through
windows that include those 90 days. Two broad ways out exist, and the choice is the learner's:
1. Compute the label-paired training snapshot at an earlier as-of date, the start of the label window,
   so every feature window ends before the label window begins.
2. Keep 2026-08-01 and drop or reshape every overlapping feature.

The candidates below are written generically in terms of `as_of`, so they work under either choice.

## 1. Source-selection rule used in this proposal

- **`transactions.db` (row-level):** exact recency, windows in days, anything needing `category`, `channel`,
  returned amounts, or any window that could include the last ~6 days before `as_of`. Those days are missing from the parquet
  after its 2026-07-26 cutoff.
- **`transactions_monthly_agg.parquet`:** month-grain features over **complete** calendar months that end before
  `as_of` *and* before the parquet's coverage end. Missing months must be reindexed to 0.
- **`customers.csv`:** static profile attributes. No source ambiguity.

## 2. Summary

| # | Candidate | Family | Source (alternative) | Suggested zero-activity default | Label overlap at 2026-08-01 | Confidence |
|---|---|---|---|---|---|---|
| P1 | `tenure_days` | Profile | customers.csv | n/a (always defined) | No | Medium |
| P2 | `acquisition_channel` | Profile | customers.csv | n/a | No | Low |
| P3 | `region` | Profile | customers.csv | n/a | No | **Low (possible filler)** |
| P4 | `days_since_last_txn` | Recency | transactions.db (parquet can't do it) | `tenure_days` | **⚠ Full** | High signal / high leakage |
| P5 | `txn_count_90d` | Frequency | transactions.db (parquet only if month-aligned) | 0 | **⚠ Full** | High signal / high leakage |
| P6 | `freq_ratio_recent_vs_baseline` | Frequency (trend) | transactions.db | 0.0 / 1.0 (two cases) | ⚠ Partial | Medium |
| P7 | `avg_monthly_spend_6m` | Monetary | parquet (or db) | 0.0 | ⚠ Partial | Medium |
| P8 | `spend_trend_3m_vs_prior_3m` | Monetary (trend) | parquet (or db) | 0.0 / 1.0 (two cases) | ⚠ Partial | Medium |
| P9 | `active_months_12m` | Engagement consistency | parquet (or db) | 0 | ⚠ Partial | Medium–High |
| P10 | `return_share_12m` | Return / quality | transactions.db (or parquet `return_count`) | 0.0 | ⚠ Partial | Low–Medium |
| P11 | `category_breadth_180d` | Engagement breadth | transactions.db only | 0 | ⚠ Partial | Low–Medium |

---

## 3. Candidates

### P1 — `tenure_days` · Profile · **[Medium]**
- **Source:** `customers.csv.signup_date`. No alternative source.
- **Computation:** `(as_of − signup_date).days`. The population is assumed to be customers with `signup_date < as_of`.
- **Zero-activity default:** not applicable. Every customer has a signup date.
- **Rationale:** a recently signed-up customer hasn't formed a purchase habit, so low activity means something
  different for them than for a long-standing customer. That makes tenure most useful as a **confound control**: it lets a
  model separate "new and not yet active" from "established and slowing down".
- **Caveat:** DQ8. First purchases come a median of 162 days after signup, so signup tenure may not reflect
  *purchasing* tenure. An alternative is days since first purchase (row-level), but that is undefined for 81 customers.

### P2 — `acquisition_channel` · Profile · **[Low]**
- **Source:** `customers.csv`. Categorical; encoding is left to the model.
- **Rationale (generic, flagged):** customers acquired through paid channels are often assumed to be promotion-driven and less
  loyal than organic or referral customers.
- **Caveat:** the data does **not** support that generic story. paid_search has the *lowest* descriptive churn
  rate (10.2%) and organic the highest (15.3%). As written, the rationale is filler. It needs a real,
  dataset-specific reason, or the feature should be dropped.

### P3 — `region` · Profile · **[Low — possible filler]**
- **Source:** `customers.csv`. Categorical.
- **Rationale:** none dataset-specific. East shows 16.4% churn vs ~12.5% elsewhere, but nothing in the data explains
  *why* region would cause churn (no delivery times or store density, for example).
- **Caveat:** included only for completeness. It fails the "why, specifically?" test in Domain Context as it stands.

### P4 — `days_since_last_txn` · Recency · **⚠ LABEL-OVERLAP (full)** · **[High signal]**
- **Source:** `transactions.db.txn_timestamp`. The parquet cannot express exact recency, only month grain.
- **Computation:** `(as_of − max(txn_timestamp where txn_timestamp < as_of)).days` (whole-day floor, matching the
  label's apparent convention).
- **Zero-activity default (suggested):** `tenure_days`, interpreted as "inactive since signup". The alternative is a fixed cap
  plus a separate `has_ever_purchased` flag.
- **Rationale:** a lengthening gap since the last purchase is the most direct behavioural sign that a customer
  has stopped buying.
- **Caveats:** at as_of = 2026-08-01 this feature *is* the label (> 90 ⇔ churned). The suggested default mixes
  recency with tenure, which needs review. A `has_ever_purchased` flag would perfectly identify the 81
  zero-activity customers, all of whom are churned.

### P5 — `txn_count_90d` · Frequency · **⚠ LABEL-OVERLAP (full)** · **[High signal]**
- **Source:** `transactions.db`. The parquet could only approximate this with 3 whole months, and a 90-day window isn't month-aligned.
- **Computation:** count of transactions with `as_of − 90 days ≤ txn_timestamp < as_of`.
- **Zero-activity default:** 0.
- **Rationale:** fewer recent purchases means a weaker current relationship.
- **Caveats:** at 2026-08-01, `count == 0` ⇔ churned. The rationale also doesn't separate a habitually
  low-frequency customer from one who is slowing down; P6 tries to.

### P6 — `freq_ratio_recent_vs_baseline` · Frequency trend · **⚠ partial overlap** · **[Medium]**
- **Source:** `transactions.db`.
- **Computation:** `(txns in last 90 days) / (txns per 90 days over days 91–360 before as_of)`.
- **Zero-activity default (suggested):** 0.0 when the customer never purchased. When there is recent activity but no baseline history (a new
  customer), 1.0, meaning no evidence of decline.
- **Rationale:** a drop in frequency **relative to the customer's own baseline** points to a customer who is
  disengaging. It is distinct from a customer who has always bought rarely, who would show a ratio near 1.0
  despite a low absolute count.
- **Caveats:** two different defaults is a design choice that needs justification. Small denominators affect
  94 single-transaction customers (DQ7). The recent window overlaps the label window at 2026-08-01.

### P7 — `avg_monthly_spend_6m` · Monetary · **⚠ partial overlap** · **[Medium]**
- **Source:** parquet `total_amount` over the last 6 **complete** months before `as_of`, with missing months reindexed to 0.
  - *Alternative:* `transactions.db`, summing `amount` by month.
  - *Why the parquet:* the window is month-aligned, which is what the aggregate is for.
  - *Caveat:* at as_of = 2026-08-01, July 2026 is incomplete in the parquet (ETL cutoff). The window must then either end at
    June 2026 or switch to the DB.
- **Computation:** `sum(total_amount over 6 months) / 6`. Gross of returns, because the parquet lacks returned amounts.
- **Zero-activity default:** 0.0.
- **Rationale:** spend level indicates how much of the customer's wallet the store holds. Low-spend customers have
  less invested in the relationship.
- **Caveats:** heavy right skew (DQ4). A few large orders dominate means. Consider winsorizing per-month or
  per-transaction values. The rationale is fairly generic: low spend is also simply correlated with low frequency.

### P8 — `spend_trend_3m_vs_prior_3m` · Monetary trend · **⚠ partial overlap** · **[Medium]**
- **Source:** parquet (complete months only, reindexed to 0), with the same as-of caveat as P7. Alternative: `transactions.db`.
- **Computation:** `spend(last 3 complete months) / spend(the 3 months before that)`.
- **Zero-activity default (suggested):** 0.0 when there is no spend in either period. When there is spend recently but none in the prior period, 1.0 or a cap.
- **Rationale:** a declining share of the customer's *own* prior spend signals trading down or moving purchases
  elsewhere. That is distinct from a customer whose spend has always been low.
- **Caveats:** the ratio is unstable for low spenders. The defaults are a judgment call. It overlaps P6 conceptually, so is it the same signal in currency instead of counts?

### P9 — `active_months_12m` · Engagement consistency · **⚠ partial overlap** · **[Medium–High]**
- **Source:** parquet. Count of distinct `txn_month` rows across the last 12 complete calendar months before `as_of`.
  - *Why the parquet:* "was the customer active in month M" is exactly the parquet's grain.
  - *Caveat:* for as_of = 2026-08-01, **102 customers' July 2026 activity is missing** from the parquet. This feature would
    undercount them unless the window ends before the ETL cutoff month or the DB is used instead.
- **Zero-activity default:** 0.
- **Rationale:** steady month-after-month activity reflects a habit. A customer who was active in 11 of 12 months and then
  stops is a different risk from one who was always sporadic. Consistency captures *breadth over time* that a
  single-window count misses.
- **Caveat:** for customers with less than 12 months of tenure, the maximum is capped by tenure. Consider normalizing by
  the number of eligible months.

### P10 — `return_share_12m` · Return / quality · **⚠ partial overlap** · **[Low–Medium]**
- **Source:** `transactions.db` (`is_returned`) over the last 365 days.
  - *Alternative:* parquet `return_count / txn_count` over 12 complete months, which carries the same ETL-cutoff caveat.
- **Computation:** `returned txns / txns` in the window.
- **Zero-activity default:** 0.0. This includes active customers with no transactions in the window, which needs review.
- **Rationale:** a high share of returned purchases signals unmet expectations, a dissatisfaction proxy that
  can precede leaving. It is distinct from low spend: a customer can buy often and still return a lot.
- **Caveats:** returns are sparse (5% of transactions; 953 of 1,419 customers never returned anything). Small
  denominators make the share noisy. A minimum-transaction threshold, or a count instead of a share, may be better.

### P11 — `category_breadth_180d` · Engagement breadth · **⚠ partial overlap** · **[Low–Medium]**
- **Source:** `transactions.db` only. `category` is not in the parquet.
- **Computation:** number of distinct `category` values purchased in the last 180 days.
- **Zero-activity default:** 0.
- **Rationale:** a customer who relies on the store across several needs (grocery *and* home *and* beauty) has
  higher switching costs than a single-category shopper.
- **Caveat:** strongly confounded with frequency, since more purchases give more categories by chance. It would need a
  normalization (e.g. distinct categories per transaction) to stand as its own signal.

---

## 4. Tunable parameters that could support the versioning exercise (options, not choices)

The skill requires **at least 2 distinct features × at least 2 versions**, each version differing in a real parameter with a
business reason. Parameters these candidates naturally expose:

| Candidate | Parameter | Example values | Material change? (evidence) |
|---|---|---|---|
| P5 | `window_days` | 30 / 60 / 90 / 180 | Likely. Customers active in the window at 2026-08-01: 767 / 1,120 / 1,297 / 1,402 |
| P6 | `baseline_days` | 180 vs 270 | Unverified |
| P7 | `lookback_months` or `winsor_cap` | 3 vs 6 months; no cap vs p99 per transaction (255.66) | Cap: likely affects only a small tail; unverified |
| P9 | `lookback_months` | 6 vs 12 | Unverified |
| P10 | `window_days` or `min_txns` | 180 vs 365; 1 vs 3 | Unverified |

Whatever the learner picks, check that the versions actually differ for a meaningful number of customers
before committing. Otherwise the bump is cosmetic by Domain Context's definition.

## 5. Things this proposal deliberately leaves open

- The as-of convention for the training snapshot (§0).
- The final feature list. P2, P3 and P11 are the weakest, and some of P4–P9 overlap conceptually.
- Every zero-activity default above. They are suggestions, and P4, P6 and P8 are the most debatable.
- Data-quality checks. The `data_dictionary.md` realities (DQ1–DQ11) suggest which failure modes are plausible.
- Parity design and the window-boundary convention (inclusive or exclusive, whole days or timestamps).
