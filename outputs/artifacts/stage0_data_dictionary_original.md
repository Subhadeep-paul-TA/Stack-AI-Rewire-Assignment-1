# data_dictionary.md — Churn Feature Pipeline

> **Status: Stage 0 DRAFT. Best-effort, not authoritative.**
> Claude generated this on 2026-10-21 directly from the three raw sources (read-only), following
> `skills/churn-feature-pipeline/skill.md` Stage 0. It is inference from raw data, not a verified
> spec. Every claim carries a confidence tag:
> **[High]** checked across the whole dataset · **[Medium]** checked but interpretation involved ·
> **[Low]** guessed meaning or partially verified.
> The learner must review, spot-check and correct this file before `feature_spec.md` is drafted.
> Once confirmed, it is frozen for the build stage.
>
> Inputs used: `data/customers.csv`, `data/transactions.db` (table `transactions`),
> `data/transactions_monthly_agg.parquet`, and the business context stated in the Assignment Doc.
> No other business requirements document was available.

---

## 1. Business context

**Stated (Assignment Doc):**
- An e-commerce business. Retention marketing wants to target customers *before* they churn.
- The churn label is already provided in `customers.csv`. It means **no purchase in the 90 days before the
  as-of date, 2026-08-01**.
- The task is to engineer features a churn model would consume, not to train the model.

**Inferred from the data (not stated anywhere):**
- A multi-category retailer: 6 product categories with near-equal volumes and similar price
  distributions. **[Medium]**
- An omnichannel retailer: purchases happen through `web`, `mobile_app` and a physical `store`. **[High]** that these values exist;
  **[Low]** that `store` means a physical shop.
- Customers are acquired through 4 channels and live in 4 regions. **[High]** values; **[Low]** exact meaning.
- **The customer base and transaction volume grew strongly over the period.** Monthly transactions rose from 151
  (2025-01) to 1,231 (2026-07). **[High]**
- The currency of `amount` is not stated anywhere. **[Low]**: assume a single currency, with no conversion needed.
- Timestamps carry no timezone. **[Medium]**: treat them as naive, consistent local time.

---

## 2. Sources at a glance

| Source | Format | Grain | Rows | Customers | Time span | Role |
|---|---|---|---|---|---|---|
| `data/customers.csv` | CSV | 1 row per customer | 1,500 | 1,500 | signups 2024-02-14 → 2026-07-01 | Customer master + churn label |
| `data/transactions.db` › `transactions` | SQLite table | 1 row per transaction | 11,742 | 1,419 | 2024-02-29 18:44 → 2026-07-31 23:50 | Row-level transaction detail (source of truth) |
| `data/transactions_monthly_agg.parquet` | Parquet | 1 row per customer × calendar month **with activity** | 8,086 | 1,409 | months 2024-02 → 2026-07 | Pre-aggregated view of the *same* transactions. **Stops at an ETL cutoff on 2026-07-26** (see §5). |

None of the source files may be edited.

---

## 3. Schemas

### 3.1 `data/customers.csv`

| Column | Raw dtype (pandas read) | Recommended dtype | Nulls observed | Values / range | Meaning | Confidence |
|---|---|---|---|---|---|---|
| `customer_id` | int64 | int64 | 0 | 1–1,500, unique, contiguous | Customer key; joins to both transaction sources | [High] |
| `signup_date` | string (`YYYY-MM-DD`) | datetime64 (date) | 0 | 2024-02-14 → 2026-07-01 (713 distinct) | Date the customer account was created | [Medium] (see DQ8: purchases start a median 162 days later) |
| `acquisition_channel` | string | category | 0 | organic 609 · paid_search 431 · referral 295 · social 165 | Marketing channel that acquired the customer | [Medium] |
| `region` | string | category | 0 | east 385 · north 381 · west 380 · south 354 | Customer's geographic region | [Low] (granularity unknown) |
| `churned` | int64 | int8 (0/1) | 0 | 0: 1,298 · **1: 202 (13.5%)** | **Label.** 1 = no purchase in the 90 days before 2026-08-01. | [High] definition (stated); see §6 for how it behaves |

### 3.2 `data/transactions.db` › table `transactions`

The schema declares no primary key and no NOT NULL constraints, so every column is nullable at the schema level.
There is an index `idx_transactions_customer_id` on `customer_id`. The SQLite storage class is uniform for every row
(`integer, text, text, text, real, integer`).

| Column | Declared type | Recommended dtype | Nulls observed | Values / range | Meaning | Confidence |
|---|---|---|---|---|---|---|
| `customer_id` | INTEGER | int64 | 0 | 1,419 distinct; all exist in `customers.csv` | FK to customers | [High] |
| `txn_timestamp` | TEXT | datetime64[ns] via `pd.to_datetime` | 0 | `YYYY-MM-DD HH:MM:SS.fffffffff` (always 29 chars, nanosecond precision, no TZ) | Purchase timestamp | [High] format; [Medium] naive local time |
| `category` | TEXT | category | 0 | home 2,008 · grocery 1,979 · sports 1,969 · beauty 1,958 · apparel 1,917 · electronics 1,911 | Product category of the purchase | [High] |
| `channel` | TEXT | category | 0 | web 5,278 · mobile_app 4,146 · store 2,318 | Purchase channel. Unrelated to `acquisition_channel`. | [High] values; [Low] meaning of `store` |
| `amount` | REAL | float64 | 0 | 3.00 → 1,684.21; always 2 decimals; never ≤ 0. Median 33.82, mean 49.72, p95 143.44, p99 255.66, p99.9 518.34 | Purchase value. Gross: returned purchases keep a positive amount. | [High] values; [Medium] gross interpretation |
| `is_returned` | INTEGER | int8 (0/1) | 0 | 0: 11,151 · 1: 591 (5.0%) | 1 = this purchase was later returned. A flag on the original row, **not** a separate refund row. | [Medium] (no negative/refund rows exist, so this is the most consistent reading) |

Per-customer activity, among the 1,419 customers with at least one transaction: median 7 transactions, mean 8.3,
max 32. **94 customers have exactly one transaction.** **[High]**

### 3.3 `data/transactions_monthly_agg.parquet`

| Column | dtype | Nulls observed | Values / range | Meaning | Confidence |
|---|---|---|---|---|---|
| `customer_id` | int64 | 0 | 1,409 distinct | FK to customers | [High] |
| `txn_month` | datetime64[us] | 0 | Always first of month, 00:00. 2024-02-01 → 2026-07-01 | Calendar month the row aggregates | [High] |
| `txn_count` | int64 | 0 | 1–6 (never 0) | `COUNT(*)` of transactions in that month | [High] (reconciled, §5) |
| `total_amount` | float64 | 0 | 3.00 → 1,753.23 | `SUM(amount)` in that month, **gross** (includes returned purchases) | [High] (reconciled, §5) |
| `return_count` | int64 | 0 | 0–3 | `SUM(is_returned)` in that month | [High] (reconciled, §5) |

Key `(customer_id, txn_month)` is unique. **Months with no activity have no row.** There are no zero-count rows.
**[High]**

The parquet carries **no** `category`, `channel`, returned *amount* or intra-month timing. Any feature needing
those must come from `transactions.db`. **[High]**

---

## 4. Time dimensions

| Item | Detail | Confidence |
|---|---|---|
| As-of date | **2026-08-01** (stated). No source contains any row dated on or after it: the last transaction is 2026-07-31 23:50. | [High] |
| Timestamp columns | `customers.signup_date` (date). `transactions.txn_timestamp` (text → datetime, ns, no TZ). `monthly_agg.txn_month` (month start). | [High] |
| Grain | customers: static per customer. transactions: event. monthly_agg: customer × calendar month. | [High] |
| Coverage end | `transactions.db` runs to **2026-07-31 23:50**. The parquet runs to **2026-07-25 23:10** (ETL cutoff, §5). | [High] |
| Partial months | A parquet row represents the **whole** calendar month. For any `as_of` that is not the first of a month, that month's row contains activity on or after `as_of`. For any `as_of` after 2026-07-26, the July 2026 row is also incomplete. | [High] |
| Feature cutoff convention (proposed) | Features use only events with `timestamp < as_of`. Monthly rows are used only when the whole month ends before `as_of` **and** before the parquet's coverage end. | Proposal — learner to confirm |

---

## 5. How the three sources relate

- **Keys.** `customer_id` joins all three. No transaction or parquet `customer_id` is missing from
  `customers.csv` (no orphans). **[High]**
- **Same signal at two grains.** For every transaction **before 2026-07-26 00:00:00**, the parquet equals
  `GROUP BY customer_id, calendar_month` of `transactions`, with
  `txn_count = COUNT(*)`, `total_amount = SUM(amount)` and `return_count = SUM(is_returned)`. This matches for all
  8,086 rows, within float tolerance. **[High]**
- **The sources disagree at the edge: an ETL cutoff.**
  - The last transaction included in the parquet is 2026-07-25 23:10:04. The first excluded is 2026-07-26 00:01:19.
    **237 transactions** (07-26 → 07-31) are missing from the parquet. **[High]**
  - Effect on July 2026: **102 customer-months are absent** and **110 are undercounted**. The parquet has 685 July rows,
    while 787 customers were active in July in the DB. **[High]**
  - **10 customers are absent from the parquet entirely.** Each has exactly one lifetime purchase, dated 07-26 → 07-30.
    A parquet-based feature would treat them as zero-activity customers. **[High]**
  - All months before July 2026 agree exactly. **[High]**
- **Float noise.** 583 parquet `total_amount` values are not exactly 2-decimal, an artefact of float summation.
  Any reconciliation must compare with a tolerance (≥ 0.01), not with equality. **[High]**
- **Practical consequence (proposed):** any feature whose window can include the last ~6 days before `as_of`, or
  is not aligned to calendar months, should use `transactions.db`. The parquet is only safe for whole months
  fully covered before its cutoff. **[Medium]** (this is a judgment, not a fact)

---

## 6. The churn label: how it actually behaves

- **Definition check.** "No transaction in the trailing 90 days before 2026-08-01" reproduces `churned` for
  **1,499 of 1,500** customers. **[High]**
- **The one exception is customer 697.** Their last purchase was 2026-05-02 17:12, which is 90.3 days before the as-of date, yet
  `churned = 0`. This is consistent with a **whole-day floor**: active if `(as_of − last_txn).days ≤ 90`. That makes
  the label window roughly `[2026-05-02, 2026-08-01)`. **[Medium]** (inferred from a single boundary case)
- **⚠ Leakage implication.** Because the label is defined *entirely by activity in the trailing 90 days before
  the as-of date*, any feature computed at as_of = 2026-08-01 from that same window reproduces the label almost
  exactly. That includes days since last transaction, transaction count over ≤ 90 days, and "any purchase in the last N ≤ 90 days".
  Longer windows that include those 90 days leak partially. **How `as_of` is chosen for the
  training snapshot is an open decision for `feature_spec.md`.** **[High]** that the overlap exists;
  the resolution is the learner's call.
- **The label mixes "lapsed" with "never activated".** All 81 zero-activity customers are `churned = 1`.
  **47 of them signed up within the last 90 days** and have never purchased. The label does not separate them
  from customers who bought and then stopped. **[High]** observation; **[Low]** whether that is intended.
- Descriptive churn rate by segment (no modelling implied): organic 15.3% · paid_search 10.2% · referral 13.9% ·
  social 14.5%; east 16.4% · north 12.3% · south 12.7% · west 12.4%. **[High]** numbers; **[Low]** that they mean anything.

---

## 7. Data-quality realities observed

| # | Reality | Evidence | Confidence |
|---|---|---|---|
| DQ1 | **81 customers have zero transactions.** All are churned. Signup ages at 2026-08-01: 29 at 31–60 days, 18 at 61–90, 32 at 91–180, 1 at 181–365, 1 over 365. | Anti-join customers ↔ transactions | [High] |
| DQ2 | **The parquet stops at an ETL cutoff on 2026-07-26.** 237 transactions are missing; July 2026 is incomplete; 10 customers are missing entirely. | Full reconciliation (§5) | [High] |
| DQ3 | The parquet has no rows for zero-activity months. A customer × month grid must be reindexed with explicit 0s. | No `txn_count = 0` rows exist | [High] |
| DQ4 | `amount` is right-skewed (median 33.82 vs max 1,684.21; p99 255.66). Means and sums are sensitive to a few large orders. The skew is similar across categories. | Quantiles, per-category describe | [High] |
| DQ5 | Returns are sparse: 591 returned transactions (5.0%). 466 of 1,419 active customers have ever returned anything. The per-customer return-count distribution is 0: 953 · 1: 371 · 2: 71 · 3: 19 · 4: 4 · 5: 1. | Group-by | [High] |
| DQ6 | Short windows are sparse. Customers active in the last 30/60/90/180/365 days before 2026-08-01: 767 / 1,120 / 1,297 / 1,402 / 1,418. | Window counts | [High] |
| DQ7 | 94 customers have exactly one transaction. Ratio and trend features have tiny or zero denominators for them. | Group-by | [High] |
| DQ8 | **There is a long gap between signup and first purchase**: min 1 day, median 162, mean 194, max 841. Either `signup_date` is account creation long before first purchase, or it means something else. Tenure since signup ≠ purchasing tenure. | signup vs first `txn_timestamp` | [Low] (meaning unclear) |
| DQ9 | Volume grows strongly over time (151 → 1,231 transactions/month, 2025-01 → 2026-07). A drift check against a snapshot from a different as-of date will flag growth as drift. Reference snapshots must be comparable. | Monthly counts | [Medium] |
| DQ10 | `txn_timestamp` is ISO-8601 **text** with nanosecond precision and no timezone. It must be parsed before any date arithmetic. | `typeof()` + string lengths | [High] |
| DQ11 | Parquet `total_amount` has float-summation noise (583 values). | Decimal check | [High] |

**Checked and *not* found:** nulls in any column of any source; duplicate transactions; zero or negative
amounts; orphan `customer_id`s; transactions dated before the customer's `signup_date`; signups after the
as-of date; any event on or after 2026-08-01. **[High]**

---

## 8. Rules for downstream use (proposed — learner to confirm or change)

1. Never edit the three source files. Note any defect found in `ARTIFACT_NOTES.md`.
2. Parse `txn_timestamp` and `signup_date` to datetimes in ingestion, not in transforms.
3. Features use only events strictly before `as_of`. Parquet months are used only when fully before `as_of` and
   fully covered by the parquet (§4, §5).
4. Every feature has an explicit zero-activity default (DQ1, DQ3). No NaN may survive the build.
5. The scoring/training population at a given `as_of` should be customers with `signup_date < as_of`. At 2026-08-01
   that is all 1,500. At earlier dates it is fewer. **[Medium]** (proposal)
6. The training snapshot's `as_of` must be chosen so that features do not encode the label window (§6). The decision belongs in `feature_spec.md`.
7. Compare monetary values across sources with a tolerance, never with equality (DQ11).

---

## 9. Open questions and low-confidence items for the learner's review

1. Is the label window `(as_of − 91 days, as_of)` with whole-day flooring (customer 697), or exactly 90 days? (§6)
2. Is it intended that never-purchased recent signups count as churned (47 customers)? (§6, DQ1)
3. What does `signup_date` represent, given the median 162-day gap to first purchase? (DQ8)
4. Is `store` a physical channel? Is `amount` gross of returns, as the reconciliation implies? (§3.2)
5. What currency is `amount` in, and does `region` represent anything finer-grained? (§1, §3.1)
6. Should the parquet be used at all for any as-of date after 2026-07-26, or only for months before its cutoff? (§5)
