# Churn feature pipeline

A versioned feature pipeline for the e-commerce customer churn dataset. It builds 7 customer features as of
**2026-05-02** from three sources:
- `data/customers.csv`;
- `data/transactions.db`;
- `data/transactions_monthly_agg.parquet`.

It stores every version of every feature in a local feature store: a SQLite registry plus parquet snapshots.
Training and scoring both go through one function, `features.build.build_features(as_of, ...)`. The confirmed
design is in `feature_spec.md`, and the data is described in `data_dictionary.md`.

## Setup

These are the steps this build used: Python 3.11 in a conda environment. Run them from the repo root.

```bash
conda create -n churn-pipeline python=3.11
conda activate churn-pipeline
python -m pip install -r requirements.txt
```

No environment variables or credentials are needed. The build was run with these versions:

| Package | Version |
|---|---|
| Python | 3.11.17 |
| pandas | 3.0.6 |
| numpy | 2.4.6 |
| pyarrow | 26.0.0 |
| pytest | 9.1.1 |

The full list is in `outputs/artifacts/environment.txt`. Run every command below from the repo root, with the
environment active.

## The three scripts

**1. Build the training features**

```bash
python -m scripts.build_training_features
```

The script does three things:
- builds the 7 features for the 1,399 customers who signed up before 2026-05-02;
- runs quality checks QC1–QC8, and stops with exit code 1 if any check fails;
- registers each version listed in `features/schema.py` and rewrites `outputs/artifacts/FEATURE_LINEAGE.md`.

The store is already built and committed: `feature_store/registry.db` and 10 snapshots in `feature_store/values/`.
So a re-run prints "unchanged — skipped" for every version and changes nothing.

| Option | Default |
|---|---|
| `--as-of` | 2026-05-02 |
| `--store-dir` | `feature_store/` |
| `--lineage-out` | `outputs/artifacts/FEATURE_LINEAGE.md` |

**2. Score customers**

```bash
python -m scripts.score_customer --customer-id 697 --customer-id 1022
```

This computes the same 7 features for the customers you name, through the same `build_features` call and the same
checks, and prints them. It doesn't read the store.
- Pass `--customer-id` once per customer.
- `--as-of` defaults to 2026-05-02.
- `--format json` prints JSON instead of a table.

**3. Compare feature versions**

```bash
python -m scripts.compare_feature_versions
```

This loads both stored versions of each versioned feature:
- `avg_spend_per_active_month_6m`;
- `spend_trend_3m_vs_prior_3m`;
- `active_month_share`.

It writes `outputs/artifacts/VERSION_COMPARISON.md`. For each feature, the report gives the customers changed, the
distribution, the rank correlation, a threshold metric, and an interpretation.

## Tests

```bash
python -m pytest
```

This runs 156 tests covering:
- the transforms;
- the quality checks;
- the feature store;
- ingestion;
- train/score parity.

The tests use temporary stores. A session fixture fails the run if anything in the real `feature_store/` changes.
To keep a copy of the output, run `python -m pytest -v 2>&1 | tee <file>`.

## Regenerating the reports

With the store as committed, both commands reproduce the committed files exactly.

| Report | Command |
|---|---|
| `outputs/artifacts/FEATURE_LINEAGE.md` | `python -m scripts.generate_lineage` (every training run also rewrites it) |
| `outputs/artifacts/VERSION_COMPARISON.md` | `python -m scripts.compare_feature_versions` |

The build session wrote `PARITY_CHECK.md`, `ARTIFACT_NOTES.md`, `review_log.md` and `pytest_output.txt` in
`outputs/artifacts/`. No script generates them.

## Things to know

- **A store holds one as_of.** This store holds 2026-05-02 snapshots, and the training script refuses to build at
  any other date. To build at another date, pass a new directory with `--store-dir`.
- **An as_of of 2026-08-01 or later fails QC6, by design, for training and scoring alike.** The month windows then
  include July 2026. The parquet extract stops at an ETL cutoff on 2026-07-26 (`data_dictionary.md`, DQ2), so July
  is incomplete. The run stops with exit code 1 and writes nothing. An as_of of 2026-07-31 still works, because its
  month windows end in June.
- **Stored versions are never overwritten.** A re-run skips unchanged versions. A new version is registered only
  when its definition changes and its values differ for at least one customer.
- **Rerun training after any change to `features/schema.py`.** Scoring always uses the latest parameters in
  `schema.py`, not the store's, and nothing detects a mismatch. This limitation is accepted in `review_log.md` #2.
- **`active_month_share` v2 is the latest version, but its rise over v1 is mostly an artefact.** v2 uses a
  6-month window. Its mean rises by 0.054, but counted from each customer's first purchase rather than from signup,
  the mean would fall by 0.020. So the rise comes from idle months between signup and first purchase dropping out
  of the shorter window, not from customers buying more consistently. The spec already accepts this limitation.
  The details are in `VERSION_COMPARISON.md` and in §5 of the verification log.

## Where things are

| Path | Contents |
|---|---|
| `data/` | The three sources: `customers.csv` (1,500 customers, with the churn label), `transactions.db` (SQLite, 11,742 transactions) and `transactions_monthly_agg.parquet` (8,086 customer-months) |
| `features/` | `ingest.py`, `transforms.py`, `schema.py` (the features, their versions and parameters), `quality_checks.py` (QC1–QC8) and `build.py` (`build_features`) |
| `feature_store/` | `store.py`, `registry.db` and `values/<feature>__v<version>.parquet` |
| `scripts/` | The three scripts above, plus `generate_lineage.py` |
| `tests/` | The test suite |
| `outputs/artifacts/` | The build's reports, and the learner's evidence: `learner_verification_log.md`, `registry_query.txt`, `pytest_output_learner.txt`, `environment.txt`, the Stage 0 originals, the spec review and the build session's transcript |
| `feature_spec.md`, `data_dictionary.md` | The confirmed feature spec and the reviewed data dictionary |
| `skills/churn-feature-pipeline/` | The Claude skill that generated the pipeline, the store, the tests and the reports |

## Authorship

Claude drafted this README at the learner's request, in a Claude Code session separate from the build. Claude ran
each command above, apart from the environment setup, to check that it works. The verification log records where
else Claude was used and what the learner checked: `outputs/artifacts/learner_verification_log.md`, together with
`ARTIFACT_NOTES.md`.
