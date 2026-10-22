#!/usr/bin/env python3
"""
Generates FEATURE_LINEAGE.md from features/schema.py and the feature store registry: one
entry per feature and per stored version, with enough detail to recompute every value by
hand from the raw files (feature_spec.md, "What a lineage entry must let someone
reconstruct").

The text stored in the registry (description, source_columns, transform_summary,
business_rationale, parameters, parquet_path) is printed as registered. The schema supplies
the population rule, the window bounds (turned into dates with the same helpers the
transforms use), the default and the parameter derivations. A registered version whose
parameters match no schema version is flagged rather than hidden.

build_training_features.py regenerates the file after every successful run; this script
regenerates it on its own.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from feature_store import store
from features import schema
from features.build import REPO_ROOT
from features.transforms import day_window, month_window

DEFAULT_LINEAGE_PATH = REPO_ROOT / "outputs" / "artifacts" / "FEATURE_LINEAGE.md"


def display_path(path: Path) -> str:
    path = path.resolve()
    return path.relative_to(REPO_ROOT).as_posix() if path.is_relative_to(REPO_ROOT) else str(path)


def _window_lines(spec: schema.FeatureSpec, params: dict, as_of: pd.Timestamp) -> list[str]:
    lines = []
    for w in spec.windows(spec.fixed, params):
        if isinstance(w, schema.DayWindow) and w.days is None:
            lines.append(f"{w.label}: every row with `txn_timestamp < as_of` → before "
                         f"{as_of:%Y-%m-%d %H:%M:%S}")
        elif isinstance(w, schema.DayWindow):
            start, end = day_window(as_of, w.days)
            lines.append(f"{w.label}: `[as_of − {w.days} days, as_of)` → {start:%Y-%m-%d %H:%M:%S} ≤ "
                         f"`txn_timestamp` < {end:%Y-%m-%d %H:%M:%S}")
        else:
            start, end = month_window(as_of, w.months, w.offset)
            last = end - pd.DateOffset(months=1)
            where = "before as_of's month" if w.offset == 0 else f"ending {w.offset} months before as_of's month"
            lines.append(f"{w.label}: {w.months} complete calendar months {where} → `txn_month` "
                         f"{start:%Y-%m} to {last:%Y-%m} ({start:%Y-%m-%d} ≤ `txn_month` < {end:%Y-%m-%d})")
    return lines or ["none: computed from `signup_date` and as_of only"]


def _matching_schema_version(spec: schema.FeatureSpec, parameters: dict) -> schema.FeatureVersion | None:
    return next((v for v in spec.versions if v.parameters == parameters), None)


def _version_entry(spec: schema.FeatureSpec, row, root: Path) -> list[str]:
    params = json.loads(row.parameters)
    snapshot = store.get_version(spec.name, int(row.version), store_dir=root)
    as_of = pd.Timestamp(snapshot["as_of_date"].iloc[0])
    values = snapshot[spec.name]
    match = _matching_schema_version(spec, params)
    tunable = {k: v for k, v in params.items() if k != "definition"}
    title = ", ".join(f"{k} = {'none' if v is None else v}" for k, v in tunable.items()) or "no tunable parameters"
    lines = [
        f"### `{spec.name}` v{row.version} — {title}{' (latest)' if row.is_latest else ''}",
        "",
        f"- **Registry row**: table `feature_registry`, key (`{spec.name}`, {row.version}); "
        f"`is_latest` = {row.is_latest}; `created_at` {row.created_at}",
        f"- **Values**: `{row.parquet_path}` — {len(snapshot):,} rows, columns `customer_id`, "
        f"`{spec.name}`, `as_of_date`; `as_of_date` = {as_of.date()}; min {values.min():.6g}, "
        f"mean {values.mean():.6g}, max {values.max():.6g}",
        f"- **Source**: `{row.source_columns}`",
        f"- **Population**: {schema.POPULATION_RULE}",
        "- **Window bounds** (relative to as_of → this snapshot):",
        *[f"  - {w}" for w in _window_lines(spec, params, as_of)],
        f"- **Computation**: {row.transform_summary}",
        f"- **Parameters** (registry `parameters`): `{row.parameters}`"
        + (f". Fixed constants: `{json.dumps(spec.fixed)}`" if spec.fixed else ""),
    ]
    if spec.parameter_notes:
        lines.append(f"- **How the parameters were set**: {spec.parameter_notes}")
    lines += [
        f"- **Zero-activity default**: {spec.default_note}",
        f"- **Business rationale** (registry): {row.business_rationale}",
    ]
    if match is None:
        lines.append("- **⚠ Not in the current schema**: no version in features/schema.py has these "
                     "parameters, so the reasons above come from the registry alone.")
    else:
        lines.append(f"- **Why this version differs from the previous one**: {match.change_reason}")
    return lines + [""]


def render_lineage(*, store_dir=None) -> str:
    root = store.store_root(store_dir=store_dir)
    as_of = store.store_as_of(store_dir=root)
    out = [
        "# FEATURE_LINEAGE.md",
        "",
        "> Generated by `scripts/generate_lineage.py` from `features/schema.py` and "
        f"`{display_path(store.registry_path(store_dir=root))}`. Do not edit by hand: rerun "
        "`scripts/build_training_features.py` (or `scripts/generate_lineage.py`) instead.",
        "",
        f"- **Store as_of**: {as_of.date() if as_of is not None else 'store is empty'} — every snapshot "
        "in the store has this `as_of_date`.",
        f"- **Feature-set version**: {schema.FEATURE_SET_VERSION}. Every feature is `nullable = False`.",
        "- **Conventions**: as_of is a calendar date read as 00:00:00 naive local time. Day windows "
        "are `[as_of − N days, as_of)` on full timestamps. Month windows are N complete calendar "
        "months before as_of's month. Recency is whole days, rounded down. `churned` is never an "
        "input.",
        "- **Shared code path**: every value below was produced by "
        "`features.build.build_features(as_of, feature_params=...)`, the same function the scoring "
        "script calls.",
        "",
        "## Registered versions",
        "",
        "| Spec | Feature | Version | Latest | Parameters | parquet_path |",
        "|---|---|---|---|---|---|",
    ]
    entries = []
    for spec in schema.FEATURE_SCHEMA:
        versions = store.list_versions(spec.name, store_dir=root).sort_values("version")
        for row in versions.itertuples():
            out.append(f"| {spec.spec_id} | `{spec.name}` | v{row.version} | "
                       f"{'yes' if row.is_latest else 'no'} | `{row.parameters}` | `{row.parquet_path}` |")
        entries += [
            f"## {spec.spec_id} — `{spec.name}`",
            "",
            f"{spec.family} · dtype `{spec.dtype}` · nullable `{spec.nullable}` · source "
            f"{spec.source} · {len(versions)} stored version(s)"
            + (f" · versioned on `{', '.join(k for k in spec.latest.parameters if k != 'definition')}`"
               if spec.is_versioned else ""),
            "",
            f"**Rationale**: {spec.rationale}",
            "",
        ]
        if spec.accepted_limitation:
            entries += [f"**Accepted limitation**: {spec.accepted_limitation}", ""]
        if versions.empty:
            entries += ["_Not registered in this store._", ""]
        for row in versions.itertuples():
            entries += _version_entry(spec, row, root)
    unknown = sorted(set(store.list_features(store_dir=root)) - set(schema.FEATURE_NAMES))
    if unknown:
        entries += ["## ⚠ Registered features not in features/schema.py", "",
                    *[f"- `{n}`" for n in unknown], ""]
    return "\n".join(out + [""] + entries).rstrip() + "\n"


def write_lineage(path: Path = DEFAULT_LINEAGE_PATH, *, store_dir=None) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_lineage(store_dir=store_dir), encoding="utf-8")
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Regenerate FEATURE_LINEAGE.md from schema.py and the registry.")
    parser.add_argument("--store-dir", default=None, help="Feature store root (default: feature_store/).")
    parser.add_argument("--out", default=str(DEFAULT_LINEAGE_PATH))
    args = parser.parse_args(argv)
    print(f"lineage written to {display_path(write_lineage(Path(args.out), store_dir=args.store_dir))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
