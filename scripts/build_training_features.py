#!/usr/bin/env python3
"""
Builds features for the full training population as of a given date and registers every
version of every candidate feature listed in features/schema.py into the feature store.

Order of work (feature_spec.md, "Registration"):
1. Build every schema version through the one shared build_features(as_of, ...) entry
   point: build k uses each feature's k-th version (or its last, if it has fewer). Each
   build runs QC1-QC6 and QC8 inside build_features.
2. Plan every (feature, version) against the store, running QC7 on each:
   - a stored version with identical parameters: the rebuilt values must equal the stored
     ones (QC7). If they do, the version is reported as "unchanged — skipped";
   - otherwise it is new ("no reference"), and its values must differ from the previous
     schema version's for at least one customer, or it would be a cosmetic bump.
3. Only if every check passed: register the new versions, oldest first, so get_latest()
   returns each feature's highest version. Then regenerate FEATURE_LINEAGE.md.

Any failure exits non-zero before anything is written.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import pandas as pd

from feature_store import store
from features import schema
from features.build import build_features
from features.quality_checks import (
    CheckResult,
    QualityCheckError,
    check_drift_against_store,
    format_results,
    raise_on_failure,
)
from features.transforms import parse_as_of
from scripts.generate_lineage import DEFAULT_LINEAGE_PATH, display_path, write_lineage

SPEC_AS_OF = "2026-05-02"


class VersioningError(Exception):
    pass


@dataclass
class PlannedVersion:
    spec: schema.FeatureSpec
    schema_version: schema.FeatureVersion
    values: pd.DataFrame
    qc7: CheckResult
    stored_version: int | None  # set when an identical-parameter version is already stored

    @property
    def action(self) -> str:
        return "skip" if self.stored_version is not None else "register"


def build_all_versions(as_of: pd.Timestamp) -> dict[int, pd.DataFrame]:
    """One full-population build per version index; feature f's k-th version is read from
    build k."""
    n = max(len(f.versions) for f in schema.FEATURE_SCHEMA)
    builds = {}
    for k in range(1, n + 1):
        params = {f.name: f.versions[min(k, len(f.versions)) - 1].parameters for f in schema.FEATURE_SCHEMA}
        builds[k] = build_features(as_of, feature_params=params)
        print(f"build {k}/{n}: {len(builds[k])} customers")
        print(format_results([CheckResult(**r) for r in builds[k].attrs["quality_checks"]]))
    return builds


def _stored_with_parameters(name: str, parameters: dict, store_dir) -> int | None:
    versions = store.list_versions(name, store_dir=store_dir)
    matches = [int(r.version) for r in versions.itertuples() if json.loads(r.parameters) == parameters]
    return max(matches) if matches else None


def plan_registrations(builds: dict[int, pd.DataFrame], store_dir) -> list[PlannedVersion]:
    plan, qc7_results, cosmetic = [], [], []
    for spec in schema.FEATURE_SCHEMA:
        previous = None
        for k, version in enumerate(spec.versions, start=1):
            values = builds[k][["customer_id", spec.name, "as_of_date"]].reset_index(drop=True)
            stored = _stored_with_parameters(spec.name, version.parameters, store_dir)
            reference = store.get_version(spec.name, stored, store_dir=store_dir) if stored else None
            qc7 = check_drift_against_store(spec.name, values, reference)
            qc7_results.append(qc7)
            if stored is None and previous is not None:
                a = previous[spec.name].to_numpy(dtype="float64")
                b = values[spec.name].to_numpy(dtype="float64")
                if np.array_equal(a, b):
                    cosmetic.append(f"{spec.name} v{version.version}: parameters {version.parameters} "
                                    f"give the same values as v{version.version - 1}")
            plan.append(PlannedVersion(spec, version, values, qc7, stored))
            previous = values
    print(format_results(qc7_results))
    raise_on_failure(qc7_results)
    if cosmetic:
        raise VersioningError("not a real version bump (values unchanged):\n" + "\n".join(cosmetic))
    return plan


def business_rationale(spec: schema.FeatureSpec, version: schema.FeatureVersion) -> str:
    return f"{spec.rationale} Version {version.version}: {version.change_reason}"


def register(plan: list[PlannedVersion], store_dir, registered: list[str]) -> None:
    """Register the plan's new versions, appending each one to `registered` as it commits."""
    for item in plan:
        spec, version = item.spec, item.schema_version
        label = f"{spec.name} schema v{version.version}"
        if item.action == "skip":
            print(f"{label}: unchanged — skipped (stored as v{item.stored_version})")
            continue
        number = store.register_version(
            spec.name,
            item.values,
            description=spec.description,
            source_columns=spec.source_columns,
            transform_summary=spec.transform_summary(version.parameters),
            business_rationale=business_rationale(spec, version),
            parameters=version.parameters,
            store_dir=store_dir,
        )
        registered.append(f"{spec.name} v{number}")
        note = "" if number == version.version else " (store numbering differs from schema.py)"
        print(f"{label}: registered as v{number}{note}, parameters {json.dumps(version.parameters)}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--as-of", default=SPEC_AS_OF,
                        help=f"Feature cutoff date YYYY-MM-DD (default: {SPEC_AS_OF}, feature_spec.md).")
    parser.add_argument("--store-dir", default=None, help="Feature store root (default: feature_store/).")
    parser.add_argument("--lineage-out", default=str(DEFAULT_LINEAGE_PATH),
                        help="Where to regenerate FEATURE_LINEAGE.md (default: outputs/artifacts/).")
    args = parser.parse_args(argv)

    try:
        as_of = parse_as_of(args.as_of)
        existing = store.store_as_of(store_dir=args.store_dir)
        if existing is not None and existing != as_of:
            raise store.StoreError(
                f"the store holds snapshots at {existing.date()}; refusing to build at {as_of.date()} "
                "(one as_of per store)"
            )
        builds = build_all_versions(as_of)
        plan = plan_registrations(builds, args.store_dir)
    except (QualityCheckError, VersioningError, store.StoreError, ValueError) as exc:
        print(f"ERROR: {exc}\nNothing was registered by this run.", file=sys.stderr)
        return 1

    # Each registration is its own transaction. If one fails part-way through the plan, say
    # exactly what was committed; a rerun skips those (identical parameters, QC7) and
    # registers the rest.
    registered: list[str] = []
    try:
        register(plan, args.store_dir, registered)
    except Exception as exc:
        done = ", ".join(registered) if registered else "nothing"
        print(f"ERROR during registration: {exc}\nRegistered before the failure: {done}. "
              "Fix the cause and rerun: already-registered versions are skipped.", file=sys.stderr)
        if registered:  # keep the lineage in step with what the registry now holds
            write_lineage(Path(args.lineage_out), store_dir=args.store_dir)
        return 1

    out = write_lineage(Path(args.lineage_out), store_dir=args.store_dir)
    print(f"lineage written to {display_path(out)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
