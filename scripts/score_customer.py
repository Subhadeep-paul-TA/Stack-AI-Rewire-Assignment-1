#!/usr/bin/env python3
"""
Builds features for one or more specific customer_ids as of a given date, using the
scoring path of the same shared build_features(as_of, ...) entry point used by
build_training_features.py. The only difference from training is `customer_ids`.

Exits non-zero, printing no features, if a requested id doesn't exist or signed up on or
after as_of (ValueError), or if any quality check fails (QualityCheckError).
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from features import schema
from features.build import build_features
from features.quality_checks import QualityCheckError

SPEC_AS_OF = "2026-05-02"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--customer-id",
        type=int,
        action="append",
        required=True,
        help="Customer ID to score. May be passed multiple times.",
    )
    parser.add_argument(
        "--as-of",
        default=SPEC_AS_OF,
        help=f"Feature cutoff date YYYY-MM-DD (default: {SPEC_AS_OF}, feature_spec.md).",
    )
    parser.add_argument("--format", choices=["table", "json"], default="table")
    args = parser.parse_args(argv)

    try:
        features = build_features(args.as_of, customer_ids=args.customer_id)
    except (ValueError, QualityCheckError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        records = features[["customer_id", *schema.FEATURE_NAMES]].to_dict(orient="records")
        print(json.dumps({"as_of": features["as_of_date"].iloc[0].date().isoformat(), "customers": records}, indent=2))
    else:
        print(f"as_of {features['as_of_date'].iloc[0].date()}")
        # object dtype keeps the int features as ints through the transpose
        print(features.set_index("customer_id")[schema.FEATURE_NAMES].astype(object).T.to_string())
    return 0


if __name__ == "__main__":
    sys.exit(main())
