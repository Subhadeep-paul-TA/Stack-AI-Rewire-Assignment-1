#!/usr/bin/env python3
"""
Builds features for one or more specific customer_ids as of a given date, using the
scoring path of the same shared build_features(as_of, ...) entry point used by
build_training_features.py.

TODO:
- Parse --customer-id (one or more) and --as-of arguments.
- Call the SAME features/build.py entry point as build_training_features.py — the
  training/scoring parity guarantee depends on there being exactly one code path.
- Print the resulting feature values for each requested customer_id.
- Handle a nonexistent customer_id explicitly (clear error, not a silent empty result).
"""

import argparse


def main() -> None:
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
        default="2026-08-01",
        help="As-of date for feature computation (default: 2026-08-01).",
    )
    args = parser.parse_args()

    # TODO: implement — see module docstring above.
    raise NotImplementedError


if __name__ == "__main__":
    main()
