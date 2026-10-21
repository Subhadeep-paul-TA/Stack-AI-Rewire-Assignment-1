#!/usr/bin/env python3
"""
Version comparison for whichever feature(s) your confirmed feature_spec.md designates as
versioned. The mechanism is required (the feature store must support multiple versions of a
feature); which feature(s), how many versions, and what parameter values are entirely your
spec's own design choice — there is no single fixed feature or fixed parameter value here.

TODO:
- For each feature feature_spec.md designates as versioned, load all of its registered
  versions via feature_store/store.py's get_version(feature_name, version) for each version
  number that feature has — do not recompute from scratch.
- For each such feature, report how many customers' values changed and the distribution
  shift between versions (e.g. summary stats or a quantile comparison).
- If a churn-risk ranking exists, report how many customers move across a reasonable
  threshold; otherwise compare the feature's own rank-order changes.
- Write the results and an interpretation to VERSION_COMPARISON.md, one section per
  versioned feature (per skill.md).
"""


def main() -> None:
    # TODO: implement — see module docstring above.
    raise NotImplementedError


if __name__ == "__main__":
    main()
