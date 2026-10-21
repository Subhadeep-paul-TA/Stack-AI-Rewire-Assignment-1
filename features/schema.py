"""
Versioned output feature schema: the authoritative list of every feature this pipeline
produces, with dtype, nullability, source(s), and feature-set version.

This should be generated/maintained from `feature_spec.md`, and is consumed by
`FEATURE_LINEAGE.md` generation and by the feature store registration step.
"""

from dataclasses import dataclass


@dataclass
class FeatureSpec:
    name: str
    dtype: str
    nullable: bool
    source: str
    feature_set_version: str


# TODO: populate with one FeatureSpec entry per candidate feature in your confirmed
# feature_spec.md. `nullable` should be False for every feature once your zero-activity
# default is correctly applied.
FEATURE_SCHEMA: list[FeatureSpec] = []
