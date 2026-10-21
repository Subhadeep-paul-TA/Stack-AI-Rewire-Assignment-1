"""
Test skeletons for each feature function in features/transforms.py, in isolation.

Per skill.md's Workflow: each feature needs, at minimum, a normal case, a zero-activity
case, a single-record case, and (where relevant) a case right at a time-window boundary.

Fill these in against your confirmed feature_spec.md and your actual transform functions —
do not leave them skipped in your submission.
"""

import pytest


def test_normal_case():
    """TODO: build a feature for a customer with a normal transaction history and assert
    a sane, non-null value."""
    pytest.skip("TODO: implement against your confirmed feature_spec.md")


def test_zero_activity_case():
    """TODO: build a feature for a customer with zero transactions and assert the
    explicit default from feature_spec.md is returned, not NaN or a crash."""
    pytest.skip("TODO: implement against your confirmed feature_spec.md")


def test_single_record_case():
    """TODO: build a feature for a customer with exactly one transaction."""
    pytest.skip("TODO: implement against your confirmed feature_spec.md")


def test_time_window_boundary_case():
    """TODO: build a feature for a customer with a transaction exactly at a time-window
    boundary (e.g. exactly 90 days before the as-of date) and assert it's handled per
    the spec's stated inclusive/exclusive convention."""
    pytest.skip("TODO: implement against your confirmed feature_spec.md")
