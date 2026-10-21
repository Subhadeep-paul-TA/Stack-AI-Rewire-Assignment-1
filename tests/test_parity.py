"""
The parity test that matters most for this pipeline: build features for the same
customer via the "training" path (historical as-of date, batch of many customers) and
via the "scoring" path (single customer, same as-of date), and assert the resulting
feature values are identical.

If these two paths can't be driven through the same test with the same expected output,
the parity guarantee in feature_spec.md isn't real — fix features/build.py, not this test.
"""

import pytest


def test_training_and_scoring_paths_agree_for_same_customer():
    """TODO: call the shared build_features(as_of, ...) entry point once in "training"
    mode (batch) and once in "scoring" mode (single customer_id), same as_of date, and
    assert every feature value matches exactly for that customer."""
    pytest.skip("TODO: implement against features/build.py")
