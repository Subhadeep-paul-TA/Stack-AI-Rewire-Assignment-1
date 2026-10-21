"""
Feed each check in features/quality_checks.py a deliberately bad input (an injected
null, an out-of-range amount, a shifted distribution) and confirm it fires; feed it
clean input and confirm it doesn't false-positive.
"""

import pytest


def test_null_check_fires_on_injected_null():
    pytest.skip("TODO: implement against features/quality_checks.py")


def test_outlier_check_fires_on_out_of_range_value():
    pytest.skip("TODO: implement against features/quality_checks.py")


def test_drift_check_fires_on_shifted_distribution():
    pytest.skip("TODO: implement against features/quality_checks.py")


def test_checks_do_not_false_positive_on_clean_input():
    pytest.skip("TODO: implement against features/quality_checks.py")
