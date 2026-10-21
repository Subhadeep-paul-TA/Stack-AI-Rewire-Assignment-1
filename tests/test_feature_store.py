"""
For each feature your confirmed feature_spec.md designates as versioned, register its
versions (using that feature's own spec'd values is fine), confirm each is independently
retrievable by version number, confirm is_latest correctly identifies the highest version
and not an earlier one, and confirm list_versions returns every version with distinct
parquet_path values that actually exist on disk.
"""

import pytest


def test_register_and_retrieve_all_versions_of_a_versioned_feature():
    pytest.skip("TODO: implement against feature_store/store.py")


def test_is_latest_points_to_highest_version_not_an_earlier_one():
    pytest.skip("TODO: implement against feature_store/store.py")


def test_list_versions_returns_distinct_parquet_paths_that_exist():
    pytest.skip("TODO: implement against feature_store/store.py")
