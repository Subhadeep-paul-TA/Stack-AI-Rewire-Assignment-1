"""Shared fixtures. Every test that writes to a feature store uses a tmp_path store; the
session guard below fails the run if anything touched the real feature_store/."""

import hashlib
from pathlib import Path

import pandas as pd
import pytest

from feature_store import store
from features.build import build_features
from tests.helpers import AS_OF, version_params


def _real_store_fingerprint() -> dict[str, str]:
    root = store.DEFAULT_STORE_DIR
    files = [root / store.REGISTRY_FILENAME, *sorted((root / store.VALUES_DIRNAME).glob("*"))]
    return {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in files if p.is_file()}


@pytest.fixture(scope="session", autouse=True)
def real_store_is_never_touched():
    before = _real_store_fingerprint()
    yield
    assert _real_store_fingerprint() == before, "a test modified the real feature_store/"


@pytest.fixture(scope="session")
def full_latest() -> pd.DataFrame:
    return build_features(AS_OF)


@pytest.fixture(scope="session")
def full_v1() -> pd.DataFrame:
    return build_features(AS_OF, feature_params=version_params(1))


@pytest.fixture(scope="session")
def built_store(tmp_path_factory) -> Path:
    """A tmp store populated by the real training script, exactly as in production."""
    from scripts import build_training_features

    root = tmp_path_factory.mktemp("built") / "feature_store"
    lineage = root.parent / "FEATURE_LINEAGE.md"
    assert build_training_features.main(["--store-dir", str(root), "--lineage-out", str(lineage)]) == 0
    return root

