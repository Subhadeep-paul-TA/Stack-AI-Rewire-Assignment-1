"""
For each feature feature_spec.md designates as versioned, register its versions (with that
feature's own spec'd values), confirm each is independently retrievable by version number,
confirm is_latest identifies the highest version and not an earlier one, and confirm
list_versions returns every version with distinct parquet_path values that exist on disk.

Also: the store's append-only guarantees (no overwrite, no edit, no delete, one as_of), and
the training script end to end — registration, idempotent rerun, QC7, the cosmetic-bump
rule, and the spec's expected version-comparison figures. Every store here is a tmp_path.
"""

import dataclasses
import json
import sqlite3

import pandas as pd
import pytest

from feature_store import store
from features import schema, transforms
from features.build import build_features
from scripts import build_training_features, compare_feature_versions, generate_lineage
from tests.helpers import AS_OF, registry_rows, store_files, version_params

VERSIONED = [f for f in schema.FEATURE_SCHEMA if f.is_versioned]


def snapshot(full: pd.DataFrame, name: str) -> pd.DataFrame:
    return full[["customer_id", name, "as_of_date"]].reset_index(drop=True)


def register(root, name, values, version_label="v", params=None):
    return store.register_version(
        name, values, description=f"{name} {version_label}", source_columns="test:col",
        transform_summary="test", business_rationale="test", parameters=params or {}, store_dir=root,
    )


def toy(values, as_of=AS_OF, name="toy_feature"):
    return pd.DataFrame({"customer_id": range(1, len(values) + 1), name: values, "as_of_date": pd.Timestamp(as_of)})


@pytest.fixture(scope="module")
def builds():
    n = max(len(f.versions) for f in schema.FEATURE_SCHEMA)
    return {k: build_features(AS_OF, feature_params=version_params(k)) for k in range(1, n + 1)}


# The versioning floor --------------------------------------------------------------------
def test_spec_versions_at_least_two_features_each_with_at_least_two_versions():
    assert len(VERSIONED) >= 2
    assert {f.name for f in VERSIONED} == {
        "avg_spend_per_active_month_6m", "spend_trend_3m_vs_prior_3m", "active_month_share"}
    for f in VERSIONED:
        numbers = [v.version for v in f.versions]
        assert numbers == list(range(1, len(numbers) + 1))
        tunable = [{k: x for k, x in v.parameters.items() if k != "definition"} for v in f.versions]
        assert len({json.dumps(t, sort_keys=True) for t in tunable}) == len(tunable), f.name


# Spec'd versions, registered and retrieved -----------------------------------------------
@pytest.mark.parametrize("spec", VERSIONED, ids=lambda f: f.name)
def test_register_and_retrieve_all_versions_of_a_versioned_feature(tmp_path, builds, spec):
    expected = {}
    for k, version in enumerate(spec.versions, start=1):
        values = snapshot(builds[k], spec.name)
        assert register(tmp_path, spec.name, values, params=version.parameters) == version.version
        expected[version.version] = values
    for number, values in expected.items():
        got = store.get_version(spec.name, number, store_dir=tmp_path)
        pd.testing.assert_frame_equal(got, values.sort_values("customer_id", ignore_index=True), check_exact=True)
    # the versions really differ from each other (not a cosmetic bump)
    firsts = [expected[n][spec.name] for n in sorted(expected)]
    assert all(not a.equals(b) for a, b in zip(firsts, firsts[1:]))


@pytest.mark.parametrize("spec", VERSIONED, ids=lambda f: f.name)
def test_is_latest_points_to_highest_version_not_an_earlier_one(tmp_path, builds, spec):
    for k, version in enumerate(spec.versions, start=1):
        register(tmp_path, spec.name, snapshot(builds[k], spec.name), params=version.parameters)
        rows = store.list_versions(spec.name, store_dir=tmp_path)
        assert rows.loc[rows["is_latest"] == 1, "version"].tolist() == [version.version]
        assert store.latest_version(spec.name, store_dir=tmp_path) == version.version
    latest = store.get_latest(spec.name, store_dir=tmp_path)
    pd.testing.assert_frame_equal(latest, store.get_version(spec.name, spec.latest.version, store_dir=tmp_path))
    assert not latest.equals(store.get_version(spec.name, 1, store_dir=tmp_path))


@pytest.mark.parametrize("spec", VERSIONED, ids=lambda f: f.name)
def test_list_versions_returns_distinct_parquet_paths_that_exist(tmp_path, builds, spec):
    for k, version in enumerate(spec.versions, start=1):
        register(tmp_path, spec.name, snapshot(builds[k], spec.name), params=version.parameters)
    rows = store.list_versions(spec.name, store_dir=tmp_path)
    assert rows["version"].tolist() == sorted((v.version for v in spec.versions), reverse=True)
    assert rows["parquet_path"].is_unique
    for row in rows.itertuples():
        assert row.parquet_path == f"{tmp_path.name}/values/{spec.name}__v{row.version}.parquet"
        assert (tmp_path.parent / row.parquet_path).is_file()
        assert json.loads(row.parameters) == spec.version(row.version).parameters


def test_more_than_two_versions_and_latest_is_the_highest(tmp_path):
    for n in range(1, 5):
        assert register(tmp_path, "toy_feature", toy([float(n)] * 3)) == n
    assert store.latest_version("toy_feature", store_dir=tmp_path) == 4
    assert store.get_latest("toy_feature", store_dir=tmp_path)["toy_feature"].tolist() == [4.0] * 3
    for n in range(1, 5):
        assert store.get_version("toy_feature", n, store_dir=tmp_path)["toy_feature"].tolist() == [float(n)] * 3
    assert store.list_versions("toy_feature", store_dir=tmp_path)["version"].tolist() == [4, 3, 2, 1]


def test_versions_are_numbered_per_feature(tmp_path):
    register(tmp_path, "toy_a", toy([1.0], name="toy_a"))
    register(tmp_path, "toy_a", toy([2.0], name="toy_a"))
    assert register(tmp_path, "toy_b", toy([1.0], name="toy_b")) == 1
    assert store.list_features(store_dir=tmp_path) == ["toy_a", "toy_b"]


# Append-only guarantees ------------------------------------------------------------------
def test_an_existing_parquet_file_is_never_overwritten(tmp_path):
    register(tmp_path, "toy_feature", toy([1.0, 2.0]))
    squatter = tmp_path / "values" / "toy_feature__v2.parquet"
    squatter.write_bytes(b"not mine")
    with pytest.raises(store.StoreError, match="refusing to overwrite"):
        register(tmp_path, "toy_feature", toy([3.0, 4.0]))
    assert squatter.read_bytes() == b"not mine"
    assert store.list_versions("toy_feature", store_dir=tmp_path)["version"].tolist() == [1]
    assert store.latest_version("toy_feature", store_dir=tmp_path) == 1


def test_a_failed_write_rolls_back_and_removes_its_file(tmp_path, monkeypatch):
    """is_latest is flipped in the same transaction as the insert: a failure part-way leaves
    the previous latest in place and no orphan file."""
    register(tmp_path, "toy_feature", toy([1.0]))

    def failing_write(self, handle, **kwargs):
        handle.write(b"partial")
        raise OSError("disk full")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", failing_write)
    with pytest.raises(OSError, match="disk full"):
        register(tmp_path, "toy_feature", toy([2.0]))
    monkeypatch.undo()
    assert store_files(tmp_path) == ["toy_feature__v1.parquet"]
    assert store.latest_version("toy_feature", store_dir=tmp_path) == 1
    assert register(tmp_path, "toy_feature", toy([2.0])) == 2


def test_registry_rows_cannot_be_edited_deleted_or_rolled_back(tmp_path):
    register(tmp_path, "toy_feature", toy([1.0]))
    register(tmp_path, "toy_feature", toy([2.0]))
    with sqlite3.connect(tmp_path / "registry.db") as con:
        for sql in (
            "UPDATE feature_registry SET parameters = '{\"x\": 1}' WHERE version = 1",
            "UPDATE feature_registry SET parquet_path = 'elsewhere' WHERE version = 2",
            "DELETE FROM feature_registry WHERE version = 1",
            "UPDATE feature_registry SET is_latest = 1 WHERE version = 1",
            "INSERT INTO feature_registry VALUES ('toy_feature', 1, 'x', 'x', 'x', 'x', 'x', '{}', 'p', 0)",
        ):
            with pytest.raises(sqlite3.IntegrityError):
                con.execute(sql)
    assert store.latest_version("toy_feature", store_dir=tmp_path) == 2


def test_one_as_of_per_store(tmp_path):
    register(tmp_path, "toy_feature", toy([1.0]))
    with pytest.raises(store.StoreError, match="one as_of per store"):
        register(tmp_path, "toy_feature", toy([2.0], as_of="2026-04-01"))
    assert store.list_versions("toy_feature", store_dir=tmp_path)["version"].tolist() == [1]
    assert store_files(tmp_path) == ["toy_feature__v1.parquet"]
    assert store.store_as_of(store_dir=tmp_path) == pd.Timestamp(AS_OF)


@pytest.mark.parametrize("bad, message", [
    (toy([1.0, None]), "null values"),
    (pd.concat([toy([1.0]), toy([2.0])]), "duplicated customer_id"),
    (toy([1.0]).drop(columns="as_of_date"), "exactly the columns"),
    (toy([1.0, 2.0]).assign(as_of_date=[pd.Timestamp("2026-05-02"), pd.Timestamp("2026-05-03")]), "one as_of_date"),
])
def test_invalid_snapshots_are_rejected_before_anything_is_written(tmp_path, bad, message):
    with pytest.raises(store.StoreError, match=message):
        register(tmp_path, "toy_feature", bad)
    assert registry_rows(tmp_path) == [] and store_files(tmp_path) == []


def test_feature_names_cannot_escape_the_values_directory(tmp_path):
    with pytest.raises(store.StoreError, match="must match"):
        register(tmp_path, "../evil", toy([1.0], name="../evil"))


def test_reads_on_an_empty_store_create_nothing(tmp_path):
    root = tmp_path / "empty"
    assert store.list_versions("toy_feature", store_dir=root).empty
    with pytest.raises(KeyError):
        store.get_latest("toy_feature", store_dir=root)
    with pytest.raises(KeyError):
        store.get_version("toy_feature", 1, store_dir=root)
    assert store.store_as_of(store_dir=root) is None
    assert not root.exists()


def test_default_store_root_is_resolved_from_the_module_not_the_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert store.store_root() == store.DEFAULT_STORE_DIR
    assert store.DEFAULT_STORE_DIR.name == "feature_store" and (store.DEFAULT_STORE_DIR / "store.py").is_file()


# The training script, end to end ---------------------------------------------------------
def test_training_script_registers_every_feature_and_every_version(built_store):
    for spec in schema.FEATURE_SCHEMA:
        rows = store.list_versions(spec.name, store_dir=built_store)
        assert rows["version"].tolist() == [v.version for v in reversed(spec.versions)], spec.name
        assert [json.loads(p) for p in rows["parameters"]] == [v.parameters for v in reversed(spec.versions)]
        latest = store.get_latest(spec.name, store_dir=built_store)
        assert len(latest) == 1399 and (latest["as_of_date"] == pd.Timestamp(AS_OF)).all()
        assert not latest[spec.name].isna().any()
        assert rows.loc[rows["is_latest"] == 1, "version"].tolist() == [spec.latest.version]
        first = rows.loc[rows["version"] == 1].iloc[0]
        assert first["transform_summary"] == spec.transform_summary(spec.versions[0].parameters)
        assert spec.versions[0].change_reason in first["business_rationale"]


def test_training_store_matches_the_shared_build(built_store, full_latest):
    for name in schema.FEATURE_NAMES:
        pd.testing.assert_frame_equal(store.get_latest(name, store_dir=built_store), snapshot(full_latest, name), check_exact=True)


def test_rerun_is_unchanged_skipped_and_writes_nothing(tmp_path, capsys):
    root = tmp_path / "feature_store"
    args = ["--store-dir", str(root), "--lineage-out", str(tmp_path / "lineage.md")]
    assert build_training_features.main(args) == 0
    before, files = registry_rows(root), store_files(root)
    capsys.readouterr()
    assert build_training_features.main(args) == 0
    out = capsys.readouterr().out
    assert out.count("unchanged — skipped") == sum(len(f.versions) for f in schema.FEATURE_SCHEMA)
    assert registry_rows(root) == before and store_files(root) == files


def test_qc7_stops_a_silent_definition_change_and_nothing_is_written(tmp_path, monkeypatch, capsys):
    root = tmp_path / "feature_store"
    args = ["--store-dir", str(root), "--lineage-out", str(tmp_path / "lineage.md")]
    assert build_training_features.main(args) == 0
    before, files = registry_rows(root), store_files(root)
    real = transforms.days_since_last_txn

    def changed(population, transactions, as_of, *, cap_days):
        """Rounding up instead of down: within QC4's range, zero-activity defaults intact,
        invisible to QC1-QC6 and QC8. Only the stored copy can show it."""
        out = real(population, transactions, as_of, cap_days=cap_days)
        return out.where(out >= cap_days, out + 1)

    monkeypatch.setattr(transforms, "days_since_last_txn", changed)
    assert build_training_features.main(args) == 1
    err = capsys.readouterr().err
    assert "FAIL QC7 drift vs store [days_since_last_txn]" in err and "Nothing was registered" in err
    assert "FAIL QC7 drift vs store [tenure_days]" not in err
    assert registry_rows(root) == before and store_files(root) == files


def test_a_version_whose_values_do_not_change_is_refused(tmp_path, monkeypatch, capsys):
    """A cosmetic bump: a new monthly_cap of 2,000 is above every customer-month in the
    window, so v2 would equal v1 for every customer."""
    spec = schema.get_spec("avg_spend_per_active_month_6m")
    cosmetic = dataclasses.replace(spec, versions=(
        spec.versions[0], dataclasses.replace(spec.versions[1], parameters={"monthly_cap": 2000.0, "definition": 1})))
    patched = [cosmetic if f.name == spec.name else f for f in schema.FEATURE_SCHEMA]
    monkeypatch.setattr(schema, "FEATURE_SCHEMA", patched)
    monkeypatch.setitem(schema._BY_NAME, spec.name, cosmetic)
    root = tmp_path / "feature_store"
    assert build_training_features.main(["--store-dir", str(root), "--lineage-out", str(tmp_path / "l.md")]) == 1
    assert "not a real version bump" in capsys.readouterr().err
    assert registry_rows(root) == [] and store_files(root) == []


def test_training_script_refuses_a_different_as_of(built_store, tmp_path, capsys):
    before = registry_rows(built_store)
    args = ["--store-dir", str(built_store), "--as-of", "2026-04-01", "--lineage-out", str(tmp_path / "l.md")]
    assert build_training_features.main(args) == 1
    assert "one as_of per store" in capsys.readouterr().err
    assert registry_rows(built_store) == before


def test_training_script_writes_nothing_when_a_build_check_fails(tmp_path, capsys):
    root = tmp_path / "feature_store"
    assert build_training_features.main(["--store-dir", str(root), "--as-of", "2026-08-01",
                                         "--lineage-out", str(tmp_path / "l.md")]) == 1
    assert "QC6" in capsys.readouterr().err
    assert registry_rows(root) == [] and store_files(root) == []


# Version comparison and lineage, from the store ------------------------------------------
@pytest.fixture(scope="module")
def comparisons(built_store):
    return {f.name: compare_feature_versions.compare_feature(f, built_store)["pairs"][0] for f in VERSIONED}


def test_version_comparison_matches_the_spec_expectations(comparisons):
    f5, f6, f7 = (comparisons[n] for n in (
        "avg_spend_per_active_month_6m", "spend_trend_3m_vs_prior_3m", "active_month_share"))
    assert (f5["changed"], round(f5["rank_correlation"], 3)) == (157, 0.997)
    assert (f5["threshold"]["entering"], f5["threshold"]["leaving"]) == (25, 25)
    assert len(f5["threshold"]["old"]["members"]) == len(f5["threshold"]["new"]["members"]) == 140
    assert (f6["changed"], round(f6["rank_correlation"], 3)) == (162, 0.999)
    assert f6["threshold"]["tied_new"] == 162 and f6["threshold"]["n_below_old"] == 458
    assert f6["threshold"]["below_unchanged"] is True
    assert (f7["changed"], round(f7["rank_correlation"], 3)) == (890, 0.849)
    assert (f7["threshold"]["entering"], f7["threshold"]["leaving"]) == (54, 46)
    assert (len(f7["threshold"]["old"]["members"]), len(f7["threshold"]["new"]["members"])) == (168, 176)


def test_version_comparison_reads_through_get_version(built_store, monkeypatch):
    calls = []
    real = store.get_version
    monkeypatch.setattr(store, "get_version", lambda *a, **k: calls.append(a[:2]) or real(*a, **k))
    spec = schema.get_spec("active_month_share")
    compare_feature_versions.compare_feature(spec, built_store)
    # every version of the feature, plus the has_ever_purchased subgroup flag, all from the store
    assert calls == [(spec.name, v.version) for v in spec.versions] + [("has_ever_purchased", 1)]


def test_lineage_has_a_separate_entry_for_every_stored_version(built_store):
    text = generate_lineage.render_lineage(store_dir=built_store)
    for spec in schema.FEATURE_SCHEMA:
        for row in store.list_versions(spec.name, store_dir=built_store).itertuples():
            assert f"### `{spec.name}` v{row.version} " in text
            assert f"`{row.parquet_path}` — 1,399 rows" in text
    assert "2025-11 to 2026-04 (2025-11-01 ≤ `txn_month` < 2026-05-01)" in text
    assert "2026-02-01 00:00:00 ≤ `txn_timestamp` < 2026-05-02 00:00:00" in text
    assert "197.098" in text
    assert "Not in the current schema" not in text


def test_a_registration_failure_part_way_reports_what_was_committed(tmp_path, capsys):
    root = tmp_path / "feature_store"
    (root / "values").mkdir(parents=True)
    (root / "values" / "txn_count_90d__v1.parquet").write_bytes(b"stray file")  # 4th feature in the plan
    args = ["--store-dir", str(root), "--lineage-out", str(tmp_path / "l.md")]
    assert build_training_features.main(args) == 1
    err = capsys.readouterr().err
    assert "Registered before the failure: tenure_days v1, days_since_last_txn v1, has_ever_purchased v1" in err
    assert "Nothing was registered" not in err
    (root / "values" / "txn_count_90d__v1.parquet").unlink()
    assert build_training_features.main(args) == 0  # the rerun skips the three and registers the rest
    assert len(registry_rows(root)) == sum(len(f.versions) for f in schema.FEATURE_SCHEMA)
    assert "tenure_days schema v1: unchanged — skipped" in capsys.readouterr().out
