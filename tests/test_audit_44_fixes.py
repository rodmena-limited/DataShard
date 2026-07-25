"""Regression tests for audit #44 fixes (tickets #45-#49).

Each test reproduces the exact defect the re-audit found, so a regression is
caught at the failure it actually causes: data loss, wrong query results, a
sandbox escape, a silent data drop, or a bricked scan.
"""
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import datashard as ds
from datashard.data_structures import DataFile, FileFormat, Schema
from datashard.garbage_collector import GarbageCollectionAborted


def _schema():
    return Schema(schema_id=0, fields=[
        {"id": 1, "name": "id", "type": "long"},
        {"id": 2, "name": "name", "type": "string"},
    ])


def _table(tmp_path, name="t"):
    return ds.create_table(str(tmp_path / name), schema=_schema())


# ------------------------------------------------------- #45 symlinked root GC
def test_gc_via_symlinked_root_keeps_live_data(tmp_path):
    """P0: GC through a symlinked table root deleted the ENTIRE live table.

    list_files computed paths relative to the RAW base while walking the
    symlink-resolved tree, so every live file came back as '../real/...' and
    matched nothing in the reachable set.
    """
    real = tmp_path / "real_table"
    link = tmp_path / "current"
    t = ds.create_table(str(real), schema=_schema())
    t.append_records([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
    os.symlink(str(real), str(link))

    linked = ds.load_table(str(link))
    stats = linked.garbage_collect(grace_period_ms=0)

    assert stats == {"data_files": 0, "manifest_files": 0, "manifest_lists": 0}
    # The table is still readable through both the symlink and the real path.
    assert linked.row_count() == 2
    assert ds.load_table(str(real)).scan() == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
    ]


def test_list_files_under_symlinked_root_is_table_relative(tmp_path):
    real = tmp_path / "real_table"
    link = tmp_path / "current"
    t = ds.create_table(str(real), schema=_schema())
    t.append_records([{"id": 1, "name": "a"}])
    os.symlink(str(real), str(link))

    from datashard.storage_backend import LocalStorageBackend

    listed = LocalStorageBackend(str(link)).list_files("data")
    assert listed, "expected at least one data file"
    for rel in listed:
        assert not rel.startswith(".."), f"escaping path from list_files: {rel}"
        assert rel.startswith("data/")


def test_gc_aborts_when_listing_escapes_table_root(tmp_path, monkeypatch):
    """Independent second guard: even if a backend returns an escaping path,
    GC must abort instead of deleting files it cannot classify."""
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "a"}])

    from datashard.storage_backend import LocalStorageBackend

    def fake_list_files(self, prefix):  # noqa: ARG001
        return ["../elsewhere/data/x.parquet"]

    monkeypatch.setattr(LocalStorageBackend, "list_files", fake_list_files)
    with pytest.raises(GarbageCollectionAborted):
        t.garbage_collect(grace_period_ms=0)


# ------------------------------------------------------------- #46 not_in NULL
def test_not_in_excludes_null_rows(tmp_path):
    t = _table(tmp_path)
    t.append_records([
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
        {"id": 3, "name": None},
        {"id": 4, "name": "c"},
    ])
    rows = t.scan(filter={"name": ("not_in", ["b"])})
    assert {r["id"] for r in rows} == {1, 4}
    assert all(r["name"] is not None for r in rows)


def test_in_and_not_in_never_match_null_even_when_null_in_value_set(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "a"}, {"id": 2, "name": None}])
    assert t.scan(filter={"name": ("in", [None])}) == []
    assert {r["id"] for r in t.scan(filter={"name": ("in", ["a", None])})} == {1}
    assert {r["id"] for r in t.scan(filter={"name": ("not_in", ["a", None])})} == set()
    # NOT IN () still excludes NULLs (documented contract)
    assert {r["id"] for r in t.scan(filter={"name": ("not_in", [])})} == {1}


def test_scan_apis_agree_on_not_in_with_nulls(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "a"}, {"id": 2, "name": None}, {"id": 3, "name": "b"}])
    f = {"name": ("not_in", ["b"])}
    scan_rows = t.scan(filter=f)
    batch_rows = [r for b in t.scan_batches(filter=f) for r in b]
    assert scan_rows == batch_rows == [{"id": 1, "name": "a"}]


# --------------------------------------------------------- #47 read-path sandbox
def test_read_path_rejects_absolute_path_outside_table(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "a"}])
    outside = tmp_path / "outside.parquet"
    pq.write_table(pa.table({"id": [9]}), str(outside))

    dfm = t.file_manager.data_file_manager
    with pytest.raises(ValueError, match="Path traversal"):
        dfm._get_arrow_path(str(outside))
    with pytest.raises(ValueError, match="Path traversal"):
        dfm.read_data_file("/etc/passwd")


def test_scan_of_tampered_manifest_entry_does_not_escape_table(tmp_path):
    """A manifest entry pointing outside the table must not be readable."""
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "a"}])
    outside = tmp_path / "secret.parquet"
    pq.write_table(pa.table({"id": [9]}), str(outside))

    tampered = DataFile(
        file_path=str(outside),
        file_format=FileFormat.PARQUET,
        partition_values={},
        record_count=1,
        file_size_in_bytes=outside.stat().st_size,
    )
    with pytest.raises(ValueError, match="Path traversal"):
        # Read of the escaping path fails; nothing outside the table is opened.
        t._read_datafile_table(tampered, None, None, False, pa, pq)


def test_absolute_path_inside_table_is_still_readable(tmp_path):
    """The sandbox must not break the legitimate 'I hold the real path' case."""
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "a"}])
    data_dir = tmp_path / "t" / "data"
    parquet = next(p for p in data_dir.iterdir() if p.suffix == ".parquet")
    rows = t.file_manager.data_file_manager.read_data_file(str(parquet))
    assert rows == [{"id": 1, "name": "a"}]


# ------------------------------------------------- #48 dangling current snapshot
def _break_current_snapshot_id(table_dir):
    """Point current_snapshot_id at a snapshot that does not exist."""
    import glob
    import json

    versions = sorted(glob.glob(os.path.join(table_dir, "metadata", "v*.metadata.json")))
    latest = versions[-1]
    with open(latest) as f:
        meta = json.load(f)
    meta["current_snapshot_id"] = 123456789
    with open(latest, "w") as f:
        json.dump(meta, f, indent=2)


def test_commit_with_dangling_current_snapshot_id_aborts(tmp_path):
    """P1: commit proceeded with an EMPTY base manifest set, silently dropping
    every pre-existing file from the new snapshot."""
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "a"}])
    _break_current_snapshot_id(str(tmp_path / "t"))

    reopened = ds.load_table(str(tmp_path / "t"))
    with pytest.raises(RuntimeError, match="inconsistent"):
        reopened.append_records([{"id": 2, "name": "b"}])

    # The original data file was never dropped from the table's storage.
    data_files = [p for p in (tmp_path / "t" / "data").iterdir() if p.suffix == ".parquet"]
    assert len(data_files) == 1


def test_scan_with_dangling_current_snapshot_id_raises_not_empty(tmp_path):
    """A broken table must never be reported as an empty one."""
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "a"}])
    _break_current_snapshot_id(str(tmp_path / "t"))

    reopened = ds.load_table(str(tmp_path / "t"))
    with pytest.raises(RuntimeError, match="inconsistent"):
        reopened.scan()
    with pytest.raises(RuntimeError, match="inconsistent"):
        reopened.row_count()


# ----------------------------------------------- #49 append_files schema checks
def _write_parquet(path, table):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(table, path)


def _data_file(rel_path, size):
    return DataFile(
        file_path=rel_path,
        file_format=FileFormat.PARQUET,
        partition_values={},
        record_count=1,
        file_size_in_bytes=size,
    )


def test_append_files_rejects_schema_divergent_parquet(tmp_path):
    """P1: a divergent file committed fine and bricked every later scan."""
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "a"}])

    bad = tmp_path / "t" / "data" / "bad.parquet"
    _write_parquet(str(bad), pa.table({"id": [2], "wrong_column": ["x"]}))

    tx = t.new_transaction()
    tx.begin()
    with pytest.raises(ValueError, match="does not match"):
        tx.append_files([_data_file("/data/bad.parquet", bad.stat().st_size)])
    tx.rollback()

    # Table still scans cleanly.
    assert t.scan() == [{"id": 1, "name": "a"}]


def test_append_files_rejects_unreadable_parquet(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "a"}])
    junk = tmp_path / "t" / "data" / "junk.parquet"
    junk.write_bytes(b"not a parquet file at all")

    tx = t.new_transaction()
    tx.begin()
    with pytest.raises(ValueError, match="Cannot read the parquet schema"):
        tx.append_files([_data_file("/data/junk.parquet", junk.stat().st_size)])
    tx.rollback()


def test_append_files_accepts_matching_parquet_and_scans(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "a"}])

    arrow_schema = t.file_manager.data_file_manager.create_arrow_schema(_schema())
    good = tmp_path / "t" / "data" / "good.parquet"
    _write_parquet(str(good), pa.table({"id": [2], "name": ["b"]}, schema=arrow_schema))

    assert t.append_data([_data_file("/data/good.parquet", good.stat().st_size)]) is True
    assert {r["id"] for r in t.scan(verify_checksums=False)} == {1, 2}


def test_append_files_on_schemaless_table_still_allowed(tmp_path):
    """Tables with no persisted schema keep the previous behaviour (nothing to
    validate against), matching append_data(records)."""
    t = ds.create_table(str(tmp_path / "noschema"))
    f = tmp_path / "noschema" / "data" / "legacy.parquet"
    os.makedirs(os.path.dirname(str(f)), exist_ok=True)
    f.write_bytes(b"opaque legacy content")

    tx = t.new_transaction()
    tx.begin()
    tx.append_files([_data_file("/data/legacy.parquet", f.stat().st_size)])
    assert tx.commit() is True
