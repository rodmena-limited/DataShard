"""Regression tests for audit #10 fixes (tickets #13-#43).

These exercise the FAILURE paths and edge behaviors the original suite missed:
fault injection around the commit point, concurrency lost-updates, fail-closed
reads/GC, checksum verification, version-hint recovery, and every corrected API.
"""
import os
import threading

import pytest

import datashard as ds
from datashard.data_structures import Schema
from datashard.garbage_collector import GarbageCollectionAborted
from datashard.integrity import CorruptDataError
from datashard.metadata_manager import MetadataManager, TableExistsError
from datashard.storage_backend import LocalStorageBackend


def _schema():
    return Schema(schema_id=0, fields=[
        {"id": 1, "name": "id", "type": "long"},
        {"id": 2, "name": "name", "type": "string"},
    ])


def _table(tmp_path, name="t"):
    return ds.create_table(str(tmp_path / name), schema=_schema())


# ---------------------------------------------------------------- #18 schema
def test_create_table_persists_schema(tmp_path):
    t = _table(tmp_path)
    assert t.append_records([{"id": 1, "name": "x"}]) is True
    assert t.row_count() == 1
    assert t.scan() == [{"id": 1, "name": "x"}]


def test_schemaless_table_append_without_schema_raises(tmp_path):
    t = ds.create_table(str(tmp_path / "noschema"))  # no schema at all
    with pytest.raises(ValueError):
        t.append_records([{"id": 1, "name": "x"}])


# ---------------------------------------------------------------- #20 null filters
def test_is_null_and_is_not_null(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "a"}, {"id": 2, "name": None}, {"id": 3, "name": "c"}])
    assert t.scan(filter={"name": ("is_null", True)}) == [{"id": 2, "name": None}]
    assert {r["id"] for r in t.scan(filter={"name": ("is_not_null", True)})} == {1, 3}


def test_scan_and_batches_agree_on_is_null(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "a"}, {"id": 2, "name": None}])
    scan_rows = t.scan(filter={"name": ("is_null", True)})
    batch_rows = [r for b in t.scan_batches(filter={"name": ("is_null", True)}) for r in b]
    assert scan_rows == batch_rows == [{"id": 2, "name": None}]


# ---------------------------------------------------------------- #21 overwrite
def test_overwrite_by_filter_raises(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "x"}])
    tx = t.new_transaction()
    tx.begin()
    with pytest.raises(NotImplementedError):
        tx.overwrite_by_filter(lambda r: True)
    tx.rollback()


# ---------------------------------------------------------------- #23 expire
def test_expire_snapshots_removes_old_keeps_current(tmp_path):
    import time
    t = _table(tmp_path)
    for i in range(3):
        t.append_records([{"id": i, "name": "n"}])
    before = len(t.snapshots())
    cutoff = int(time.time() * 1000) + 10_000_000
    with t.new_transaction() as tx:
        tx.expire_snapshots(older_than_ms=cutoff)
    after = t.snapshots()
    assert len(after) < before
    assert any(s["snapshot_id"] == t.current_snapshot().snapshot_id for s in after)
    assert t.row_count() == 3  # data still readable


# ---------------------------------------------------------------- #25 operators
def test_unknown_operator_raises(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "apple"}])
    with pytest.raises(ValueError):
        t.scan(filter={"name": ("startswith", "a")})


def test_empty_in_and_not_in(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
    assert t.scan(filter={"id": ("in", [])}) == []
    assert {r["id"] for r in t.scan(filter={"id": ("not_in", [])})} == {1, 2}


# ---------------------------------------------------------------- #26 schema validation
def test_extra_field_on_append_raises_and_table_survives(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "x"}])
    with pytest.raises(ValueError):
        t.append_records([{"id": 2, "name": "y", "extra": 9}])
    assert t.scan() == [{"id": 1, "name": "x"}]


def test_divergent_schema_append_rejected(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "x"}])
    other = Schema(schema_id=0, fields=[{"id": 1, "name": "id", "type": "long"}])
    with pytest.raises(ValueError):
        t.append_records([{"id": 2}], schema=other)


# ---------------------------------------------------------------- #27 lineage
def test_manifest_entry_snapshot_id_matches_committed_snapshot(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "x"}])
    snap = t.current_snapshot()
    data_files = t._get_all_data_files()
    assert data_files
    for df in data_files:
        assert df.added_snapshot_id == snap.snapshot_id


# ---------------------------------------------------------------- #41 schema ids
def test_duplicate_field_id_raises():
    with pytest.raises(ValueError):
        Schema(schema_id=0, fields=[
            {"id": 1, "name": "a", "type": "long"},
            {"id": 1, "name": "b", "type": "long"},
        ])


# ---------------------------------------------------------------- #29 path traversal
def test_sibling_prefix_path_rejected(tmp_path):
    base = tmp_path / "wh"
    base.mkdir()
    (tmp_path / "wh2").mkdir()
    storage = LocalStorageBackend(str(base))
    with pytest.raises(ValueError):
        storage.exists("../wh2/secret")


# ---------------------------------------------------------------- #42 checksums / #19 fail closed
def test_corrupt_data_file_raises_on_scan(tmp_path):
    import glob
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "x"}])
    pq_file = glob.glob(str(tmp_path / "t" / "data" / "*.parquet"))[0]
    with open(pq_file, "r+b") as f:
        f.seek(0)
        f.write(b"\x00\x00\x00\x00")
    with pytest.raises((CorruptDataError, Exception)):
        t.scan()


def test_scan_fails_closed_on_missing_manifest(tmp_path):
    import glob
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "x"}])
    # Delete a manifest referenced by the current snapshot
    for m in glob.glob(str(tmp_path / "t" / "metadata" / "manifests" / "manifest_*.avro")):
        os.remove(m)
    with pytest.raises((RuntimeError, OSError)):
        t.scan(verify_checksums=False)


# ---------------------------------------------------------------- #16/#32 GC
def test_gc_fails_closed_on_unreadable_manifest(tmp_path):
    import glob
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "x"}])
    manifests = glob.glob(str(tmp_path / "t" / "metadata" / "manifests" / "manifest_*.avro"))
    with open(manifests[0], "r+b") as f:
        f.seek(0)
        f.write(b"garbage-not-avro")
    with pytest.raises(GarbageCollectionAborted):
        t.garbage_collect(grace_period_ms=0)


def test_gc_reports_reachable_files_not_deleted(tmp_path):
    t = _table(tmp_path)
    for i in range(3):
        t.append_records([{"id": i, "name": "n"}])
    stats = t.garbage_collect(grace_period_ms=0)
    # All manifests/data are reachable (no expiry) -> nothing deleted, table intact
    assert stats["data_files"] == 0
    assert t.row_count() == 3


# ---------------------------------------------------------------- #22 version hint
def test_recovers_from_corrupt_version_hint(tmp_path):
    t = _table(tmp_path)
    for i in range(2):
        t.append_records([{"id": i, "name": "n"}])
    hint = tmp_path / "t" / "metadata.version-hint.text"
    hint.write_text("not-a-number")
    reopened = ds.load_table(str(tmp_path / "t"))
    assert reopened.row_count() == 2


def test_create_table_does_not_destroy_existing_on_bad_hint(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "x"}])
    (tmp_path / "t" / "metadata.version-hint.text").write_text("garbage")
    again = ds.create_table(str(tmp_path / "t"), schema=_schema())
    assert again.row_count() == 1


def test_initialize_table_refuses_existing(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "x"}])
    from datashard.data_structures import TableMetadata
    with pytest.raises(TableExistsError):
        t.metadata_manager.initialize_table(TableMetadata(location=str(tmp_path / "t")))


# ---------------------------------------------------------------- #24 delete_snapshot
def test_delete_current_snapshot_repoints_by_lineage(tmp_path):
    t = _table(tmp_path)
    ids = []
    for i in range(3):
        t.append_records([{"id": i, "name": "n"}])
        ids.append(t.current_snapshot().snapshot_id)
    assert t.snapshot_manager.delete_snapshot(ids[-1]) is True
    assert t.current_snapshot().snapshot_id == ids[-2]


# ---------------------------------------------------------------- #14 no post-commit rollback
def test_committed_data_survives_post_commit_failure(tmp_path, monkeypatch):
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "keep"}])
    tx = t.new_transaction()
    tx.begin()
    tx.append_data(records=[{"id": 2, "name": "committed"}])

    real = MetadataManager.refresh
    state = {"n": 0}

    def flaky(self):
        state["n"] += 1
        if state["n"] == 3:
            raise OSError("injected post-commit failure")
        return real(self)

    monkeypatch.setattr(MetadataManager, "refresh", flaky)
    try:
        tx.commit()
    except Exception:
        pass
    monkeypatch.setattr(MetadataManager, "refresh", real)

    reopened = ds.load_table(str(tmp_path / "t"))
    assert {r["id"] for r in reopened.scan()} == {1, 2}


# ---------------------------------------------------------------- #43 hygiene
def test_empty_commit_creates_no_snapshot(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "x"}])
    n = len(t.snapshots())
    with t.new_transaction():
        pass  # no operations
    assert len(t.snapshots()) == n


def test_transaction_reuse_does_not_double_apply(tmp_path):
    t = _table(tmp_path)
    tx = t.new_transaction()
    tx.begin()
    tx.append_data(records=[{"id": 1, "name": "a"}])
    tx.commit()
    tx.begin()
    tx.append_data(records=[{"id": 2, "name": "b"}])
    tx.commit()
    assert {r["id"] for r in t.scan()} == {1, 2}
    assert t.row_count() == 2  # 'a' not appended twice


def test_version_is_not_stale_constant():
    assert ds.__version__ != "0.3.2"


# ---------------------------------------------------------------- #31 FileLock timeout
def test_filelock_timeout_enforced(tmp_path):
    from datashard.file_lock import FileLock
    lock_path = str(tmp_path / "l.lock")
    a = FileLock(lock_path, timeout=0.5)
    b = FileLock(lock_path, timeout=0.5)
    assert a.acquire() is True
    try:
        with pytest.raises(TimeoutError):
            b.acquire()
    finally:
        a.release()


# ---------------------------------------------------------------- OCC lost-update
def test_concurrent_appends_no_lost_update(tmp_path):
    t = _table(tmp_path)
    n_workers = 8

    def worker(i):
        t.append_records([{"id": i, "name": f"w{i}"}])

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_workers)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    rows = t.scan()
    # CONTENT assertion (not just snapshot count): every worker's row must survive
    assert {r["id"] for r in rows} == set(range(n_workers))
    assert t.row_count() == n_workers


# ---------------------------------------------------------------- #40 write_pandas_file
def test_write_pandas_file_local(tmp_path):
    pd = pytest.importorskip("pandas")
    t = _table(tmp_path)
    dfm = t.file_manager.data_file_manager
    df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    data_file = dfm.write_pandas_file("data/pw.parquet", df, _schema())
    assert data_file.record_count == 2
    assert data_file.file_size_in_bytes > 0
    assert data_file.checksum  # checksum populated
