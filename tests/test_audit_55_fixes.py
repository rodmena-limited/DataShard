"""Regression tests for the audit #55 findings (tickets #56-#74).

Each test is the unit-sized twin of a live probe under audit/evaluations/.
"""
import glob
import io
import os
import shutil
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import fastavro
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

import datashard.disk_utils as disk_utils
import datashard.metadata_manager as mm_module
from datashard import (
    AmbiguousMetadataError,
    ConcurrentModificationException,
    CorruptDataError,
    DataFile,
    FileFormat,
    GarbageCollectionAborted,
    Schema,
    SchemaMismatchError,
    create_table,
    load_table,
)
from datashard.data_structures import ManifestContent
from datashard.garbage_collector import GarbageCollector

SCHEMA = Schema(
    schema_id=1,
    fields=[
        {"id": 1, "name": "id", "type": "long", "required": True},
        {"id": 2, "name": "name", "type": "string", "required": False},
    ],
)


def _table(tmp_path, name="t"):
    return create_table(str(tmp_path / name), SCHEMA)


def _age(path, seconds=7200):
    old = time.time() - seconds
    os.utime(path, (old, old))


def _manifests(tmp_path, name="t"):
    return sorted(glob.glob(str(tmp_path / name / "metadata" / "manifests" / "*.avro")))


# ---------------------------------------------------------------- #56
def test_gc_keeps_live_data_for_a_table_named_data(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    t = create_table("data", SCHEMA)
    for i in range(3):
        t.append_records([{"id": i}], SCHEMA)
    stats = t.garbage_collect(grace_period_ms=0, allow_short_grace=True)
    assert stats["data_files"] == 0 and stats["manifest_files"] == 0
    assert len(load_table("data").scan()) == 3


# ---------------------------------------------------------------- #57
def test_gc_refuses_short_grace_without_opt_in(tmp_path):
    with pytest.raises(ValueError, match="allow_short_grace"):
        _table(tmp_path).garbage_collect(grace_period_ms=1000)


def test_gc_never_deletes_a_commit_that_lands_while_it_runs(tmp_path):
    """Cutoff is relative to the GC start instant, so files written after it are safe."""
    t = _table(tmp_path)
    t.append_records([{"id": 0}], SCHEMA)
    writer = load_table(t.table_path)
    gc = GarbageCollector(t.table_path, t.metadata_manager, t.file_manager)
    orig, state = gc.file_manager.read_manifest_file, {"done": False}

    def slow(*a, **k):
        if not state["done"]:
            state["done"] = True
            writer.append_records([{"id": 1}], SCHEMA)  # lands during reachability
            time.sleep(0.25)
        return orig(*a, **k)

    gc.file_manager.read_manifest_file = slow
    stats = gc.collect(grace_period_ms=100, allow_short_grace=True)
    assert stats["data_files"] == 0 and stats["manifest_files"] == 0
    assert len(load_table(t.table_path).scan()) == 2


def test_gc_loads_markers_before_metadata_so_long_transactions_survive(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 0}], SCHEMA)
    tx = load_table(t.table_path).new_transaction().begin()
    tx.append_data([{"id": 1}], SCHEMA)  # file + marker exist, far older than the grace
    for f in glob.glob(str(tmp_path / "t" / "data" / "*.parquet")):
        _age(f)
    gc = GarbageCollector(t.table_path, t.metadata_manager, t.file_manager)
    orig, state = gc.file_manager.read_manifest_file, {"done": False}

    def slow(*a, **k):
        if not state["done"]:
            state["done"] = True
            tx.commit()  # marker removed while GC is mid-reachability
        return orig(*a, **k)

    gc.file_manager.read_manifest_file = slow
    stats = gc.collect(grace_period_ms=1000, allow_short_grace=True)
    assert stats["data_files"] == 0
    assert len(load_table(t.table_path).scan()) == 2


# ---------------------------------------------------------------- #58
def test_truncated_manifest_is_rejected_not_read_short(tmp_path):
    fm = _table(tmp_path).file_manager
    files = [
        DataFile(file_path=f"/data/f{i:04d}.parquet", file_format=FileFormat.PARQUET,
                 partition_values={}, record_count=1, file_size_in_bytes=1)
        for i in range(3000)
    ]
    mf = fm.create_manifest_file(files, ManifestContent.DATA, snapshot_id=1, sequence_number=1)
    mpath = tmp_path / "t" / mf.manifest_path
    raw = mpath.read_bytes()
    sync = fastavro.reader(io.BytesIO(raw))._header["sync"]
    second = raw.find(sync, raw.find(sync) + len(sync))
    mpath.write_bytes(raw[: second + len(sync)])  # cut exactly on a block boundary
    assert len(fm.read_manifest_file(mf.manifest_path)) < 3000  # what fastavro alone sees
    with pytest.raises(CorruptDataError, match="bytes were recorded"):
        fm.read_manifest_file(mf.manifest_path, expected_length=mf.manifest_length,
                              expected_checksum=mf.checksum)


def test_manifest_list_integrity_recorded_and_enforced(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1}], SCHEMA)
    snap = t.current_snapshot()
    assert snap.summary["manifest-list-length"].isdigit()
    assert len(snap.summary["manifest-list-sha256"]) == 64
    (tmp_path / "t" / snap.manifest_list).write_bytes(b"Obj\x01 not really avro")
    with pytest.raises(CorruptDataError):
        t.scan()
    with pytest.raises(GarbageCollectionAborted):
        t.garbage_collect(grace_period_ms=0, allow_short_grace=True)


# ---------------------------------------------------------------- #60
def test_clean_commit_failure_removes_its_metadata_file(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1}], SCHEMA)
    before = set(os.listdir(tmp_path / "t" / "metadata"))
    real_write = t.storage.write_file

    def failing_hint(path, content):
        if path == t.metadata_manager.HINT_PATH:
            raise OSError("simulated disk error at the commit point")
        return real_write(path, content)

    t.storage.write_file = failing_hint
    with pytest.raises(OSError):
        t.append_records([{"id": 2}], SCHEMA)
    t.storage.write_file = real_write
    new_files = {f for f in os.listdir(tmp_path / "t" / "metadata") if f.endswith(".metadata.json")} - before
    assert not new_files
    assert t.row_count() == 1


def test_ambiguous_hint_recovery_refuses_and_repair_restores(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1}], SCHEMA)
    v1 = t.metadata_manager._current_version_info()[1]
    t.append_records([{"id": 2}], SCHEMA)
    v2 = t.metadata_manager._current_version_info()[1]
    meta = tmp_path / "t" / "metadata"
    shutil.copy(meta / v1, meta / "v2-deadbeef.metadata.json")  # a crashed racer's leftover
    os.remove(tmp_path / "t" / "metadata.version-hint.text")
    with pytest.raises(AmbiguousMetadataError, match="v2-deadbeef"):
        load_table(str(tmp_path / "t"))
    t.repair_version_hint(v2)
    fixed = load_table(str(tmp_path / "t"))
    assert fixed.row_count() == 2
    fixed.append_records([{"id": 3}], SCHEMA)
    assert load_table(str(tmp_path / "t")).row_count() == 3
    with pytest.raises(ValueError):
        t.repair_version_hint("not-a-metadata-file")


# ---------------------------------------------------------------- #61
def test_delete_of_unknown_path_raises_and_creates_no_snapshot(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1}], SCHEMA)
    with pytest.raises(FileNotFoundError, match="not part of the current snapshot"):
        with t.new_transaction() as tx:
            tx.delete_files(["/data/nope.parquet"])
            tx.commit()
    assert len(t.snapshots()) == 1 and t.row_count() == 1


def test_delete_matches_paths_with_or_without_leading_slash(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1}], SCHEMA)
    t.append_records([{"id": 2}], SCHEMA)
    files = t._get_all_data_files()
    with t.new_transaction() as tx:
        tx.delete_files([files[0].file_path.lstrip("/")])  # manifest stores '/data/...'
        tx.commit()
    assert t.row_count() == 1
    with t.new_transaction() as tx:
        tx.delete_files(["/" + files[1].file_path.lstrip("/")])
        tx.commit()
    assert t.row_count() == 0


# ---------------------------------------------------------------- #62
def test_reordered_schema_is_written_in_table_order(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "a"}], SCHEMA)
    reordered = Schema(schema_id=1, fields=list(reversed(SCHEMA.fields)))
    fresh = load_table(t.table_path)  # empty arrow-schema cache, like a new process
    fresh.append_records([{"id": 2, "name": "b"}], reordered)
    for f in glob.glob(str(tmp_path / "t" / "data" / "*.parquet")):
        assert pq.read_schema(f).names == ["id", "name"]
    assert sorted(r["id"] for r in load_table(t.table_path).scan()) == [1, 2]


# ---------------------------------------------------------------- #64
def test_create_table_with_conflicting_schema_raises(tmp_path):
    _table(tmp_path)
    other = Schema(schema_id=2, fields=[{"id": 1, "name": "price", "type": "double"}])
    with pytest.raises(SchemaMismatchError):
        create_table(str(tmp_path / "t"), other)
    kept = create_table(str(tmp_path / "t"), other, if_exists="ignore")
    assert [f["name"] for f in kept._get_current_schema().fields] == ["id", "name"]
    assert not create_table(str(tmp_path / "t"), SCHEMA).created  # same schema: fine


# ---------------------------------------------------------------- #66
def test_default_integrity_mode_reads_only_projected_columns(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": i, "name": "x" * 100} for i in range(5000)], SCHEMA)
    data_reads = []
    real = t.storage.read_file
    t.storage.read_file = lambda p: (data_reads.append(p) if p.lstrip("/").startswith("data/") else None) or real(p)
    assert len(t.scan(columns=["id"])) == 5000
    assert data_reads == []  # no whole-file read in "page" mode
    assert len(t.scan(columns=["id"], verify_checksums="full")) == 5000
    assert len(data_reads) == 1  # "full" downloads the file to hash it
    with pytest.raises(ValueError):
        t.scan(verify_checksums="sometimes")


# ---------------------------------------------------------------- #69
def test_append_writes_one_row_group_and_empty_append_is_a_noop(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": i} for i in range(50_000)], SCHEMA)
    (f,) = glob.glob(str(tmp_path / "t" / "data" / "*.parquet"))
    assert pq.ParquetFile(f).metadata.num_row_groups == 1
    assert t.append_records([], SCHEMA) is True
    assert len(t.snapshots()) == 1 and len(t._get_all_data_files()) == 1


def test_append_pandas_native_path_and_strictness(tmp_path):
    t = _table(tmp_path)
    df = pd.DataFrame({"id": np.arange(10, dtype="int64"), "name": ["n"] * 10})
    t.append_pandas(df, SCHEMA)
    assert t.row_count() == 10
    with pytest.raises(ValueError, match="not in the table schema"):
        t.append_pandas(df.assign(extra=1), SCHEMA)
    with pytest.raises(ValueError, match="null"):
        t.append_pandas(pd.DataFrame({"id": [1, None], "name": ["a", "b"]}), SCHEMA)


# ---------------------------------------------------------------- #70
def test_disk_guard_uses_an_absolute_floor(monkeypatch):
    TB = 1024**4
    monkeypatch.setattr(disk_utils.shutil, "disk_usage",
                        lambda p: shutil._ntuple_diskusage(10 * TB, int(9.6 * TB), int(0.4 * TB)))
    disk_utils.check_disk_space("/", 1024)  # 96% used, 400 GB free: fine
    monkeypatch.setattr(disk_utils.shutil, "disk_usage",
                        lambda p: shutil._ntuple_diskusage(10 * TB, 10 * TB - 100, 100))
    with pytest.raises(IOError):
        disk_utils.check_disk_space("/", 1024)


# ---------------------------------------------------------------- #68
def test_expire_snapshots_retain_last_and_properties(tmp_path):
    t = _table(tmp_path)
    for i in range(6):
        t.append_records([{"id": i}], SCHEMA)
    assert t.expire_snapshots(retain_last=2) == 4
    assert len(t.snapshots()) == 2 and t.row_count() == 6
    assert t.set_properties({"datashard.manifest.compaction-threshold": "3", "x": "1"})
    assert t.properties()["x"] == "1"
    t.set_properties({"x": None})
    assert "x" not in t.properties()
    with pytest.raises(ValueError):
        t.new_transaction().begin().expire_snapshots()


def test_manifests_compact_at_threshold_and_on_demand(tmp_path):
    t = _table(tmp_path)
    t.set_properties({"datashard.manifest.compaction-threshold": "3"})
    for i in range(4):
        t.append_records([{"id": i}], SCHEMA)
    active = t.file_manager.read_manifest_list_file(t.current_snapshot().manifest_list)
    assert len(active) == 2  # 3 manifests compacted into 1, then the 4th append
    assert t.row_count() == 4 and sorted(r["id"] for r in t.scan()) == [0, 1, 2, 3]
    assert t.compact_manifests() is True
    assert len(t.file_manager.read_manifest_list_file(t.current_snapshot().manifest_list)) == 1
    assert t.compact_manifests() is False  # nothing left to compact
    assert t.current_snapshot().operation == "replace"


def test_gc_reclaims_superseded_metadata_files(tmp_path):
    t = _table(tmp_path)
    for i in range(15):
        t.append_records([{"id": i}], SCHEMA)
    meta = tmp_path / "t" / "metadata"
    assert len(glob.glob(str(meta / "v*.metadata.json"))) == 16
    (meta / ".tmp.leftover").write_bytes(b"x")
    for f in glob.glob(str(meta / "*")):
        if os.path.isfile(f):
            _age(f)
    stats = t.garbage_collect(grace_period_ms=0, allow_short_grace=True)
    remaining = glob.glob(str(meta / "v*.metadata.json"))
    assert stats["metadata_files"] == 16 - 11 + 1
    assert len(remaining) == 11  # current + write.metadata.previous-versions-max (10)
    assert not (meta / ".tmp.leftover").exists()
    assert load_table(t.table_path).row_count() == 15


# ---------------------------------------------------------------- #72
def test_scan_as_of_snapshot(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1}], SCHEMA)
    first = t.current_snapshot().snapshot_id
    t.append_records([{"id": 2}], SCHEMA)
    assert [r["id"] for r in t.scan(snapshot_id=first)] == [1]
    assert t.row_count(snapshot_id=first) == 1 and t.row_count() == 2
    assert sum(len(b) for b in t.scan_batches(snapshot_id=first)) == 1
    with pytest.raises(ValueError, match="does not exist"):
        t.scan(snapshot_id=12345)


# ---------------------------------------------------------------- #73
def test_decimal_and_timestamptz_round_trip_exactly(tmp_path):
    schema = Schema(schema_id=1, fields=[
        {"id": 1, "name": "px", "type": "decimal(18,8)", "required": True},
        {"id": 2, "name": "ts", "type": "timestamptz", "required": True},
    ])
    t = create_table(str(tmp_path / "fin"), schema)
    t.append_records([{"px": Decimal("101.12345678"),
                       "ts": datetime(2026, 1, 1, 12, 0, tzinfo=timezone(timedelta(hours=2)))}], schema)
    t.append_records([{"px": Decimal("99.5"), "ts": datetime(2026, 1, 2, tzinfo=timezone.utc)}], schema)
    rows = t.scan(filter={"px": (">", Decimal("100"))})
    assert rows[0]["px"] == Decimal("101.12345678")
    assert rows[0]["ts"] == datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    (df,) = [d for d in t._get_all_data_files() if d.lower_bounds[1] == Decimal("99.50000000")]
    assert isinstance(df.upper_bounds[1], Decimal)
    with pytest.raises(ValueError, match="decimal"):
        Schema(schema_id=1, fields=[{"id": 1, "name": "x", "type": "decimal"}])


# ---------------------------------------------------------------- #74
def test_same_millisecond_metadata_only_commits_conflict(tmp_path, monkeypatch):
    t = _table(tmp_path)
    t.append_records([{"id": 1}], SCHEMA)
    frozen = datetime(2030, 1, 1, 12, 0, 0)

    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return frozen

    monkeypatch.setattr(mm_module, "datetime", FrozenDatetime)
    mm = t.metadata_manager
    base = mm.refresh()
    first = mm.refresh()
    first.properties["a"] = "1"
    mm.commit(base, first)
    stale_second = mm.refresh()
    stale_second.properties["b"] = "2"
    with pytest.raises(ConcurrentModificationException):
        mm.commit(base, stale_second)  # same last_updated_ms and snapshot id as base


def test_lost_occ_attempt_removes_its_manifests(tmp_path):
    tA = _table(tmp_path)
    tA.append_records([{"id": 0}], SCHEMA)
    tB = load_table(tA.table_path)
    tx = tA.new_transaction().begin()
    tx.append_data([{"id": 1}], SCHEMA)
    real_commit, state = tA.metadata_manager.commit, {"raced": False}

    def racing_commit(base, new):
        if not state["raced"]:
            state["raced"] = True
            tB.append_records([{"id": 2}], SCHEMA)  # B wins the race
        return real_commit(base, new)

    tA.metadata_manager.commit = racing_commit
    assert tx.commit() is True
    # base commit (2) + B (2) + A's retry (2); A's lost attempt left nothing behind
    assert len(_manifests(tmp_path)) == 6
    assert sorted(r["id"] for r in load_table(tA.table_path).scan()) == [0, 1, 2]


def test_local_write_is_complete_even_with_short_os_write(tmp_path, monkeypatch):
    import datashard.storage_backend as sb

    real_write = os.write
    monkeypatch.setattr(sb.os, "write", lambda fd, buf: real_write(fd, memoryview(buf)[:7]))
    t = _table(tmp_path)  # every metadata/hint write goes through the loop
    t.append_records([{"id": 1}], SCHEMA)
    assert load_table(t.table_path).row_count() == 1
