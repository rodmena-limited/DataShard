"""Regression tests for the re-audit of 0.8.0 (issuedb #75): tickets #76 and #77."""
import glob
import os
import time

import datashard.garbage_collector as gcmod
from datashard import Schema, create_table, load_table
from datashard.garbage_collector import GarbageCollector

SCHEMA = Schema(schema_id=1, fields=[
    {"id": 1, "name": "id", "type": "long", "required": True},
    {"id": 2, "name": "name", "type": "string", "required": False},
])


def _commit_during_reachability(gc, path, row):
    """Make the first manifest read of a GC run inject a commit from another handle."""
    orig, state = gc.file_manager.read_manifest_file, {"done": False}

    def hook(*a, **k):
        if not state["done"]:
            state["done"] = True
            load_table(path).append_records([row], SCHEMA)
        return orig(*a, **k)

    gc.file_manager.read_manifest_file = hook


# ---------------------------------------------------------------- #76
def test_gc_keeps_metadata_chain_when_a_commit_lands_during_gc(tmp_path):
    path = str(tmp_path / "t")
    t = create_table(path, SCHEMA)
    for i in range(15):
        t.append_records([{"id": i}], SCHEMA)
    meta = tmp_path / "t" / "metadata"
    for f in glob.glob(str(meta / "*.json")):
        os.utime(f, (1, 1))  # only the keep-rules protect anything now
    gc = GarbageCollector(path, t.metadata_manager, t.file_manager)
    _commit_during_reachability(gc, path, {"id": 99})
    gc.collect(grace_period_ms=0, allow_short_grace=True)
    after = load_table(path)
    for entry in after.metadata_manager.refresh().metadata_log:
        assert os.path.exists(os.path.join(path, entry["metadata-file"])), entry
    versions = sorted(int(os.path.basename(f)[1:].split(".")[0]) for f in glob.glob(str(meta / "v*.json")))
    assert versions[-1] == 17 and versions[0] >= 17 - 10  # v1 create + 16 commits; the retention window survived
    assert after.row_count() == 16


# ---------------------------------------------------------------- #77
def test_gc_uses_listing_mtimes_not_one_stat_per_object(tmp_path, monkeypatch):
    t = create_table(str(tmp_path / "t"), SCHEMA)
    t.append_records([{"id": 1}], SCHEMA)
    for i in range(5):
        orphan = tmp_path / "t" / "data" / f"orphan{i}.parquet"
        orphan.write_bytes(b"x")
        os.utime(orphan, (1, 1))
    calls = []
    real = t.storage.get_modified_time
    monkeypatch.setattr(t.storage, "get_modified_time", lambda p: calls.append(p) or real(p))
    stats = t.garbage_collect(grace_period_ms=0, allow_short_grace=True)
    assert stats["data_files"] == 5
    assert calls == []  # ages came from the listing


def test_gc_local_clock_is_the_storage_clock(tmp_path):
    t = create_table(str(tmp_path / "t"), SCHEMA)
    assert abs(t.storage.clock_ms() - time.time() * 1000) < 5000


def test_s3_gc_survives_a_fast_client_clock(s3_env):
    """The exposure: a commit landing during GC, judged against a client clock 2 h fast."""
    path = s3_env.table_name()
    t = create_table(path, SCHEMA)
    for i in range(3):
        t.append_records([{"id": i}], SCHEMA)
    time.sleep(1.1)
    gc = GarbageCollector(path, t.metadata_manager, t.file_manager)
    _commit_during_reachability(gc, path, {"id": 99})
    real_time = time.time
    gcmod.time.time = lambda: real_time() + 7200  # only the clock; the fixture env must stay
    try:
        server_ms = t.storage.clock_ms()
        assert abs(server_ms - real_time() * 1000) < 60_000  # the server clock is not the patched one
        stats = gc.collect(grace_period_ms=3600000)
    finally:
        gcmod.time.time = real_time
    assert stats["data_files"] == 0 and stats["manifest_files"] == 0 and stats["metadata_files"] == 0
    assert load_table(path).row_count() == 4


def test_s3_listing_carries_mtimes(s3_env):
    t = create_table(s3_env.table_name(), SCHEMA)
    t.append_records([{"id": 1}], SCHEMA)
    listed = t.storage.list_files_with_mtime("data")
    assert len(listed) == 1 and listed[0][0].startswith("data/") and abs(listed[0][1] - time.time()) < 300
