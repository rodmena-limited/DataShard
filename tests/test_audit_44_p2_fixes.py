"""Regression tests for audit #44 P2/P3 clusters (tickets #50-#52).

Covers Iceberg fidelity + auditability (#51), S3/lock robustness (#50, exercised
with a fake S3 client since CI has no bucket) and the hygiene items that change
behaviour (#52).
"""
import os
import time

import pytest

import datashard as ds
from datashard.data_structures import Schema
from datashard.filters import FilterExpression, FilterOp, to_pyarrow_compute_expression


def _schema():
    return Schema(schema_id=0, fields=[
        {"id": 1, "name": "id", "type": "long"},
        {"id": 2, "name": "name", "type": "string"},
    ])


def _table(tmp_path, name="t"):
    return ds.create_table(str(tmp_path / name), schema=_schema())


# ==================================================== #51 Iceberg fidelity
def test_snapshots_get_monotonic_sequence_numbers(tmp_path):
    t = _table(tmp_path)
    for i in range(3):
        t.append_records([{"id": i, "name": "n"}])

    metadata = t.metadata_manager.refresh()
    seqs = [s.sequence_number for s in metadata.snapshots]
    assert seqs == [1, 2, 3]
    assert metadata.last_sequence_number == 3


def test_manifest_entries_carry_and_inherit_sequence_numbers(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "a"}])
    t.append_records([{"id": 2, "name": "b"}])

    snapshot = t.current_snapshot()
    manifests = t.file_manager.read_manifest_list_file(snapshot.manifest_list.lstrip("/"))
    assert [m.sequence_number for m in manifests] == [1, 2]

    seq_by_file = {}
    for m in manifests:
        for df in t.file_manager.read_manifest_file(m.manifest_path.lstrip("/")):
            seq_by_file[df.file_path] = df.sequence_number
    assert sorted(seq_by_file.values()) == [1, 2]

    # A delete rewrites a manifest; survivors must KEEP their original sequence
    # number rather than being re-dated by the rewriting snapshot.
    victim = min(seq_by_file, key=lambda p: seq_by_file[p])
    with t.new_transaction() as tx:
        tx.delete_files([victim])
    survivors = {}
    snapshot = t.current_snapshot()
    for m in t.file_manager.read_manifest_list_file(snapshot.manifest_list.lstrip("/")):
        for df in t.file_manager.read_manifest_file(m.manifest_path.lstrip("/")):
            survivors[df.file_path] = df.sequence_number
    assert survivors and all(seq == 2 for seq in survivors.values())


def test_snapshots_record_schema_id(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "a"}])
    metadata = t.metadata_manager.refresh()
    assert all(s.schema_id == metadata.current_schema_id for s in metadata.snapshots)


def test_metadata_log_records_superseded_versions(tmp_path):
    t = _table(tmp_path)
    for i in range(3):
        t.append_records([{"id": i, "name": "n"}])

    log = t.metadata_manager.refresh().metadata_log
    assert len(log) == 3
    for entry in log:
        assert entry["metadata-file"].startswith("metadata/v")
        assert isinstance(entry["timestamp-ms"], int)
        # The superseded file must actually exist (the chain is walkable).
        assert (tmp_path / "t" / entry["metadata-file"]).exists()
    # No duplicates: every commit records a distinct predecessor.
    assert len({e["metadata-file"] for e in log}) == len(log)


def test_metadata_log_is_trimmed_to_property(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 0, "name": "n"}])
    base = t.metadata_manager.refresh()
    base.properties["write.metadata.previous-versions-max"] = "2"
    t.metadata_manager.commit(t.metadata_manager.refresh(), base)

    for i in range(3):
        t.append_records([{"id": i, "name": "n"}])
    assert len(t.metadata_manager.refresh().metadata_log) == 2


def test_expire_snapshots_leaves_no_dangling_parent(tmp_path):
    t = _table(tmp_path)
    for i in range(4):
        t.append_records([{"id": i, "name": "n"}])

    cutoff = int(time.time() * 1000) + 10_000_000
    with t.new_transaction() as tx:
        tx.expire_snapshots(older_than_ms=cutoff)

    metadata = t.metadata_manager.refresh()
    live_ids = {s.snapshot_id for s in metadata.snapshots}
    for s in metadata.snapshots:
        assert s.parent_snapshot_id in live_ids or s.parent_snapshot_id in (None, -1)
    assert t.row_count() == 4  # data still readable


def test_delete_snapshot_repoints_children_to_surviving_ancestor(tmp_path):
    t = _table(tmp_path)
    for i in range(3):
        t.append_records([{"id": i, "name": "n"}])

    metadata = t.metadata_manager.refresh()
    ordered = sorted(metadata.snapshots, key=lambda s: s.sequence_number or 0)
    first, middle, last = ordered
    assert last.parent_snapshot_id == middle.snapshot_id

    assert t.snapshot_manager.delete_snapshot(middle.snapshot_id) is True

    after = t.metadata_manager.refresh()
    live_ids = {s.snapshot_id for s in after.snapshots}
    assert middle.snapshot_id not in live_ids
    new_last = next(s for s in after.snapshots if s.snapshot_id == last.snapshot_id)
    assert new_last.parent_snapshot_id == first.snapshot_id


def test_gc_protects_in_flight_manifests(tmp_path):
    """A commit in progress has written manifests that no snapshot references
    yet; a concurrent GC with a zero grace period must not delete them."""
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "a"}])

    manifests_dir = tmp_path / "t" / "metadata" / "manifests"
    inflight_dir = tmp_path / "t" / "metadata" / "inflight"
    os.makedirs(inflight_dir, exist_ok=True)

    # Simulate a commit that wrote its manifest but has not committed metadata.
    pending = manifests_dir / "manifest_pending_1.avro"
    pending.write_bytes(b"pending manifest content")
    (inflight_dir / "manifest_pending_1.avro.inflight").write_text(
        '{"file_path": "metadata/manifests/manifest_pending_1.avro"}'
    )

    stats = t.garbage_collect(grace_period_ms=0)
    assert pending.exists(), "in-flight manifest was deleted by GC"
    assert stats["manifest_files"] == 0


def test_gc_still_collects_unprotected_orphan_manifest(tmp_path):
    """The protection above must not turn GC into a no-op."""
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "a"}])
    orphan = tmp_path / "t" / "metadata" / "manifests" / "manifest_orphan.avro"
    orphan.write_bytes(b"orphan manifest")
    old = time.time() - 7200
    os.utime(orphan, (old, old))

    stats = t.garbage_collect(grace_period_ms=3600000)
    assert stats["manifest_files"] == 1
    assert not orphan.exists()
    assert t.row_count() == 1


def test_legacy_cleanup_orphaned_files_is_disabled(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "a"}])
    with pytest.raises(NotImplementedError, match="garbage_collect"):
        t.file_manager.cleanup_orphaned_files([])
    assert t.row_count() == 1


# ============================================================== #50 S3 / locks
class _FakeClientError(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


def test_permanent_s3_errors_are_not_retried(monkeypatch):
    from datashard import s3_consistency

    handler = s3_consistency.S3ConsistencyHandler(
        max_retries=4,
        initial_delay=0,
        retryable_exceptions=(_FakeClientError,),
    )
    calls = {"n": 0}

    def op():
        calls["n"] += 1
        raise _FakeClientError("AccessDenied")

    with pytest.raises(_FakeClientError):
        handler.retry_with_backoff(op, "denied op")
    assert calls["n"] == 1, "a permanent error must not be retried"


def test_transient_s3_errors_are_still_retried():
    from datashard import s3_consistency

    handler = s3_consistency.S3ConsistencyHandler(
        max_retries=3,
        initial_delay=0,
        retryable_exceptions=(_FakeClientError,),
    )
    calls = {"n": 0}

    def op():
        calls["n"] += 1
        if calls["n"] < 3:
            raise _FakeClientError("SlowDown")
        return "ok"

    assert handler.retry_with_backoff(op, "flaky op") == "ok"
    assert calls["n"] == 3


def test_polling_lock_refuses_to_renew_after_lease_lapsed():
    """The interleaving that let two writers hold the same lock: a late renewal
    writing our id back over the process that legitimately broke our lock."""
    from datashard.lock_provider import S3PollingLockProvider

    class FakeS3:
        def __init__(self):
            self.puts = 0

        def get_object(self, Bucket, Key):  # noqa: N803
            class Body:
                @staticmethod
                def read():
                    return provider.lock_id.encode()
            return {"Body": Body()}

        def put_object(self, **kwargs):
            self.puts += 1

    fake = FakeS3()
    provider = S3PollingLockProvider(fake, "b", "k", timeout=1, lease_seconds=60)
    provider.is_locked = True
    provider._lease_deadline = time.monotonic() - 1  # lease already lapsed

    provider._renew_once()
    assert fake.puts == 0, "renewed a lapsed lease (would resurrect a stolen lock)"
    assert provider.is_locked is False
    assert provider.is_held() is False


def test_polling_lock_is_held_false_after_lease_expiry():
    from datashard.lock_provider import S3PollingLockProvider

    class FakeS3:
        def get_object(self, Bucket, Key):  # noqa: N803
            raise AssertionError("must not consult S3 once our lease has lapsed")

    provider = S3PollingLockProvider(FakeS3(), "b", "k", timeout=1, lease_seconds=60)
    provider.is_locked = True
    provider._lease_deadline = time.monotonic() - 0.001
    assert provider.is_held() is False


def test_s3_exists_does_not_treat_prefix_as_object(monkeypatch):
    """exists('data/x.parquet') must be False when only 'data/x.parquet/...'
    objects exist - otherwise a missing data file passes validation."""
    from datashard.storage_backend import S3StorageBackend

    class FakeS3:
        def head_object(self, Bucket, Key):  # noqa: N803
            raise _FakeClientError("404")

        def list_objects_v2(self, **kwargs):
            return {"Contents": [{"Key": "whatever"}]}

    backend = S3StorageBackend.__new__(S3StorageBackend)
    backend.s3 = FakeS3()
    backend.bucket = "b"
    backend.prefix = ""

    import datashard.storage_backend as sb

    monkeypatch.setattr(sb, "ClientError", _FakeClientError)
    assert backend.exists("data/x.parquet") is False
    assert backend.exists("data/") is True


# ==================================================================== #52
def test_equality_filter_with_none_raises(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1, "name": "a"}, {"id": 2, "name": None}])
    with pytest.raises(ValueError, match="is_null"):
        t.scan(filter={"name": None})
    # The documented alternative works.
    assert t.scan(filter={"name": ("is_null", True)}) == [{"id": 2, "name": None}]


def test_compute_expression_keeps_null_operators():
    assert to_pyarrow_compute_expression(
        [FilterExpression("name", FilterOp.IS_NULL, None)]
    ) is not None


def test_dead_to_pyarrow_filter_is_gone():
    import datashard.filters as filters

    assert not hasattr(filters, "to_pyarrow_filter")


def test_docs_version_matches_pyproject():
    import re
    import runpy

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, "pyproject.toml")) as f:
        pyproject_version = re.search(r'^version\s*=\s*"([^"]+)"', f.read(), re.MULTILINE)
    conf = runpy.run_path(os.path.join(root, "docs", "conf.py"))
    assert conf["release"] == pyproject_version.group(1)
