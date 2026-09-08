"""The operational gaps a production review of a 299-table, 7.8 GB lake found (#94).

The reporter ran datashard as the tape store for a live market-making system and hit
three things the library never said out loud:

  * one append = one commit = a full rewrite of a metadata document that lists every
    snapshot, so metadata bytes grow with the SQUARE of the commit count. Their lake
    reached 3.47 GB of metadata carrying 9.5 MB of data;
  * `row_count()` answers from metadata, so a table whose files cannot be read at all
    still reports a healthy count - a monitor built on it reads green;
  * `expire_snapshots` frees no bytes and `garbage_collect` measures age from mtime,
    which together read as "GC is broken".
"""
import json
import os
import tarfile

import pytest

from datashard import Schema, create_table, load_table, migrate_table
from datashard.metadata_manager import MetadataManager

SCHEMA = Schema(schema_id=1, fields=[
    {"id": 1, "name": "id", "type": "long", "required": True},
    {"id": 2, "name": "payload", "type": "string", "required": False},
])
FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "legacy_table_0_9_1.tar.gz")


def _table(tmp_path, name="t", commits=6):
    t = create_table(str(tmp_path / name), SCHEMA)
    for i in range(commits):
        t.append_records([{"id": i, "payload": "x"}], SCHEMA)
    return t


# ---------------------------------------------------------------- 1. the cost is visible
def test_a_large_metadata_document_warns_with_the_cost_model_and_the_remedy(tmp_path, caplog):
    t = create_table(str(tmp_path / "t"), SCHEMA)
    t.set_properties({MetadataManager.METADATA_WARN_BYTES_PROPERTY: "4096"})  # tiny, to trigger it
    caplog.clear()
    with caplog.at_level("WARNING", logger="datashard.metadata_manager"):
        for i in range(40):
            t.append_records([{"id": i, "payload": "y" * 200}], SCHEMA)
    warnings = [r.message for r in caplog.records if "metadata document" in r.message]
    assert warnings, "a metadata document over the threshold must say so"
    first = warnings[0]
    assert "SQUARE" in first, first                      # the cost model
    assert "new_transaction" in first, first             # remedy 1: batch
    assert "expire_snapshots" in first, first            # remedy 2: prune
    assert "garbage_collect" in first, first             # ... and reclaim
    assert MetadataManager.METADATA_WARN_BYTES_PROPERTY in first, first  # how to silence it
    # once per DOUBLING, not once per commit: 40 commits must not mean 40 warnings
    assert len(warnings) <= 4, f"{len(warnings)} warnings in 40 commits is a flood: {warnings}"


def test_the_metadata_warning_is_silent_below_the_threshold_and_can_be_disabled(tmp_path, caplog):
    t = create_table(str(tmp_path / "t"), SCHEMA)
    with caplog.at_level("WARNING", logger="datashard.metadata_manager"):
        for i in range(10):
            t.append_records([{"id": i}], SCHEMA)
    assert not [r for r in caplog.records if "metadata document" in r.message]

    t.set_properties({MetadataManager.METADATA_WARN_BYTES_PROPERTY: "0"})  # 0 disables
    caplog.clear()
    with caplog.at_level("WARNING", logger="datashard.metadata_manager"):
        for i in range(30):
            t.append_records([{"id": i, "payload": "z" * 400}], SCHEMA)
    assert not [r for r in caplog.records if "metadata document" in r.message]


def test_expire_snapshots_collapses_the_manifest_chain_in_the_same_commit(tmp_path):
    t = _table(tmp_path, commits=6)
    before = len(t.file_manager.read_manifest_list_file(t.current_snapshot().manifest_list))
    assert before >= 6, "fixture must actually have a chain to collapse"
    snapshots_before = len(t.snapshots())

    removed = t.expire_snapshots(retain_last=2)

    assert removed == snapshots_before - len(t.snapshots()) == snapshots_before - 2
    after = len(t.file_manager.read_manifest_list_file(t.current_snapshot().manifest_list))
    assert after == 1, f"manifests {before} -> {after}; expire must fold compaction in"
    assert t.row_count() == 6 and len(t.scan()) == 6  # and lose nothing doing it


def test_expire_snapshots_can_keep_the_two_operations_separate(tmp_path):
    t = _table(tmp_path, commits=6)
    t.expire_snapshots(retain_last=2, compact_manifests=False)
    assert len(t.file_manager.read_manifest_list_file(t.current_snapshot().manifest_list)) > 1
    assert t.row_count() == 6


# ---------------------------------------------------------------- 2. row_count is not health
def test_verify_reads_the_data_while_row_count_only_reads_metadata(tmp_path):
    t = _table(tmp_path, commits=3)
    healthy = t.verify()
    assert healthy["ok"] and healthy["errors"] == []
    assert healthy["data_files"] == healthy["checked_files"] == 3
    assert healthy["rows"] == healthy["rows_read"] == 3
    assert healthy["snapshots"] == 3 and healthy["deep"] is False
    assert json.dumps(healthy)  # a health endpoint must be able to serialise it

    victim = os.path.join(t.table_path, t._get_all_data_files()[0].file_path.lstrip("/"))
    with open(victim, "r+b") as fh:
        fh.seek(0)
        fh.write(b"not parquet at all")

    broken = load_table(t.table_path)
    assert broken.row_count() == 3, "row_count answers from metadata - that is the documented trap"
    report = broken.verify()
    assert report["ok"] is False
    assert len(report["errors"]) == 1 and os.path.basename(victim) in report["errors"][0]
    assert report["checked_files"] == 3  # it keeps going and reports every bad file
    assert json.dumps(report)


def test_verify_never_raises_even_when_the_table_is_unopenable(tmp_path):
    """A health check that throws is one whose caller writes an except and loses the
    diagnosis."""
    missing = create_table(str(tmp_path / "t"), SCHEMA)
    missing.append_records([{"id": 1}], SCHEMA)
    for f in os.listdir(os.path.join(missing.table_path, "data")):
        os.remove(os.path.join(missing.table_path, "data", f))
    report = load_table(missing.table_path).verify()
    assert report["ok"] is False and report["errors"]

    empty = create_table(str(tmp_path / "empty"), SCHEMA)
    assert empty.verify()["ok"] is True  # an empty table is healthy, not broken

    gone = load_table(missing.table_path)
    for f in os.listdir(os.path.join(gone.table_path, "metadata")):
        p = os.path.join(gone.table_path, "metadata", f)
        if os.path.isfile(p):
            os.remove(p)
    report = gone.verify()
    assert report["ok"] is False and "no table" in report["errors"][0].lower()


def test_verify_deep_checks_whole_file_checksums_and_limit_samples(tmp_path):
    t = _table(tmp_path, commits=4)
    assert t.verify(deep=True)["ok"] and t.verify(deep=True)["deep"] is True
    sampled = t.verify(limit=2)
    assert sampled["checked_files"] == 2 and sampled["data_files"] == 4 and sampled["ok"]
    assert t.verify(snapshot_id=t.snapshots()[0]["snapshot_id"])["rows"] == 1


def test_row_count_and_gc_docstrings_carry_the_warnings_people_need(tmp_path):
    """These sentences ARE the deliverable for two of the reported gaps."""
    from datashard import Table

    assert "NOT a health check" in Table.row_count.__doc__
    assert "verify" in Table.row_count.__doc__
    gc_doc = Table.garbage_collect.__doc__
    assert "modification time" in gc_doc and "just copied" in gc_doc
    assert "frees bytes" in gc_doc
    expire_doc = Table.expire_snapshots.__doc__
    assert "frees no disk space" in expire_doc
    assert "REMOVED" in expire_doc  # what the int counts


# ---------------------------------------------------------------- 4/5. migration headroom
@pytest.mark.skipif(not os.path.exists(FIXTURE), reason="legacy fixture missing")
def test_migration_dry_run_projects_the_headroom_it_will_need(tmp_path):
    def extract(name):
        d = tmp_path / name
        d.mkdir()
        with tarfile.open(FIXTURE) as tar:
            tar.extractall(d, filter="data")
        return str(d / "legacy_091")

    dry = migrate_table(extract("dry"), dry_run=True)
    real_path = extract("real")
    before_bytes = dry["metadata_bytes_now"]
    real = migrate_table(real_path)

    for key in ("status", "table", "location", "from", "to", "snapshots", "manifests",
                "data_files", "metadata_bytes_now", "metadata_bytes_added", "peak_bytes",
                "dropped_partition_fields", "columns_foreign_readers_may_reject"):
        assert key in dry and key in real, key

    assert dry["metadata_bytes_now"] == real["metadata_bytes_now"] == before_bytes
    assert dry["peak_bytes"] == dry["metadata_bytes_now"] + dry["metadata_bytes_added"]
    # the projection must be close, and erring high is the safe direction for headroom
    assert real["metadata_bytes_added"] <= dry["metadata_bytes_added"] <= real["metadata_bytes_added"] * 1.05
    assert dry["metadata_bytes_added"] > 0

    # the dry run really wrote nothing
    assert dry["status"] == "dry-run" and real["status"] == "migrated"
    assert migrate_table(real_path)["status"] == "already-migrated"
