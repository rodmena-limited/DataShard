"""create_table must accept a partition spec, and since 0.11 APPLY it (#97, #98).

0.7.2 accepted `partition_spec=PartitionSpec(fields=[...])`; 0.10.0 turned that into a
NotImplementedError, which removed a capability rather than deferring one, and it fired
only on CREATE - so a recorder that makes one table per (symbol, day) upgraded cleanly,
appended all day, and would have died at 00:00Z building the next day's tables. 0.10.5
accepted the call again without applying the spec; 0.11 applies it.
"""
import glob
import json
import logging
import os

import pytest

from datashard import PartitionField, PartitionSpec, Schema, create_table, load_table

SCHEMA = Schema(schema_id=1, fields=[
    {"id": 1, "name": "hour", "type": "int", "required": True},
    {"id": 2, "name": "sym", "type": "string"},
    {"id": 3, "name": "px", "type": "double"},
])
# the reporter's exact spec
SPEC = PartitionSpec(spec_id=0, fields=[
    PartitionField(source_id=1, field_id=1000, name="hour", transform="identity")])
ROWS = [{"hour": h, "sym": "ABC", "px": float(h)} for h in (9, 9, 19, 19, 23)]


def _fill(table):
    """One commit for all the rows, so a file count measures PARTITIONS, not commits."""
    table.append_records(ROWS, SCHEMA)
    return table


def test_the_reporters_call_succeeds_and_the_table_works(tmp_path, caplog):
    with caplog.at_level(logging.WARNING, logger="datashard.table"):
        t = _fill(create_table(str(tmp_path / "t"), schema=SCHEMA, partition_spec=SPEC))
    assert t.created and t.row_count() == 5
    assert load_table(str(tmp_path / "t")).row_count() == 5

    # three partitions in the data => three files, one per partition
    assert len(t._get_all_data_files()) == 3


def test_the_spec_is_persisted_and_the_data_is_laid_out_by_it(tmp_path):
    """Since 0.11 the metadata and the files agree: a spec is recorded AND applied."""
    t = _fill(create_table(str(tmp_path / "t"), schema=SCHEMA, partition_spec=SPEC))
    doc = json.load(open(sorted(glob.glob(str(tmp_path / "t" / "metadata" / "v*.metadata.json")))[-1]))
    assert doc["partition-specs"] == [{"spec-id": 0, "fields": [
        {"source-id": 1, "field-id": 1000, "name": "hour", "transform": "identity"}]}]
    assert doc["last-partition-id"] == 1000
    dirs = {os.path.basename(os.path.dirname(f))
            for f in glob.glob(str(tmp_path / "t" / "data" / "*" / "*.parquet"))}
    assert dirs == {"hour=9", "hour=19", "hour=23"}, dirs
    assert {tuple(df.partition_values.items()) for df in t._get_all_data_files()} == {
        (("hour", 9),), (("hour", 19),), (("hour", 23),)}


def test_filters_return_the_same_rows_with_and_without_the_spec(tmp_path):
    """What the reporter relies on while waiting for 0.11."""
    with_spec = _fill(create_table(str(tmp_path / "a"), schema=SCHEMA, partition_spec=SPEC))
    without = _fill(create_table(str(tmp_path / "b"), schema=SCHEMA))
    for f in ({"hour": 19}, {"hour": ("between", (19, 19))}, {"hour": (">=", 19)}):
        assert with_spec.scan(filter=f) == without.scan(filter=f) != [], f
    assert with_spec.verify()["ok"]


def test_an_empty_spec_leaves_the_table_unpartitioned(tmp_path):
    t = _fill(create_table(str(tmp_path / "t"), schema=SCHEMA, partition_spec=PartitionSpec(spec_id=0, fields=[])))
    assert t.created and t.row_count() == 5
    assert len(t._get_all_data_files()) == 1        # one commit, one file
    assert not glob.glob(str(tmp_path / "t" / "data" / "*" / "*.parquet"))


def test_a_spec_without_a_schema_is_accepted_and_checked_at_the_first_append(tmp_path):
    """create_table(path, partition_spec=...) with no schema has no columns to check the
    transform against yet; the check happens when the first append adopts a schema."""
    t = create_table(str(tmp_path / "t"), partition_spec=SPEC)
    assert t.created
    t.append_records(ROWS[:1], SCHEMA)
    assert load_table(str(tmp_path / "t")).row_count() == 1

    bad = PartitionSpec(spec_id=0, fields=[
        PartitionField(source_id=99, field_id=1000, name="nope", transform="identity")])
    late = create_table(str(tmp_path / "late"), partition_spec=bad)
    with pytest.raises(ValueError, match="not a column"):
        late.append_records(ROWS[:1], SCHEMA)


def test_a_migrated_table_carries_a_note_for_whoever_meets_the_old_client(tmp_path):
    """A pre-0.10 client dies on a migrated table with a bare KeyError, which reads as
    'my lake is corrupt'. That client is released and cannot be changed, so the note goes
    where the operator will look next: the table directory."""
    import tarfile

    from datashard.migrate import MIGRATION_NOTICE_PATH, migrate_table

    fixture = os.path.join(os.path.dirname(__file__), "fixtures", "legacy_table_0_9_1.tar.gz")
    if not os.path.exists(fixture):
        pytest.skip("legacy fixture missing")
    with tarfile.open(fixture) as tar:
        tar.extractall(tmp_path, filter="data")
    path = str(tmp_path / "legacy_091")

    assert migrate_table(path, dry_run=True)["status"] == "dry-run"
    assert not os.path.exists(os.path.join(path, MIGRATION_NOTICE_PATH)), "a dry run writes nothing"

    migrate_table(path)
    note = open(os.path.join(path, MIGRATION_NOTICE_PATH)).read()
    assert "KeyError" in note and "TOO OLD" in note and "pip install --upgrade" in note
    assert "no downgrade" in note.lower()
    assert load_table(path).verify()["ok"], "the note must not disturb the table"

    os.remove(os.path.join(path, MIGRATION_NOTICE_PATH))
    assert migrate_table(path)["status"] == "already-migrated"
    assert os.path.exists(os.path.join(path, MIGRATION_NOTICE_PATH)), "back-filled for tables migrated earlier"
