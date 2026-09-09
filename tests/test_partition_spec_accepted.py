"""create_table must accept a partition spec it cannot apply (#97).

0.7.2 accepted `partition_spec=PartitionSpec(fields=[...])`; 0.10.0 turned that into a
NotImplementedError, which removed a capability rather than deferring one. The shape is
what made it urgent: the raise only fires on CREATE, so a recorder that makes one table
per (symbol, day) upgraded cleanly, appended successfully all day, and would have died
at 00:00Z building the next day's tables - green smoke tests, then a timed outage.

The fields are still not APPLIED, and must not be: this version writes unpartitioned
data files, so a spec recorded in the metadata would promise DuckDB, pyiceberg and Spark
a layout the files do not have, and those engines prune on it.
"""
import glob
import json
import logging
import os

import pytest

from datashard import PartitionField, PartitionSpec, Schema, create_table, load_table
from datashard.table import REQUESTED_PARTITION_FIELDS_PROPERTY

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
    for row in ROWS:
        table.append_records([row], SCHEMA)
    return table


def test_the_reporters_call_succeeds_and_the_table_works(tmp_path, caplog):
    with caplog.at_level(logging.WARNING, logger="datashard.table"):
        t = _fill(create_table(str(tmp_path / "t"), schema=SCHEMA, partition_spec=SPEC))
    assert t.created and t.row_count() == 5
    assert load_table(str(tmp_path / "t")).row_count() == 5

    warning = " ".join(r.message for r in caplog.records)
    assert "hour" in warning and "NOT applied" in warning
    assert "0.11" in warning, "the warning must say when partitioning arrives"
    assert "column statistics" in warning, "and that filters still work meanwhile"


def test_the_dropped_fields_are_recorded_not_lost(tmp_path):
    t = create_table(str(tmp_path / "t"), schema=SCHEMA, partition_spec=SPEC)
    assert t.properties()[REQUESTED_PARTITION_FIELDS_PROPERTY] == "hour"
    assert load_table(str(tmp_path / "t")).properties()[REQUESTED_PARTITION_FIELDS_PROPERTY] == "hour"


def test_the_metadata_never_promises_a_layout_the_files_do_not_have(tmp_path):
    """The reason the fields are dropped rather than recorded: foreign readers PRUNE on
    a partition spec, and these data files carry empty partition structs."""
    _fill(create_table(str(tmp_path / "t"), schema=SCHEMA, partition_spec=SPEC))
    doc = json.load(open(sorted(glob.glob(str(tmp_path / "t" / "metadata" / "v*.metadata.json")))[-1]))
    assert doc["partition-specs"] == [{"spec-id": 0, "fields": []}]
    assert doc["default-spec-id"] == 0
    assert doc["last-partition-id"] == 999  # no partition field ids were assigned


def test_filters_return_the_same_rows_with_and_without_the_spec(tmp_path):
    """What the reporter relies on while waiting for 0.11."""
    with_spec = _fill(create_table(str(tmp_path / "a"), schema=SCHEMA, partition_spec=SPEC))
    without = _fill(create_table(str(tmp_path / "b"), schema=SCHEMA))
    for f in ({"hour": 19}, {"hour": ("between", (19, 19))}, {"hour": (">=", 19)}):
        assert with_spec.scan(filter=f) == without.scan(filter=f) != [], f
    assert with_spec.verify()["ok"]


def test_an_empty_spec_is_untouched_and_warns_about_nothing(tmp_path, caplog):
    with caplog.at_level(logging.WARNING, logger="datashard.table"):
        t = create_table(str(tmp_path / "t"), schema=SCHEMA, partition_spec=PartitionSpec(spec_id=0, fields=[]))
    assert t.created
    assert REQUESTED_PARTITION_FIELDS_PROPERTY not in t.properties()
    assert not [r for r in caplog.records if "partition spec" in r.message]


def test_a_spec_without_a_schema_is_also_accepted(tmp_path):
    """create_table(path, partition_spec=...) with no schema took the other branch."""
    t = create_table(str(tmp_path / "t"), partition_spec=SPEC)
    assert t.created
    assert t.properties()[REQUESTED_PARTITION_FIELDS_PROPERTY] == "hour"
    t.append_records(ROWS[:1], SCHEMA)
    assert load_table(str(tmp_path / "t")).row_count() == 1


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
