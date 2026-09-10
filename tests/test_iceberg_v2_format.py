"""Regression tests for the Iceberg v2 on-disk format (0.10.0, #84-#89).

Foreign-reader acceptance lives in audit/evaluations/probe_v0100_foreign_readers.py
(DuckDB and pyiceberg are the real oracle); these tests pin the encoding, the commit
protocol and the migration so a change that breaks them fails fast in CI.
"""
import glob
import json
import os
import struct
import tarfile
import uuid
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal

import fastavro
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from datashard import LegacyLayoutError, Schema, create_table, load_table
from datashard.iceberg_bounds import decode_bound, encode_bound
from datashard.metadata_serde import NAME_MAPPING_PROPERTY
from datashard.migrate import migrate_table
from datashard.storage_backend import CASConflictError
from datashard.table_paths import to_relative

SCHEMA = Schema(schema_id=1, fields=[
    {"id": 1, "name": "id", "type": "long", "required": True},
    {"id": 2, "name": "sym", "type": "string", "required": False},
    {"id": 3, "name": "px", "type": "decimal(18,8)", "required": False},
])
FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "legacy_table_0_9_1.tar.gz")


def _table(tmp_path, name="t"):
    return create_table(str(tmp_path / name), SCHEMA)


# ---------------------------------------------------------------- #85 bounds
@pytest.mark.parametrize("value,itype,expected_bytes", [
    (True, "boolean", b"\x01"),
    (False, "boolean", b"\x00"),
    (1, "int", struct.pack("<i", 1)),
    (-2, "long", struct.pack("<q", -2)),
    (1.5, "double", struct.pack("<d", 1.5)),
    ("abc", "string", b"abc"),
    (date(1970, 1, 1), "date", struct.pack("<i", 0)),
    (date(2026, 9, 6), "date", struct.pack("<i", (date(2026, 9, 6) - date(1970, 1, 1)).days)),
    (dt_time(1, 0, 0), "time", struct.pack("<q", 3600 * 1_000_000)),
    (datetime(1970, 1, 1, tzinfo=timezone.utc), "timestamptz", struct.pack("<q", 0)),
    (Decimal("1.00000000"), "decimal(18,8)", (100000000).to_bytes(4, "big", signed=True)),
    (Decimal("-1.00000000"), "decimal(18,8)", (-100000000).to_bytes(4, "big", signed=True)),
])
def test_bound_encoding_matches_the_iceberg_single_value_form(value, itype, expected_bytes):
    """Foreign readers PRUNE on these bytes: a wrong encoding returns wrong rows."""
    assert encode_bound(value, itype) == expected_bytes
    assert decode_bound(expected_bytes, itype) == value


@pytest.mark.parametrize("value,itype", [
    (datetime(1960, 5, 4, 3, 2, 1, 123456, tzinfo=timezone.utc), "timestamptz"),
    (datetime(1960, 5, 4, 3, 2, 1, 123456), "timestamp"),
    (Decimal("-0.00000001"), "decimal(18,8)"),
    (Decimal("99999999999.99999999"), "decimal(18,8)"),
    (date(1900, 1, 1), "date"),
    ("↯ unicode ↯", "string"),
    (-(2**62), "long"),
])
def test_bounds_round_trip_exactly_for_awkward_values(value, itype):
    assert decode_bound(encode_bound(value, itype), itype) == value


@pytest.mark.parametrize("value,itype", [
    (Decimal("12345678901234567890123456789012.12345678"), "decimal(38,8)"),
    (Decimal("-99999999999999999999999999999.999999999"), "decimal(38,9)"),
    (Decimal("99999999999999999999999999999999999999"), "decimal(38,0)"),
])
def test_high_precision_decimal_bounds_survive_the_decimal_context(value, itype):
    """quantize()/scaleb() honour the 28-digit default context and silently dropped or
    rounded every bound of a wide decimal column - exactly the columns a price uses."""
    encoded = encode_bound(value, itype)
    assert encoded is not None
    assert decode_bound(encoded, itype) == value


@pytest.mark.parametrize("value,itype", [
    (Decimal("1.5"), "long"),      # a bound is not a cast: 1.5 must not become 1 ...
    ("5", "long"),                 # ... and "5" must not become 5
    (1.5, "long"),
    (True, "int"),
    (1, "boolean"),
    (1.0, "decimal(18,8)"),        # a binary float is not an exact decimal
    ("2026-01-01", "date"),
])
def test_a_mistyped_value_gets_no_bound_rather_than_a_coerced_one(value, itype):
    """A bound that is off by any amount makes DuckDB and pyiceberg prune real rows."""
    assert encode_bound(value, itype) is None


def test_bounds_are_omitted_rather_than_guessed_when_they_cannot_be_exact():
    """A bound that would round is worse than none: the reader would prune real rows."""
    assert encode_bound(Decimal("1.123456789"), "decimal(18,8)") is None  # more scale than the type
    assert encode_bound(1.5, "long") is None
    assert encode_bound("nope", "int") is None
    assert encode_bound(datetime(2026, 1, 1), "date") is None
    assert encode_bound(None, "long") is None


def test_manifest_entries_carry_field_ids_and_binary_bounds(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 5, "sym": "A", "px": Decimal("1.5")}], SCHEMA)
    (manifest,) = glob.glob(str(tmp_path / "t" / "metadata" / "*-m0.avro"))
    with open(manifest, "rb") as fh:
        reader = fastavro.reader(fh)
        header, (entry,) = dict(reader.metadata), list(reader)
    assert header["format-version"] == "2" and header["content"] == "data"
    assert json.loads(header["schema"])["fields"][0]["id"] == 1
    ids = {f["name"]: f.get("field-id") for f in reader.writer_schema["fields"]}
    assert ids["status"] == 0 and ids["snapshot_id"] == 1 and ids["data_file"] == 2
    df = entry["data_file"]
    assert df["content"] == 0 and df["file_format"] == "PARQUET"
    assert df["file_path"].startswith("file:///") and df["file_path"].endswith(".parquet")
    assert {b["key"]: b["value"] for b in df["lower_bounds"]}[1] == struct.pack("<q", 5)
    assert len(df["datashard_sha256"]) == 64


def test_manifest_list_carries_iceberg_counts_and_sequence_numbers(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1}], SCHEMA)
    t.append_records([{"id": 2}], SCHEMA)
    snap = t.current_snapshot()
    (lst,) = glob.glob(str(tmp_path / "t" / "metadata" / f"snap-{snap.snapshot_id}-*.avro"))
    with open(lst, "rb") as fh:
        reader = fastavro.reader(fh)
        header, records = dict(reader.metadata), list(reader)
    assert header["format-version"] == "2" and header["snapshot-id"] == str(snap.snapshot_id)
    assert {f["name"]: f.get("field-id") for f in reader.writer_schema["fields"]}["manifest_path"] == 500
    assert sum(r["added_files_count"] for r in records) == 2
    assert sum(r["added_rows_count"] for r in records) == 2
    assert all(r["manifest_path"].startswith("file:///") for r in records)
    assert max(r["sequence_number"] for r in records) == snap.sequence_number == 2


# ---------------------------------------------------------------- #84 metadata.json
def test_metadata_json_is_iceberg_v2(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1}], SCHEMA)
    doc = json.load(open(tmp_path / "t" / "metadata" / "v2.metadata.json"))
    assert doc["format-version"] == 2
    assert doc["location"] == f"file://{os.path.realpath(tmp_path / 't')}"
    assert doc["current-schema-id"] == 1 and doc["schemas"][0]["type"] == "struct"
    assert doc["schemas"][0]["fields"][0] == {"id": 1, "name": "id", "required": True, "type": "long"}
    assert doc["last-column-id"] == 3
    assert doc["refs"]["main"] == {"snapshot-id": doc["current-snapshot-id"], "type": "branch"}
    (snap,) = doc["snapshots"]
    assert snap["summary"]["operation"] == "append"
    assert snap["summary"]["added-records"] == "1"
    assert snap["manifest-list"].startswith("file:///")
    assert snap["sequence-number"] == 1 and doc["last-sequence-number"] == 1
    assert all(isinstance(v, str) for v in snap["summary"].values())
    assert doc["metadata-log"][0]["metadata-file"].endswith("v1.metadata.json")
    assert "last_commit_id" not in doc and "format_version" not in doc
    assert json.loads(doc["properties"][NAME_MAPPING_PROPERTY])[0] == {"field-id": 1, "names": ["id"]}


def test_a_table_created_without_a_schema_adopts_the_first_appends_schema(tmp_path):
    t = create_table(str(tmp_path / "noschema"))
    t.append_records([{"id": 1, "sym": "x", "px": Decimal("1.5")}], SCHEMA)
    doc = json.load(open(tmp_path / "noschema" / "metadata" / "v2.metadata.json"))
    assert [f["name"] for f in doc["schemas"][-1]["fields"]] == ["id", "sym", "px"]
    assert doc["current-schema-id"] == 1 and doc["last-column-id"] == 3
    assert NAME_MAPPING_PROPERTY in doc["properties"]
    assert load_table(str(tmp_path / "noschema")).row_count() == 1


# ---------------------------------------------------------------- #88 parquet field ids
def test_every_written_parquet_column_carries_a_field_id(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1, "sym": "a", "px": Decimal("1.5")}], SCHEMA)
    t.append_arrow(pa.table({"id": pa.array([2], pa.int64())}))
    for f in glob.glob(str(tmp_path / "t" / "data" / "*.parquet")):
        schema = pq.read_schema(f)
        for i, name in enumerate(["id", "sym", "px"][: len(schema.names)]):
            assert schema.field(name).metadata[b"PARQUET:field_id"] == str(i + 1).encode(), f


# ---------------------------------------------------------------- #86 commit protocol
def test_commit_point_is_an_exclusive_create(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1}], SCHEMA)
    meta = tmp_path / "t" / "metadata"
    assert (meta / "version-hint.text").read_text() == "2"
    with pytest.raises(CASConflictError):
        t.storage.create_exclusive("metadata/v2.metadata.json", b"{}")
    assert json.loads((meta / "v2.metadata.json").read_text())["format-version"] == 2  # untouched
    t.storage.create_exclusive("metadata/v99.metadata.json", b'{"x": 1}')
    assert (meta / "v99.metadata.json").read_text() == '{"x": 1}'


def test_a_lagging_hint_never_hides_a_committed_version(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1}], SCHEMA)
    t.append_records([{"id": 2}], SCHEMA)
    hint = tmp_path / "t" / "metadata" / "version-hint.text"
    hint.write_text("1")
    assert load_table(str(tmp_path / "t")).row_count() == 2  # reader probes past the hint
    assert hint.read_text() == "3"  # and heals it
    hint.write_text("1")
    load_table(str(tmp_path / "t")).append_records([{"id": 3}], SCHEMA)  # writer heals via the conflict
    assert load_table(str(tmp_path / "t")).row_count() == 3


def test_write_path_does_not_pay_for_the_probe(tmp_path):
    """The read-your-writes probe must not be on the commit path (#86 performance)."""
    t = _table(tmp_path)
    t.append_records([{"id": 1}], SCHEMA)
    seen = []
    real_exists = t.storage.exists
    t.storage.exists = lambda p: (seen.append(p), real_exists(p))[1]
    t.append_records([{"id": 2}], SCHEMA)
    t.storage.exists = real_exists
    assert not [p for p in seen if p.endswith(".metadata.json")], seen


# ---------------------------------------------------------------- #87 URIs and relocation
def test_paths_are_absolute_uris_and_a_moved_table_still_reads(tmp_path):
    t = _table(tmp_path)
    t.append_records([{"id": 1}], SCHEMA)
    assert t.location == f"file://{os.path.realpath(tmp_path / 't')}"
    moved = tmp_path / "moved"
    os.rename(tmp_path / "t", moved)
    m = load_table(str(moved))
    assert m.row_count() == 1  # resolves under the actual root, not the recorded one
    counts = m.relocate()
    assert counts["snapshots"] == 1
    doc = json.load(open(sorted(glob.glob(str(moved / "metadata" / "v*.metadata.json")))[-1]))
    assert doc["location"] == f"file://{os.path.realpath(moved)}"
    assert doc["snapshots"][0]["manifest-list"].startswith(f"file://{os.path.realpath(moved)}/")
    assert load_table(str(moved)).row_count() == 1


def test_to_relative_refuses_paths_outside_the_table():
    locations = ["file:///lake/t", None]
    assert to_relative("file:///lake/t/data/x.parquet", locations) == "data/x.parquet"
    assert to_relative("/data/x.parquet", locations) == "data/x.parquet"
    assert to_relative("data/x.parquet", locations) == "data/x.parquet"
    with pytest.raises(ValueError):
        to_relative("file:///etc/passwd", locations)
    with pytest.raises(ValueError):
        to_relative("s3://other-bucket/t/data/x.parquet", locations)
    with pytest.raises(ValueError):
        to_relative("/etc/passwd", locations)


# ---------------------------------------------------------------- #86/#89 legacy layout
def _extract_fixture(tmp_path):
    with tarfile.open(FIXTURE) as tar:
        tar.extractall(tmp_path, filter="data")
    return str(tmp_path / "legacy_091"), json.load(open(tmp_path / "legacy_091_rows.json"))


@pytest.mark.skipif(not os.path.exists(FIXTURE), reason="legacy fixture missing")
def test_a_pre_010_table_is_refused_with_the_migration_instruction(tmp_path):
    path, _rows = _extract_fixture(tmp_path)
    before = sorted(os.listdir(os.path.join(path, "metadata")))
    with pytest.raises(LegacyLayoutError, match="datashard migrate"):
        load_table(path)
    with pytest.raises(LegacyLayoutError):
        create_table(path, SCHEMA)
    assert sorted(os.listdir(os.path.join(path, "metadata"))) == before  # refusing wrote nothing


@pytest.mark.skipif(not os.path.exists(FIXTURE), reason="legacy fixture missing")
def test_migration_preserves_every_row_and_blocks_the_downgrade(tmp_path):
    path, expected = _extract_fixture(tmp_path)
    dry = migrate_table(path, dry_run=True)
    assert dry["status"] == "dry-run" and dry["snapshots"] == 7
    assert not glob.glob(os.path.join(path, "metadata", "v*.metadata.json")).count("v8.metadata.json")

    report = migrate_table(path)
    assert report["status"] == "migrated" and report["to"] == "v8.metadata.json"
    t = load_table(path)
    got = sorted(({"id": r["id"], "sym": r["sym"], "px": str(r["px"])} for r in t.scan()), key=lambda r: r["id"])
    want = sorted(({"id": int(r["id"]), "sym": r["sym"], "px": str(Decimal(str(r["px"])))} for r in expected), key=lambda r: r["id"])
    assert got == want
    assert len(t.snapshots()) == 7
    assert os.path.exists(os.path.join(path, "metadata.version-hint.text.migrated"))
    assert not os.path.exists(os.path.join(path, "metadata.version-hint.text"))
    assert migrate_table(path)["status"] == "already-migrated"  # idempotent

    t.append_records([{"id": 12345, "sym": "new", "px": Decimal("1.5")}], t._get_current_schema())
    assert load_table(path).row_count() == len(want) + 1
    # the free-form partition labels of the pre-0.10 API survive as a datashard extra
    assert any(df.partition_values for df in t._get_all_data_files())


@pytest.mark.skipif(not os.path.exists(FIXTURE), reason="legacy fixture missing")
def test_migration_of_a_table_with_a_missing_hint_is_unambiguous(tmp_path):
    path, expected = _extract_fixture(tmp_path)
    os.remove(os.path.join(path, "metadata.version-hint.text"))
    migrate_table(path)
    assert load_table(path).row_count() == len(expected)


# ---------------------------------------------------------------- reading foreign shapes
def test_a_foreign_manifest_without_datashard_keys_is_unverified_not_corrupt(tmp_path):
    """Absence of our integrity extras must never read as corruption (#58 semantics)."""
    t = _table(tmp_path)
    t.append_records([{"id": 1}], SCHEMA)
    base = t.metadata_manager.refresh()
    stripped = t.metadata_manager._dict_to_metadata(t.metadata_manager._metadata_to_dict(base))
    for snap in stripped.snapshots:
        snap.summary = {k: v for k, v in (snap.summary or {}).items() if not k.startswith("datashard.")}
    t.metadata_manager.commit(base, stripped)
    reopened = load_table(str(tmp_path / "t"))
    assert reopened.row_count() == 1
    length, checksum = type(reopened.file_manager).snapshot_list_integrity(reopened.current_snapshot())
    assert length is None and checksum is None


def test_delete_manifests_from_another_engine_are_refused_not_ignored(tmp_path):
    """Reading a merge-on-read table as if the deletes did not exist would return rows
    the writer deleted. Until 1.0 applies them, such a table must raise."""
    from datashard.manifest_codec import manifest_from_iceberg

    with pytest.raises(NotImplementedError, match="delete files"):
        manifest_from_iceberg(
            {"manifest_path": "file:///t/metadata/x.avro", "manifest_length": 1, "partition_spec_id": 0,
             "content": 1, "added_snapshot_id": 1, "added_files_count": 1, "existing_files_count": 0,
             "deleted_files_count": 0}, lambda p: "metadata/x.avro")


def test_scan_still_reads_a_table_whose_files_lack_field_ids(tmp_path):
    """A parquet file written elsewhere (no PARQUET:field_id) appended via append_files."""
    from datashard import DataFile, FileFormat

    t = _table(tmp_path)
    t.append_records([{"id": 1, "sym": "a", "px": Decimal("1.5")}], SCHEMA)
    plain_name = f"plain_{uuid.uuid4().hex[:8]}.parquet"
    plain = tmp_path / "t" / "data" / plain_name
    # written by another tool: same columns and nullability, but no PARQUET:field_id
    foreign_schema = pa.schema([pa.field("id", pa.int64(), False), pa.field("sym", pa.string()),
                                pa.field("px", pa.decimal128(18, 8))])
    pq.write_table(pa.table({"id": pa.array([2], pa.int64()), "sym": ["b"],
                             "px": pa.array([Decimal("2.5")], pa.decimal128(18, 8))},
                            schema=foreign_schema), plain)
    assert not (pq.read_schema(plain).field("id").metadata or {})
    with t.new_transaction() as tx:
        tx.append_files([DataFile(file_path="/data/" + plain_name, file_format=FileFormat.PARQUET,
                                  partition_values={}, record_count=1, file_size_in_bytes=os.path.getsize(plain))])
        tx.commit()
    assert sorted(r["id"] for r in load_table(str(tmp_path / "t")).scan()) == [1, 2]


# ---------------------------------------------------------------- adversarial pass
def test_a_racing_schema_for_a_schema_less_table_fails_before_the_commit_point(tmp_path):
    """Two writers adopting different schemas into one schema-less table: the loser must
    not commit a data file the table's schema cannot describe."""
    other = Schema(schema_id=1, fields=[{"id": 1, "name": "totally_different", "type": "string"}])
    t = create_table(str(tmp_path / "race"))
    tx = t.new_transaction().begin()
    tx.append_data([{"id": 1, "sym": "a", "px": Decimal("1.5")}], SCHEMA)  # writes a file
    load_table(str(tmp_path / "race")).append_records([{"totally_different": "x"}], other)  # other writer wins
    with pytest.raises(ValueError, match="different schema"):
        tx.commit()
    reopened = load_table(str(tmp_path / "race"))
    assert [f["name"] for f in reopened._get_current_schema().fields] == ["totally_different"]
    assert reopened.row_count() == 1
    assert not glob.glob(str(tmp_path / "race" / "data" / "*.parquet")) or len(
        glob.glob(str(tmp_path / "race" / "data" / "*.parquet"))) == 1  # the loser's file was rolled back


def test_metadata_log_entry_outside_the_table_is_kept_verbatim(tmp_path):
    """A foreign writer's metadata file elsewhere must not be re-joined onto our location
    (which would invent a path like file:///our/table/s3://their/bucket/...)."""
    from datashard.metadata_serde import dict_to_metadata, metadata_to_dict

    t = _table(tmp_path)
    t.append_records([{"id": 1}], SCHEMA)
    md = t.metadata_manager.refresh()
    foreign = "s3://someone-elses/bucket/metadata/00000-abc.metadata.json"
    md.metadata_log = [{"timestamp-ms": 1, "metadata-file": foreign}]
    doc = metadata_to_dict(md)
    assert doc["metadata-log"][0]["metadata-file"] == foreign
    assert dict_to_metadata(doc, actual_location=md.location).metadata_log[0]["metadata-file"] == foreign


@pytest.mark.skipif(not os.path.exists(FIXTURE), reason="legacy fixture missing")
def test_migration_reports_a_dropped_decorative_partition_spec(tmp_path):
    """Pre-0.10 specs were never applied to the data; carrying one into Iceberg metadata
    would promise a layout the files do not have."""
    path, _rows = _extract_fixture(tmp_path)
    legacy = json.load(open(os.path.join(path, "metadata", "v7-a37f3d22.metadata.json")))
    legacy["partition_specs"] = [{"spec_id": 0, "fields": [
        {"source_id": 2, "field_id": 1000, "name": "sym", "transform": "identity"}]}]
    json.dump(legacy, open(os.path.join(path, "metadata", "v7-a37f3d22.metadata.json"), "w"))
    report = migrate_table(path)
    assert report["dropped_partition_fields"] == ["sym"]
    doc = json.load(open(os.path.join(path, "metadata", "v8.metadata.json")))
    assert doc["partition-specs"] == [{"spec-id": 0, "fields": []}]
    assert load_table(path).row_count() == 10


def test_migrating_a_table_that_has_no_snapshots(tmp_path):
    """A created-but-never-appended legacy table must migrate, not crash."""
    from datashard.metadata_serde_legacy import legacy_metadata_to_dict

    path = str(tmp_path / "empty_legacy")
    t = _table(tmp_path, "empty_legacy")
    md = t.metadata_manager.refresh()
    meta_dir = tmp_path / "empty_legacy" / "metadata"
    for f in meta_dir.glob("v*.metadata.json"):
        f.unlink()
    (meta_dir / "version-hint.text").unlink()
    md.last_commit_id = "legacy"
    json.dump(legacy_metadata_to_dict(md), open(meta_dir / "v0.metadata.json", "w"))
    (tmp_path / "empty_legacy" / "metadata.version-hint.text").write_text("v0.metadata.json")
    assert migrate_table(path)["status"] == "migrated"
    t2 = load_table(path)
    assert t2.row_count() == 0 and t2.snapshots() == []
    t2.append_records([{"id": 1}], SCHEMA)
    assert load_table(path).row_count() == 1


def test_a_column_type_no_iceberg_engine_can_read_is_refused_at_create(tmp_path):
    """A bare 'fixed' is not an Iceberg type - the width is part of it - and pyiceberg's
    parser rejects it outright. Since 0.11.2 it is the only refusal left: uuid and
    fixed[L] are written as the fixed-width binary Iceberg specifies (#92)."""
    schema = Schema(schema_id=1, fields=[
        {"id": 1, "name": "k", "type": "long", "required": True},
        {"id": 2, "name": "v", "type": "fixed"},
    ])
    with pytest.raises(ValueError, match=r"use fixed\[L\]"):
        create_table(str(tmp_path / "fixed"), schema)
    assert not os.path.exists(tmp_path / "fixed" / "metadata")  # refusing wrote nothing
    for good in ("fixed[16]", "binary", "uuid"):
        ok = Schema(schema_id=1, fields=[
            {"id": 1, "name": "k", "type": "long", "required": True},
            {"id": 2, "name": "v", "type": good},
        ])
        assert create_table(str(tmp_path / good.replace("[", "").replace("]", "")), ok).created


def test_a_migrated_table_reports_the_columns_foreign_readers_will_still_refuse(tmp_path):
    """A table old enough to migrate stores its uuid column as a parquet STRING, and
    migration does not rewrite data files - so the report must name that column even
    though the type itself is fine to write today."""
    from datashard.metadata_serde import legacy_unreadable_fields, unrepresentable_fields

    schema = Schema(schema_id=1, fields=[
        {"id": 1, "name": "k", "type": "long", "required": True},
        {"id": 2, "name": "v", "type": "uuid"},
    ])
    t = create_table(str(tmp_path / "legacy_uuid"), SCHEMA)  # created with a clean schema
    base = t.metadata_manager.refresh()
    patched = t.metadata_manager._dict_to_metadata(t.metadata_manager._metadata_to_dict(base))
    patched.schemas = [schema]
    patched.current_schema_id = 1
    t.metadata_manager.commit(base, patched)  # simulate a table written before 0.11.2
    reopened = load_table(str(tmp_path / "legacy_uuid"))
    current = reopened._get_current_schema()
    assert unrepresentable_fields(current) == {}, "the type is writable now"
    assert "v" in legacy_unreadable_fields(current), "its OLD files are still not readable"
    reopened.append_records([{"k": 1, "v": str(uuid.uuid4())}], current)
    assert reopened.row_count() == 1


def test_a_pre_08_table_plain_filenames_legacy_content_is_refused_without_writing(tmp_path):
    """Before 0.8 the metadata files were named v{N}.metadata.json - the same names 0.10
    uses - with snake_case content. Detecting that must not leave a hint file behind."""
    from datashard.metadata_serde_legacy import legacy_metadata_to_dict

    path = tmp_path / "pre08"
    t = _table(tmp_path, "pre08")
    t.append_records([{"id": 1}], SCHEMA)
    md = t.metadata_manager.refresh()
    meta = path / "metadata"
    for f in meta.glob("v*.metadata.json"):
        f.unlink()
    (meta / "version-hint.text").unlink()
    json.dump(legacy_metadata_to_dict(md), open(meta / "v1.metadata.json", "w"))  # plain name, old content
    before = sorted(os.listdir(meta))

    with pytest.raises(LegacyLayoutError):
        load_table(str(path))
    assert sorted(os.listdir(meta)) == before, "refusing a legacy table wrote into it"
    assert not (meta / "version-hint.text").exists()

    migrate_table(str(path), metadata_file="v1.metadata.json")
    assert load_table(str(path)).row_count() == 1
