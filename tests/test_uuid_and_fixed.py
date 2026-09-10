"""uuid and fixed[L] columns, as Iceberg specifies them (#92).

datashard wrote a parquet STRING for a `uuid` column, which pyiceberg refuses to read as
a uuid ("Cannot promote an string to uuid"), and had no way to express `fixed` at all
because Iceberg's fixed carries a width. 0.10 responded by refusing both types at create.
They are now written as fixed-width binary - 16 bytes for uuid, L for fixed[L] - which is
what every Iceberg reader expects.

Callers still see strings for uuid: the value written is the value read, results stay
JSON serialisable, and a table written before this change (whose uuid column really is a
parquet string) reads in the same scan as one written after.
"""
import glob
import uuid as uuidlib

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from datashard import Schema, create_table, load_table

SCHEMA = Schema(schema_id=1, fields=[
    {"id": 1, "name": "tid", "type": "uuid", "required": True},
    {"id": 2, "name": "tag", "type": "fixed[4]"},
    {"id": 3, "name": "n", "type": "long"},
])


STRING_SCHEMA = Schema(schema_id=1, fields=[
    {"id": 1, "name": "tid", "type": "string", "required": True},
    {"id": 2, "name": "n", "type": "long"},
])
UUID_SCHEMA = Schema(schema_id=1, fields=[
    {"id": 1, "name": "tid", "type": "uuid", "required": True},
    {"id": 2, "name": "n", "type": "long"},
])


def legacy_table(path, ids):
    """A table as <=0.11.1 wrote one: a uuid column whose parquet type is really a string.

    Built by writing the column as a string and then committing the schema that calls it a
    uuid - which is exactly the state those tables are in on disk.
    """
    t = create_table(str(path), STRING_SCHEMA)
    t.append_records([{"tid": tid, "n": i} for i, tid in enumerate(ids)], STRING_SCHEMA)
    mm = t.metadata_manager
    base = mm.refresh()
    patched = mm._dict_to_metadata(mm._metadata_to_dict(base))
    patched.schemas = [UUID_SCHEMA]
    patched.current_schema_id = 1
    mm.commit(base, patched)
    return load_table(t.table_path)


def uuid_types_on_disk(path):
    return {str(pq.read_schema(f).field("tid").type)
            for f in glob.glob(str(path) + "/data/*.parquet")}


def test_a_uuid_column_is_sixteen_bytes_on_disk_and_a_string_in_python(tmp_path):
    t = create_table(str(tmp_path / "t"), SCHEMA)
    written = str(uuidlib.uuid4())
    t.append_records([{"tid": written, "tag": b"abcd", "n": 1}], SCHEMA)

    (data_file,) = glob.glob(str(tmp_path / "t" / "data" / "*.parquet"))
    on_disk = pq.read_schema(data_file)
    assert on_disk.field("tid").type == pa.binary(16)
    assert on_disk.field("tag").type == pa.binary(4)

    (row,) = load_table(t.table_path).scan()
    assert row["tid"] == written and isinstance(row["tid"], str)
    assert row["tag"] == b"abcd"
    assert load_table(t.table_path).to_arrow().schema.field("tid").type == pa.string()


def test_a_uuid_accepts_a_string_a_uuid_object_or_raw_bytes(tmp_path):
    t = create_table(str(tmp_path / "t"), SCHEMA)
    as_str, as_obj = str(uuidlib.uuid4()), uuidlib.uuid4()
    as_bytes = uuidlib.uuid4()
    t.append_records([
        {"tid": as_str, "tag": None, "n": 1},
        {"tid": as_obj, "tag": None, "n": 2},
        {"tid": as_bytes.bytes, "tag": None, "n": 3},
    ], SCHEMA)
    rows = sorted(load_table(t.table_path).scan(), key=lambda r: r["n"])
    assert [r["tid"] for r in rows] == [as_str, str(as_obj), str(as_bytes)]
    assert all(isinstance(r["tid"], str) for r in rows)


def test_a_value_that_is_not_a_uuid_is_refused_rather_than_stored(tmp_path):
    t = create_table(str(tmp_path / "t"), SCHEMA)
    with pytest.raises(ValueError, match="not a uuid"):
        t.append_records([{"tid": "not-a-uuid", "tag": None, "n": 1}], SCHEMA)
    with pytest.raises(ValueError, match="not a uuid"):
        t.append_records([{"tid": b"too-short", "tag": None, "n": 1}], SCHEMA)
    assert load_table(t.table_path).row_count() == 0


def test_every_read_api_returns_the_same_string(tmp_path):
    t = create_table(str(tmp_path / "t"), SCHEMA)
    ids = [str(uuidlib.uuid4()) for _ in range(3)]
    t.append_records([{"tid": ids[i], "tag": b"aa%02d" % i, "n": i} for i in range(3)], SCHEMA)
    r = load_table(t.table_path)
    assert sorted(x["tid"] for x in r.scan()) == sorted(ids)
    assert sorted(r.to_arrow().column("tid").to_pylist()) == sorted(ids)
    assert sorted(r.to_pandas()["tid"].tolist()) == sorted(ids)
    assert sorted(x["tid"] for x in r.iter_records()) == sorted(ids)
    assert sorted(x["tid"] for b in r.scan_batches() for x in b) == sorted(ids)


def test_a_uuid_column_can_be_filtered_and_keeps_its_bounds(tmp_path):
    t = create_table(str(tmp_path / "t"), SCHEMA)
    ids = sorted(str(uuidlib.uuid4()) for _ in range(4))
    t.append_records([{"tid": ids[i], "tag": None, "n": i} for i in range(4)], SCHEMA)
    r = load_table(t.table_path)
    assert [x["n"] for x in r.scan(filter={"tid": ids[2]})] == [2]
    bounds = r._get_all_data_files()[0].lower_bounds
    assert isinstance(bounds[1], str) and bounds[1] == min(ids)


def test_a_uuid_filter_survives_projection_streaming_and_several_files(tmp_path):
    """The predicate cannot be pushed into the parquet reader - the file holds bytes, the
    caller filtered on a string - so it runs after decoding, and must still work when the
    uuid column is not projected, when the rows arrive in batches, and when file-level
    pruning has had a chance to drop the wrong file."""
    t = create_table(str(tmp_path / "t"), SCHEMA)
    ids = [str(uuidlib.uuid4()) for _ in range(3)]
    for i, tid in enumerate(ids):                  # one file per append
        t.append_records([{"tid": tid, "tag": None, "n": i}], SCHEMA)
    r = load_table(t.table_path)
    assert len(r._get_all_data_files()) == 3

    wanted = ids[2]
    assert [x["n"] for x in r.scan(filter={"tid": wanted})] == [2]
    assert r.scan(columns=["n"], filter={"tid": wanted}) == [{"n": 2}]
    assert [x["n"] for x in r.iter_records(filter={"tid": wanted})] == [2]
    assert [x for b in r.scan_batches(columns=["n"], filter={"tid": wanted}) for x in b] == [{"n": 2}]
    assert r.to_arrow(filter={"tid": wanted}).column("n").to_pylist() == [2]

    assert sorted(x["n"] for x in r.scan(filter={"tid": ("in", ids[:2])})) == [0, 1]
    assert [x["n"] for x in r.scan(filter={"tid": ("!=", wanted), "n": (">", 0)})] == [1]
    assert r.scan(filter={"tid": str(uuidlib.uuid4())}) == []


def test_a_uuid_filter_takes_a_string_a_uuid_object_or_bytes(tmp_path):
    t = create_table(str(tmp_path / "t"), SCHEMA)
    wanted = uuidlib.uuid4()
    t.append_records([{"tid": wanted, "tag": None, "n": 1},
                      {"tid": str(uuidlib.uuid4()), "tag": None, "n": 2}], SCHEMA)
    r = load_table(t.table_path)
    for form in (str(wanted), wanted, wanted.bytes):
        assert [x["n"] for x in r.scan(filter={"tid": form})] == [1], form


def test_every_write_api_accepts_a_uuid(tmp_path):
    """Records, Arrow (either encoding) and pandas all reach the same 16 bytes. The
    partitioned append and the pandas path build their own Arrow table, so each needs the
    conversion of its own."""
    import pandas as pd

    ids = [str(uuidlib.uuid4()) for _ in range(4)]
    t = create_table(str(tmp_path / "t"), SCHEMA)
    t.append_records([{"tid": ids[0], "tag": b"aaaa", "n": 0}], SCHEMA)
    t.append_arrow(pa.table({"tid": pa.array([ids[1]], pa.string()),
                             "tag": pa.array([b"bbbb"], pa.binary(4)),
                             "n": pa.array([1], pa.int64())}))
    t.append_arrow(pa.table({"tid": pa.array([uuidlib.UUID(ids[2]).bytes], pa.binary(16)),
                             "tag": pa.array([b"cccc"], pa.binary(4)),
                             "n": pa.array([2], pa.int64())}))
    t.append_pandas(pd.DataFrame([{"tid": ids[3], "tag": b"dddd", "n": 3}]))
    r = load_table(t.table_path)
    assert sorted(x["tid"] for x in r.scan()) == sorted(ids)
    assert sorted(r.to_pandas()["tid"].tolist()) == sorted(ids)
    assert {str(pq.read_schema(f).field("tid").type)
            for f in glob.glob(str(tmp_path / "t" / "data" / "*.parquet"))} == {"fixed_size_binary[16]"}


def test_a_uuid_column_can_be_partitioned_by_bucket_but_not_by_identity(tmp_path):
    """bucket[N] of a uuid is Iceberg's hash of exactly the 16 bytes datashard stores, and
    its partition value is an int - unambiguous in the manifest and in the path. An
    identity partition would have to spell the uuid itself in both, which readers disagree
    about, so it is refused at create rather than written and misread."""
    from datashard.data_structures import PartitionField, PartitionSpec
    from datashard.partitioning import UnsupportedTransform

    def spec(name, transform, source_id=1):
        return PartitionSpec(spec_id=0, fields=[
            PartitionField(source_id=source_id, field_id=1000, name=name, transform=transform)])

    t = create_table(str(tmp_path / "bucketed"), SCHEMA, partition_spec=spec("tid_bucket", "bucket[8]"))
    ids = [str(uuidlib.uuid4()) for _ in range(6)]
    t.append_records([{"tid": tid, "tag": None, "n": i} for i, tid in enumerate(ids)], SCHEMA)
    r = load_table(t.table_path)
    assert sorted(x["tid"] for x in r.scan()) == sorted(ids)
    buckets = {f.partition_values["tid_bucket"] for f in r._get_all_data_files()}
    assert buckets and all(isinstance(b, int) and 0 <= b < 8 for b in buckets)
    assert [x["n"] for x in r.scan(filter={"tid": ids[3]})] == [3]

    for name, transform, source in [("tid", "identity", 1), ("tag", "identity", 2)]:
        with pytest.raises(UnsupportedTransform, match="bucket"):
            create_table(str(tmp_path / f"bad_{name}"), SCHEMA, partition_spec=spec(name, transform, source))


def test_a_bare_fixed_is_still_refused_and_says_what_to_use(tmp_path):
    """Iceberg has no zero-width `fixed`; pyiceberg's parser rejects it outright."""
    bare = Schema(schema_id=1, fields=[
        {"id": 1, "name": "b", "type": "fixed"},
        {"id": 2, "name": "n", "type": "long", "required": True}])
    with pytest.raises(ValueError, match=r"fixed\[L\]"):
        create_table(str(tmp_path / "bare"), bare)

    sized = Schema(schema_id=1, fields=[
        {"id": 1, "name": "b", "type": "fixed[8]"},
        {"id": 2, "name": "n", "type": "long", "required": True}])
    t = create_table(str(tmp_path / "sized"), sized)
    t.append_records([{"b": b"12345678", "n": 1}], sized)
    assert load_table(t.table_path).scan()[0]["b"] == b"12345678"


def test_a_table_written_before_this_change_still_reads_and_can_be_appended_to(tmp_path):
    """A <=0.11.1 table's uuid column really is a parquet string. It must keep reading, and
    a new append - which writes 16 bytes - must land in the SAME scan."""
    old = [str(uuidlib.uuid4()) for _ in range(2)]
    reopened = legacy_table(tmp_path / "legacy", old)
    assert sorted(r["tid"] for r in reopened.scan()) == sorted(old)

    fresh = str(uuidlib.uuid4())
    reopened.append_records([{"tid": fresh, "n": 9}], reopened._get_current_schema())
    final = load_table(reopened.table_path)
    assert sorted(r["tid"] for r in final.scan()) == sorted(old + [fresh])
    assert uuid_types_on_disk(tmp_path / "legacy") == {"string", "fixed_size_binary[16]"}, \
        "the fixture must really hold both encodings"
    assert final.verify()["ok"]
    assert [r["n"] for r in final.scan(filter={"tid": old[0]})] == [0]
    assert [r["n"] for r in final.scan(filter={"tid": fresh})] == [9]


def test_a_rewrite_merges_the_two_encodings_and_converts_the_old_file(tmp_path):
    """Merging files reads them before writing them out, and concatenating a string column
    with a binary one casts the strings to 36-byte binary - which would either corrupt the
    uuid or, as it happened, abort the rewrite. Decoding per file first is also how an old
    file gets converted to the Iceberg encoding."""
    old = [str(uuidlib.uuid4()) for _ in range(2)]
    t = legacy_table(tmp_path / "legacy", old)
    fresh = str(uuidlib.uuid4())
    t.append_records([{"tid": fresh, "n": 9}], t._get_current_schema())

    report = t.rewrite_data_files(min_input_files=2)
    assert report["committed"] and report["added_files"] == 1

    after = load_table(t.table_path)
    assert sorted(r["tid"] for r in after.scan()) == sorted(old + [fresh])
    assert uuid_types_on_disk(tmp_path / "legacy") != {"string"}
    live = {str(pq.read_schema(str(tmp_path / "legacy") + f.file_path).field("tid").type)
            for f in after._get_all_data_files()}
    assert live == {"fixed_size_binary[16]"}, "the rewrite must leave only the Iceberg encoding"
    assert after.verify()["ok"]
    assert [r["n"] for r in after.scan(filter={"tid": old[0]})] == [0]
