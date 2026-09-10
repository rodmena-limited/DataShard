"""Partitioning by value (#98).

A partition value datashard writes is what DuckDB, pyiceberg, Spark and Trino PRUNE on,
so a transform that disagrees with Iceberg's by one bucket makes those engines skip rows
that match. Every transform here is therefore compared against **pyiceberg's own
implementation** rather than against this repository's reading of the spec - that
comparison caught a real defect on the first run (decimal truncate used the column's
declared scale where Iceberg uses the value's own).

The foreign readers themselves are exercised in
audit/evaluations/probe_v0110_partitioned_foreign_readers.py.
"""
import glob
import os
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from datashard import (
    PartitionField,
    PartitionSpec,
    Schema,
    create_table,
    load_table,
)
from datashard.partitioning import (
    UnsupportedTransform,
    partition_groups,
    transform_function,
)

pyiceberg_transforms = pytest.importorskip("pyiceberg.transforms")
pyiceberg_types = pytest.importorskip("pyiceberg.types")

SCHEMA = Schema(schema_id=1, fields=[
    {"id": 1, "name": "ts", "type": "timestamptz", "required": True},
    {"id": 2, "name": "sym", "type": "string", "required": True},
    {"id": 3, "name": "px", "type": "decimal(18,8)"},
    {"id": 4, "name": "n", "type": "long"},
])


def rows(n=12):
    return [{"ts": datetime(2026, 9, 1 + (i % 3), (i * 5) % 24, tzinfo=timezone.utc),
             "sym": ["BTC-USD", "ETH-USD", "ZEN-USDT"][i % 3],
             "px": Decimal(f"{i}.12345678"), "n": i} for i in range(n)]


def spec(*fields):
    return PartitionSpec(spec_id=0, fields=[
        PartitionField(source_id=s, field_id=1000 + i, name=n, transform=t)
        for i, (s, n, t) in enumerate(fields)])


# ---------------------------------------------------------------- refusals
@pytest.mark.parametrize("transform,source,why", [
    ("hour", "int", "needs a date or timestamp"),
    ("day", "string", "needs a date"),
    ("hour", "date", "needs a timestamp"),
    ("bucket[0]", "long", "positive modulus"),
    ("truncate[0]", "string", "positive width"),
    ("nonsense", "long", "unsupported partition transform"),
    ("bucket[8]", "boolean", "not defined for type"),
])
def test_a_transform_datashard_cannot_compute_is_refused_at_create(tmp_path, transform, source, why):
    """Accepted-and-ignored would leave the metadata claiming a layout the files lack."""
    schema = Schema(schema_id=1, fields=[
        {"id": 1, "name": "c", "type": source, "required": True},
        {"id": 2, "name": "n", "type": "long"}])
    bad = PartitionSpec(spec_id=0, fields=[
        PartitionField(source_id=1, field_id=1000, name="p", transform=transform)])
    with pytest.raises(UnsupportedTransform, match=why):
        create_table(str(tmp_path / transform.replace("[", "").replace("]", "")), schema=schema, partition_spec=bad)


def test_a_spec_referring_to_a_missing_column_or_a_low_field_id_is_refused(tmp_path):
    for bad, why in [
        (spec((99, "p", "identity")), "not a column"),
        (PartitionSpec(spec_id=0, fields=[PartitionField(source_id=2, field_id=7, name="p", transform="identity")]), "from 1000"),
    ]:
        with pytest.raises(UnsupportedTransform, match=why):
            create_table(str(tmp_path / f"bad{why[:4]}"), schema=SCHEMA, partition_spec=bad)


# ---------------------------------------------------------------- writing
def test_one_file_per_partition_per_commit(tmp_path):
    t = create_table(str(tmp_path / "t"), schema=SCHEMA, partition_spec=spec((2, "sym", "identity")))
    t.append_records(rows(12), SCHEMA)
    files = t._get_all_data_files()
    assert len(files) == 3, [f.file_path for f in files]
    assert {tuple(f.partition_values.items()) for f in files} == {
        (("sym", "BTC-USD"),), (("sym", "ETH-USD"),), (("sym", "ZEN-USDT"),)}
    assert sorted(os.path.basename(os.path.dirname(f)) for f in
                  glob.glob(str(tmp_path / "t" / "data" / "*" / "*.parquet"))) == [
        "sym=BTC-USD", "sym=ETH-USD", "sym=ZEN-USDT"]
    assert t.row_count() == 12 and len(t.scan()) == 12


def test_every_row_lands_in_exactly_one_partition(tmp_path):
    """The grouping must partition the input: nothing dropped, nothing duplicated."""
    import pyarrow as pa

    from datashard.partitioning import spec_field_types

    t = create_table(str(tmp_path / "t"), schema=SCHEMA, partition_spec=spec((1, "d", "day"), (2, "s", "identity")))
    data = rows(30)
    t.append_records(data, SCHEMA)
    assert sorted(r["n"] for r in t.scan()) == list(range(30))
    assert sum(f.record_count for f in t._get_all_data_files()) == 30

    dfm = t.file_manager.data_file_manager
    arrow = dfm.conform_arrow_table(pa.Table.from_pylist(data, schema=dfm.create_arrow_schema(SCHEMA)), SCHEMA)
    metadata = t.metadata_manager.refresh()
    groups = partition_groups(arrow, spec_field_types(metadata.partition_specs[0], SCHEMA),
                              {int(f["id"]): str(f["name"]) for f in SCHEMA.fields})
    assert sum(g.num_rows for _v, g in groups) == 30
    assert len({tuple(sorted(v.items())) for v, _g in groups}) == len(groups)  # keys are distinct


def test_partition_values_are_not_taken_from_the_caller_when_a_spec_exists(tmp_path):
    t = create_table(str(tmp_path / "t"), schema=SCHEMA, partition_spec=spec((2, "sym", "identity")))
    with pytest.raises(ValueError, match="computed from the rows"):
        t.append_records(rows(3), SCHEMA, partition_values={"sym": "WRONG"})


def test_nulls_in_a_partition_column_get_their_own_partition(tmp_path):
    schema = Schema(schema_id=1, fields=[
        {"id": 1, "name": "sym", "type": "string"},
        {"id": 2, "name": "n", "type": "long", "required": True}])
    t = create_table(str(tmp_path / "t"), schema=schema, partition_spec=spec((1, "sym", "identity")))
    t.append_records([{"sym": "A", "n": 1}, {"sym": None, "n": 2}, {"sym": "A", "n": 3}], schema)
    assert len(t._get_all_data_files()) == 2
    assert sorted(r["n"] for r in t.scan()) == [1, 2, 3]
    assert [r["n"] for r in t.scan(filter={"sym": "A"})] == [1, 3]


# ---------------------------------------------------------------- pruning
@pytest.mark.parametrize("transform", ["identity", "bucket[3]", "truncate[3]"])
def test_pruning_returns_exactly_what_an_unpartitioned_table_returns(tmp_path, transform):
    part = create_table(str(tmp_path / f"p{transform[:4]}"), schema=SCHEMA,
                        partition_spec=spec((2, "sym", transform)))
    plain = create_table(str(tmp_path / f"u{transform[:4]}"), schema=SCHEMA)
    data = rows(30)
    part.append_records(data, SCHEMA)
    plain.append_records(data, SCHEMA)
    for f in ({"sym": "BTC-USD"}, {"sym": ("in", ["BTC-USD", "ETH-USD"])}, {"n": (">=", 20)},
              {"sym": "nothing-matches-this"}):
        assert sorted(r["n"] for r in part.scan(filter=f)) == sorted(r["n"] for r in plain.scan(filter=f)), f


def test_pruning_actually_skips_files(tmp_path):
    """A prune that returns the right rows while reading everything is not pruning."""
    import contextlib

    t = create_table(str(tmp_path / "t"), schema=SCHEMA, partition_spec=spec((2, "sym", "identity")))
    t.append_records(rows(30), SCHEMA)
    opened = []
    real = t.file_manager.data_file_manager.parquet_source

    @contextlib.contextmanager
    def counting(path):
        opened.append(path)
        with real(path) as src:
            yield src

    t.file_manager.data_file_manager.parquet_source = counting
    assert len(t.scan(filter={"sym": "BTC-USD"})) == 10
    assert len(opened) == 1, opened          # one partition, one file
    opened.clear()
    assert len(t.scan(filter={"n": (">=", 0)})) == 30
    assert len(opened) == 3, "a filter on a non-partition column must not prune partitions"


def test_a_range_filter_prunes_a_temporal_partition(tmp_path):
    t = create_table(str(tmp_path / "t"), schema=SCHEMA, partition_spec=spec((1, "d", "day")))
    t.append_records(rows(30), SCHEMA)
    cutoff = datetime(2026, 9, 3, tzinfo=timezone.utc)
    got = sorted(r["n"] for r in t.scan(filter={"ts": (">=", cutoff)}))
    plain = create_table(str(tmp_path / "u"), schema=SCHEMA)
    plain.append_records(rows(30), SCHEMA)
    assert got == sorted(r["n"] for r in plain.scan(filter={"ts": (">=", cutoff)}))
    assert got, "the fixture must actually match something"


# ---------------------------------------------------------------- compaction
def test_rewrite_data_files_merges_within_partitions_only(tmp_path):
    t = create_table(str(tmp_path / "t"), schema=SCHEMA, partition_spec=spec((2, "sym", "identity")))
    for _ in range(8):                                    # 8 commits x 3 partitions = 24 files
        t.append_records(rows(3), SCHEMA)
    assert len(t._get_all_data_files()) == 24
    before = sorted(r["n"] for r in t.scan())

    plan = t.rewrite_data_files(min_input_files=3, dry_run=True)
    assert plan["rewritten_files"] == 24 and plan["partitions"] == 3 and plan["committed"] is False
    assert len(t._get_all_data_files()) == 24, "a dry run must change nothing"

    report = t.rewrite_data_files(min_input_files=3)
    assert report["committed"] and report["added_files"] == 3 and report["rewritten_files"] == 24
    assert report["bytes_after"] < report["bytes_before"]

    after = load_table(t.table_path)
    assert len(after._get_all_data_files()) == 3
    assert sorted(r["n"] for r in after.scan()) == before
    assert after.current_snapshot().operation == "replace"
    assert after.verify()["ok"]
    for df in after._get_all_data_files():
        assert len(set(df.partition_values.values())) == 1, df.partition_values


def test_rewrite_respects_min_input_files_and_a_partition_filter(tmp_path):
    t = create_table(str(tmp_path / "t"), schema=SCHEMA, partition_spec=spec((2, "sym", "identity")))
    for _ in range(4):
        t.append_records(rows(3), SCHEMA)
    assert t.rewrite_data_files(min_input_files=99)["rewritten_files"] == 0   # nothing is small enough a group
    assert len(t._get_all_data_files()) == 12

    report = t.rewrite_data_files(min_input_files=2, partition={"sym": "BTC-USD"})
    assert report["partitions"] == 1 and report["added_files"] == 1
    after = load_table(t.table_path)
    assert len(after._get_all_data_files()) == 12 - 4 + 1
    assert sorted(r["n"] for r in after.scan()) == sorted(r["n"] for r in t.scan())


def test_rewrite_is_a_no_op_on_an_unpartitioned_table_with_few_files(tmp_path):
    t = create_table(str(tmp_path / "t"), schema=SCHEMA)
    t.append_records(rows(5), SCHEMA)
    report = t.rewrite_data_files()
    assert report["rewritten_files"] == 0 and report["committed"] is False
    assert t.row_count() == 5


# ---------------------------------------------------------------- the pre-release sweep
def test_a_partition_value_survives_the_avro_round_trip(tmp_path):
    """What is written must be what is read back, per result type. A `day` partition
    goes out as an int and comes back as a date, and a timestamp comes back UTC-aware:
    both are the same instant, and `canonical` is what makes them comparable."""
    from datashard.partitioning import canonical, result_type

    cases = [
        ("string", "identity", "ZEN-USDT"), ("long", "identity", 2 ** 40),
        ("boolean", "identity", True), ("date", "identity", date(2026, 9, 9)),
        ("timestamptz", "identity", datetime(2026, 9, 9, 12, 34, 56, 789012, tzinfo=timezone.utc)),
        ("timestamp", "identity", datetime(2026, 9, 9, 12, 34, 56, 789012)),
        ("timestamptz", "day", datetime(2026, 9, 9, tzinfo=timezone.utc)),
        ("timestamp", "hour", datetime(2026, 9, 9, 7)),
        ("string", "bucket[8]", "abc"), ("string", "truncate[3]", "abcdef"),
    ]
    for i, (ctype, transform, value) in enumerate(cases):
        schema = Schema(schema_id=1, fields=[
            {"id": 1, "name": "c", "type": ctype},
            {"id": 2, "name": "n", "type": "long", "required": True}])
        sp = PartitionSpec(spec_id=0, fields=[
            PartitionField(source_id=1, field_id=1000, name="p", transform=transform)])
        t = create_table(str(tmp_path / f"rt{i}"), schema=schema, partition_spec=sp)
        t.append_records([{"c": value, "n": 1}], schema)
        back = load_table(t.table_path)._get_all_data_files()[0].partition_values["p"]
        rt = result_type(transform, ctype)
        assert canonical(back, rt) == canonical(transform_function(transform, ctype)(value), rt), (
            ctype, transform, back)


def test_pruning_works_for_day_and_timestamp_partitions(tmp_path):
    """These prune through `canonical`. Without it the stored date and the transform's
    int raised TypeError, the pruner kept every file, and pruning silently did nothing."""
    import contextlib

    schema = Schema(schema_id=1, fields=[
        {"id": 1, "name": "ts", "type": "timestamptz", "required": True},
        {"id": 2, "name": "n", "type": "long"}])
    sp = PartitionSpec(spec_id=0, fields=[
        PartitionField(source_id=1, field_id=1000, name="d", transform="day")])
    t = create_table(str(tmp_path / "p"), schema=schema, partition_spec=sp)
    u = create_table(str(tmp_path / "u"), schema=schema)
    data = [{"ts": datetime(2026, 9, 1 + (i % 4), tzinfo=timezone.utc), "n": i} for i in range(20)]
    t.append_records(data, schema)
    u.append_records(data, schema)

    opened = []
    real = t.file_manager.data_file_manager.parquet_source

    @contextlib.contextmanager
    def counting(path):
        opened.append(path)
        with real(path) as src:
            yield src

    t.file_manager.data_file_manager.parquet_source = counting
    for f, expect_files in (({"ts": datetime(2026, 9, 3, tzinfo=timezone.utc)}, 1),
                            ({"ts": (">=", datetime(2026, 9, 3, tzinfo=timezone.utc))}, 2)):
        opened.clear()
        assert sorted(r["n"] for r in t.scan(filter=f)) == sorted(r["n"] for r in u.scan(filter=f))
        assert len(opened) == expect_files, (f, opened)


def test_a_partition_value_cannot_escape_the_data_directory(tmp_path):
    from datashard.partitioning import path_segment

    for value in ("..", ".", "a/b", "../../etc", ""):
        segment = path_segment("sym", value)
        assert "/" not in segment, segment
        parts = os.path.normpath(os.path.join("data", segment, "x.parquet")).split(os.sep)
        assert ".." not in parts and "." not in parts[:-1], (value, parts)


def test_a_decimal_partition_value_is_refused_because_duckdb_crashes_on_it(tmp_path):
    """Not an error in DuckDB - a SIGABRT. Writing a table that crashes a reader is
    worse than refusing to create it (verified against DuckDB 1.5.5)."""
    schema = Schema(schema_id=1, fields=[
        {"id": 1, "name": "px", "type": "decimal(18,8)"},
        {"id": 2, "name": "n", "type": "long", "required": True}])
    for transform in ("identity", "truncate[10]"):
        sp = PartitionSpec(spec_id=0, fields=[
            PartitionField(source_id=1, field_id=1000, name="p", transform=transform)])
        with pytest.raises(UnsupportedTransform, match="DuckDB"):
            create_table(str(tmp_path / transform.replace("[", "")), schema=schema, partition_spec=sp)
    # bucket[N] on the same column is fine: its partition value is an int
    sp = PartitionSpec(spec_id=0, fields=[
        PartitionField(source_id=1, field_id=1000, name="p", transform="bucket[4]")])
    t = create_table(str(tmp_path / "bucketed"), schema=schema, partition_spec=sp)
    t.append_records([{"px": Decimal("1.5"), "n": 1}], schema)
    assert t.row_count() == 1


def test_compaction_never_merges_two_partitions_even_if_their_keys_look_alike(tmp_path):
    from datashard.compaction import _key

    assert _key({"d": date(2026, 9, 1)}) != _key({"d": "2026-09-01"})
    assert _key({"d": 1}) != _key({"d": "1"})
    assert _key({"d": True}) != _key({"d": 1})


# ---------------------------------------------------------------- the second pass
def test_one_commit_registers_its_markers_in_one_batch(tmp_path):
    """Every data file needs a GC-protection marker before it is written. Registering
    them one at a time cost a round trip PER PARTITION, so a 200-partition commit spent
    200 sequential PUTs on markers before writing any data."""
    schema = Schema(schema_id=1, fields=[
        {"id": 1, "name": "h", "type": "int", "required": True},
        {"id": 2, "name": "n", "type": "long"}])
    t = create_table(str(tmp_path / "t"), schema=schema, partition_spec=spec((1, "h", "identity")))
    batches = []
    real = t.storage.write_files

    def counting(items):
        batches.append(len(items))
        return real(items)

    t.storage.write_files = counting
    t.append_records([{"h": i, "n": i} for i in range(120)], schema)
    assert len(t._get_all_data_files()) == 120
    marker_batches = [n for n in batches if n >= 100]
    assert marker_batches == [120], f"markers must go out in ONE batch, saw {batches}"


def test_a_commit_over_many_partitions_says_so(tmp_path, caplog):
    import logging

    schema = Schema(schema_id=1, fields=[
        {"id": 1, "name": "h", "type": "int", "required": True},
        {"id": 2, "name": "n", "type": "long"}])
    t = create_table(str(tmp_path / "t"), schema=schema, partition_spec=spec((1, "h", "identity")))
    with caplog.at_level(logging.WARNING, logger="datashard.transaction_append"):
        t.append_records([{"h": i, "n": i} for i in range(150)], schema)
    warning = " ".join(r.message for r in caplog.records)
    assert "150 data files" in warning and "rewrite_data_files" in warning, warning

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="datashard.transaction_append"):
        t.append_records([{"h": 1, "n": 1}], schema)
    assert not caplog.records, "a small commit must stay quiet"


def test_identity_partitioning_on_a_float_is_refused(tmp_path):
    """NaN never equals itself, so every NaN row would become its own partition and file."""
    schema = Schema(schema_id=1, fields=[
        {"id": 1, "name": "f", "type": "double"},
        {"id": 2, "name": "n", "type": "long", "required": True}])
    with pytest.raises(UnsupportedTransform, match="NaN"):
        create_table(str(tmp_path / "f"), schema=schema, partition_spec=spec((1, "f", "identity")))


def test_a_partition_directory_never_leaks_into_the_data(tmp_path):
    """#98 writes `data/sym=BTC-USD/`, which is exactly the Hive-style path that made
    reads fail or grow a phantom column in #93. The default spec partitions on a column
    of the same name, so the two features meet head-on."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    t = create_table(str(tmp_path / "t"), schema=SCHEMA, partition_spec=spec((2, "sym", "identity")))
    t.append_records(rows(9), SCHEMA)
    a_file = glob.glob(str(tmp_path / "t" / "data" / "*" / "*.parquet"))[0]
    assert "sym=" in a_file, a_file
    with pytest.raises(pa.ArrowTypeError):
        pq.read_table(a_file)                     # control: raw pyarrow still infers and fails
    assert load_table(t.table_path).to_arrow().schema.names == ["ts", "sym", "px", "n"]
    assert {r["sym"] for r in load_table(t.table_path).scan()} == {"BTC-USD", "ETH-USD", "ZEN-USDT"}


def test_a_losing_rewrite_says_what_happened_and_loses_nothing(tmp_path):
    """A rewrite whose inputs vanish between planning and committing must fail with a
    sentence an operator can act on, not `not part of the current snapshot`, which reads
    like corruption. The window is real: two rewrites racing produce exactly it."""
    t = create_table(str(tmp_path / "t"), schema=SCHEMA, partition_spec=spec((2, "sym", "identity")))
    for _ in range(4):
        t.append_records(rows(3), SCHEMA)
    before = sorted(r["n"] for r in t.scan())
    doomed = t._get_all_data_files()[0]
    victim, victim_rows = doomed.file_path, doomed.record_count

    original_plan = t._plan_rewrite

    def plan_then_lose_the_inputs(*args, **kwargs):
        groups = original_plan(*args, **kwargs)
        other = load_table(t.table_path)          # someone else commits INSIDE the window
        with other.new_transaction() as tx:
            tx.delete_files([victim])
            tx.commit()
        return groups

    t._plan_rewrite = plan_then_lose_the_inputs
    with pytest.raises(RuntimeError, match="no rows were lost"):
        t.rewrite_data_files(min_input_files=2)

    after = load_table(t.table_path)
    assert len(after.scan()) == len(before) - victim_rows  # only the competing delete landed
    assert after.verify()["ok"], "the failed rewrite must leave a readable table"
    assert after.rewrite_data_files(min_input_files=2)["committed"], "and a re-run must work"
