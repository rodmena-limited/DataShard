"""A table's directory is a LOCATION, never a partition scheme (#93).

pyarrow's `pq.read_table` defaults to `partitioning="hive"`: it parses the
directories above a parquet file for `key=value` segments and folds them into the
result. datashard reads local files by path (since #74), so with inference left on
the physical location of a table leaked into its data:

  * `<root>/symbol=ZEN-USD/...` and a `symbol` column  -> every scan raised
    ArrowTypeError ("string vs dictionary<...>"), and had the merge succeeded the
    directory would have OVERWRITTEN the value the rows carry;
  * `<root>/venue=binance/...` and no `venue` column   -> a `venue` column was
    silently added to every row, which no caller ever asked for.

Reported by crypto-trader 2026-09-08 against 0.10.0, blocking their upgrade.

Every test here first proves, through raw pyarrow, that the fixture really does
trigger Hive inference - otherwise a passing datashard read would be evidence of
nothing.
"""
import glob
import os
import re

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from datashard import Schema, create_table, load_table

SCHEMA = Schema(schema_id=1, fields=[
    {"id": 1, "name": "symbol", "type": "string", "required": True},
    {"id": 2, "name": "n", "type": "long", "required": True},
])
# The row value differs from the directory value ON PURPOSE: in the reporter's
# tables the directory holds the exchange pair and the column holds the venue's
# pair, and they are legitimately different. Path inference would silently
# replace one with the other.
PATH_VALUE, ROW_VALUE = "ZEN-USD", "ZEN-USDT"


def _table_at(tmp_path, *segments, files=2):
    path = tmp_path.joinpath(*segments)
    path.parent.mkdir(parents=True, exist_ok=True)
    t = create_table(str(path), schema=SCHEMA)
    for i in range(1, files + 1):
        t.append_records(records=[{"symbol": ROW_VALUE, "n": i}], schema=SCHEMA)
    return str(path)


def _one_data_file(table_path):
    (f,) = sorted(glob.glob(os.path.join(table_path, "data", "*.parquet")))[:1]
    return f


@pytest.mark.parametrize("files", [1, 2])
def test_a_colliding_path_segment_never_replaces_the_column_value(tmp_path, files):
    path = _table_at(tmp_path, "t", f"symbol={PATH_VALUE}", "day=2026-09-08", files=files)

    # control: the fixture really does trigger inference, so the assertions below mean something
    with pytest.raises(pa.ArrowTypeError, match="symbol"):
        pq.read_table(_one_data_file(path))

    t = load_table(path)
    assert [r["symbol"] for r in t.scan()] == [ROW_VALUE] * files
    assert t.to_arrow().column("symbol").to_pylist() == [ROW_VALUE] * files
    assert t.to_pandas()["symbol"].tolist() == [ROW_VALUE] * files
    assert [r["symbol"] for r in t.iter_records()] == [ROW_VALUE] * files
    assert [r["symbol"] for b in t.scan_batches() for r in b] == [ROW_VALUE] * files
    assert t.row_count() == files
    assert [r["symbol"] for r in t.scan(columns=["symbol"])] == [ROW_VALUE] * files
    assert [r["n"] for r in t.scan(filter={"n": 1})] == [1]


def test_a_non_colliding_path_segment_does_not_add_a_column(tmp_path):
    path = _table_at(tmp_path, "t", "venue=binance")

    # control: raw pyarrow really does inject the directory as a column here
    injected = pq.read_table(_one_data_file(path))
    assert injected.column("venue").to_pylist() == ["binance"], injected.schema

    t = load_table(path)
    assert t.to_arrow().schema.names == ["symbol", "n"]
    assert set(t.scan()[0]) == {"symbol", "n"}
    assert list(t.to_pandas().columns) == ["symbol", "n"]
    assert set(next(iter(t.scan_batches()))[0]) == {"symbol", "n"}


def test_the_same_table_reads_identically_wherever_it_is_stored(tmp_path):
    """The property behind both cases: data and metadata decide, location does not."""
    flat = load_table(_table_at(tmp_path, "flat")).scan()
    hive = load_table(_table_at(tmp_path, "h", f"symbol={PATH_VALUE}", "day=2026-09-08")).scan()
    deep = load_table(_table_at(tmp_path, "d", "venue=binance", "year=2026", "n=99")).scan()
    assert flat == hive == deep


def test_integrity_modes_and_verification_are_unaffected(tmp_path):
    path = _table_at(tmp_path, "t", f"symbol={PATH_VALUE}", files=2)
    t = load_table(path)
    for mode in ("page", "full", "off"):
        assert [r["symbol"] for r in t.scan(verify_checksums=mode)] == [ROW_VALUE] * 2, mode
    assert [r["symbol"] for r in t.scan(parallel=2)] == [ROW_VALUE] * 2


def test_appending_a_prebuilt_file_still_validates_under_a_hive_path(tmp_path):
    """append_files reads the parquet footer to check the schema; that path must not
    see an injected column either, or the check would reject a valid file."""
    from datashard import DataFile, FileFormat

    path = _table_at(tmp_path, "t", "venue=binance", files=1)
    extra = os.path.join(path, "data", "prebuilt.parquet")
    pq.write_table(pa.table({"symbol": pa.array([ROW_VALUE]), "n": pa.array([7], pa.int64())},
                            schema=pa.schema([pa.field("symbol", pa.string(), False),
                                              pa.field("n", pa.int64(), False)])), extra)
    with load_table(path).new_transaction() as tx:
        tx.append_files([DataFile(file_path="/data/prebuilt.parquet", file_format=FileFormat.PARQUET,
                                  partition_values={}, record_count=1,
                                  file_size_in_bytes=os.path.getsize(extra))])
        tx.commit()
    assert sorted(r["n"] for r in load_table(path).scan()) == [1, 7]


def test_no_read_goes_around_the_helper():
    """A structural guard: `pq.read_table` re-enables Hive inference by default, so a
    new call site would silently reintroduce #93. Every read goes through
    data_io.read_parquet_table, which turns it off."""
    src = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src", "datashard")
    offenders = []
    for name in os.listdir(src):
        if not name.endswith(".py") or name == "data_io.py":  # data_io defines the helper
            continue
        text = open(os.path.join(src, name)).read()
        for m in re.finditer(r"\bpq\.read_table\s*\(", text):
            offenders.append(f"{name}:{text[:m.start()].count(chr(10)) + 1}")
    assert not offenders, (
        f"bare pq.read_table call(s) at {offenders} - use data_io.read_parquet_table, "
        f"which disables filesystem partition inference (#93)"
    )


def test_s3_table_under_a_hive_prefix_reads_its_own_values(s3_env):
    """The reporter could not test S3. S3 reads go through a file object, which never
    triggers dataset discovery - this pins that, so the two backends cannot drift."""
    name = f"{s3_env.table_name()}/symbol={PATH_VALUE}/day=2026-09-08"
    s3_env.created.append(name)
    t = create_table(name, SCHEMA)
    for i in (1, 2):
        t.append_records(records=[{"symbol": ROW_VALUE, "n": i}], schema=SCHEMA)
    reopened = load_table(name)
    assert [r["symbol"] for r in reopened.scan()] == [ROW_VALUE] * 2
    assert reopened.to_arrow().schema.names == ["symbol", "n"]
