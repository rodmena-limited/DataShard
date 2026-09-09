"""Claim (#98): a partitioned datashard table is read IDENTICALLY by DuckDB and
pyiceberg, for every transform - and the partition values are what those engines prune
on, so a transform that disagrees with Iceberg's makes them skip rows that match.

The oracle is full-row equality against the two foreign readers, plus a NEGATIVE control
that falsifies one partition value and requires them to go wrong: without it, a passing
comparison would not prove they consume our partition values at all.
"""
import os
import tempfile
from datetime import datetime, timezone
from decimal import Decimal

import _harness as H

H.local_env()
H.quiet_logs()
try:
    import duckdb
    from pyiceberg.table import StaticTable
except ImportError:
    H.skip("partitioned-foreign-readers", "duckdb and/or pyiceberg not installed")
    H.finish()

import pyarrow as pa  # noqa: E402

from datashard import (  # noqa: E402
    PartitionField,
    PartitionSpec,
    Schema,
    create_table,
    load_table,
)

SCHEMA = Schema(schema_id=1, fields=[
    {"id": 1, "name": "ts", "type": "timestamptz", "required": True},
    {"id": 2, "name": "sym", "type": "string", "required": True},
    {"id": 3, "name": "px", "type": "decimal(18,8)"},
    {"id": 4, "name": "n", "type": "long"},
])
CANON = pa.schema([pa.field("n", pa.int64()), pa.field("sym", pa.string(), False)])
con = duckdb.connect()
con.execute("LOAD iceberg;")
tmp = tempfile.mkdtemp(prefix="audit_partitioned_")


def rows(n=12, base=0):
    return [{"ts": datetime(2026, 9, 1 + (i % 3), (i * 5) % 24, tzinfo=timezone.utc),
             "sym": ["BTC-USD", "ETH-USD", "ZEN-USDT"][i % 3],
             "px": Decimal(f"{i}.12345678"), "n": base + i} for i in range(n)]


def norm(table):
    return sorted(table.select(CANON.names).cast(CANON).to_pylist(), key=lambda r: r["n"])


def metadata_uri(t):
    return f"{t.table_path}/metadata/{t.metadata_manager._current_version_info()[1]}"


def spec(*fields):
    return PartitionSpec(spec_id=0, fields=[
        PartitionField(source_id=s, field_id=1000 + i, name=n, transform=t)
        for i, (s, n, t) in enumerate(fields)])


LAYOUTS = [
    ("identity(sym)", spec((2, "sym", "identity"))),
    ("year(ts)", spec((1, "y", "year"))),
    ("month(ts)", spec((1, "m", "month"))),
    ("day(ts)", spec((1, "d", "day"))),
    ("hour(ts)", spec((1, "h", "hour"))),
    ("bucket[4](sym)", spec((2, "b", "bucket[4]"))),
    ("bucket[8](px)", spec((3, "pb", "bucket[8]"))),
    ("truncate[3](sym)+day(ts)", spec((2, "st", "truncate[3]"), (1, "d", "day"))),
]

agreed, detail = [], []
for label, sp in LAYOUTS:
    path = os.path.join(tmp, label.replace("[", "").replace("]", "").replace("(", "_").replace(")", "").replace("+", "_"))
    t = create_table(path, schema=SCHEMA, partition_spec=sp)
    t.append_records(rows(12), SCHEMA)
    t.append_records(rows(6, base=100), SCHEMA)      # a second commit into the same partitions
    ours = norm(t.to_arrow())
    try:
        d = norm(con.execute(f"SELECT * FROM iceberg_scan('{path}')").to_arrow_table())
    except Exception as e:  # noqa: BLE001
        d = f"ERROR {type(e).__name__}: {str(e).splitlines()[0][:70]}"
    try:
        p = norm(StaticTable.from_metadata(metadata_uri(t), properties={}).scan().to_arrow())
    except Exception as e:  # noqa: BLE001
        p = f"ERROR {type(e).__name__}: {str(e).splitlines()[0][:70]}"
    ok = d == ours and p == ours
    agreed.append(ok)
    detail.append(f"{label} ({len(t._get_all_data_files())} files){'' if ok else ' MISMATCH d=' + str(d)[:60] + ' p=' + str(p)[:60]}")
H.report(
    "every-partition-transform-reads-identically-in-duckdb-and-pyiceberg",
    all(agreed),
    f"{sum(agreed)}/{len(agreed)} layouts, 18 rows each, values compared not counts: {'; '.join(detail)}",
)

# compaction must not change what a foreign reader sees
t = create_table(os.path.join(tmp, "compact"), schema=SCHEMA, partition_spec=spec((2, "sym", "identity")))
for i in range(8):
    t.append_records(rows(3, base=i * 10), SCHEMA)
before_files = len(t._get_all_data_files())
before = norm(t.to_arrow())
report = t.rewrite_data_files(min_input_files=3)
after = load_table(t.table_path)
d = norm(con.execute(f"SELECT * FROM iceberg_scan('{t.table_path}')").to_arrow_table())
p = norm(StaticTable.from_metadata(metadata_uri(after), properties={}).scan().to_arrow())
H.report(
    "compaction-leaves-both-foreign-readers-unchanged",
    d == before and p == before and norm(after.to_arrow()) == before,
    f"{before_files} files -> {len(after._get_all_data_files())} "
    f"({report['bytes_before']} -> {report['bytes_after']} bytes), snapshot operation="
    f"{after.current_snapshot().operation}; duckdb and pyiceberg both return the same {len(before)} rows",
)

# NEGATIVE CONTROL: falsify one partition value; the readers must then go wrong
import fastavro  # noqa: E402

ctrl = create_table(os.path.join(tmp, "control"), schema=SCHEMA, partition_spec=spec((2, "sym", "identity")))
ctrl.append_records(rows(12), SCHEMA)
manifest = [f for f in os.listdir(os.path.join(ctrl.table_path, "metadata")) if f.endswith("-m0.avro")][0]
mpath = os.path.join(ctrl.table_path, "metadata", manifest)
with open(mpath, "rb") as fh:
    reader = fastavro.reader(fh)
    meta, records, writer_schema = dict(reader.metadata), [dict(r) for r in reader], reader.writer_schema
for r in records:                      # claim every file belongs to a symbol that has no rows
    r["data_file"]["partition"]["sym"] = "NOT-A-REAL-SYMBOL"
with open(mpath, "wb") as fh:
    fastavro.writer(fh, fastavro.parse_schema(writer_schema), records, metadata=meta)
d_ctrl = con.execute(
    f"SELECT count(*) FROM iceberg_scan('{ctrl.table_path}') WHERE sym = 'BTC-USD'").fetchone()[0]
p_ctrl = len(StaticTable.from_metadata(metadata_uri(ctrl), properties={}).scan(
    row_filter="sym == 'BTC-USD'").to_arrow())
H.report(
    "control: both readers PRUNE on our partition values",
    d_ctrl == 0 and p_ctrl == 0,
    f"with every partition value falsified to 'NOT-A-REAL-SYMBOL', a query for sym='BTC-USD' "
    f"returned duckdb={d_ctrl} rows and pyiceberg={p_ctrl} rows - 0 means they believed our "
    f"partition values, so a transform that disagrees with Iceberg would lose rows",
)
H.finish()
