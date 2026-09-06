"""Claim (0.10, #85-#90): a datashard table IS an Iceberg v2 table - DuckDB's iceberg
extension and pyiceberg read it natively, and see exactly the rows datashard sees.

The oracle is FULL ROW EQUALITY against the foreign readers, not counts and not our own
round trip: reading our own files back would only prove our conventions are consistent
with themselves. Covered: create, appends (decimal / timestamptz / date / nulls),
delete_files, manifest compaction, expire_snapshots + garbage_collect, time travel, a
table migrated from a released version, a parquet file appended without field ids
(resolved through schema.name-mapping.default), and S3 through DuckDB's httpfs.

Includes a NEGATIVE control: with a deliberately corrupted bound the foreign readers
must return the WRONG rows - proof that they consume our bounds, so a bounds-encoding
regression cannot pass this probe silently.
"""
import os
import shutil
import tempfile
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal

import _harness as H

H.local_env()
H.quiet_logs()
try:
    import duckdb
except ImportError:
    duckdb = None
try:
    from pyiceberg.table import StaticTable
except ImportError:
    StaticTable = None
if duckdb is None or StaticTable is None:
    H.skip("foreign-readers", "duckdb and/or pyiceberg not installed (pip install datashard[dev])")
    H.finish()

import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

from datashard import Schema, create_table, load_table  # noqa: E402

SCHEMA = Schema(schema_id=1, fields=[
    {"id": 1, "name": "id", "type": "long", "required": True},
    {"id": 2, "name": "sym", "type": "string", "required": False},
    {"id": 3, "name": "px", "type": "decimal(18,8)", "required": False},
    {"id": 4, "name": "ts", "type": "timestamptz", "required": False},
    {"id": 5, "name": "d", "type": "date", "required": False},
    {"id": 6, "name": "ok", "type": "boolean", "required": False},
])
CANON = pa.schema([
    pa.field("id", pa.int64(), False), pa.field("sym", pa.string()), pa.field("px", pa.decimal128(18, 8)),
    pa.field("ts", pa.timestamp("us", tz="UTC")), pa.field("d", pa.date32()), pa.field("ok", pa.bool_()),
])
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD iceberg; LOAD httpfs;")


def rows(t):
    """Canonical, order-independent row list from a pyarrow table."""
    return sorted(t.select(CANON.names).cast(CANON).to_pylist(), key=lambda r: r["id"])


def duck_rows(target, **kw):
    args = "".join(f", {k}={v}" for k, v in kw.items())
    return rows(con.execute(f"SELECT * FROM iceberg_scan('{target}'{args})").to_arrow_table())


def ice_rows(metadata_uri, snapshot_id=None, **props):
    t = StaticTable.from_metadata(metadata_uri, properties=dict(props))
    scan = t.scan(snapshot_id=snapshot_id) if snapshot_id else t.scan()
    return rows(scan.to_arrow())


def metadata_uri(table):
    v, name = table.metadata_manager._current_version_info()
    return f"{table.table_path}/metadata/{name}"


def compare(label, table, extra=""):
    """datashard == DuckDB == pyiceberg, on values."""
    ours = rows(table.to_arrow())
    try:
        d = duck_rows(table.table_path)
    except Exception as e:  # noqa: BLE001
        d = f"ERROR {type(e).__name__}: {str(e).splitlines()[0][:120]}"
    try:
        p = ice_rows(metadata_uri(table))
    except Exception as e:  # noqa: BLE001
        p = f"ERROR {type(e).__name__}: {str(e).splitlines()[0][:120]}"
    ok = ours == d and ours == p
    detail = f"{len(ours)} rows; duckdb {'==' if d == ours else '!='} datashard; pyiceberg {'==' if p == ours else '!='} datashard"
    if not ok:
        detail += f" | datashard={ours[:2]} duckdb={d if isinstance(d, str) else d[:2]} pyiceberg={p if isinstance(p, str) else p[:2]}"
    H.report(label, ok, detail + (f" | {extra}" if extra else ""))
    return ours


def batch(start, n):
    return [{"id": start + i,
             "sym": None if i % 3 == 1 else f"S{i}",
             "px": None if i % 4 == 3 else Decimal(f"{start + i}.12345678"),
             "ts": datetime(2026, 9, 6, 12, i % 60, tzinfo=timezone.utc),
             "d": date(2026, 1, 1 + (i % 28)),
             "ok": bool(i % 2)} for i in range(n)]


tmp = tempfile.mkdtemp(prefix="audit_foreign_")
path = os.path.join(tmp, "trades")
t = create_table(path, SCHEMA)

# 1. empty table, then appends through both ingestion paths
H.report("empty-table-is-readable-by-duckdb", duck_rows(path) == [], f"duckdb rows on a table with no snapshot: {duck_rows(path)}")
t.append_records(batch(1, 5), SCHEMA)
compare("after-append_records", t)
t.append_arrow(pa.Table.from_pylist(batch(100, 4), schema=CANON))
compare("after-append_arrow", t)

# 2. time travel: every snapshot must agree with datashard's view of it
first_snap = t.snapshots()[0]["snapshot_id"]
ours_first = rows(t.to_arrow(snapshot_id=first_snap))
d_first = duck_rows(path, snapshot_from_id=first_snap)
p_first = ice_rows(metadata_uri(t), snapshot_id=first_snap)
H.report(
    "time-travel-agrees-with-both-readers",
    ours_first == d_first == p_first and len(ours_first) == 5,
    f"snapshot {first_snap}: datashard {len(ours_first)} rows, duckdb {len(d_first)}, pyiceberg {len(p_first)}",
)

# 3. deletes, manifest compaction, expiry + GC
victim = t._get_all_data_files()[0].file_path
with t.new_transaction() as tx:
    tx.delete_files([victim])
    tx.commit()
compare("after-delete_files", t)
t.append_records(batch(200, 3), SCHEMA)
t.compact_manifests()
compare("after-compact_manifests", t)
t.expire_snapshots(retain_last=1)
gc_stats = t.garbage_collect(grace_period_ms=0, allow_short_grace=True)
compare("after-expire_snapshots-and-gc", t, f"gc_stats={gc_stats}")

# 4. a parquet file with NO field ids, appended through append_files, must still resolve
#    for foreign readers via schema.name-mapping.default
plain = os.path.join(path, "data", f"external_{uuid.uuid4().hex[:8]}.parquet")
pq.write_table(pa.Table.from_pylist(batch(300, 2), schema=CANON), plain)  # CANON carries no field ids
assert not pq.read_schema(plain).field("id").metadata, "fixture must have no PARQUET:field_id"
from datashard import DataFile, FileFormat  # noqa: E402

with t.new_transaction() as tx:
    tx.append_files([DataFile(
        file_path="/data/" + os.path.basename(plain), file_format=FileFormat.PARQUET, partition_values={},
        record_count=2, file_size_in_bytes=os.path.getsize(plain),
    )])
    tx.commit()
compare("parquet-without-field-ids-via-name-mapping", t)

# 5. NEGATIVE CONTROL: corrupt one file's bound and the foreign readers must go wrong.
#    If they did not consume our bounds, a bounds-encoding bug would be invisible here.
ctrl_path = os.path.join(tmp, "bounds_control")
ctrl = create_table(ctrl_path, SCHEMA)
ctrl.append_records(batch(1, 3), SCHEMA)
target = rows(ctrl.to_arrow())[1]["id"]
import fastavro  # noqa: E402

from datashard.iceberg_avro import manifest_entry_schema  # noqa: E402
from datashard.iceberg_bounds import encode_bound  # noqa: E402

manifest = [f for f in os.listdir(os.path.join(ctrl_path, "metadata")) if f.endswith("-m0.avro")][0]
mpath = os.path.join(ctrl_path, "metadata", manifest)
with open(mpath, "rb") as fh:
    reader = fastavro.reader(fh)
    meta, records = dict(reader.metadata), [dict(r) for r in reader]
for r in records:  # claim the file holds ids 9000..9001, so a lookup for `target` prunes it away
    r["data_file"]["lower_bounds"] = [{"key": 1, "value": encode_bound(9000, "long")}]
    r["data_file"]["upper_bounds"] = [{"key": 1, "value": encode_bound(9001, "long")}]
with open(mpath, "wb") as fh:
    fastavro.writer(fh, fastavro.parse_schema(manifest_entry_schema([])), records, metadata=meta)
d_ctrl = con.execute(f"SELECT count(*) FROM iceberg_scan('{ctrl_path}') WHERE id = {target}").fetchone()[0]
p_ctrl = len(StaticTable.from_metadata(metadata_uri(ctrl)).scan(row_filter=f"id == {target}").to_arrow())
H.report(
    "control: both readers consume our column bounds",
    d_ctrl == 0 and p_ctrl == 0,
    f"with the manifest's id bounds falsified to [9000, 9001], a lookup for id={target} returned "
    f"duckdb={d_ctrl} rows, pyiceberg={p_ctrl} rows (0 = the bound was believed, so real bounds must be exact)",
)

# 6. a table migrated from a released version, read by both
fixture = os.path.join(os.path.dirname(__file__), "..", "..", "tests", "fixtures", "legacy_table_0_9_1.tar.gz")
if os.path.exists(fixture):
    import json
    import tarfile

    mig_dir = os.path.join(tmp, "migrated")
    os.makedirs(mig_dir)
    with tarfile.open(fixture) as tar:
        tar.extractall(mig_dir, filter="data")
    mig_path = os.path.join(mig_dir, "legacy_091")
    expected = json.load(open(os.path.join(mig_dir, "legacy_091_rows.json")))
    from datashard.migrate import migrate_table  # noqa: E402

    migrate_table(mig_path)
    mt = load_table(mig_path)
    ours = sorted(({"id": r["id"], "sym": r["sym"], "px": str(r["px"]), "ts": str(r["ts"])} for r in mt.scan()), key=lambda r: r["id"])
    exp = sorted(({"id": int(r["id"]), "sym": r["sym"], "px": str(Decimal(str(r["px"]))), "ts": str(r["ts"])} for r in expected), key=lambda r: r["id"])
    d = sorted((({"id": r["id"], "sym": r["sym"], "px": str(r["px"]), "ts": str(r["ts"])})
                for r in con.execute(f"SELECT id, sym, px, ts FROM iceberg_scan('{mig_path}')").to_arrow_table().to_pylist()), key=lambda r: r["id"])
    p = sorted((({"id": r["id"], "sym": r["sym"], "px": str(r["px"]), "ts": str(r["ts"])})
                for r in StaticTable.from_metadata(metadata_uri(mt)).scan().to_arrow().select(["id", "sym", "px", "ts"]).to_pylist()), key=lambda r: r["id"])
    H.report(
        "migrated-0.9.1-table-reads-identically-in-datashard-duckdb-and-pyiceberg",
        ours == exp and d == exp and p == exp,
        f"{len(exp)} rows written by the released 0.9.1: datashard {'==' if ours == exp else '!='}, "
        f"duckdb {'==' if d == exp else '!='}, pyiceberg {'==' if p == exp else '!='}",
    )
else:
    H.skip("migrated-table-foreign-read", f"fixture missing: {fixture}")

# 7. S3: DuckDB reads the same table through httpfs against the moto server
bucket = f"audit-foreign-{uuid.uuid4().hex[:8]}"
s3 = H.s3_env(bucket)
if s3 is None:
    H.skip("s3-foreign-read", "moto_server unavailable")
else:
    s3t = create_table(f"s3trades_{uuid.uuid4().hex[:6]}", SCHEMA)
    s3t.append_records(batch(1, 4), SCHEMA)
    s3t.append_arrow(pa.Table.from_pylist(batch(50, 3), schema=CANON))
    # The credentials come from the product's own helper, not hand-rolled SQL: if
    # duckdb_s3_secret_sql() emits a statement DuckDB rejects, this claim goes red.
    con.execute(s3t.duckdb_s3_secret_sql("audit_s3"))
    ours = rows(s3t.to_arrow())
    try:
        d = duck_rows(s3t.location)
    except Exception as e:  # noqa: BLE001
        d = f"ERROR {type(e).__name__}: {str(e).splitlines()[0][:140]}"
    H.report(
        "s3-table-read-by-duckdb-over-httpfs",
        ours == d,
        f"{len(ours)} rows at {s3t.location}: duckdb {'==' if d == ours else '!= ' + str(d)[:160]} datashard, "
        f"through the CREATE SECRET statement Table.duckdb_s3_secret_sql() produced",
    )

    # Reverse direction: a foreign WRITER. pyiceberg commits with its own metadata
    # naming and does not touch version-hint.text (spike #83), so datashard must
    # ignore that commit rather than half-merge it - and its own rows must survive.
    try:
        from pyiceberg.catalog.sql import SqlCatalog

        cat = SqlCatalog("audit", **{
            "uri": f"sqlite:///{tempfile.mkdtemp(prefix='audit_cat_')}/c.db",
            "warehouse": f"s3://{bucket}/wh",
            "s3.endpoint": os.environ["DATASHARD_S3_ENDPOINT"],
            "s3.access-key-id": os.environ["DATASHARD_S3_ACCESS_KEY"],
            "s3.secret-access-key": os.environ["DATASHARD_S3_SECRET_KEY"],
            "s3.region": os.environ.get("DATASHARD_S3_REGION", "us-east-1"),
        })
        cat.create_namespace("ns")
        foreign = cat.register_table("ns.t", f"{s3t.location}/metadata/{s3t.metadata_manager._current_version_info()[1]}")
        foreign.append(pa.Table.from_pylist(batch(900, 2), schema=CANON))
        ours_after = rows(load_table(s3t.table_path).to_arrow())
        H.report(
            "a-foreign-writers-commit-is-ignored-not-half-merged",
            ours_after == ours,
            f"pyiceberg appended 2 rows and wrote {foreign.metadata_location.rsplit('/', 1)[-1]} (its own naming, "
            f"hint untouched); datashard still reads its own {len(ours_after)} rows unchanged "
            f"({'as documented' if ours_after == ours else 'DIVERGED'}): datashard must be the sole writer "
            f"until the REST catalog (1.0)",
        )
    except ImportError:
        H.skip("foreign-writer-direction", "pyiceberg[sql-sqlite] not installed")
shutil.rmtree(tmp, ignore_errors=True)
H.finish()
