"""Claim (0.11.2, #92): a `uuid` column and a `fixed[L]` column are what Iceberg says they
are - fixed_size_binary on disk - so DuckDB and pyiceberg read them, row for row.

Until 0.11.2 datashard wrote a parquet STRING for uuid, which pyiceberg refuses ("Cannot
promote an string to uuid"), and had no way to spell fixed at all; 0.10 responded by
refusing both types at create time. The oracle here is full row equality against the two
foreign readers, on VALUES, with the uuid canonicalised on all three sides.

Two controls keep the claim from being vacuous:
  - the OLD representation (a parquet string tagged uuid) is built deliberately and
    pyiceberg must still refuse it - the same check that passes below goes red on it;
  - a table holding both encodings at once must read completely in datashard, because a
    table written before 0.11.2 keeps its string files forever.
"""
import os
import shutil
import tempfile
import uuid as uuidlib

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
    H.skip("uuid-fixed-foreign-readers", "duckdb and/or pyiceberg not installed (pip install datashard[dev])")
    H.finish()

import pyarrow.parquet as pq  # noqa: E402

from datashard import Schema, create_table, load_table  # noqa: E402
from datashard.uuid_columns import to_text  # noqa: E402

SCHEMA = Schema(schema_id=1, fields=[
    {"id": 1, "name": "n", "type": "long", "required": True},
    {"id": 2, "name": "tid", "type": "uuid", "required": True},
    {"id": 3, "name": "tag", "type": "fixed[4]", "required": False},
])
con = duckdb.connect()
con.execute("LOAD iceberg;")


def norm(records):
    """Row list canonicalised on values, whatever Python type each reader hands back:
    DuckDB gives a UUID object, pyiceberg an arrow.uuid extension, datashard a string."""
    out = []
    for r in records:
        out.append({
            "n": int(r["n"]),
            "tid": to_text(r["tid"]),
            "tag": bytes(r["tag"]) if r["tag"] is not None else None,
        })
    return sorted(out, key=lambda r: r["n"])


def metadata_uri(table):
    return f"{table.table_path}/metadata/{table.metadata_manager._current_version_info()[1]}"


tmp = tempfile.mkdtemp(prefix="audit_uuid_")
path = os.path.join(tmp, "trades")
t = create_table(path, SCHEMA)
ids = [str(uuidlib.uuid4()) for _ in range(6)]
t.append_records([{"n": i, "tid": ids[i], "tag": b"t%03d" % i} for i in range(4)], SCHEMA)
t.append_records([{"n": 4, "tid": uuidlib.UUID(ids[4]), "tag": None},
                  {"n": 5, "tid": uuidlib.UUID(ids[5]).bytes, "tag": b"last"}], SCHEMA)

ours = norm(t.to_arrow().to_pylist())
H.report(
    "datashard-returns-the-uuid-strings-it-was-given",
    [r["tid"] for r in ours] == ids,
    f"{len(ours)} rows written as str / uuid.UUID / raw bytes, all read back as the same "
    f"canonical strings (first: {ours[0]['tid']}, tag {ours[0]['tag']!r})",
)

(data_file,) = sorted(f for f in os.listdir(os.path.join(path, "data")) if f.endswith(".parquet"))[:1]
on_disk = pq.read_schema(os.path.join(path, "data", data_file))
H.report(
    "on-disk-types-are-what-iceberg-promises",
    str(on_disk.field("tid").type) == "fixed_size_binary[16]" and str(on_disk.field("tag").type) == "fixed_size_binary[4]",
    f"parquet: tid={on_disk.field('tid').type}, tag={on_disk.field('tag').type} "
    f"(a string here is what pyiceberg refuses to promote)",
)

try:
    d = norm(con.execute(f"SELECT n, tid, tag FROM iceberg_scan('{path}')").to_arrow_table().to_pylist())
except Exception as e:  # noqa: BLE001
    d = f"ERROR {type(e).__name__}: {str(e).splitlines()[0][:140]}"
try:
    ptab = StaticTable.from_metadata(metadata_uri(t)).scan().to_arrow()
    p, p_types = norm(ptab.to_pylist()), (str(ptab.schema.field("tid").type), str(ptab.schema.field("tag").type))
except Exception as e:  # noqa: BLE001
    p, p_types = f"ERROR {type(e).__name__}: {str(e).splitlines()[0][:140]}", ("", "")
H.report(
    "duckdb-and-pyiceberg-read-uuid-and-fixed-row-for-row",
    ours == d and ours == p,
    f"{len(ours)} rows: duckdb {'==' if d == ours else '!= ' + str(d)[:160]} datashard; "
    f"pyiceberg {'==' if p == ours else '!= ' + str(p)[:160]} datashard; pyiceberg types {p_types}",
)

# CONTROL: the representation datashard used before 0.11.2 - a parquet string tagged
# `uuid` - must FAIL the very check that passed above, or the check proves nothing.
legacy_path = os.path.join(tmp, "legacy_string_uuid")
legacy_schema = Schema(schema_id=1, fields=[
    {"id": 1, "name": "n", "type": "long", "required": True},
    {"id": 2, "name": "tid", "type": "string", "required": True},
])
legacy = create_table(legacy_path, legacy_schema)
legacy.append_records([{"n": i, "tid": ids[i]} for i in range(2)], legacy_schema)
mm = legacy.metadata_manager
base = mm.refresh()
patched = mm._dict_to_metadata(mm._metadata_to_dict(base))
patched.schemas = [Schema(schema_id=1, fields=[
    {"id": 1, "name": "n", "type": "long", "required": True},
    {"id": 2, "name": "tid", "type": "uuid", "required": True},
])]
patched.current_schema_id = 1
mm.commit(base, patched)
try:
    legacy_rows = norm(StaticTable.from_metadata(metadata_uri(legacy)).scan().to_arrow().to_pylist())
    refusal = f"pyiceberg ACCEPTED it and returned {legacy_rows}"
    control_ok = False
except Exception as e:  # noqa: BLE001
    refusal = f"{type(e).__name__}: {str(e).splitlines()[0][:120]}"
    control_ok = True
H.report(
    "control: the pre-0.11.2 string representation is still rejected by pyiceberg",
    control_ok,
    f"a uuid column stored as a parquet string -> {refusal} (so the PASS above is the new "
    f"encoding being read, not pyiceberg being lenient)",
)

# ...and datashard must keep reading that same old table, including in one scan with new
# files, because tables written before 0.11.2 keep their string data files forever.
reopened = load_table(legacy_path)
old_rows = sorted(r["tid"] for r in reopened.scan())
fresh = str(uuidlib.uuid4())
reopened.append_records([{"n": 9, "tid": fresh}], reopened._get_current_schema())
mixed = load_table(legacy_path)
types = {str(pq.read_schema(os.path.join(legacy_path, "data", f)).field("tid").type)
         for f in os.listdir(os.path.join(legacy_path, "data")) if f.endswith(".parquet")}
all_rows = sorted(r["tid"] for r in mixed.scan())
H.report(
    "a-table-written-before-0.11.2-keeps-reading-and-can-still-be-appended-to",
    old_rows == sorted(ids[:2]) and all_rows == sorted(ids[:2] + [fresh])
    and types == {"string", "fixed_size_binary[16]"} and mixed.verify()["ok"],
    f"string-backed rows {len(old_rows)} + a new binary-backed row read together: "
    f"{len(all_rows)} rows across files of types {sorted(types)}, verify ok",
)

# A filter on a uuid column crosses the same boundary: the caller filters on a string and
# the file holds bytes, so a lookup must find the row in EITHER kind of file.
found_old = [r["n"] for r in mixed.scan(filter={"tid": ids[0]})]
found_new = [r["n"] for r in mixed.scan(filter={"tid": fresh})]
missing = mixed.scan(filter={"tid": str(uuidlib.uuid4())})
H.report(
    "a-uuid-filter-finds-rows-in-both-old-and-new-files",
    found_old == [0] and found_new == [9] and missing == [],
    f"lookup in the string file -> n={found_old}, in the binary file -> n={found_new}, "
    f"for an absent uuid -> {len(missing)} rows",
)
shutil.rmtree(tmp, ignore_errors=True)
H.finish()
