"""Claims (0.9.0, #79/#80): append_arrow never coerces a column across type families, never
drops a column silently, fills absent optional columns with nulls; and when a GC-marker
write fails no data file is written (fail closed).
"""
import glob
import os
import tempfile

import _harness as H

H.local_env()
H.quiet_logs()
import pyarrow as pa  # noqa: E402

from datashard import Schema, create_table  # noqa: E402

schema = Schema(schema_id=1, fields=[
    {"id": 1, "name": "id", "type": "long", "required": True},
    {"id": 2, "name": "sym", "type": "string", "required": True},
    {"id": 3, "name": "px", "type": "double", "required": False},
])
path = os.path.join(tempfile.mkdtemp(prefix="audit_arrow_"), "t")
t = create_table(path, schema)

cases = {
    "string-into-long-refused": pa.table({"id": ["12"], "sym": ["A"]}),
    "long-into-string-refused": pa.table({"id": [1], "sym": [99]}),
    "unknown-column-refused": pa.table({"id": [1], "sym": ["A"], "extra": [0]}),
    "null-in-required-refused": pa.table({"id": pa.array([None], pa.int64()), "sym": ["A"]}),
    "fractional-into-long-refused": pa.table({"id": [1.5], "sym": ["A"]}),
}
for name, tbl in cases.items():
    try:
        t.append_arrow(tbl)
        H.report(name, False, "accepted")
    except ValueError as e:
        H.report(name, True, str(e)[:70])
ok = t.append_arrow(pa.table({"sym": ["B"], "id": pa.array([7], pa.int32())}))  # reordered, narrower int, px absent
rows = t.to_arrow().to_pylist()
H.report("reorder-widen-nullfill-accepted", ok and rows == [{"id": 7, "sym": "B", "px": None}], f"rows={rows}")

# marker write failure -> no data file
real = t.storage.write_files


def failing(items):
    raise OSError("simulated marker write failure")


t.storage.write_files = failing
before = set(glob.glob(os.path.join(path, "data", "*.parquet")))
try:
    t.append_arrow(pa.table({"id": [8], "sym": ["C"]}))
    outcome = "append succeeded"
except OSError:
    outcome = "append raised OSError"
finally:
    t.storage.write_files = real
after = set(glob.glob(os.path.join(path, "data", "*.parquet")))
H.report("marker-write-failure-writes-no-data-file", outcome == "append raised OSError" and after == before and t.row_count() == 1,
         f"{outcome}; new data files={sorted(os.path.basename(f) for f in after - before) or 'none'}; rows={t.row_count()}")
H.finish()
