"""Claim (issuedb memory 'no_silent_noop_apis'): no public API silently no-ops and
reports success.

Suspects: (1) delete_files() of a path no snapshot references commits a new 'delete'
snapshot that changes nothing and returns True; (2) delete_files() matches by exact
string, so a leading-slash mismatch against the manifest entry is a silent no-op.
"""
import os
import tempfile

import _harness as H

H.local_env()
H.quiet_logs()
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

from datashard import DataFile, FileFormat, create_table  # noqa: E402

tmp = tempfile.mkdtemp(prefix="audit_del_")
path = os.path.join(tmp, "t")
schema = H.simple_schema()
t = create_table(path, schema)
t.append_records([{"id": 1, "name": "a", "value": 1.0}], schema)
snaps0, rows0 = len(t.snapshots()), t.row_count()

try:
    with t.new_transaction() as tx:
        tx.delete_files(["/data/does_not_exist.parquet"])
        ok = tx.commit()
    H.report(
        "delete-of-unknown-path-is-rejected",
        False,
        f"silently succeeded: commit returned {ok}, snapshots {snaps0}->{len(t.snapshots())} "
        f"(a new '{t.current_snapshot().operation}' snapshot), rows {rows0}->{t.row_count()}",
    )
except Exception as e:  # noqa: BLE001
    H.report("delete-of-unknown-path-is-rejected", True, f"raised {type(e).__name__}: {str(e)[:100]}")

arrow_schema = t.file_manager.data_file_manager.create_arrow_schema(schema)
dfs = []
for n in ("ext", "ext2"):
    fp = os.path.join(path, "data", f"{n}.parquet")
    pq.write_table(pa.table({"id": [7], "name": [n], "value": [7.0]}, schema=arrow_schema), fp)
    dfs.append(DataFile(
        file_path=f"data/{n}.parquet",  # appended WITHOUT a leading slash
        file_format=FileFormat.PARQUET,
        partition_values={},
        record_count=1,
        file_size_in_bytes=os.path.getsize(fp),
    ))
t.append_data(dfs)
rows1 = t.row_count()
with t.new_transaction() as tx:
    tx.delete_files(["/data/ext.parquet"])  # same file, Iceberg-style leading slash
    ok = tx.commit()
rows2 = t.row_count()
H.report(
    "delete-with-leading-slash-removes-file-appended-without-it",
    rows2 == rows1 - 1,
    f"commit returned {ok}; rows {rows1}->{rows2} (expected {rows1 - 1})",
)
with t.new_transaction() as tx:
    tx.delete_files(["data/ext2.parquet"])  # exact string form
    tx.commit()
rows3 = t.row_count()
H.report("delete-with-exact-path-removes-file (control)", rows3 == rows1 - 2, f"rows now {rows3} (expected {rows1 - 2})")
H.finish()
