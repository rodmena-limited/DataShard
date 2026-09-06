"""Claim (README): 'Parquet format for efficient columnar storage'.

Suspect: write_data_file feeds records to ParquetWriter.write_batch in slices of 1000,
and each write_batch call closes a row group -> a 50k-row append produces 50 row groups
of 1000 rows: bloated footers, poor compression, slow reads.
"""
import os
import tempfile
import time

import _harness as H

H.local_env()
H.quiet_logs()
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

from datashard import create_table  # noqa: E402

tmp = tempfile.mkdtemp(prefix="audit_rg_")
path = os.path.join(tmp, "t")
schema = H.simple_schema()
t = create_table(path, schema)
N = 50_000
records = [{"id": i, "name": f"name-{i % 97}", "value": i * 0.5} for i in range(N)]
t0 = time.perf_counter()
t.append_records(records, schema)
t_append = time.perf_counter() - t0
df = t._get_all_data_files()[0]
fp = os.path.join(path, df.file_path.lstrip("/"))
pf = pq.ParquetFile(fp)
groups = pf.metadata.num_row_groups
size_ds = os.path.getsize(fp)
# Reference: the same rows written as pyarrow would by default (one row group).
ref = os.path.join(tmp, "ref.parquet")
tbl = pa.Table.from_pylist(records, schema=t.file_manager.data_file_manager.create_arrow_schema(schema))
pq.write_table(tbl, ref, compression="lz4")
size_ref = os.path.getsize(ref)
t0 = time.perf_counter()
for _ in range(5):
    pq.read_table(fp, columns=["id"])
r_ds = (time.perf_counter() - t0) / 5
t0 = time.perf_counter()
for _ in range(5):
    pq.read_table(ref, columns=["id"])
r_ref = (time.perf_counter() - t0) / 5
H.report(
    "append-writes-reasonably-sized-row-groups",
    groups <= 5,
    f"{N} rows -> {groups} row groups ({N // groups} rows each); file {size_ds / 1024:.0f} KiB vs "
    f"{size_ref / 1024:.0f} KiB single-group reference ({size_ds / size_ref:.2f}x); "
    f"1-column read {r_ds * 1000:.1f} ms vs {r_ref * 1000:.1f} ms ({r_ds / r_ref:.1f}x); append took {t_append:.2f}s",
)
H.finish()
