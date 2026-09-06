"""Claim: rollback deletes only files the transaction wrote itself; a caller-provided
file passed to append_files() survives, and its GC marker (new in 0.8.0) is removed.
"""
import glob
import os
import tempfile

import _harness as H

H.local_env()
H.quiet_logs()
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

from datashard import DataFile, FileFormat, create_table  # noqa: E402

tmp = tempfile.mkdtemp(prefix="audit_rb_")
path = os.path.join(tmp, "t")
schema = H.simple_schema()
t = create_table(path, schema)
arrow_schema = t.file_manager.data_file_manager.create_arrow_schema(schema)
fp = os.path.join(path, "data", "user.parquet")
pq.write_table(pa.table({"id": [1], "name": ["u"], "value": [1.0]}, schema=arrow_schema), fp)
tx = t.new_transaction().begin()
tx.append_files([DataFile(file_path="data/user.parquet", file_format=FileFormat.PARQUET, partition_values={},
                          record_count=1, file_size_in_bytes=os.path.getsize(fp))])
markers_during = glob.glob(os.path.join(path, "metadata", "inflight", "*.inflight"))
tx.rollback()
markers_after = glob.glob(os.path.join(path, "metadata", "inflight", "*.inflight"))
H.report("rollback-keeps-caller-file-and-clears-its-marker",
         os.path.exists(fp) and len(markers_during) == 1 and not markers_after and t.row_count() == 0,
         f"user file exists={os.path.exists(fp)}; markers during={len(markers_during)} after={len(markers_after)}; rows={t.row_count()}")
H.finish()
