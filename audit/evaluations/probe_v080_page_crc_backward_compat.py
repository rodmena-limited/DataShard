"""Claim (0.8.0, #66): the default 'page' integrity mode reads files written before
0.8.0 (no page CRCs) without error, and files appended via append_files() that were
written by other tools.
"""
import os
import tempfile

import _harness as H

H.local_env()
H.quiet_logs()
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

from datashard import DataFile, FileFormat, create_table  # noqa: E402

tmp = tempfile.mkdtemp(prefix="audit_crc_")
path = os.path.join(tmp, "t")
schema = H.simple_schema()
t = create_table(path, schema)
arrow_schema = t.file_manager.data_file_manager.create_arrow_schema(schema)
fp = os.path.join(path, "data", "legacy.parquet")
pq.write_table(pa.table({"id": [1, 2], "name": ["a", "b"], "value": [1.0, 2.0]}, schema=arrow_schema), fp,
               write_page_checksum=False)
t.append_data([DataFile(file_path="data/legacy.parquet", file_format=FileFormat.PARQUET, partition_values={},
                        record_count=2, file_size_in_bytes=os.path.getsize(fp))])
t.append_records([{"id": 3, "name": "c", "value": 3.0}], schema)
try:
    rows = sorted(r["id"] for r in t.scan())
    batches = sum(len(b) for b in t.scan_batches(batch_size=1))
    H.report("page-mode-reads-files-without-page-crcs", rows == [1, 2, 3] and batches == 3, f"rows={rows} batches_rows={batches}")
except Exception as e:  # noqa: BLE001
    H.report("page-mode-reads-files-without-page-crcs", False, f"{type(e).__name__}: {str(e)[:120]}")
H.finish()
