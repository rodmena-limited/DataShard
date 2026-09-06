"""Claim (README): 'pandas Integration: Native DataFrame support'.

Suspect: append_pandas() does df.to_dict('records') and re-validates every row in
Python, instead of pa.Table.from_pandas -> orders of magnitude slower than the native
Arrow path for the frame sizes a trading desk writes.
"""
import os
import tempfile
import time

import _harness as H

H.local_env()
H.quiet_logs()
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

from datashard import create_table  # noqa: E402

tmp = tempfile.mkdtemp(prefix="audit_pd_")
path = os.path.join(tmp, "t")
schema = H.simple_schema()
t = create_table(path, schema)
N = 300_000
df = pd.DataFrame({"id": np.arange(N, dtype="int64"), "name": ["n"] * N, "value": np.random.rand(N)})
t0 = time.perf_counter()
t.append_pandas(df, schema)
dt_ds = time.perf_counter() - t0
arrow_schema = t.file_manager.data_file_manager.create_arrow_schema(schema)
ref = os.path.join(tmp, "ref.parquet")
t0 = time.perf_counter()
pq.write_table(pa.Table.from_pandas(df, schema=arrow_schema, preserve_index=False), ref, compression="lz4")
dt_ref = time.perf_counter() - t0
H.report(
    "append_pandas-is-within-5x-of-native-arrow-write",
    dt_ds < 5 * dt_ref,
    f"{N} rows: append_pandas {dt_ds:.2f} s vs from_pandas+write_table {dt_ref:.2f} s ({dt_ds / dt_ref:.0f}x)",
)
H.finish()
