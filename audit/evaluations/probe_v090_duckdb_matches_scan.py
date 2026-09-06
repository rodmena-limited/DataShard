"""Claim (0.9.0, #78/#79): DuckDB sees exactly what scan() sees, for the current and a
historical snapshot, and an Arrow table appended through append_arrow() reads back
identically; the DuckDB path costs bounded overhead over to_arrow().
"""
import os
import tempfile
import time
from decimal import Decimal

import _harness as H

H.local_env()
H.quiet_logs()
try:
    import duckdb  # noqa: F401
except ImportError:
    H.skip("duckdb-matches-scan", "duckdb not installed (pip install datashard[duckdb])")
    H.finish()
import pyarrow as pa  # noqa: E402

from datashard import Schema, create_table  # noqa: E402

schema = Schema(schema_id=1, fields=[
    {"id": 1, "name": "id", "type": "long", "required": True},
    {"id": 2, "name": "sym", "type": "string", "required": True},
    {"id": 3, "name": "px", "type": "decimal(18,8)", "required": True},
])
t = create_table(os.path.join(tempfile.mkdtemp(prefix="audit_duck_"), "t"), schema)
FILES, ROWS = 6, 2000
for f in range(FILES):
    t.append_arrow(pa.table({
        "id": pa.array(range(f * ROWS, (f + 1) * ROWS), pa.int64()),
        "sym": pa.array([f"S{i % 7}" for i in range(ROWS)]),
        "px": pa.array([Decimal(i) / 100 for i in range(ROWS)], pa.decimal128(18, 8)),
    }))
    if f == 1:
        early = t.current_snapshot().snapshot_id
t0 = time.perf_counter()
arrow = t.to_arrow()
d_arrow = time.perf_counter() - t0
t0 = time.perf_counter()
n_sql = t.sql("SELECT count(*) AS n, sum(px) AS s FROM t").to_pylist()[0]
d_sql = time.perf_counter() - t0
expected_sum = sum((Decimal(i) / 100 for i in range(ROWS)), Decimal(0)) * FILES  # px repeats per file
H.report("duckdb-sql-matches-arrow-and-exact-decimals",
         n_sql["n"] == FILES * ROWS == arrow.num_rows and Decimal(str(n_sql["s"])) == expected_sum,
         f"count={n_sql['n']} sum={n_sql['s']} (expected {expected_sum}); to_arrow {d_arrow * 1000:.0f} ms, sql {d_sql * 1000:.0f} ms")
early_rows = t.sql("SELECT count(*) AS n FROM t", snapshot_id=early).to_pylist()[0]["n"]
H.report("duckdb-time-travel-via-snapshot_id", early_rows == 2 * ROWS, f"rows at early snapshot={early_rows} (expected {2 * ROWS})")
by_sym = t.sql("SELECT sym, count(*) AS n FROM t GROUP BY sym ORDER BY sym").to_pylist()
scan_counts = {}
for r in t.scan(columns=["sym"]):
    scan_counts[r["sym"]] = scan_counts.get(r["sym"], 0) + 1
H.report("duckdb-group-by-matches-scan", {r["sym"]: r["n"] for r in by_sym} == scan_counts, f"{len(by_sym)} groups compared")
H.report("duckdb-overhead-bounded", d_sql < max(2 * d_arrow, 0.5), f"sql/to_arrow = {d_sql / d_arrow:.2f}x")
H.finish()
