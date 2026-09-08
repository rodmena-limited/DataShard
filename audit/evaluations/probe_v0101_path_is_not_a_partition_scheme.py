"""Claim (#93): what a scan returns depends on the table's data and metadata ONLY -
never on the directory the table happens to live in.

pyarrow's parquet reader defaults to partitioning="hive": it parses `key=value`
segments out of the path above a file and folds them into the result. datashard reads
local files by path, so with inference left on a table stored under
`<root>/symbol=ZEN-USD/day=2026-09-08/` either failed every scan (when the key matched
a column) or silently grew a column (when it did not). Reported by crypto-trader
2026-09-08 against 0.10.0; the silent half was never reported because it does not
raise.

Both claims here are paired with a CONTROL that shows raw pyarrow doing the wrong
thing on the very same file, so a passing datashard read is evidence rather than a
coincidence of layout.
"""
import glob
import os
import tempfile

import _harness as H
import pyarrow as pa
import pyarrow.parquet as pq

H.local_env()
H.quiet_logs()
from datashard import Schema, create_table, load_table  # noqa: E402

SCHEMA = Schema(schema_id=1, fields=[
    {"id": 1, "name": "symbol", "type": "string", "required": True},
    {"id": 2, "name": "n", "type": "long", "required": True},
])
# The directory value and the column value differ ON PURPOSE: in the reporting user's
# tables the directory holds the exchange pair and the column the venue's pair.
PATH_VALUE, ROW_VALUE = "ZEN-USD", "ZEN-USDT"
tmp = tempfile.mkdtemp(prefix="audit_hivepath_")


def build(*segments, files=2):
    path = os.path.join(tmp, *segments)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    t = create_table(path, schema=SCHEMA)
    for i in range(1, files + 1):
        t.append_records(records=[{"symbol": ROW_VALUE, "n": i}], schema=SCHEMA)
    return path


def a_data_file(path):
    return sorted(glob.glob(os.path.join(path, "data", "*.parquet")))[0]


# 1. a path segment whose key matches a column must not replace the column's value
path = build("collide", f"symbol={PATH_VALUE}", "day=2026-09-08")
try:
    pq.read_table(a_data_file(path))
    control = "raw pyarrow read it cleanly - THIS FIXTURE NO LONGER TRIGGERS INFERENCE"
    control_ok = False
except pa.ArrowTypeError as e:
    control, control_ok = f"raw pyarrow: {str(e).splitlines()[0][:80]}", True
t = load_table(path)
try:
    values = {r["symbol"] for r in t.scan()}
    apis = {
        "scan": values,
        "to_arrow": set(t.to_arrow().column("symbol").to_pylist()),
        "scan_batches": {r["symbol"] for b in t.scan_batches() for r in b},
        "iter_records": {r["symbol"] for r in t.iter_records()},
        "to_pandas": set(t.to_pandas()["symbol"].tolist()),
    }
    ok = control_ok and all(v == {ROW_VALUE} for v in apis.values())
    detail = f"every read API returned {ROW_VALUE!r} (the column), not {PATH_VALUE!r} (the directory); control: {control}"
except Exception as e:  # noqa: BLE001
    ok, detail = False, f"{type(e).__name__}: {str(e)[:140]}"
H.report("a-colliding-path-segment-never-replaces-a-column-value", ok, detail)

# 2. a path segment whose key matches nothing must not add a column
path = build("inject", "venue=binance")
injected = pq.read_table(a_data_file(path)).schema.names
t = load_table(path)
cols = t.to_arrow().schema.names
H.report(
    "a-non-colliding-path-segment-does-not-inject-a-column",
    cols == ["symbol", "n"] and "venue" in injected,
    f"datashard returned {cols}; control: raw pyarrow returned {injected} for the same file "
    f"(the injected 'venue' is what a scan used to hand callers)",
)

# 3. the property behind both: location does not change the answer
flat = load_table(build("flat")).scan()
hive = load_table(build("h", f"symbol={PATH_VALUE}", "day=2026-09-08")).scan()
deep = load_table(build("d", "venue=binance", "year=2026", "n=99")).scan()
H.report(
    "the-same-table-reads-identically-wherever-it-is-stored",
    flat == hive == deep,
    f"three identical tables at three layouts returned {'the same' if flat == hive == deep else 'DIFFERENT'} rows: "
    f"flat={flat}, hive={hive}, deep={deep}",
)
H.finish()
