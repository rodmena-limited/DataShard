"""Claims: metadata growth is mitigated by (a) the opt-in table property
'datashard.snapshot.retention-count' (snapshot_manager.py) and (b)
Transaction.expire_snapshots(). Checks whether (a) is reachable through ANY public API
(there is no set_property / alter table), and whether (b) works end to end including
GC of the manifest lists the expired snapshots owned.
"""
import glob
import os
import tempfile
import time

import _harness as H

H.local_env()
H.quiet_logs()
from datashard import Table, create_table, load_table  # noqa: E402

tmp = tempfile.mkdtemp(prefix="audit_ret_")
path = os.path.join(tmp, "t")
schema = H.simple_schema()
t = create_table(path, schema)
public = [n for n in dir(Table) if not n.startswith("_")]
setters = [n for n in public if "propert" in n.lower() or "alter" in n.lower() or "retention" in n.lower()]
H.report(
    "snapshot-retention-property-settable-via-public-api",
    bool(setters),
    f"public Table methods mentioning property/alter/retention: {setters or 'none'} "
    f"(the only lever against unbounded snapshot growth is a table property no API can set)",
)
for i in range(20):
    t.append_records([{"id": i, "name": "x", "value": 1.0}], schema)
n_before = len(t.snapshots())
lists_before = len(glob.glob(os.path.join(path, "metadata", "snap-*.avro")))
time.sleep(0.05)
with t.new_transaction() as tx:
    tx.expire_snapshots(older_than_ms=int(time.time() * 1000))
    tx.commit()
n_after = len(t.snapshots())
H.report("expire_snapshots-removes-old-snapshots-keeps-current", n_after == 1 and n_before == 20, f"snapshots {n_before}->{n_after}")
time.sleep(0.05)
stats = t.garbage_collect(grace_period_ms=0, allow_short_grace=True)
lists_after = len(glob.glob(os.path.join(path, "metadata", "snap-*.avro")))
rows = load_table(path).row_count()
H.report(
    "gc-after-expiry-reclaims-unreachable-manifest-lists-and-keeps-data",
    lists_after == 1 and rows == 20,
    f"manifest lists {lists_before}->{lists_after}; rows still readable={rows}; gc_stats={stats}",
)
H.finish()
