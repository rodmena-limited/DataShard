"""Claim (0.8.0, #68): manifest compaction never changes the visible file set - not for
the current snapshot, not for time travel - across interleaved appends and deletes.
"""
import os
import tempfile

import _harness as H

H.local_env()
H.quiet_logs()
from datashard import create_table, load_table  # noqa: E402

tmp = tempfile.mkdtemp(prefix="audit_compact_")
path = os.path.join(tmp, "t")
schema = H.simple_schema()
t = create_table(path, schema)
t.set_properties({"datashard.manifest.compaction-threshold": "5"})
live = {}          # id -> data file path
history = []       # (snapshot_id, expected id set)
for i in range(40):
    t.append_records([{"id": i, "name": f"r{i}", "value": float(i)}], schema)
    (new,) = [d for d in t._get_all_data_files() if d.file_path not in live.values()]
    live[i] = new.file_path
    if i % 7 == 6:
        victim = i - 3
        with t.new_transaction() as tx:
            tx.delete_files([live.pop(victim)])
            tx.commit()
    history.append((t.current_snapshot().snapshot_id, set(live)))
got = sorted(r["id"] for r in t.scan())
active = len(t.file_manager.read_manifest_list_file(t.current_snapshot().manifest_list))
H.report("compaction-preserves-current-file-set", got == sorted(live) and t.row_count() == len(live),
         f"{len(got)} rows == {len(live)} expected; active manifests {active} (threshold 5)")
bad = []
for snap_id, expected in history[::5]:
    seen = {r["id"] for r in t.scan(snapshot_id=snap_id)}
    if seen != expected:
        bad.append((snap_id, sorted(expected - seen)[:3], sorted(seen - expected)[:3]))
H.report("time-travel-reads-unaffected-by-compaction", not bad, f"checked {len(history[::5])} historical snapshots; mismatches={bad or 'none'}")
stats = t.garbage_collect(grace_period_ms=0, allow_short_grace=True)
assert t.compact_manifests() is True
H.report("explicit-compaction-and-gc-keep-data", load_table(path).row_count() == len(live) and stats["data_files"] == 0,
         f"gc_stats={stats}; rows after compact_manifests={load_table(path).row_count()}")
H.finish()
