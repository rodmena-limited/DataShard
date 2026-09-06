"""Claim (0.8.0, #68): garbage_collect() reclaims superseded metadata files but keeps the
current file and every file the metadata log references, so the version chain stays
auditable.

Suspect: GC reads the metadata (version N) first and the hint again later; if a commit
lands in between, the hint says N+1 while GC still holds N's log, which does not list
vN itself - so vN, referenced by N+1's log, can be reclaimed.
"""
import glob
import os
import tempfile

import _harness as H

H.local_env()
H.quiet_logs()
from datashard import create_table, load_table  # noqa: E402
from datashard.garbage_collector import GarbageCollector  # noqa: E402

tmp = tempfile.mkdtemp(prefix="audit_chain_")
path = os.path.join(tmp, "t")
schema = H.simple_schema()
t = create_table(path, schema)
for i in range(15):
    t.append_records([{"id": i, "name": "x", "value": 1.0}], schema)
current_before = t.metadata_manager._current_version_info()[1]
meta = os.path.join(path, "metadata")
for f in glob.glob(os.path.join(meta, "*.json")):
    os.utime(f, (1, 1))  # everything is old: only the keep-rules protect files now

gc = GarbageCollector(path, t.metadata_manager, t.file_manager)
orig, state = gc.file_manager.read_manifest_file, {"done": False}


def commit_during_gc(*a, **k):
    if not state["done"]:
        state["done"] = True
        load_table(path).append_records([{"id": 99, "name": "late", "value": 1.0}], schema)
    return orig(*a, **k)


gc.file_manager.read_manifest_file = commit_during_gc
stats = gc.collect(grace_period_ms=0, allow_short_grace=True)
after = load_table(path)
current_after = after.metadata_manager._current_version_info()[1]
logged = [os.path.basename(e["metadata-file"]) for e in after.metadata_manager.refresh().metadata_log]
missing = [f for f in logged if not os.path.exists(os.path.join(meta, f))]
H.report(
    "gc-keeps-every-metadata-file-the-current-log-references",
    not missing and after.row_count() == 16,
    f"current {current_before} -> {current_after}; metadata_log entries missing on disk: {missing or 'none'}; "
    f"gc_stats={stats}; rows={after.row_count()}",
)
H.finish()
