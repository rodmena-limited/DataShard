"""Claims (README): 'Writes: O(1) for append', 'Scalable: handles millions of log
entries efficiently', 'Metadata: efficient'.

Suspects: (1) every commit rewrites the full metadata JSON with ALL snapshots and the
full snapshot_log, and nothing expires by default -> per-commit metadata read/write
cost grows linearly with commit count; (2) old v*.metadata.json files are never
removed (GC covers only data/ and manifests/), so metadata storage grows
quadratically; (3) one manifest per commit, never compacted, so a scan issues
O(commits) storage calls even for a tiny table.  N tunable via AUDIT_COMMITS.
"""
import glob
import os
import tempfile
import time

import _harness as H

H.local_env()
H.quiet_logs()
from datashard import create_table  # noqa: E402

N = int(os.environ.get("AUDIT_COMMITS", "300"))
tmp = tempfile.mkdtemp(prefix="audit_growth_")
path = os.path.join(tmp, "t")
schema = H.simple_schema()
t = create_table(path, schema)
checkpoints = {}
lat = []
for i in range(1, N + 1):
    t0 = time.perf_counter()
    t.append_records([{"id": i, "name": "x", "value": 1.0}], schema)
    lat.append(time.perf_counter() - t0)
    if i in (N // 6, N // 2, N):
        cur = t.metadata_manager._current_version_info()[1]
        cur_size = os.path.getsize(os.path.join(path, "metadata", cur))
        meta_files = glob.glob(os.path.join(path, "metadata", "v*.metadata.json"))
        meta_bytes = sum(os.path.getsize(f) for f in meta_files)
        manifests = glob.glob(os.path.join(path, "metadata", "manifests", "manifest_*.avro"))
        calls = {"n": 0}
        oe, oo = t.storage.exists, t.storage.open_file

        def cx(p, _calls=calls, _oe=oe):
            _calls["n"] += 1
            return _oe(p)

        def co(p, _calls=calls, _oo=oo):
            _calls["n"] += 1
            return _oo(p)

        t.storage.exists, t.storage.open_file = cx, co
        t0 = time.perf_counter()
        t.scan()
        dt_scan = time.perf_counter() - t0
        t.storage.exists, t.storage.open_file = oe, oo
        checkpoints[i] = {
            "cur_kib": cur_size / 1024, "files": len(meta_files), "meta_mib": meta_bytes / 2**20,
            "manifests": len(manifests), "scan_ms": dt_scan * 1000, "scan_calls": calls["n"],
            "append_ms": sum(lat[-10:]) / 10 * 1000,
        }
for i, c in checkpoints.items():
    print(f"  after {i:5d} commits: current metadata {c['cur_kib']:7.1f} KiB | {c['files']:5d} metadata files "
          f"= {c['meta_mib']:6.2f} MiB | {c['manifests']:5d} manifests | scan {c['scan_ms']:7.1f} ms "
          f"({c['scan_calls']} storage calls) | append {c['append_ms']:.1f} ms")
a, b = checkpoints[N // 6], checkpoints[N]
growth = b["cur_kib"] / a["cur_kib"]
H.report(
    "per-commit-metadata-size-is-bounded-by-default",
    growth < 2.0,
    f"current metadata grew {growth:.1f}x between commit {N // 6} and {N} (linear in commits; "
    f"projected {b['cur_kib'] / N * 100_000 / 1024:.0f} MiB at 100k commits, re-read 4+ times per commit)",
)
stats = t.garbage_collect(grace_period_ms=0, allow_short_grace=True)
left = len(glob.glob(os.path.join(path, "metadata", "v*.metadata.json")))
H.report(
    "superseded-metadata-files-are-reclaimed",
    left <= 2,
    f"{left} v*.metadata.json files remain after garbage_collect() ({b['meta_mib']:.2f} MiB for {N} commits; "
    f"quadratic: ~{(b['meta_mib'] / N**2) * 100_000**2 / 1024:.0f} GiB at 100k commits); gc_stats={stats}",
)
H.report(
    "scan-io-does-not-grow-linearly-with-commit-count",
    b["scan_calls"] < 3 * a["scan_calls"],
    f"scan storage calls {a['scan_calls']} -> {b['scan_calls']} for {a['manifests']} -> {b['manifests']} manifests "
    f"(no manifest compaction; ~{b['scan_calls'] / N:.1f} calls per commit ever made)",
)
H.finish()
