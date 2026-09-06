"""Claims (README): 'Writes: O(1) for append', 'Scalable: handles millions of log
entries efficiently', 'Metadata: efficient'.

0.7.2 behaviour: every commit rewrote the full metadata JSON with ALL snapshots and
nothing ever expired; superseded v*.metadata.json files were never removed (quadratic
storage); one manifest per commit, never compacted, so scan I/O grew with the number of
commits ever made (905 storage calls at 300 tiny commits).

0.8.0 contract checked here: manifests compact automatically (property
datashard.manifest.compaction-threshold, default 64) so scan I/O stays bounded; the
documented maintenance - Table.expire_snapshots(retain_last=N) + garbage_collect() -
shrinks the current metadata and reclaims superseded metadata files. N tunable via
AUDIT_COMMITS.
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


def snapshot_state():
    cur = t.metadata_manager._current_version_info()[1]
    meta_files = glob.glob(os.path.join(path, "metadata", "v*.metadata.json"))
    calls = {"n": 0}
    oe, oo, orf = t.storage.exists, t.storage.open_file, t.storage.read_file

    def cx(p, _c=calls, _f=oe):
        _c["n"] += 1
        return _f(p)

    def co(p, _c=calls, _f=oo):
        _c["n"] += 1
        return _f(p)

    def cr(p, _c=calls, _f=orf):
        _c["n"] += 1
        return _f(p)

    t.storage.exists, t.storage.open_file, t.storage.read_file = cx, co, cr
    t0 = time.perf_counter()
    t.scan()
    dt_scan = time.perf_counter() - t0
    t.storage.exists, t.storage.open_file, t.storage.read_file = oe, oo, orf
    return {
        "cur_kib": os.path.getsize(os.path.join(path, "metadata", cur)) / 1024,
        "files": len(meta_files),
        "meta_mib": sum(os.path.getsize(f) for f in meta_files) / 2**20,
        "manifests": len(t.file_manager.read_manifest_list_file(t.current_snapshot().manifest_list.lstrip("/"))),
        "scan_ms": dt_scan * 1000,
        "scan_calls": calls["n"],
    }


checkpoints = {}
lat = []
for i in range(1, N + 1):
    t0 = time.perf_counter()
    t.append_records([{"id": i, "name": "x", "value": 1.0}], schema)
    lat.append(time.perf_counter() - t0)
    if i in (N // 6, N // 2, N):
        checkpoints[i] = dict(snapshot_state(), append_ms=sum(lat[-10:]) / 10 * 1000)
for i, c in checkpoints.items():
    print(f"  after {i:5d} commits: current metadata {c['cur_kib']:7.1f} KiB | {c['files']:5d} metadata files "
          f"= {c['meta_mib']:6.2f} MiB | {c['manifests']:5d} manifests | scan {c['scan_ms']:7.1f} ms "
          f"({c['scan_calls']} storage calls) | append {c['append_ms']:.1f} ms")
a, b = checkpoints[N // 6], checkpoints[N]
H.report(
    "scan-io-bounded-by-manifest-compaction",
    b["scan_calls"] < 100 and b["manifests"] <= 65,
    f"scan storage calls {a['scan_calls']} -> {b['scan_calls']} at {N} commits; active manifests {a['manifests']} -> {b['manifests']} "
    f"(0.7.2: {N} manifests, ~3 calls per commit ever made)",
)
# Documented maintenance: keep the last 20 snapshots, reclaim what nothing references.
expired = t.expire_snapshots(retain_last=20)
stats = t.garbage_collect(grace_period_ms=0, allow_short_grace=True)
after = snapshot_state()
print(f"  after maintenance: expired {expired} snapshots; gc={stats}; current metadata {after['cur_kib']:.1f} KiB; "
      f"{after['files']} metadata files = {after['meta_mib']:.2f} MiB; scan {after['scan_calls']} calls")
H.report(
    "current-metadata-shrinks-once-retention-applies",
    after["cur_kib"] < a["cur_kib"],
    f"current metadata {b['cur_kib']:.1f} KiB before maintenance -> {after['cur_kib']:.1f} KiB after "
    f"(smaller than at commit {N // 6}: {a['cur_kib']:.1f} KiB)",
)
H.report(
    "superseded-metadata-files-are-reclaimed",
    after["files"] <= 11 and after["meta_mib"] < 0.1 * b["meta_mib"],
    f"{b['files']} files / {b['meta_mib']:.2f} MiB before GC -> {after['files']} files / {after['meta_mib']:.2f} MiB "
    f"(current + write.metadata.previous-versions-max [10] retained)",
)
H.finish()
