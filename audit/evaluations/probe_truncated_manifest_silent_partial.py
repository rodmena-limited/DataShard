"""Claim: a corrupt/truncated manifest is detected (fail closed), never read as a
shorter list.

Suspect: read_manifest_file streams Avro blocks and stops cleanly at EOF, and never
compares what it read with the manifest_length recorded in the manifest list. A
manifest truncated at a block boundary yields a silently shorter file list -> partial
scans, wrong row_count, and GC deleting the files that fell off the list.
"""
import io
import os
import tempfile
import time

import _harness as H
import fastavro

H.local_env()
H.quiet_logs()
from datashard import DataFile, FileFormat, create_table  # noqa: E402
from datashard.data_structures import ManifestContent  # noqa: E402

tmp = tempfile.mkdtemp(prefix="audit_trunc_")
path = os.path.join(tmp, "t")
schema = H.simple_schema()
t = create_table(path, schema)
fm = t.file_manager
N = 3000
files = [
    DataFile(
        file_path=f"/data/f{i:05d}.parquet",
        file_format=FileFormat.PARQUET,
        partition_values={},
        record_count=10,
        file_size_in_bytes=100,
        checksum="x" * 64,
    )
    for i in range(N)
]
mf = fm.create_manifest_file(files, ManifestContent.DATA, snapshot_id=123, sequence_number=1)
mpath = os.path.join(path, mf.manifest_path)
raw = open(mpath, "rb").read()
sync = fastavro.reader(io.BytesIO(raw))._header["sync"]
positions, start = [], 0
while True:
    i = raw.find(sync, start)
    if i < 0:
        break
    positions.append(i)
    start = i + len(sync)
n_blocks = len(positions) - 1
if n_blocks < 2:
    H.skip("truncated-manifest-detected-on-read", f"only {n_blocks} block(s)")
    H.finish()
# Commit a snapshot over the intact manifest (list + summary carry its integrity data)...
ml = fm.create_manifest_list_file([mf], snapshot_id=123)
listed_mf = fm.read_manifest_list_file(ml)[0]
summary = {}
try:
    from datashard.file_manager import (  # 0.8.0+
        SUMMARY_LIST_LENGTH,
        SUMMARY_LIST_SHA256,
    )
    from datashard.integrity import IntegrityChecker
    raw_list = open(os.path.join(path, ml), "rb").read()
    summary = {SUMMARY_LIST_LENGTH: str(len(raw_list)), SUMMARY_LIST_SHA256: IntegrityChecker.compute_checksum(raw_list)}
except ImportError:
    pass
t.snapshot_manager.create_snapshot(manifest_list_path=ml, operation="append", parent_snapshot_id=-1, snapshot_id=123, summary=summary)
assert t.row_count() == N * 10
# ... then truncate the manifest at a block boundary, as disk/object corruption would.
cut = positions[1] + len(sync)  # header + first complete block, ending on its sync marker
open(mpath, "wb").write(raw[:cut])
try:
    got = fm.read_manifest_file(
        mf.manifest_path, expected_length=listed_mf.manifest_length, expected_checksum=getattr(listed_mf, "checksum", None)
    )
    n, err = len(got), None
except Exception as e:  # noqa: BLE001
    n, err = None, f"{type(e).__name__}: {str(e)[:100]}"
H.report(
    "truncated-manifest-detected-on-read",
    n is None,
    f"manifest_length recorded={mf.manifest_length}B, file truncated to {cut}B at block "
    f"boundary 1/{n_blocks}; read returned {n} of {N} entries, error={err}",
)
try:
    rc = t.row_count()
    rc_err = None
except Exception as e:  # noqa: BLE001
    rc, rc_err = None, f"{type(e).__name__}"
H.report(
    "row_count-refuses-a-partial-manifest",
    rc is None,
    f"row_count() -> {rc if rc is not None else rc_err} (expected an error, not a partial count; full table = {N * 10})",
)
os.makedirs(os.path.join(path, "data"), exist_ok=True)
old = time.time() - 7200
for f in files:
    fp = os.path.join(path, f.file_path.lstrip("/"))
    open(fp, "wb").close()
    os.utime(fp, (old, old))
try:
    stats = t.garbage_collect(grace_period_ms=3600000)
    deleted, gc_err = stats["data_files"], None
except Exception as e:  # noqa: BLE001
    deleted, gc_err = 0, type(e).__name__
H.report(
    "gc-does-not-delete-files-listed-in-a-truncated-manifest",
    deleted == 0,
    f"GC deleted {deleted} of {N} live data files (outcome: {gc_err or 'ran'}); a corrupt manifest must abort GC",
)
H.finish()
