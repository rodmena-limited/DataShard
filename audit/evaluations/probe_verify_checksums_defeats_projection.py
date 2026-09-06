"""Claims (README): 'Column projection (only read specified columns)', 'Predicate
pushdown ... 90%+ I/O reduction', 'Streaming API: memory-efficient iteration'.

Suspect: verify_checksums defaults to ON, and the verify path reads EVERY byte of every
data file through storage.read_file() before parsing, so projection, pushdown and
streaming all degrade to whole-file reads.  Positive control: corruption IS detected.
"""
import os
import random
import tempfile
import time

import _harness as H

H.local_env()
H.quiet_logs()
from datashard import Schema, create_table  # noqa: E402
from datashard.integrity import CorruptDataError  # noqa: E402

tmp = tempfile.mkdtemp(prefix="audit_verify_")
path = os.path.join(tmp, "t")
fields = [{"id": 1, "name": "id", "type": "long", "required": True}]
for k in range(2, 12):
    fields.append({"id": k, "name": f"c{k}", "type": "double" if k % 2 else "string", "required": False})
schema = Schema(schema_id=1, fields=fields)
t = create_table(path, schema)
FILES, ROWS = 6, 60_000
rng = random.Random(1)
for f in range(FILES):
    recs = []
    for i in range(ROWS):
        r = {"id": f * ROWS + i}
        for k in range(2, 12):
            r[f"c{k}"] = rng.random() * 1000 if k % 2 else f"str-{rng.randrange(100000)}"
        recs.append(r)
    t.append_records(recs, schema)
total_bytes = sum(os.path.getsize(os.path.join(path, d.file_path.lstrip("/"))) for d in t._get_all_data_files())

read_calls = {"n": 0, "bytes": 0}
orig_read = t.storage.read_file


def counting_read(p):
    b = orig_read(p)
    read_calls["n"] += 1
    read_calls["bytes"] += len(b)
    return b


t.storage.read_file = counting_read


def timed(fn):
    read_calls["n"] = read_calls["bytes"] = 0
    t0 = time.perf_counter()
    out = fn()
    return time.perf_counter() - t0, dict(read_calls), out


dt_on, rc_on, _ = timed(lambda: t.scan(columns=["id"]))                       # default verify
dt_off, rc_off, _ = timed(lambda: t.scan(columns=["id"], verify_checksums=False))
H.report(
    "column-projection-reads-only-projected-columns-with-default-settings",
    rc_on["bytes"] == 0,
    f"scan(columns=['id']) default: {dt_on * 1000:.0f} ms, whole-file reads={rc_on['n']} "
    f"({rc_on['bytes'] / 1e6:.1f} MB of {total_bytes / 1e6:.1f} MB table); verify_checksums=False: "
    f"{dt_off * 1000:.0f} ms, whole-file reads={rc_off['n']} -> default is {dt_on / dt_off:.1f}x slower",
)
dt_f_on, rc_f_on, rows = timed(lambda: t.scan(filter={"id": ("==", 12345)}))
dt_f_off, rc_f_off, _ = timed(lambda: t.scan(filter={"id": ("==", 12345)}, verify_checksums=False))
H.report(
    "predicate-pushdown-does-not-read-whole-file-with-default-settings",
    rc_f_on["bytes"] == 0 and len(rows) == 1,
    f"point lookup (1 file after pruning): default {dt_f_on * 1000:.0f} ms reading {rc_f_on['bytes'] / 1e6:.1f} MB whole-file; "
    f"verify off {dt_f_off * 1000:.0f} ms ({dt_f_on / dt_f_off:.1f}x)",
)
read_calls["n"] = read_calls["bytes"] = 0
first = next(iter(t.scan_batches(batch_size=1000, columns=["id"])))
H.report(
    "scan_batches-streams-instead-of-materialising-whole-files",
    read_calls["bytes"] == 0 and len(first) == 1000,
    f"first 1000-row batch forced {read_calls['n']} whole-file read(s) of {read_calls['bytes'] / 1e6:.1f} MB into memory",
)
# Positive control: the guard the cost buys.
victim = os.path.join(path, t._get_all_data_files()[0].file_path.lstrip("/"))
with open(victim, "r+b") as fh:
    fh.seek(os.path.getsize(victim) // 2)
    b = fh.read(1)
    fh.seek(-1, 1)
    fh.write(bytes([b[0] ^ 0xFF]))
try:
    t.scan(columns=["id"])
    H.report("checksum-verification-detects-corruption (control)", False, "corrupted file scanned without error")
except CorruptDataError as e:
    H.report("checksum-verification-detects-corruption (control)", True, str(e)[:80])
H.finish()
