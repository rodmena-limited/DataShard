"""Claim (CHANGELOG 0.8.0): tables written by 0.7.x read unchanged, and 0.8.0 can append
to and garbage-collect them. Also checks the downgrade direction (0.7.2 reading a table
0.8.0 has written to), since a rollback must stay possible.

Installs the released datashard==0.7.2 from PyPI into a scratch venv (network needed).
"""
import os
import shutil
import subprocess
import tempfile

import _harness as H

H.local_env()
H.quiet_logs()
scratch = os.environ.get("AUDIT_SCRATCH", tempfile.gettempdir())
venv = os.path.join(scratch, "datashard-0.7.2-venv")
uv = shutil.which("uv") or os.path.expanduser("~/.local/bin/uv")
if not os.path.exists(os.path.join(venv, "bin", "python")):
    subprocess.run([uv, "venv", "-q", venv, "--python", "3.13"], check=True)
    r = subprocess.run([uv, "pip", "install", "-q", "--python", os.path.join(venv, "bin", "python"), "datashard==0.7.2", "pandas"],
                       capture_output=True, text=True)
    if r.returncode != 0:
        H.skip("compat-0.7.2", f"could not install datashard==0.7.2: {r.stderr[-200:]}")
        H.finish()
old_py = os.path.join(venv, "bin", "python")
tmp = tempfile.mkdtemp(prefix="audit_compat_")
path = os.path.join(tmp, "legacy")
SCHEMA = ('Schema(schema_id=1, fields=[{"id":1,"name":"id","type":"long","required":True},'
          '{"id":2,"name":"name","type":"string","required":False},{"id":3,"name":"value","type":"double","required":False}])')


def run_old(code):
    r = subprocess.run([old_py, "-c", "from datashard import *\nimport datashard\ns=" + SCHEMA + "\n" + code],
                       env={**os.environ, "DATASHARD_STORAGE_TYPE": "local", "PYTHONPATH": ""}, capture_output=True, text=True)
    return r.returncode, (r.stdout.strip().splitlines() or [""])[-1], r.stderr.strip()[-300:]


rc, out, err = run_old(f"""
print(datashard.__version__)
t = create_table({path!r}, s)
for i in range(3): t.append_records([{{"id": i, "name": "old", "value": float(i)}}], s)
victim = t._get_all_data_files()[0].file_path
with t.new_transaction() as tx:
    tx.delete_files([victim]); tx.commit()
print("OLD_ROWS", t.row_count(), len(t.snapshots()))""")
H.report("0.7.2-fixture-table-created", rc == 0 and out.startswith("OLD_ROWS 2 4"), f"rc={rc} {out} {err[-120:] if rc else ''}")

from datashard import load_table  # noqa: E402  (0.8.0 from src)

try:
    t = load_table(path)
    rows = sorted(r["id"] for r in t.scan())
    ok_read = rows == [1, 2] and t.row_count() == 2
    t.append_records([{"id": 3, "name": "new", "value": 3.0}], H.simple_schema())
    stats = t.garbage_collect(grace_period_ms=0, allow_short_grace=True)
    rows2 = sorted(r["id"] for r in load_table(path).scan())
    H.report("0.8.0-reads-appends-and-gcs-a-0.7.2-table", ok_read and rows2 == [1, 2, 3],
             f"read rows={rows}; after append+GC rows={rows2}; gc_stats={stats} (the delete's 0.7.2 orphan is the only reclaimable data file)")
except Exception as e:  # noqa: BLE001
    H.report("0.8.0-reads-appends-and-gcs-a-0.7.2-table", False, f"{type(e).__name__}: {str(e)[:160]}")

rc, out, err = run_old(f"""
t = load_table({path!r})
print("DOWNGRADE", sorted(r["id"] for r in t.scan()), t.row_count())""")
H.report("0.7.2-can-still-read-a-table-0.8.0-wrote-to", rc == 0 and out == "DOWNGRADE [1, 2, 3] 3", f"rc={rc} {out} {err[-160:] if rc else ''}")
H.finish()
