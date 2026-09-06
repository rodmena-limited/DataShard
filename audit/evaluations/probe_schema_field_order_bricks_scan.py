"""Claim (#49 fix): an append whose schema diverges from the table's is rejected so a
scan can never fail on concat.

Suspect: _validate_schema_against_table compares an unordered SET of
(name, type, required); pa.concat_tables requires identical field ORDER. A writer in a
fresh process (empty arrow-schema cache) passing the same fields in a different order
is accepted and writes parquet in that order -> every full scan then raises.
"""
import os
import subprocess
import sys
import tempfile

import _harness as H

H.local_env()
tmp = tempfile.mkdtemp(prefix="audit_order_")
path = os.path.join(tmp, "t")
env = dict(os.environ, PYTHONPATH=H.SRC)


def run(code):
    r = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True, timeout=180)
    return r.returncode, (r.stdout + r.stderr).strip()


A = '[{"id":1,"name":"id","type":"long","required":True},{"id":2,"name":"name","type":"string","required":False}]'
B = '[{"id":2,"name":"name","type":"string","required":False},{"id":1,"name":"id","type":"long","required":True}]'
rc1, out1 = run(
    f"from datashard import create_table, Schema\n"
    f"s = Schema(schema_id=1, fields={A})\n"
    f"t = create_table({path!r}, s); t.append_records([{{'id':1,'name':'a'}}], s); print('rows', t.row_count())"
)
rc2, out2 = run(
    f"from datashard import load_table, Schema\n"
    f"s = Schema(schema_id=1, fields={B})\n"
    f"t = load_table({path!r}); t.append_records([{{'id':2,'name':'b'}}], s); print('rows', t.row_count())"
)
rc3, out3 = run(f"from datashard import load_table\nt = load_table({path!r}); r = t.scan(); print('SCAN_OK', len(r))")
if rc2 != 0:
    H.report("reordered-schema-append-rejected-or-scan-works", True, f"append rejected: {out2[-200:]}")
else:
    H.report(
        "reordered-schema-append-rejected-or-scan-works",
        rc3 == 0 and "SCAN_OK 2" in out3,
        f"append with reordered fields ACCEPTED (rc={rc2}); scan rc={rc3}: {out3[-260:]}",
    )
H.finish()
