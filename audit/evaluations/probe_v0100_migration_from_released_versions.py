"""Claim (0.10 CHANGELOG, #86/#89): a table written by a RELEASED earlier datashard
is refused until it is migrated, `datashard migrate` converts it losslessly, and after
migration the old client can no longer write to it (there is no downgrade).

The counterparties are the real released packages (0.7.2 and 0.9.1) installed from PyPI
into scratch venvs - not a fixture this repo wrote, which would only prove our own
conventions. Network needed; skipped without it.
"""
import json
import os
import shutil
import subprocess
import tempfile

import _harness as H

H.local_env()
H.quiet_logs()
scratch = os.environ.get("AUDIT_SCRATCH", tempfile.gettempdir())
uv = shutil.which("uv") or os.path.expanduser("~/.local/bin/uv")

SCHEMA = ('Schema(schema_id=1, fields=[{"id":1,"name":"id","type":"long","required":True},'
          '{"id":2,"name":"name","type":"string","required":False},{"id":3,"name":"value","type":"double","required":False}])')


def old_python(version):
    """A venv with the released datashard==version, or None when it cannot be installed."""
    venv = os.path.join(scratch, f"datashard-{version}-venv")
    py = os.path.join(venv, "bin", "python")
    if not os.path.exists(py):
        subprocess.run([uv, "venv", "-q", venv, "--python", "3.13"], check=True)
        r = subprocess.run([uv, "pip", "install", "-q", "--python", py, f"datashard=={version}", "pandas"],
                           capture_output=True, text=True)
        if r.returncode != 0:
            return None
    return py


def run_old(py, code):
    r = subprocess.run([py, "-c", "from datashard import *\nimport datashard\ns=" + SCHEMA + "\n" + code],
                       env={**os.environ, "DATASHARD_STORAGE_TYPE": "local", "PYTHONPATH": ""},
                       capture_output=True, text=True)
    return r.returncode, (r.stdout.strip().splitlines() or [""])[-1], r.stderr.strip()[-300:]


def tree(root):
    return sorted(os.path.relpath(os.path.join(d, f), root) for d, _s, fs in os.walk(root) for f in fs)


from datashard import LegacyLayoutError, load_table  # noqa: E402
from datashard.migrate import migrate_table  # noqa: E402

for version, expect_rows in (("0.7.2", 2), ("0.9.1", 2)):
    py = old_python(version)
    if py is None:
        H.skip(f"migration-from-{version}", f"could not install datashard=={version} from PyPI")
        continue
    tmp = tempfile.mkdtemp(prefix=f"audit_mig_{version.replace('.', '')}_")
    path = os.path.join(tmp, "legacy")
    rc, out, err = run_old(py, f"""
t = create_table({path!r}, s)
for i in range(3): t.append_records([{{"id": i, "name": "old", "value": float(i)}}], s)
victim = t._get_all_data_files()[0].file_path
with t.new_transaction() as tx:
    tx.delete_files([victim]); tx.commit()
import json; print("OLD", json.dumps(sorted(r["id"] for r in t.scan()), separators=(",", ":")), t.row_count(), len(t.snapshots()))""")
    ok_fixture = rc == 0 and out.startswith("OLD ")
    H.report(f"{version}-fixture-table-created", ok_fixture, f"rc={rc} {out} {err[-120:] if rc else ''}")
    if not ok_fixture:
        continue
    old_rows = json.loads(out.split(None, 2)[1])

    # 1. the un-migrated table is refused, and refusing writes nothing
    before = tree(path)
    try:
        load_table(path)
        refused = "load_table returned a table"
    except LegacyLayoutError as e:
        refused = f"LegacyLayoutError mentioning migrate: {'datashard migrate' in str(e)}"
    except Exception as e:  # noqa: BLE001
        refused = f"{type(e).__name__} (expected LegacyLayoutError)"
    H.report(
        f"{version}-table-is-refused-until-migrated",
        refused.startswith("LegacyLayoutError mentioning migrate: True") and tree(path) == before,
        f"{refused}; files changed by the refusal: {sorted(set(tree(path)) ^ set(before)) or 'none'}",
    )

    # 2. migrate, then the rows must be exactly what the old client wrote
    report = migrate_table(path)
    t = load_table(path)
    rows_after = sorted(r["id"] for r in t.scan())
    H.report(
        f"{version}-migration-preserves-every-row-and-snapshot",
        rows_after == old_rows and len(t.snapshots()) == report["snapshots"] and t.row_count() == expect_rows,
        f"rows {old_rows} -> {rows_after}; snapshots={len(t.snapshots())}; migrate report={report}",
    )

    # 3. the migrated table takes appends and garbage collection
    try:
        t.append_records([{"id": 99, "name": "new", "value": 9.0}], H.simple_schema())
        stats = t.garbage_collect(grace_period_ms=0, allow_short_grace=True)
        rows2 = sorted(r["id"] for r in load_table(path).scan())
        ok = rows2 == sorted(old_rows + [99])
        detail = f"rows after append+GC={rows2}; gc_stats={stats}"
    except Exception as e:  # noqa: BLE001
        ok, detail = False, f"{type(e).__name__}: {str(e)[:160]}"
    H.report(f"{version}-migrated-table-accepts-appends-and-gc", ok, detail)

    # 4. no downgrade: the old client must fail closed and write nothing
    before = tree(path)
    rc, out, err = run_old(py, f"""
t = load_table({path!r})
t.append_records([{{"id": 500, "name": "downgrade", "value": 5.0}}], s)
print("DOWNGRADE_WROTE", t.row_count())""")
    unchanged = tree(path) == before
    H.report(
        f"{version}-cannot-write-to-a-migrated-table",
        rc != 0 and "DOWNGRADE_WROTE" not in out and unchanged,
        f"old client rc={rc} ({err.splitlines()[-1][:90] if err else out}); files changed: "
        f"{sorted(set(tree(path)) ^ set(before)) or 'none'}",
    )
H.finish()
