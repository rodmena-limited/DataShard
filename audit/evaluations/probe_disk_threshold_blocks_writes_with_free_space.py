"""Claim (disk_utils.check_disk_space): writes are refused when the disk is
'critically full'.

Suspect: the threshold is a PERCENTAGE (95% used) regardless of absolute free space,
so on a large volume datashard refuses every write - including the commit-point hint
- while hundreds of GB remain free. Deterministic: shutil.disk_usage is stubbed to a
10 TB volume with 400 GB free.
"""
import os
import shutil
import tempfile

import _harness as H

H.local_env()
H.quiet_logs()
import datashard.disk_utils as du  # noqa: E402
from datashard import create_table  # noqa: E402

tmp = tempfile.mkdtemp(prefix="audit_disk_")
t = create_table(os.path.join(tmp, "t"), H.simple_schema())
TB = 1024**4
fake = shutil._ntuple_diskusage(total=10 * TB, used=int(9.6 * TB), free=int(0.4 * TB))
orig = du.shutil.disk_usage
du.shutil.disk_usage = lambda p: fake
try:
    t.append_records([{"id": 1, "name": "a", "value": 1.0}], H.simple_schema())
    H.report("writes-proceed-with-400GB-free-on-a-10TB-volume", True, "append succeeded")
except Exception as e:  # noqa: BLE001
    H.report("writes-proceed-with-400GB-free-on-a-10TB-volume", False, f"{type(e).__name__}: {str(e)[:110]}")
finally:
    du.shutil.disk_usage = orig
H.finish()
