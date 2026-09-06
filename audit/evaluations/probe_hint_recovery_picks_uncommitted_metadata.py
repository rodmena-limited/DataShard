"""Claim (issuedb memory 'version_hint_is_a_hint'): if the hint is lost, the latest
version is recovered by scanning v*.metadata.json.

Suspect: a committer that wrote its vN-<rand>.metadata.json and then failed before the
hint flip (lost CAS race, crash, max retries) leaves a file that was NEVER committed.
Recovery picks the highest version and, among same-version files, the newest mtime -
i.e. it can resurrect the loser's uncommitted state and silently drop committed data.
"""
import glob
import os
import shutil
import tempfile
import time

import _harness as H

H.local_env()
H.quiet_logs()
from datashard import create_table, load_table  # noqa: E402

tmp = tempfile.mkdtemp(prefix="audit_hint_")
path = os.path.join(tmp, "t")
schema = H.simple_schema()
t = create_table(path, schema)
t.append_records([{"id": 1, "name": "A", "value": 1.0}], schema)
v_before = t.metadata_manager._current_version_info()[1]  # v1-....metadata.json
t.append_records([{"id": 2, "name": "B", "value": 2.0}], schema)
v_after = t.metadata_manager._current_version_info()[1]  # v2-....metadata.json
assert t.row_count() == 2
# A racing writer based on v1 wrote its own v2 file (never committed) and died.
loser = os.path.join(path, "metadata", "v2-deadbeef.metadata.json")
shutil.copy(os.path.join(path, "metadata", v_before), loser)
now = time.time() + 1
os.utime(loser, (now, now))
os.remove(os.path.join(path, "metadata.version-hint.text"))  # the case recovery exists for
t2 = load_table(path)
picked = t2.metadata_manager._current_version_info()[1]
rows = t2.row_count()
H.report(
    "hint-recovery-selects-the-committed-version",
    picked == v_after and rows == 2,
    f"committed={v_after}, uncommitted leftover=v2-deadbeef.metadata.json; recovery picked {picked}; rows={rows} (expected 2)",
)
t2.append_records([{"id": 3, "name": "C", "value": 3.0}], schema)
rows2 = load_table(path).row_count()
H.report(
    "commit-after-recovery-keeps-all-committed-data",
    rows2 == 3,
    f"after appending C on the recovered table rows={rows2} (expected 3); metadata files now "
    f"{sorted(os.path.basename(p) for p in glob.glob(os.path.join(path, 'metadata', 'v*.json')))}",
)
H.finish()
