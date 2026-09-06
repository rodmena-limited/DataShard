"""Claim (issuedb memory 'version_hint_is_a_hint'): if the hint is lost, the latest
version is recovered by scanning v*.metadata.json - safely.

A committer that wrote its vN-<rand>.metadata.json and then failed before the hint
flip leaves a file that was NEVER committed. 0.7.2 picked the newest mtime among
same-version files and could resurrect it, dropping committed rows (#60). Since
0.8.0: (1) a clean commit failure removes its own metadata file; (2) an ambiguous
recovery (several files at the top version) REFUSES with AmbiguousMetadataError;
(3) the operator resolves it explicitly with Table.repair_version_hint().
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

try:
    from datashard import AmbiguousMetadataError  # 0.8.0+
except ImportError:  # pragma: no cover - 0.7.2 has no such class
    AmbiguousMetadataError = None  # type: ignore[assignment,misc]

tmp = tempfile.mkdtemp(prefix="audit_hint_")
path = os.path.join(tmp, "t")
schema = H.simple_schema()
t = create_table(path, schema)
t.append_records([{"id": 1, "name": "A", "value": 1.0}], schema)
v_before = t.metadata_manager._current_version_info()[1]  # v1-....metadata.json
t.append_records([{"id": 2, "name": "B", "value": 2.0}], schema)
v_after = t.metadata_manager._current_version_info()[1]  # v2-....metadata.json
assert t.row_count() == 2

# (1) a commit that fails cleanly at the hint write must not leave its metadata file behind
files_before = set(os.listdir(os.path.join(path, "metadata")))
orig_write = t.storage.write_file


def failing_hint_write(p, content):
    if p == t.metadata_manager.HINT_PATH:
        raise OSError("simulated: hint write failed (disk error) - atomic backend, nothing landed")
    return orig_write(p, content)


t.storage.write_file = failing_hint_write
try:
    t.append_records([{"id": 9, "name": "fail", "value": 9.0}], schema)
    outcome = "commit unexpectedly succeeded"
except Exception as e:  # noqa: BLE001
    outcome = f"commit failed with {type(e).__name__}"
finally:
    t.storage.write_file = orig_write
leftover = sorted(set(os.listdir(os.path.join(path, "metadata"))) - files_before - {"inflight"})
leftover = [f for f in leftover if f.endswith(".metadata.json")]
H.report("clean-commit-failure-removes-its-uncommitted-metadata-file", not leftover, f"{outcome}; new metadata files left behind: {leftover or 'none'}")

# (2) a crashed racer's leftover at the same version + a lost hint: recovery must not guess
loser = os.path.join(path, "metadata", "v2-deadbeef.metadata.json")
shutil.copy(os.path.join(path, "metadata", v_before), loser)
now = time.time() + 1
os.utime(loser, (now, now))
os.remove(os.path.join(path, "metadata.version-hint.text"))
try:
    t2 = load_table(path)
    picked = t2.metadata_manager._current_version_info()[1]
    rows = t2.row_count()
    ok = picked == v_after and rows == 2
    H.report("hint-recovery-selects-the-committed-version-or-refuses", ok, f"recovery silently picked {picked}; rows={rows} (committed={v_after})")
except Exception as e:  # noqa: BLE001
    refused = AmbiguousMetadataError is not None and isinstance(e, AmbiguousMetadataError)
    H.report("hint-recovery-selects-the-committed-version-or-refuses", refused, f"refused with {type(e).__name__}: {str(e)[:110]}...")

# (3) explicit operator repair restores the committed state and later commits build on it
try:
    from datashard.metadata_manager import MetadataManager
    from datashard.storage_backend import LocalStorageBackend

    MetadataManager(path, LocalStorageBackend(path)).repair_version_hint(v_after)
    t3 = load_table(path)
    rows = t3.row_count()
    t3.append_records([{"id": 3, "name": "C", "value": 3.0}], schema)
    rows2 = load_table(path).row_count()
    H.report(
        "repair_version_hint-restores-committed-state-and-commits-continue",
        rows == 2 and rows2 == 3,
        f"after repair rows={rows} (expected 2); after appending C rows={rows2} (expected 3); metadata files: "
        f"{sorted(os.path.basename(p) for p in glob.glob(os.path.join(path, 'metadata', 'v*.json')))}",
    )
except Exception as e:  # noqa: BLE001
    H.report("repair_version_hint-restores-committed-state-and-commits-continue", False, f"{type(e).__name__}: {str(e)[:120]}")
H.finish()
