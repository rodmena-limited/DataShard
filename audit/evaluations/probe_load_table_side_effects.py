"""Claim: load_table() is a read-only open; a typo'd path fails without side effects.

Suspect: Table.__init__ constructs MetadataManager/FileManager, whose constructors
call makedirs() before load_table checks that any metadata exists.
"""
import os
import tempfile

import _harness as H

H.local_env()
H.quiet_logs()
from datashard import load_table  # noqa: E402

tmp = tempfile.mkdtemp(prefix="audit_lt_")
path = os.path.join(tmp, "typo_table")
try:
    load_table(path)
    H.report("load_table-on-missing-table-raises", False, "no exception")
except ValueError as e:
    H.report("load_table-on-missing-table-raises", True, str(e)[:80])
created = [p or "<root>" for p in ("", "data", "metadata", "metadata/manifests", ".locks") if os.path.exists(os.path.join(path, p))]
H.report("load_table-is-side-effect-free", not created, f"paths created by a FAILED load_table: {created or 'none'}")
H.finish()
