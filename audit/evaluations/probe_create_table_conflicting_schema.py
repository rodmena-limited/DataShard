"""Claim (create_table docstring): a provided schema that is not applied is warned
about 'loudly rather than silently ignoring correctness-relevant input'.

Suspect: the warning only fires when the existing table has NO schema; create_table
on a table with a DIFFERENT persisted schema returns silently with the old schema.
"""
import io
import logging
import os
import tempfile

import _harness as H

H.local_env()
from datashard import Schema, create_table  # noqa: E402

tmp = tempfile.mkdtemp(prefix="audit_ct_")
path = os.path.join(tmp, "t")
A = Schema(schema_id=1, fields=[{"id": 1, "name": "id", "type": "long", "required": True}])
B = Schema(schema_id=2, fields=[{"id": 1, "name": "price", "type": "double", "required": True}])
create_table(path, A)
buf = io.StringIO()
lg = logging.getLogger("datashard")
lg.addHandler(logging.StreamHandler(buf))
lg.setLevel(logging.WARNING)
try:
    t2 = create_table(path, B)
    cur = t2._get_current_schema()
    warned = "schema" in buf.getvalue().lower()
    H.report(
        "create_table-with-conflicting-schema-raises-or-warns",
        warned,
        f"no exception; table keeps fields={[f['name'] for f in cur.fields]} while caller asked for "
        f"{[f['name'] for f in B.fields]}; warning logged={warned}",
    )
except Exception as e:  # noqa: BLE001
    H.report("create_table-with-conflicting-schema-raises-or-warns", True, f"raised {type(e).__name__}")
H.finish()
