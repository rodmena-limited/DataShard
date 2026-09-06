"""Claims: Iceberg type system; a data lake for a trading desk.

Checks (1) whether exact decimal columns exist at all (Iceberg has decimal(P,S);
prices/quantities in double lose exactness), (2) whether timestamptz exists, and
(3) what happens to a timezone-aware datetime written into a 'timestamp' column.
"""
import os
import tempfile
from datetime import datetime, timedelta, timezone

import _harness as H

H.local_env()
H.quiet_logs()
from datashard import Schema, create_table  # noqa: E402

tmp = tempfile.mkdtemp(prefix="audit_types_")
for tname in ("decimal(18,8)", "timestamptz"):
    try:
        Schema(schema_id=1, fields=[{"id": 1, "name": "x", "type": tname}])
        H.report(f"type-{tname}-supported", True, "accepted")
    except ValueError as e:
        H.report(f"type-{tname}-supported", False, str(e)[:90])
try:  # Iceberg decimals carry precision and scale; a bare 'decimal' must be refused clearly
    Schema(schema_id=1, fields=[{"id": 1, "name": "x", "type": "decimal"}])
    H.report("bare-decimal-without-precision-is-rejected", False, "accepted without precision/scale")
except ValueError as e:
    H.report("bare-decimal-without-precision-is-rejected", "decimal(P,S)" in str(e), str(e)[-60:])
schema = Schema(schema_id=1, fields=[{"id": 1, "name": "ts", "type": "timestamp", "required": True}])
t = create_table(os.path.join(tmp, "ts"), schema)
plus2 = datetime(2026, 1, 1, 12, 0, tzinfo=timezone(timedelta(hours=2)))
utc = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
try:
    t.append_records([{"ts": plus2}, {"ts": utc}], schema)
    got = [r["ts"] for r in t.scan()]
    ok = got == [datetime(2026, 1, 1, 10, 0), datetime(2026, 1, 1, 12, 0)]
    H.report("tz-aware-timestamps-normalised-to-utc-or-rejected", ok, f"wrote 12:00+02:00 and 12:00Z, read back {got}")
except Exception as e:  # noqa: BLE001
    H.report("tz-aware-timestamps-normalised-to-utc-or-rejected", True, f"rejected: {type(e).__name__}: {str(e)[:80]}")
H.finish()
