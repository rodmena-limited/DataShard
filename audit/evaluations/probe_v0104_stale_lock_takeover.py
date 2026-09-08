"""Claim (#96): a lock left behind by a process that died is broken automatically, and
a lock a live holder is still renewing is not.

datashard shipped an acquire timeout (30 s) SHORTER than the lock's lease (60 s). A lock
is only takeable once it is older than the lease, so the deadline always expired first
and every acquire after a crash raised TimeoutError - which is what a user's S3
migration hit. Both directions are probed, because a lock that always yields is not a
lock; the control shows the old arrangement failing on the same fixture.
"""
import time
import uuid

import _harness as H

H.quiet_logs()
bucket = f"audit-lock-{uuid.uuid4().hex[:8]}"
s3 = H.s3_env(bucket)
if s3 is None:
    H.skip("stale-lock-takeover", "moto_server unavailable")
    H.finish()
from datashard.lock_provider import S3LockProvider  # noqa: E402

KEY = f"probe-{uuid.uuid4().hex[:8]}/.locks/metadata.lock"
LEASE, MARGIN = 2, 0.5

# 1. a lock whose holder is gone must be taken over inside ONE acquire
s3.put_object(Bucket=bucket, Key=KEY, Body=b"a-process-that-died")
lp = S3LockProvider(s3, bucket, KEY, timeout=1.0, lease_seconds=LEASE, takeover_margin_seconds=MARGIN)
t0 = time.time()
try:
    acquired = lp.acquire()
    detail = f"acquired after {time.time() - t0:.1f}s (lease {LEASE}s)"
except TimeoutError as e:
    acquired, detail = False, f"TimeoutError after {time.time() - t0:.1f}s: {str(e)[:80]}"
H.report("a-lock-left-by-a-dead-process-is-taken-over", acquired, detail)
if acquired:
    lp.release()

# control: the shipped-before arrangement (deadline inside the lease) on the same fixture
s3.put_object(Bucket=bucket, Key=KEY, Body=b"a-process-that-died")
old = S3LockProvider(s3, bucket, KEY, timeout=0.4, lease_seconds=LEASE, takeover_margin_seconds=-1.7)
t0 = time.time()
try:
    old.acquire()
    control_failed = False
except TimeoutError:
    control_failed = True
H.report(
    "control: a deadline INSIDE the lease can never break that same lock",
    control_failed,
    f"with the deadline {time.time() - t0:.1f}s < lease {LEASE}s the identical stale lock is "
    f"{'refused, as it was before the fix' if control_failed else 'WRONGLY acquired'}",
)

# 2. a lock a live holder keeps renewing must still be refused
holder = S3LockProvider(s3, bucket, KEY, timeout=1.0, lease_seconds=LEASE, takeover_margin_seconds=MARGIN)
try:
    s3.delete_object(Bucket=bucket, Key=KEY)
except Exception:  # noqa: BLE001
    pass
holder.acquire()
rival = S3LockProvider(s3, bucket, KEY, timeout=1.0, lease_seconds=LEASE, takeover_margin_seconds=MARGIN)
t0 = time.time()
try:
    rival.acquire()
    refused, msg = False, "the rival ACQUIRED a lock that is being renewed - two writers hold it"
except TimeoutError as e:
    refused, msg = True, str(e)
still_ours = s3.get_object(Bucket=bucket, Key=KEY)["Body"].read() == holder.lock_id.encode()
H.report(
    "a-lock-that-is-still-being-renewed-is-not-stolen",
    refused and still_ours,
    f"refused after {time.time() - t0:.1f}s and the object still holds the owner's id; "
    f"message names the age: {'last renewed' in msg}",
)
holder.release()
H.finish()
