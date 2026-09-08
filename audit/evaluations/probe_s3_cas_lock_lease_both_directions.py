"""Claim: with conditional writes, the S3 lock excludes a second writer while the
lease is valid AND is taken over once the holder dies (lease expiry). Both directions
of the guard, plus the fence: the dead holder must see is_held() == False.
"""
import time
import uuid

import _harness as H

H.quiet_logs()
bucket = "audit-lease"
s3 = H.s3_env(bucket, conditional=True)
if s3 is None:
    H.skip("cas-lock-lease", "moto_server unavailable")
    H.finish()
from botocore.exceptions import ClientError  # noqa: E402

# Does this S3 counterparty honour If-None-Match at all? (moto's support, not datashard's)
s3.put_object(Bucket=bucket, Key="probe.txt", Body=b"1")
try:
    s3.put_object(Bucket=bucket, Key="probe.txt", Body=b"2", IfNoneMatch="*")
    H.skip("cas-lock-lease", "this S3 server ignores If-None-Match; cannot test CAS locking here")
    H.finish()
except ClientError:
    pass
from datashard.lock_provider import S3LockProvider  # noqa: E402

# S3 LastModified has 1 s granularity, so the lease must be several seconds long for the
# age comparison to be unambiguous; the key is unique per run.
key = f"locks/lease-{uuid.uuid4().hex[:8]}.lock"
LEASE, MARGIN = 5, 1.0

# 1. A LIVE holder - one whose heartbeat is still renewing the lease - is never displaced.
#    (Before #96 this claim was tested against a holder whose heartbeat had been stopped,
#    which only proved that the rival gave up before the lease expired: it asserted the
#    bug. A lock a dead process left behind SHOULD be taken over.)
a = S3LockProvider(s3, bucket, key, timeout=5, lease_seconds=LEASE, takeover_margin_seconds=MARGIN)
assert a.acquire()
b = S3LockProvider(s3, bucket, key, timeout=2, lease_seconds=LEASE, takeover_margin_seconds=MARGIN)
t0 = time.time()
try:
    b.acquire()
    blocked = False
except TimeoutError:
    blocked = True
still_a = s3.get_object(Bucket=bucket, Key=key)["Body"].read() == a.lock_id.encode()
H.report(
    "cas-lock-is-not-taken-from-a-holder-that-is-still-renewing-it",
    blocked and still_a,
    f"rival refused={blocked} after {time.time() - t0:.1f}s (lease {LEASE}s, renewed by the "
    f"holder's heartbeat); lock object still the holder's={still_a}",
)

# 2. The holder DIES. Its lock must be taken over inside ONE acquire, without the caller
#    having to know to retry after the lease (#96).
a._stop_heartbeat_thread()
c = S3LockProvider(s3, bucket, key, timeout=2, lease_seconds=LEASE, takeover_margin_seconds=MARGIN)
t0 = time.time()
try:
    got, waited = c.acquire(), time.time() - t0
except TimeoutError:
    got, waited = False, time.time() - t0
H.report(
    "cas-lock-left-by-a-dead-holder-is-taken-over-in-one-acquire",
    got,
    f"acquired={got} after {waited:.1f}s with a caller timeout of 2s and a {LEASE}s lease - "
    f"an acquire that gave up inside the lease could never break this lock",
)
H.report("dead-holder-fence-reports-lock-lost", not a.is_held(), f"stale holder is_held()={a.is_held()}")
if got:
    c.release()
H.finish()
