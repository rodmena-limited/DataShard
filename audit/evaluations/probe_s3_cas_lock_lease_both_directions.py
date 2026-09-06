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
LEASE = 5
a = S3LockProvider(s3, bucket, key, timeout=5, lease_seconds=LEASE)
assert a.acquire()
a._stop_heartbeat_thread()  # the holder dies: no more renewals
b = S3LockProvider(s3, bucket, key, timeout=2, lease_seconds=LEASE)
t0 = time.time()
try:
    b.acquire()
    blocked = False
except TimeoutError:
    blocked = True
H.report("cas-lock-blocks-second-acquirer-while-lease-valid", blocked, f"second acquire within lease: blocked={blocked} after {time.time() - t0:.1f}s")
time.sleep(LEASE + 1.5)
c = S3LockProvider(s3, bucket, key, timeout=5, lease_seconds=LEASE)
t0 = time.time()
got = c.acquire()
H.report("cas-lock-taken-over-after-lease-expiry", got, f"acquired={got} in {time.time() - t0:.1f}s after lease lapsed")
H.report("dead-holder-fence-reports-lock-lost", not a.is_held(), f"stale holder is_held()={a.is_held()}")
c.release()
H.finish()
