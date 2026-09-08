"""A lock whose holder is gone must be breakable within one acquire() call (#96).

A lock is only takeable once it is OLDER than its lease, so an acquire deadline inside
the lease can never break one left behind by a killed process: the call fails, and only
a retry made after the lease has run down succeeds. datashard shipped exactly that -
a 30 s timeout against a 60 s lease - so every first attempt after a crash raised

    TimeoutError: Failed to acquire S3 lock at <table>/.locks/metadata.lock within 30.0s

with nothing in it to say the condition was transient. Reported by crypto-trader when
an S3 migration hit it, and reproduced against OVH before this fix.

Both directions are tested, because a lock that always yields is not a lock: a holder
that is alive and renewing must STILL be refused.
"""
import threading
import time

import pytest

from datashard.lock_provider import S3LockProvider


class FakeS3:
    """Enough S3 for the lock protocol: conditional create, conditional replace, HEAD."""

    def __init__(self):
        self.objects = {}   # key -> (body, etag, last_modified)
        self._n = 0
        self.lock = threading.Lock()

    def put_object(self, Bucket, Key, Body, IfNoneMatch=None, IfMatch=None, **kw):
        from datetime import datetime, timezone

        import botocore.exceptions
        with self.lock:
            existing = self.objects.get(Key)
            if IfNoneMatch == "*" and existing is not None:
                raise botocore.exceptions.ClientError(
                    {"Error": {"Code": "PreconditionFailed"}}, "PutObject")
            if IfMatch is not None and (existing is None or existing[1] != IfMatch):
                raise botocore.exceptions.ClientError(
                    {"Error": {"Code": "PreconditionFailed"}}, "PutObject")
            self._n += 1
            etag = f'"{self._n}"'
            self.objects[Key] = (Body, etag, datetime.now(timezone.utc))
            return {"ETag": etag}

    def head_object(self, Bucket, Key, **kw):
        import botocore.exceptions
        obj = self.objects.get(Key)
        if obj is None:
            raise botocore.exceptions.ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ETag": obj[1], "LastModified": obj[2], "ContentLength": len(obj[0])}

    def get_object(self, Bucket, Key, **kw):
        import io

        import botocore.exceptions
        obj = self.objects.get(Key)
        if obj is None:
            raise botocore.exceptions.ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(obj[0]), "ETag": obj[1]}

    def delete_object(self, Bucket, Key, **kw):
        self.objects.pop(Key, None)
        return {}


KEY = "t/.locks/metadata.lock"


def test_the_acquire_deadline_always_outlives_the_lease():
    """The invariant the bug violated: a caller cannot ask for a deadline so short that
    a dead holder's lock becomes unbreakable."""
    s3 = FakeS3()
    lp = S3LockProvider(s3, "b", KEY, timeout=30.0)
    assert lp.lease_seconds == 60
    effective = max(lp.timeout, lp.lease_seconds + lp.takeover_margin_seconds)
    assert effective > lp.lease_seconds, "an acquire that expires inside the lease can never take over"
    assert lp.takeover_margin_seconds == lp.TAKEOVER_MARGIN_SECONDS == 15  # the shipped default


def test_a_lock_left_by_a_dead_process_is_taken_over(monkeypatch):
    s3 = FakeS3()
    s3.put_object(Bucket="b", Key=KEY, Body=b"a-process-that-died")   # no holder, no renewal
    lp = S3LockProvider(s3, "b", KEY, timeout=1.0, lease_seconds=2, takeover_margin_seconds=0.5)
    monkeypatch.setattr(lp, "_start_heartbeat", lambda: None)
    t0 = time.time()
    assert lp.acquire() is True
    waited = time.time() - t0
    assert waited > 2, "it must have waited for the lease to run down, not skipped the check"
    assert s3.objects[KEY][0] == lp.lock_id.encode()   # the lock is now genuinely ours


def test_a_lock_that_is_still_being_renewed_is_refused(monkeypatch):
    """The other direction: a live holder keeps the lock, and the message says so."""
    s3 = FakeS3()
    holder = S3LockProvider(s3, "b", KEY, timeout=1.0, lease_seconds=2, takeover_margin_seconds=0.5)
    monkeypatch.setattr(holder, "_start_heartbeat", lambda: None)
    assert holder.acquire()

    renewing = threading.Event()

    def keep_renewing():
        while not renewing.is_set():
            holder._renew_once()
            time.sleep(0.3)

    t = threading.Thread(target=keep_renewing, daemon=True)
    t.start()
    try:
        rival = S3LockProvider(s3, "b", KEY, timeout=1.0, lease_seconds=2, takeover_margin_seconds=0.5)
        with pytest.raises(TimeoutError) as exc:
            rival.acquire()
        message = str(exc.value)
        assert "last renewed" in message and "lease 2s" in message, message
        assert "deleting" in message.lower(), "the message must warn against deleting a held lock"
        assert s3.objects[KEY][0] == holder.lock_id.encode()  # never stolen
    finally:
        renewing.set()
        t.join(5)


def test_the_timeout_message_survives_an_uninspectable_lock(monkeypatch):
    """The diagnostic must never mask the timeout it is explaining."""
    s3 = FakeS3()
    s3.put_object(Bucket="b", Key=KEY, Body=b"held")
    lp = S3LockProvider(s3, "b", KEY, timeout=0.5, lease_seconds=1, takeover_margin_seconds=0.2)
    monkeypatch.setattr(lp, "_try_takeover_expired", lambda: False)
    monkeypatch.setattr(lp, "_holder_description", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    with pytest.raises(RuntimeError):
        lp.acquire()
    monkeypatch.setattr(s3, "head_object", lambda **kw: (_ for _ in ()).throw(RuntimeError("no head")))
    lp2 = S3LockProvider(s3, "b", KEY, timeout=0.5, lease_seconds=1, takeover_margin_seconds=0.2)
    monkeypatch.setattr(lp2, "_try_takeover_expired", lambda: False)
    with pytest.raises(TimeoutError) as exc:
        lp2.acquire()
    assert "could not be inspected" in str(exc.value)
