"""
Best-effort polling lock for S3 providers without conditional writes (split out of
lock_provider.py, #71). Reachable only behind DATASHARD_S3_ALLOW_UNSAFE_LOCK=1 (#59).
"""

import logging
import random
import time
from typing import Any

from .lock_provider import S3LockProviderBase

logger = logging.getLogger(__name__)


class S3PollingLockProvider(S3LockProviderBase):
    """S3-based distributed lock using polling (for S3 providers without conditional writes).

    Fallback for the rare S3 provider that does not honour If-None-Match /
    If-Match preconditions. Uses a check-then-write approach with verification.

    WARNING - BEST-EFFORT ONLY: without conditional writes there are
    interleavings (delayed PUTs landing after another writer's verification
    read) in which two processes both believe they hold the lock, and the
    stale-lock break below has a window in which it can delete a freshly
    acquired lock. MetadataManager's pre-commit ownership re-check narrows but
    cannot close these windows. Do not rely on this provider where a lost
    commit is unacceptable.

    Selected only when the provider fails the conditional-write probe or
    DATASHARD_S3_USE_CONDITIONAL_WRITES=false is set, and even then only with
    DATASHARD_S3_ALLOW_UNSAFE_LOCK=1 (#59): a lost commit is the failure mode.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        logger.warning(
            "S3PollingLockProvider in use (DATASHARD_S3_USE_CONDITIONAL_WRITES=false): "
            "distributed locking is BEST EFFORT and cannot rule out two writers "
            "believing they hold the same lock. Enable conditional writes for "
            "compare-and-swap locking wherever a lost commit is unacceptable."
        )

    def is_held(self) -> bool:
        """Ownership check that also honours our local lease clock.

        Past the lease deadline another process may have broken the lock, so
        ownership is not ours to claim even if the object still reads back with
        our id (our own late renewal could have put it there).
        """
        if self._lease_deadline is not None and time.monotonic() > self._lease_deadline:
            self.is_locked = False
            return False
        return super().is_held()

    def _try_acquire(self) -> bool:
        import botocore.exceptions

        # Step 1: Check if lock file exists
        try:
            self.s3.head_object(Bucket=self.bucket, Key=self.key)
            # Lock exists, can't acquire
            return False
        except botocore.exceptions.ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code != '404':
                # Unexpected error
                raise e
            # Lock doesn't exist, proceed to acquire

        # Step 2: Write our lock ID. The lease is counted from BEFORE the write
        # (the conservative end of the window).
        write_started = time.monotonic()
        self.s3.put_object(
            Bucket=self.bucket,
            Key=self.key,
            Body=self.lock_id.encode('utf-8')
        )

        # Step 3: Wait briefly to allow for race condition detection
        time.sleep(random.uniform(0.1, 0.3))

        # Step 4: Read back and verify we own it
        try:
            resp = self.s3.get_object(Bucket=self.bucket, Key=self.key)
            content = resp['Body'].read().decode('utf-8')

            if content == self.lock_id:
                self._lease_deadline = write_started + self.lease_seconds
                return True
            else:
                # Someone else won the race
                logger.debug(f"Lost lock race at {self.key}: expected {self.lock_id}, got {content}")
                return False
        except botocore.exceptions.ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == '404':
                # Lock disappeared (someone deleted it), retry
                return False
            raise e

    def _renew_once(self) -> None:
        """Read-verify-then-write renewal (non-atomic; best this provider can do).

        Refuses to write once our own lease has lapsed: past that point another
        process is entitled to break the lock, and a late PUT would resurrect
        our ownership over theirs - the interleaving that makes two writers
        believe they hold the same lock. Giving up instead fails closed (the
        pre-commit fence sees is_locked=False).
        """
        import botocore.exceptions

        if self._lease_deadline is not None and time.monotonic() > self._lease_deadline:
            logger.warning(
                f"S3 lock lease at {self.key} lapsed before renewal could complete; "
                f"treating the lock as lost instead of resurrecting it."
            )
            self.is_locked = False
            return

        try:
            resp = self.s3.get_object(Bucket=self.bucket, Key=self.key)
            content = resp['Body'].read().decode('utf-8')

            if content != self.lock_id:
                logger.warning(f"Lost S3 lock at {self.key} (content mismatch). Stopping heartbeat.")
                self.is_locked = False
                return

            # Renew: Overwrite with same content to update LastModified
            write_started = time.monotonic()
            if self._lease_deadline is not None and write_started > self._lease_deadline:
                # The verification read itself took us past the lease.
                logger.warning(f"S3 lock lease at {self.key} lapsed mid-renewal; giving up.")
                self.is_locked = False
                return
            self.s3.put_object(
                Bucket=self.bucket,
                Key=self.key,
                Body=self.lock_id.encode('utf-8')
            )
            self._lease_deadline = write_started + self.lease_seconds
            logger.debug(f"Renewed S3 lock at {self.key}")
        except botocore.exceptions.ClientError as e:
            logger.warning(f"Failed to renew S3 lock: {e}")
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code in ('404', 'NoSuchKey'):
                self.is_locked = False

    def _check_and_break_expired_lock(self) -> bool:
        """Check if the lock file is older than lease_seconds. If so, delete it.

        Non-atomic (no conditional delete available): a double head-check with a
        randomized pause narrows - but cannot close - the window in which the
        delete can hit a lock that was just renewed or re-acquired.
        """
        from datetime import datetime, timezone

        import botocore.exceptions

        try:
            resp = self.s3.head_object(Bucket=self.bucket, Key=self.key)
            last_modified_1 = resp['LastModified']
            etag_1 = resp.get('ETag')

            # S3 returns offset-aware datetime (usually UTC)
            now = datetime.now(timezone.utc)

            age = (now - last_modified_1).total_seconds()

            if age > self.lease_seconds:
                # Potential expiration. Wait and double check to avoid racing with a renewal or new lock.
                time.sleep(random.uniform(0.5, 1.5))

                try:
                    resp2 = self.s3.head_object(Bucket=self.bucket, Key=self.key)
                    last_modified_2 = resp2['LastModified']
                    etag_2 = resp2.get('ETag')

                    # If lock changed while we waited, don't break it
                    if last_modified_1 != last_modified_2 or etag_1 != etag_2:
                        return False
                except botocore.exceptions.ClientError:
                    # Lock disappeared? Treat as handled
                    return False

                logger.warning(f"Breaking expired S3 lock at {self.key} (Age: {age}s > {self.lease_seconds}s)")
                # We delete the object. The next acquire loop will try to create it.
                # This handles the crash scenario.
                self.s3.delete_object(Bucket=self.bucket, Key=self.key)
                return True

            return False
        except botocore.exceptions.ClientError as e:
             error_code = e.response.get('Error', {}).get('Code', '')
             if error_code == '404':
                 # Lock doesn't exist, so it's not expired (it's free)
                 return False
             # Other errors (permission, etc)
             logger.warning(f"Failed to check S3 lock expiration: {e}")
             return False
