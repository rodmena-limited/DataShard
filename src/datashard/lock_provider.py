"""
Abstract lock provider and implementations for Local/S3.

Locking model
-------------
- LocalLockProvider: kernel flock on a persistent inode. Real mutual exclusion
  on one host; the kernel releases the lock if the holder dies.
- S3LockProvider (conditional writes): every mutation of the lock object is a
  conditional PUT (If-None-Match on create, If-Match on renewal/takeover), so
  create, renewal, and stale-lock takeover are all atomic compare-and-swap
  operations - two processes can never both believe they won.
- S3PollingLockProvider (no conditional writes): check-then-write with
  read-back verification. BEST-EFFORT ONLY - there are interleavings where two
  processes both acquire. Callers on such providers must rely on the CAS-less
  commit protocol's double-checks and accept residual risk (documented).

Locks alone are never fully sufficient on S3 (a paused holder can resume after
its lease was broken), so MetadataManager additionally fences the commit point:
it re-validates lock ownership immediately before the version-hint write and,
where the backend supports CAS, performs the hint write itself as a conditional
PUT.
"""

import logging
import random
import threading
import time
import uuid
from abc import ABC, abstractmethod
from typing import Any, Optional

from .file_lock import FileLock

logger = logging.getLogger(__name__)

class LockProvider(ABC):
    """Abstract base class for distributed locks."""

    @abstractmethod
    def acquire(self) -> bool:
        """Acquire the lock. Blocks until acquired or timeout."""
        pass

    @abstractmethod
    def release(self) -> None:
        """Release the lock."""
        pass

    @abstractmethod
    def is_held(self) -> bool:
        """Best-effort check that this instance still holds the lock.

        Used as a fencing check immediately before the commit point. May
        perform I/O. Must return False if ownership was lost or is unknown.
        """
        pass

class LocalLockProvider(LockProvider):
    """Local filesystem lock using flock/msvcrt."""

    def __init__(self, lock_path: str, timeout: float = 30.0):
        self.lock = FileLock(lock_path, timeout)

    def acquire(self) -> bool:
        return self.lock.acquire()

    def release(self) -> None:
        self.lock.release()

    def is_held(self) -> bool:
        # flock cannot be stolen while the fd is open; local flag is authoritative.
        return self.lock.is_held()

class S3LockProviderBase(LockProvider):
    """Base class for S3-based distributed locks."""

    def __init__(
        self,
        s3_client: Any,
        bucket: str,
        key: str,
        timeout: float = 30.0,
        lease_seconds: int = 60
    ):
        self.s3 = s3_client
        self.bucket = bucket
        self.key = key
        self.timeout = timeout
        self.lease_seconds = lease_seconds
        self.lock_id = str(uuid.uuid4())
        self.is_locked = False
        self._heartbeat_thread: Any = None  # Typed as Any to avoid Thread import issues
        self._stop_heartbeat = threading.Event()
        # ETag of the lock object as last written by us (CAS providers only).
        # Guarded by _state_lock: accessed from both the owner and heartbeat threads.
        self._etag: Optional[str] = None
        self._state_lock = threading.Lock()

    def acquire(self) -> bool:
        start_time = time.time()
        while True:
            # 1. Try to acquire lock (subclasses may also take over expired locks atomically)
            if self._try_acquire():
                self.is_locked = True
                self._start_heartbeat()
                return True

            # 2. Check if existing lock is expired (deadlock prevention)
            self._check_and_break_expired_lock()

            # 3. Check timeout
            if time.time() - start_time >= self.timeout:
                raise TimeoutError(f"Failed to acquire S3 lock at {self.key} within {self.timeout}s")

            # Wait with randomized jitter before retrying. random.uniform
            # de-correlates contending processes; the previous time-based
            # jitter gave all waiters nearly identical sleeps.
            time.sleep(random.uniform(0.3, 0.9))

    def _try_acquire(self) -> bool:
        """Subclasses must implement the actual lock acquisition logic."""
        raise NotImplementedError

    def _check_and_break_expired_lock(self) -> bool:
        """Subclasses handle expired locks; CAS providers take over atomically in _try_acquire."""
        return False

    def is_held(self) -> bool:
        """Verify ownership by reading the lock object content."""
        if not self.is_locked:
            return False
        import botocore.exceptions

        try:
            resp = self.s3.get_object(Bucket=self.bucket, Key=self.key)
            content = resp['Body'].read().decode('utf-8')
            if content != self.lock_id:
                self.is_locked = False
                return False
            return True
        except botocore.exceptions.ClientError:
            # Missing object or transient failure: ownership unknown -> report False
            # (fencing must fail closed).
            return False
        except Exception:
            return False

    def _start_heartbeat(self) -> None:
        """Start the heartbeat thread to renew lock lease."""
        self._stop_heartbeat.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"S3Lock-Heartbeat-{self.lock_id[:8]}",
            daemon=True
        )
        self._heartbeat_thread.start()

    def _stop_heartbeat_thread(self) -> None:
        """Stop the heartbeat thread."""
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._stop_heartbeat.set()
            self._heartbeat_thread.join(timeout=2.0)
            self._heartbeat_thread = None

    def _renew_once(self) -> None:
        """Perform one lease renewal. Subclasses override with their safety model."""
        raise NotImplementedError

    def _heartbeat_loop(self) -> None:
        """Periodically renew the lock lease."""
        # Renew every 1/3 of the lease time to be safe
        interval = self.lease_seconds / 3.0

        while not self._stop_heartbeat.wait(interval):
            if not self.is_locked:
                break
            try:
                self._renew_once()
            except Exception as e:
                logger.error(f"Unexpected error in lock heartbeat: {e}")

    def release(self) -> None:
        if not self.is_locked:
            return

        # Stop heartbeat BEFORE deleting file
        self._stop_heartbeat_thread()

        import botocore.exceptions

        try:
            # Safe release: Check if we still own the lock
            resp = self.s3.get_object(Bucket=self.bucket, Key=self.key)
            content = resp['Body'].read().decode('utf-8')

            if content == self.lock_id:
                self.s3.delete_object(Bucket=self.bucket, Key=self.key)
            else:
                logger.warning(f"Skipping release of S3 lock at {self.key}: Lock owner changed (expected {self.lock_id}, got {content})")

        except botocore.exceptions.ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == '404':
                # Already gone
                pass
            else:
                logger.warning(f"Error releasing S3 lock: {e}")
        except Exception as e:
            logger.warning(f"Error releasing S3 lock: {e}")

        self.is_locked = False
        with self._state_lock:
            self._etag = None


class S3LockProvider(S3LockProviderBase):
    """S3-based distributed lock using conditional writes (If-None-Match / If-Match).

    Requires S3 provider support for conditional PUT operations.
    For providers without this support (e.g., OVH), use S3PollingLockProvider.

    All lock-object mutations are compare-and-swap:
    - create: PUT If-None-Match:* (only one creator wins)
    - stale takeover: PUT If-Match:<observed etag> (only one breaker wins, and
      taking over IS acquiring - there is no delete window for a third party)
    - renewal: PUT If-Match:<our last etag> (a renewal after theft fails
      instead of silently resurrecting our lock)
    """

    def _try_acquire(self) -> bool:
        import botocore.exceptions
        try:
            # Conditional Write: Only succeed if object does NOT exist
            resp = self.s3.put_object(
                Bucket=self.bucket,
                Key=self.key,
                Body=self.lock_id.encode('utf-8'),
                IfNoneMatch='*'
            )
            with self._state_lock:
                self._etag = resp.get('ETag')
            return True
        except botocore.exceptions.ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code in ('PreconditionFailed', '412', 'ConditionalRequestConflict'):
                # Lock already exists - maybe expired; try atomic takeover
                return self._try_takeover_expired()
            raise e

    def _try_takeover_expired(self) -> bool:
        """Atomically take over an expired lock via conditional PUT If-Match.

        Unlike delete-then-recreate, this leaves no window in which a third
        process can slip in between our decision and our write: if anything
        about the lock object changed (renewal, another breaker), our If-Match
        precondition fails and we simply retry the outer loop.
        """
        from datetime import datetime, timezone

        import botocore.exceptions

        try:
            resp = self.s3.head_object(Bucket=self.bucket, Key=self.key)
        except botocore.exceptions.ClientError:
            # 404 or transient: lock vanished/unknown; the outer loop retries.
            return False

        last_modified = resp['LastModified']
        etag = resp.get('ETag')
        age = (datetime.now(timezone.utc) - last_modified).total_seconds()
        if age <= self.lease_seconds:
            return False

        logger.warning(
            f"Taking over expired S3 lock at {self.key} (age {age:.0f}s > lease {self.lease_seconds}s)"
        )
        try:
            put_resp = self.s3.put_object(
                Bucket=self.bucket,
                Key=self.key,
                Body=self.lock_id.encode('utf-8'),
                IfMatch=etag,
            )
            with self._state_lock:
                self._etag = put_resp.get('ETag')
            return True
        except botocore.exceptions.ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code in ('PreconditionFailed', '412', 'ConditionalRequestConflict', '404',
                              'NoSuchKey'):
                # Someone else renewed, took over, or released meanwhile - not ours.
                return False
            raise e

    def _renew_once(self) -> None:
        """Renew the lease with a conditional PUT keyed to our last-known ETag."""
        import botocore.exceptions

        with self._state_lock:
            etag = self._etag
        if etag is None:
            return

        try:
            resp = self.s3.put_object(
                Bucket=self.bucket,
                Key=self.key,
                Body=self.lock_id.encode('utf-8'),
                IfMatch=etag,
            )
            with self._state_lock:
                self._etag = resp.get('ETag')
            logger.debug(f"Renewed S3 lock at {self.key}")
        except botocore.exceptions.ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code in ('PreconditionFailed', '412', 'ConditionalRequestConflict', '404',
                              'NoSuchKey'):
                logger.warning(f"Lost S3 lock at {self.key} (stolen or expired). Stopping heartbeat.")
                self.is_locked = False
            else:
                logger.warning(f"Failed to renew S3 lock: {e}")


class S3PollingLockProvider(S3LockProviderBase):
    """S3-based distributed lock using polling (for S3 providers without conditional writes).

    This is a fallback for S3 providers like OVH that don't support If-None-Match
    headers. Uses a check-then-write approach with verification.

    WARNING - BEST-EFFORT ONLY: without conditional writes there are
    interleavings (delayed PUTs landing after another writer's verification
    read) in which two processes both believe they hold the lock, and the
    stale-lock break below has a window in which it can delete a freshly
    acquired lock. MetadataManager's pre-commit ownership re-check narrows but
    cannot close these windows. Do not rely on this provider where a lost
    commit is unacceptable.

    Set DATASHARD_S3_USE_CONDITIONAL_WRITES=false to use this provider.
    """

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

        # Step 2: Write our lock ID
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
        """Read-verify-then-write renewal (non-atomic; best this provider can do)."""
        import botocore.exceptions

        try:
            resp = self.s3.get_object(Bucket=self.bucket, Key=self.key)
            content = resp['Body'].read().decode('utf-8')

            if content != self.lock_id:
                logger.warning(f"Lost S3 lock at {self.key} (content mismatch). Stopping heartbeat.")
                self.is_locked = False
                return

            # Renew: Overwrite with same content to update LastModified
            self.s3.put_object(
                Bucket=self.bucket,
                Key=self.key,
                Body=self.lock_id.encode('utf-8')
            )
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
