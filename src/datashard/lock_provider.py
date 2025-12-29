"""
Abstract lock provider and implementations for Local/S3.
"""

import logging
import random
import threading
import time
import uuid
from abc import ABC, abstractmethod
from typing import Any

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

class LocalLockProvider(LockProvider):
    """Local filesystem lock using flock/msvcrt."""

    def __init__(self, lock_path: str, timeout: float = 30.0):
        self.lock = FileLock(lock_path, timeout)

    def acquire(self) -> bool:
        return self.lock.acquire()

    def release(self) -> None:
        self.lock.release()

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

    def acquire(self) -> bool:
        start_time = time.time()
        while True:
            # 1. Try to acquire lock
            if self._try_acquire():
                self.is_locked = True
                self._start_heartbeat()
                return True

            # 2. Check if existing lock is expired (deadlock prevention)
            self._check_and_break_expired_lock()

            # 3. Check timeout
            if time.time() - start_time >= self.timeout:
                raise TimeoutError(f"Failed to acquire S3 lock at {self.key} within {self.timeout}s")

            # Wait with jitter before retrying
            time.sleep(0.5 + (time.time() % 0.5))

    def _try_acquire(self) -> bool:
        """Subclasses must implement the actual lock acquisition logic."""
        raise NotImplementedError

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

    def _heartbeat_loop(self) -> None:
        """Periodically renew the lock lease."""
        import botocore.exceptions

        # Renew every 1/3 of the lease time to be safe
        interval = self.lease_seconds / 3.0

        while not self._stop_heartbeat.wait(interval):
            if not self.is_locked:
                break

            try:
                # Verify we still own the lock by checking content
                # Optimization: We could skip this and just write if we are confident,
                # but checking guards against split-brain if we were partitioned.
                resp = self.s3.get_object(Bucket=self.bucket, Key=self.key)
                content = resp['Body'].read().decode('utf-8')

                if content != self.lock_id:
                    logger.warning(f"Lost S3 lock at {self.key} (content mismatch). Stopping heartbeat.")
                    self.is_locked = False
                    break

                # Renew: Overwrite with same content to update LastModified
                self.s3.put_object(
                    Bucket=self.bucket,
                    Key=self.key,
                    Body=self.lock_id.encode('utf-8')
                )
                logger.debug(f"Renewed S3 lock at {self.key}")

            except botocore.exceptions.ClientError as e:
                logger.warning(f"Failed to renew S3 lock: {e}")
                # If 404, we lost it.
                error_code = e.response.get('Error', {}).get('Code', '')
                if error_code == '404':
                    self.is_locked = False
                    break
            except Exception as e:
                logger.error(f"Unexpected error in lock heartbeat: {e}")

    def _check_and_break_expired_lock(self) -> bool:
        """Check if the lock file is older than lease_seconds. If so, delete it."""
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

    def release(self) -> None:
        if not self.is_locked:
            return

        # Stop heartbeat BEFORE deleting file
        self._stop_heartbeat_thread()

        import botocore.exceptions

        try:
            # Safe release: Check if we still own the lock
            # Optimization: We can just read it.
            # Ideally we would use delete_object with expected version/etag, but strict consistency
            # + unique lock_id check is sufficient for this implementation level.
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


class S3LockProvider(S3LockProviderBase):
    """S3-based distributed lock using conditional writes (If-None-Match).

    Requires S3 provider support for conditional PUT operations.
    For providers without this support (e.g., OVH), use S3PollingLockProvider.
    """

    def _try_acquire(self) -> bool:
        import botocore.exceptions
        try:
            # Conditional Write: Only succeed if object does NOT exist
            # We use If-None-Match: * to ensure we don't overwrite an existing lock
            self.s3.put_object(
                Bucket=self.bucket,
                Key=self.key,
                Body=self.lock_id.encode('utf-8'),
                IfNoneMatch='*'
            )
            return True
        except botocore.exceptions.ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code in ('PreconditionFailed', '412'):
                # Lock already exists
                return False
            raise e


class S3PollingLockProvider(S3LockProviderBase):
    """S3-based distributed lock using polling (for S3 providers without conditional writes).

    This is a fallback for S3 providers like OVH that don't support If-None-Match headers.
    Uses a check-then-write approach with verification, which is less robust than
    conditional writes but provides best-effort locking.

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
