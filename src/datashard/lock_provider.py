"""
Abstract lock provider and implementations for Local/S3.
"""

import time
import uuid
import logging
import os
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

class LocalLockProvider(LockProvider):
    """Local filesystem lock using flock/msvcrt."""
    
    def __init__(self, lock_path: str, timeout: float = 30.0):
        self.lock = FileLock(lock_path, timeout)
        
    def acquire(self) -> bool:
        return self.lock.acquire()
        
    def release(self) -> None:
        self.lock.release()

class S3LockProvider(LockProvider):
    """S3-based distributed lock using conditional writes (If-None-Match)."""
    
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
        
    def acquire(self) -> bool:
        start_time = time.time()
        while True:
            # 1. Try to acquire lock
            if self._try_acquire():
                self.is_locked = True
                return True
                
            # 2. Check if existing lock is expired (deadlock prevention)
            self._check_and_break_expired_lock()

            # 3. Check timeout
            if time.time() - start_time >= self.timeout:
                raise TimeoutError(f"Failed to acquire S3 lock at {self.key} within {self.timeout}s")
                
            # Wait with jitter before retrying
            time.sleep(0.5 + (time.time() % 0.5))
            
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

    def _check_and_break_expired_lock(self) -> bool:
        """Check if the lock file is older than lease_seconds. If so, delete it."""
        import botocore.exceptions
        from datetime import datetime, timezone
        
        try:
            resp = self.s3.head_object(Bucket=self.bucket, Key=self.key)
            last_modified = resp['LastModified']
            
            # S3 returns offset-aware datetime (usually UTC)
            now = datetime.now(timezone.utc)
            
            age = (now - last_modified).total_seconds()
            
            if age > self.lease_seconds:
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
