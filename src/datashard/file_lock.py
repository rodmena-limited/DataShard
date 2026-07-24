"""
Multi-process file-based locking for datashard.

Provides cross-process synchronization using file-based locks compatible with
both local filesystem and network filesystems (NFS, etc.).
"""

import errno
import os
import time
from contextlib import contextmanager
from typing import Any, Generator, Optional

from .logging_config import get_logger

logger = get_logger(__name__)

try:
    import fcntl

    FCNTL_AVAILABLE = True
except ImportError:
    FCNTL_AVAILABLE = False

try:
    import msvcrt

    MSVCRT_AVAILABLE = True
except ImportError:
    MSVCRT_AVAILABLE = False


class FileLock:
    """File-based lock for multi-process synchronization.

    Uses fcntl on Unix-like systems and msvcrt on Windows. On platforms with
    neither primitive, falls back to O_CREAT|O_EXCL existence locking with
    stale-lock breaking (weaker, but still provides mutual exclusion).

    The configured timeout is enforced on ALL paths, including blocking
    acquisition: acquisition is implemented as a non-blocking attempt loop
    with a deadline, so a wedged lock holder cannot block callers forever
    beyond the timeout.
    """

    # Poll interval between non-blocking acquisition attempts
    _POLL_INTERVAL = 0.01
    # Fallback (O_EXCL) locks older than timeout * this factor are considered stale
    _STALE_FACTOR = 10.0

    def __init__(self, lock_file: str, timeout: float = 30.0):
        """Initialize file lock.

        Args:
            lock_file: Path to lock file
            timeout: Maximum time to wait for lock acquisition (seconds)
        """
        self.lock_file = lock_file
        self.timeout = timeout
        self._lock_fd: Optional[int] = None
        self._locked = False
        self._used_excl_fallback = False

    def is_held(self) -> bool:
        """Whether this instance currently holds the lock."""
        return self._locked

    def acquire(self, blocking: bool = True) -> bool:
        """Acquire the lock.

        Args:
            blocking: If True, wait for lock up to the configured timeout.
                If False, return immediately.

        Returns:
            True if lock acquired, False if non-blocking and lock unavailable.

        Raises:
            TimeoutError: If the timeout expires while waiting for the lock.
        """
        # Ensure lock file directory exists
        lock_dir = os.path.dirname(self.lock_file)
        if lock_dir:
            os.makedirs(lock_dir, exist_ok=True)

        deadline = time.monotonic() + self.timeout

        while True:
            acquired = self._try_acquire_once()
            if acquired:
                return True

            if not blocking:
                return False

            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Failed to acquire lock on {self.lock_file} within {self.timeout}s"
                )

            time.sleep(self._POLL_INTERVAL)

    def _try_acquire_once(self) -> bool:
        """Single non-blocking acquisition attempt."""
        if FCNTL_AVAILABLE or MSVCRT_AVAILABLE:
            try:
                fd = os.open(self.lock_file, os.O_CREAT | os.O_RDWR)
            except (IOError, OSError):
                return False

            try:
                if FCNTL_AVAILABLE:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                else:
                    msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)  # type: ignore[attr-defined]
                self._lock_fd = fd
                self._locked = True
                self._used_excl_fallback = False
                return True
            except (IOError, OSError):
                os.close(fd)
                return False

        # Fallback: O_CREAT|O_EXCL existence locking with stale-lock breaking.
        # Weaker than kernel locks (no automatic release on crash), but still
        # provides real mutual exclusion instead of silently degrading to none.
        return self._try_acquire_excl_fallback()

    def _try_acquire_excl_fallback(self) -> bool:
        """Existence-based locking for platforms without fcntl/msvcrt."""
        try:
            fd = os.open(self.lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode("utf-8"))
            self._lock_fd = fd
            self._locked = True
            self._used_excl_fallback = True
            return True
        except (IOError, OSError) as e:
            if e.errno != errno.EEXIST:
                return False
            # Lock file exists - break it only if it looks abandoned
            try:
                age = time.time() - os.path.getmtime(self.lock_file)
                if age > self.timeout * self._STALE_FACTOR:
                    logger.warning(
                        f"Breaking stale fallback lock {self.lock_file} (age {age:.0f}s)"
                    )
                    os.unlink(self.lock_file)
            except (IOError, OSError):
                pass
            return False

    def release(self) -> None:
        """Release the lock.

        IMPORTANT (fcntl/msvcrt mode): We intentionally do NOT delete the lock
        file after releasing. fcntl.flock() operates on file inodes, not paths.
        If we delete the file, a new process creating the same path gets a
        different inode, so they won't actually synchronize. By keeping the
        lock file, all processes lock the same inode and proper
        synchronization is maintained.

        In O_EXCL fallback mode the file's existence IS the lock, so there we
        do delete it.
        """
        if not self._locked or self._lock_fd is None:
            return

        try:
            if self._used_excl_fallback:
                os.close(self._lock_fd)
                try:
                    os.unlink(self.lock_file)
                except (IOError, OSError):
                    pass
            else:
                if FCNTL_AVAILABLE:
                    fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
                elif MSVCRT_AVAILABLE:
                    msvcrt.locking(self._lock_fd, msvcrt.LK_UNLCK, 1)  # type: ignore[attr-defined]
                os.close(self._lock_fd)

            self._lock_fd = None
            self._locked = False
        except Exception:
            # Best effort cleanup
            pass

    def __enter__(self) -> "FileLock":
        """Context manager entry."""
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.release()

    def __del__(self) -> None:
        """Ensure lock is released when object is garbage collected."""
        if self._locked:
            self.release()


@contextmanager
def file_lock(lock_file: str, timeout: float = 30.0) -> Generator[FileLock, None, None]:
    """Context manager for file-based locking.

    Usage:
        with file_lock("/path/to/lock"):
            # Critical section
            pass
    """
    lock = FileLock(lock_file, timeout)
    lock.acquire()
    try:
        yield lock
    finally:
        lock.release()
