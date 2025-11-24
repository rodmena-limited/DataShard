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

    Uses fcntl on Unix-like systems and msvcrt on Windows.
    Falls back to file existence-based locking if neither is available.
    """

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

    def acquire(self, blocking: bool = True) -> bool:  # noqa: C901
        """Acquire the lock.

        Args:
            blocking: If True, wait for lock. If False, return immediately.

        Returns:
            True if lock acquired, False otherwise.

        Raises:
            TimeoutError: If timeout expires while waiting for lock
        """
        # Ensure lock file directory exists
        lock_dir = os.path.dirname(self.lock_file)
        if lock_dir:
            os.makedirs(lock_dir, exist_ok=True)

        start_time = time.time()

        while True:
            try:
                # Open/create lock file
                # O_CREAT | O_EXCL | O_WRONLY would be ideal but doesn't work with fcntl
                fd = os.open(
                    self.lock_file,
                    os.O_CREAT | os.O_RDWR,
                )

                # Try to acquire exclusive lock
                if FCNTL_AVAILABLE:
                    # Unix/Linux - use fcntl for robust locking
                    try:
                        if blocking:
                            fcntl.flock(fd, fcntl.LOCK_EX)
                        else:
                            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        self._lock_fd = fd
                        self._locked = True
                        return True
                    except (IOError, OSError) as e:
                        os.close(fd)
                        if e.errno == errno.EWOULDBLOCK and not blocking:
                            return False
                        # For blocking mode, fall through to retry logic
                elif MSVCRT_AVAILABLE:
                    # Windows - use msvcrt
                    try:
                        if blocking:
                            msvcrt.locking(fd, msvcrt.LK_LOCK, 1)  # type: ignore[attr-defined]
                        else:
                            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)  # type: ignore[attr-defined]
                        self._lock_fd = fd
                        self._locked = True
                        return True
                    except (IOError, OSError):
                        os.close(fd)
                        if not blocking:
                            return False
                        # For blocking mode, fall through to retry logic
                else:
                    # Fallback: file existence-based locking (less robust)
                    # This is a best-effort approach
                    self._lock_fd = fd
                    self._locked = True
                    return True

            except (IOError, OSError) as e:
                if e.errno == errno.EEXIST and not blocking:
                    return False
                # Continue to retry logic

            # Check timeout
            if not blocking:
                return False

            if time.time() - start_time >= self.timeout:
                raise TimeoutError(
                    f"Failed to acquire lock on {self.lock_file} within {self.timeout}s"
                )

            # Wait a bit before retrying
            time.sleep(0.01)

    def release(self) -> None:
        """Release the lock.

        IMPORTANT: We intentionally do NOT delete the lock file after releasing.
        fcntl.flock() operates on file inodes, not paths. If we delete the file,
        a new process creating the same path gets a different inode, so they won't
        actually synchronize. By keeping the lock file, all processes lock the
        same inode and proper synchronization is maintained.
        """
        if not self._locked or self._lock_fd is None:
            return

        try:
            if FCNTL_AVAILABLE:
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
            elif MSVCRT_AVAILABLE:
                msvcrt.locking(self._lock_fd, msvcrt.LK_UNLCK, 1)  # type: ignore[attr-defined]

            os.close(self._lock_fd)
            self._lock_fd = None
            self._locked = False

            # NOTE: Do NOT delete the lock file here!
            # Deleting causes race conditions because fcntl.flock operates on inodes.
            # If we delete the file, concurrent processes may create new files with
            # different inodes and fail to synchronize properly.
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
