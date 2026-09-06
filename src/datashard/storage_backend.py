"""
Storage backend abstraction for DataShard.

Supports both local filesystem and S3-compatible storage (AWS S3, MinIO, etc.)
Configuration via environment variables:

Local filesystem (default):
    No configuration needed

S3-compatible storage:
    DATASHARD_STORAGE_TYPE=s3
    DATASHARD_S3_ENDPOINT=https://s3.amazonaws.com (or MinIO endpoint)
    DATASHARD_S3_ACCESS_KEY=your-access-key
    DATASHARD_S3_SECRET_KEY=your-secret-key
    DATASHARD_S3_BUCKET=your-bucket-name
    DATASHARD_S3_REGION=us-east-1
    DATASHARD_S3_PREFIX=optional/prefix/ (optional, default: "")
"""

import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from .logging_config import get_logger

if TYPE_CHECKING:
    from .lock_provider import LockProvider

logger = get_logger(__name__)


class StorageBackend(ABC):
    """Abstract base class for storage backends"""

    @abstractmethod
    def read_file(self, path: str) -> bytes:
        """Read file contents as bytes"""
        pass

    def open_seekable(self, path: str) -> Any:
        """Open `path` as a SEEKABLE binary file object.

        Parquet needs to seek (its schema is in the footer), and pyarrow's own
        S3 filesystem cannot read from every S3-compatible provider — see
        S3RangeFile (#54). Reading through the backend keeps one HTTP client in
        the picture instead of two with different compatibility.

        Deliberately NOT @abstractmethod: making it abstract would stop any
        third-party StorageBackend subclass from instantiating at all, turning a
        bug fix into a breaking change. Subclasses that do not override it fail
        loudly when a parquet read is attempted, not at construction.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement open_seekable(); "
            f"parquet reads require a seekable file object"
        )

    @abstractmethod
    def open_file(self, path: str) -> Any:
        """Open file as a binary stream context manager"""
        pass

    @abstractmethod
    def write_file(self, path: str, content: bytes) -> None:
        """Write bytes to file"""
        pass

    def write_files(self, items: List[Tuple[str, bytes]]) -> None:
        """Write several independent files; backends with latency per request do it
        concurrently (#80). Every write completes (or raises) before this returns.
        """
        for path, content in items:
            self.write_file(path, content)

    @abstractmethod
    def read_json(self, path: str) -> Dict[str, Any]:
        """Read JSON file"""
        pass

    @abstractmethod
    def write_json(self, path: str, data: Dict[str, Any]) -> None:
        """Write JSON to file"""
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if file exists"""
        pass

    @abstractmethod
    def list_files(self, prefix: str) -> List[str]:
        """List files with given prefix"""
        pass

    @abstractmethod
    def delete_file(self, path: str) -> None:
        """Delete file"""
        pass

    def delete_files(self, paths: List[str]) -> None:
        """Delete several files; backends with a bulk primitive override this (#67)."""
        for path in paths:
            self.delete_file(path)

    @abstractmethod
    def makedirs(self, path: str, exist_ok: bool = True) -> None:
        """Create directory (no-op for S3)"""
        pass

    @abstractmethod
    def get_size(self, path: str) -> int:
        """Get file size in bytes"""
        pass

    @abstractmethod
    def get_modified_time(self, path: str) -> float:
        """Get file modification time as unix timestamp"""
        pass

    def list_files_with_mtime(self, prefix: str) -> List[Tuple[str, float]]:
        """(table-relative path, modification time) for every file under prefix.

        Backends whose listing already carries timestamps override this so a
        garbage-collection sweep costs O(listing pages), not one stat per object (#77).
        """
        return [(p, self.get_modified_time(p)) for p in self.list_files(prefix)]

    def clock_ms(self) -> float:
        """The clock modification times are measured on, in ms since the epoch.

        Age decisions must compare like with like: for local files that is this
        host's clock; an object store reports ITS clock, which S3StorageBackend
        reads from a response header. A GC host running fast once deleted a commit
        that landed during the run because it compared its own clock with the
        server's LastModified (#77).
        """
        return time.time() * 1000

    @abstractmethod
    def create_lock(self, path: str, timeout: float = 30.0) -> "LockProvider":
        """Create a distributed lock for the given path"""
        pass

    @property
    def supports_cas(self) -> bool:
        """Whether this backend supports compare-and-swap writes (conditional PUT)."""
        return False

    @property
    def atomic_write_failures(self) -> bool:
        """Whether a write_file() exception guarantees the write did NOT become visible.

        True for local storage (temp file + atomic rename: an exception means the
        rename never happened). False for S3 (a PUT that raises client-side may
        still have succeeded server-side), where a failed critical write must be
        treated as ambiguous.
        """
        return False

    def read_file_with_etag(self, path: str) -> "Tuple[bytes, Optional[str]]":
        """Read file contents plus an entity tag usable for CAS (None if unsupported)."""
        return self.read_file(path), None

    def write_file_cas(self, path: str, content: bytes, etag: Optional[str]) -> None:
        """Conditionally write: succeed only if the object's current etag matches.

        etag=None means "create only if absent". Raises CASConflictError on
        precondition failure. Only valid when supports_cas is True.
        """
        raise NotImplementedError("This backend does not support CAS writes")

    def create_exclusive(self, path: str, content: bytes) -> None:
        """Create `path` only if it does not exist yet - the commit point (#86).

        Raises CASConflictError when the file already exists. CAS backends use a
        conditional PUT; the local backend links a fsynced temp file into place;
        a backend with neither (S3 without conditional writes, only reachable with
        DATASHARD_S3_ALLOW_UNSAFE_LOCK=1) falls back to exists()+write, which is
        best-effort - as documented for that mode (#59).
        """
        if self.supports_cas:
            self.write_file_cas(path, content, etag=None)
            return
        if self.exists(path):
            raise CASConflictError(f"{path} already exists")
        self.write_file(path, content)


class CASConflictError(Exception):
    """Raised when a compare-and-swap write loses the race (precondition failed)."""

    pass


def create_storage_backend(table_path: str) -> StorageBackend:
    """Create the storage backend selected by the environment (see backend_factory)."""
    from .backend_factory import create_storage_backend as _factory

    return _factory(table_path)


_S3_NAMES = {"S3StorageBackend", "S3RangeFile", "S3FileStream", "UNSAFE_LOCK_MESSAGE", "BOTO3_AVAILABLE"}
_LOCAL_NAMES = {"LocalStorageBackend"}


def __getattr__(name: str) -> Any:
    """S3 classes live in s3_backend / s3_range_file and LocalStorageBackend in
    local_backend (#71, #85); the old import paths keep working."""
    if name in _S3_NAMES:
        from . import s3_backend

        return getattr(s3_backend, name)
    if name in _LOCAL_NAMES:
        from . import local_backend

        return getattr(local_backend, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
