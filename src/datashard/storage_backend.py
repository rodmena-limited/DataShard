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

import json
import os
import tempfile
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from .disk_utils import check_disk_space, estimate_write_size
from .integrity import IntegrityChecker
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


class CASConflictError(Exception):
    """Raised when a compare-and-swap write loses the race (precondition failed)."""

    pass


class LocalStorageBackend(StorageBackend):
    """Local filesystem storage backend"""

    def __init__(self, base_path: str):
        self.base_path = base_path

    def _real_base_path(self) -> str:
        """Canonical (symlink-resolved) table root.

        EVERY path computation - containment checks and the table-relative
        paths returned by list_files - must agree on one canonical base.
        Mixing the raw base with a realpath-resolved tree yields '../' paths
        that no reachability set can match, which is how GC once classified
        live files as orphans (#45). Resolved on each call so a base directory
        created (or re-pointed) after construction is still handled correctly.
        """
        return os.path.realpath(self.base_path)

    def _resolve_path(self, path: str) -> str:
        """Resolve path relative to base_path, rejecting escapes from the table root.

        Uses realpath (resolving symlinks) plus a true path-boundary check, so
        neither '..' components, sibling directories sharing the base as a
        string prefix (/data/wh vs /data/wh2), nor symlinks pointing outside
        the table can escape the base directory.
        """
        if path.startswith("/"):
            # Iceberg-style absolute path relative to table
            joined_path = os.path.join(self.base_path, path.lstrip("/"))
        elif os.path.isabs(path):
            # True system absolute path - treat as relative to base for security
            # This handles cases where a user might provide "/etc/passwd"
            joined_path = os.path.join(self.base_path, path.lstrip("/"))
        else:
            # Relative path
            joined_path = os.path.join(self.base_path, path)

        # Canonicalize: resolve '..' AND symlinks
        full_path = os.path.realpath(joined_path)
        base_path = self._real_base_path()

        # Ensure the resolved path is within the base directory (true boundary
        # check, not a string prefix check)
        try:
            inside = os.path.commonpath([base_path, full_path]) == base_path
        except ValueError:
            # Different drives (Windows) or mixed abs/rel - definitely outside
            inside = False
        if not inside:
            raise ValueError(f"Security Error: Path traversal attempt detected. Resolved path '{full_path}' is outside base directory '{base_path}'")

        return full_path

    def read_file(self, path: str) -> bytes:
        full_path = self._resolve_path(path)
        with open(full_path, "rb") as f:
            return f.read()

    def open_file(self, path: str) -> Any:
        """Open local file for reading as a stream."""
        full_path = self._resolve_path(path)
        return open(full_path, "rb")

    def open_seekable(self, path: str) -> Any:
        """Local files are already seekable; nothing to wrap."""
        return open(self._resolve_path(path), "rb")

    def write_file(self, path: str, content: bytes) -> None:
        """Atomically write file with fsync for durability.

        Uses temp file + fsync + atomic rename pattern to ensure:
        1. No partial writes visible to readers
        2. Crash during write doesn't corrupt existing file
        3. Data is persisted to disk before success is reported

        PHASE 2 improvements:
        - Disk space checking before write
        - Comprehensive logging
        - Checksum computation for integrity
        """
        logger.debug(f"Writing file: {path} ({len(content)} bytes)")

        full_path = self._resolve_path(path)
        dir_path = os.path.dirname(full_path)
        os.makedirs(dir_path, exist_ok=True)

        # PHASE 2: Check disk space before writing
        required_space = estimate_write_size(content)
        try:
            check_disk_space(dir_path, required_space)
        except IOError as e:
            logger.error(f"Disk space check failed for {path}: {e}")
            raise

        # PHASE 2: Compute checksum for integrity verification
        checksum = IntegrityChecker.compute_checksum(content)
        logger.debug(f"Computed checksum for {path}: {checksum[:16]}...")

        # Create temp file in same directory for atomic rename
        # Using same filesystem ensures os.replace() is atomic
        fd, temp_path = tempfile.mkstemp(
            dir=dir_path,
            prefix=".tmp.",
            suffix=f".{os.path.basename(full_path)}"
        )

        try:
            # Write ALL of the content: a single os.write() may write fewer bytes
            # than asked (large buffers, network filesystems, signals), and a short
            # metadata file would have passed fsync + rename unnoticed (#74).
            view = memoryview(content)
            while view:
                written = os.write(fd, view)
                view = view[written:]
            if os.fstat(fd).st_size != len(content):
                raise IOError(
                    f"Short write to {temp_path}: {os.fstat(fd).st_size} of {len(content)} bytes"
                )

            # Ensure data is written to disk (durability guarantee)
            os.fsync(fd)

            # Close file descriptor
            os.close(fd)

            # Atomic rename - makes new content visible atomically
            # os.replace() is atomic on both POSIX and Windows
            os.replace(temp_path, full_path)

            # Sync directory to ensure rename is persisted
            # This is critical for crash recovery
            try:
                dir_fd = os.open(dir_path, os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except (OSError, AttributeError):
                # Some filesystems/OSes don't support directory fsync
                # This is acceptable - the file fsync is the critical part
                pass

        except Exception:
            # Clean up temp file on any error
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception:
                    # Ignore cleanup errors - we're already handling an exception
                pass
            raise

        logger.debug(f"Successfully wrote file: {path}")

    @property
    def atomic_write_failures(self) -> bool:
        """Local writes go through temp file + os.replace: an exception means the
        rename never happened, so a failed write is guaranteed not visible."""
        return True

    def read_json(self, path: str) -> Dict[str, Any]:
        content = self.read_file(path)
        parsed: Dict[str, Any] = json.loads(content.decode("utf-8"))
        return parsed

    def write_json(self, path: str, data: Dict[str, Any]) -> None:
        """Atomically write JSON file with fsync.

        JSON serialization happens before any file I/O to minimize
        time spent with file handles open.
        """
        content = json.dumps(data, indent=2).encode("utf-8")
        self.write_file(path, content)  # Uses atomic write

    def exists(self, path: str) -> bool:
        full_path = self._resolve_path(path)
        return os.path.exists(full_path)

    def list_files(self, prefix: str) -> List[str]:
        """List files under `prefix`, as paths relative to the table root.

        The walk starts from the CANONICAL (symlink-resolved) prefix, so the
        returned paths must be computed against the CANONICAL base as well.
        Using the raw base here made every path under a symlinked table root
        come back as '../real_table/...', which never matches the reachability
        set the garbage collector compares against - so GC deleted the entire
        live table (#45).
        """
        full_prefix = self._resolve_path(prefix)
        if not os.path.exists(full_prefix):
            return []

        base_path = self._real_base_path()
        result = []
        for root, _dirs, files in os.walk(full_prefix):
            for file in files:
                full_path = os.path.join(root, file)
                # Return path relative to the canonical base_path
                rel_path = os.path.relpath(full_path, base_path)
                # Defence in depth: never hand out a path that escapes the
                # table root. os.walk does not follow directory symlinks, so
                # this is unreachable in practice - if it ever fires, listing
                # is not trustworthy and callers (GC) must not act on it.
                if rel_path == os.pardir or rel_path.startswith(os.pardir + os.sep):
                    raise ValueError(
                        f"Security Error: listing under '{prefix}' produced a path outside "
                        f"the table root: '{rel_path}' (resolved '{full_path}', base '{base_path}')"
                    )
                result.append(rel_path)
        return result

    def delete_file(self, path: str) -> None:
        full_path = self._resolve_path(path)
        if os.path.exists(full_path):
            os.remove(full_path)

    def makedirs(self, path: str, exist_ok: bool = True) -> None:
        full_path = self._resolve_path(path)
        os.makedirs(full_path, exist_ok=exist_ok)

    def get_size(self, path: str) -> int:
        full_path = self._resolve_path(path)
        return os.path.getsize(full_path)

    def get_modified_time(self, path: str) -> float:
        full_path = self._resolve_path(path)
        return os.path.getmtime(full_path)

    def create_lock(self, path: str, timeout: float = 30.0) -> "LockProvider":
        from .lock_provider import LocalLockProvider
        full_path = self._resolve_path(path)
        return LocalLockProvider(full_path, timeout)


def create_storage_backend(table_path: str) -> StorageBackend:
    """
    Create storage backend based on environment configuration.

    Environment variables:
        DATASHARD_STORAGE_TYPE: "local" (default) or "s3"

        For S3:
            DATASHARD_S3_ENDPOINT: S3 endpoint URL (optional, for MinIO/custom endpoints)
            DATASHARD_S3_ACCESS_KEY: AWS access key
            DATASHARD_S3_SECRET_KEY: AWS secret key
            DATASHARD_S3_BUCKET: S3 bucket name
            DATASHARD_S3_REGION: AWS region (default: us-east-1)
            DATASHARD_S3_PREFIX: Optional prefix for all objects (default: "")
            DATASHARD_S3_USE_CONDITIONAL_WRITES: unset (default) = probe the
                provider once and use CAS locking when it honours conditional PUTs
                (AWS S3, OVH Object Storage, MinIO and most others do); "true" forces
                CAS; "false" selects the best-effort polling lock, which REQUIRES
                DATASHARD_S3_ALLOW_UNSAFE_LOCK=1 because it can lose commits (#59).

    Args:
        table_path: Table location (local path or S3-style identifier)

    Returns:
        StorageBackend instance
    """
    storage_type = os.getenv("DATASHARD_STORAGE_TYPE", "local").lower()

    if storage_type == "s3":
        # S3 configuration
        bucket = os.getenv("DATASHARD_S3_BUCKET")
        if not bucket:
            raise ValueError("DATASHARD_S3_BUCKET environment variable is required for S3 storage")

        endpoint_url = os.getenv("DATASHARD_S3_ENDPOINT")
        access_key = os.getenv("DATASHARD_S3_ACCESS_KEY")
        secret_key = os.getenv("DATASHARD_S3_SECRET_KEY")
        region = os.getenv("DATASHARD_S3_REGION", "us-east-1")
        env_prefix = os.getenv("DATASHARD_S3_PREFIX", "")
        raw_cas = os.getenv("DATASHARD_S3_USE_CONDITIONAL_WRITES", "").strip().lower()
        use_conditional_writes: Optional[bool] = (
            None if raw_cas == "" else raw_cas in ("true", "1", "yes")
        )
        allow_unsafe_lock = os.getenv("DATASHARD_S3_ALLOW_UNSAFE_LOCK", "").strip().lower() in (
            "true", "1", "yes",
        )

        # Combine environment prefix with table path for full S3 prefix
        # table_path is the logical location of the table (e.g., "logs/workflow_logs")
        table_prefix = table_path.strip("/")
        if env_prefix and table_prefix:
            full_prefix = f"{env_prefix.rstrip('/')}/{table_prefix}"
        elif env_prefix:
            full_prefix = env_prefix.rstrip("/")
        elif table_prefix:
            full_prefix = table_prefix
        else:
            full_prefix = ""

        # Validate credentials - Optional, falls back to IAM/Env if missing
        if not (access_key and secret_key):
            logger.info("No explicit S3 credentials provided. Using default AWS credential chain (IAM Role, Env Vars, etc.)")

        from .s3_backend import S3StorageBackend

        return S3StorageBackend(
            bucket=bucket,
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
            region=region,
            prefix=full_prefix,
            use_conditional_writes=use_conditional_writes,
            allow_unsafe_lock=allow_unsafe_lock,
        )
    else:
        # Local filesystem (default)
        return LocalStorageBackend(table_path)


_S3_NAMES = {"S3StorageBackend", "S3RangeFile", "S3FileStream", "UNSAFE_LOCK_MESSAGE", "BOTO3_AVAILABLE"}


def __getattr__(name: str) -> Any:
    """The S3 classes moved to s3_backend / s3_range_file (#71); keep the old import path."""
    if name in _S3_NAMES:
        from . import s3_backend

        return getattr(s3_backend, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
