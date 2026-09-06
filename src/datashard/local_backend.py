"""
Local filesystem storage backend (split out of storage_backend.py for the 500-line
file cap). Every path is resolved and boundary-checked against the table root (#45,
#47); writes are temp file + fsync + atomic rename, and the commit point is a hard
link that fails with EEXIST when another committer won the version (#86).
"""

import errno
import json
import os
import tempfile
from typing import TYPE_CHECKING, Any, Dict, List, Tuple

from .disk_utils import check_disk_space, estimate_write_size
from .integrity import IntegrityChecker
from .logging_config import get_logger
from .storage_backend import CASConflictError, StorageBackend

if TYPE_CHECKING:
    from .lock_provider import LockProvider

logger = get_logger(__name__)


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

    def create_exclusive(self, path: str, content: bytes) -> None:
        """Create-if-absent via a hard link: the temp file is written and fsynced,
        then os.link()ed to the final name. EEXIST means another committer won -
        the link never happened and their file is untouched. A crash can never
        leave a truncated file at the final name (#86)."""
        full_path = self._resolve_path(path)
        dir_path = os.path.dirname(full_path)
        os.makedirs(dir_path, exist_ok=True)
        check_disk_space(dir_path, estimate_write_size(content))
        fd, temp_path = tempfile.mkstemp(dir=dir_path, prefix=".tmp.", suffix=f".{os.path.basename(full_path)}")
        try:
            view = memoryview(content)
            while view:
                view = view[os.write(fd, view):]
            os.fsync(fd)
            os.close(fd)
            fd = -1
            try:
                os.link(temp_path, full_path)
            except FileExistsError as e:
                raise CASConflictError(f"{path} already exists") from e
            except OSError as e:
                if e.errno not in (errno.EPERM, errno.EACCES, errno.ENOSYS, errno.EXDEV, errno.EMLINK):
                    raise
                # No hard links here (some FUSE / SMB / overlay mounts). O_CREAT|O_EXCL is
                # still an atomic create, but the file exists before its bytes do: a crash
                # mid-write can leave a truncated v{N} that an operator must remove.
                logger.warning(
                    f"{dir_path} does not support hard links ({e.strerror}); using an "
                    f"exclusive open for the commit point, which is atomic but not crash-safe"
                )
                self._create_exclusive_open(full_path, content)
            self._fsync_dir(dir_path)
        finally:
            if fd >= 0:
                os.close(fd)
            try:
                os.remove(temp_path)
            except OSError:
                pass

    @staticmethod
    def _create_exclusive_open(full_path: str, content: bytes) -> None:
        """Exclusive create without hard links (fallback path only)."""
        try:
            fd = os.open(full_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError as e:
            raise CASConflictError(f"{full_path} already exists") from e
        try:
            view = memoryview(content)
            while view:
                view = view[os.write(fd, view):]
            os.fsync(fd)
        finally:
            os.close(fd)

    @staticmethod
    def _fsync_dir(dir_path: str) -> None:
        try:
            dir_fd = os.open(dir_path, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except (OSError, AttributeError):
            pass  # some filesystems/OSes do not support directory fsync

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

    def list_files_with_mtime(self, prefix: str) -> List[Tuple[str, float]]:
        """Walk once and stat each file in place (no second resolve per file, #77)."""
        full_prefix = self._resolve_path(prefix)
        if not os.path.exists(full_prefix):
            return []
        base_path = self._real_base_path()
        result: List[Tuple[str, float]] = []
        for root, _dirs, files in os.walk(full_prefix):
            for file in files:
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, base_path)
                if rel_path == os.pardir or rel_path.startswith(os.pardir + os.sep):
                    raise ValueError(
                        f"Security Error: listing under '{prefix}' produced a path outside "
                        f"the table root: '{rel_path}' (resolved '{full_path}', base '{base_path}')"
                    )
                try:
                    mtime = os.path.getmtime(full_path)
                except FileNotFoundError:
                    continue  # removed between walk and stat (a concurrent GC or rollback)
                result.append((rel_path, mtime))
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
