"""
Version-hint handling for MetadataManager: parse / read / recover / repair
(mixed into MetadataManager; split out of metadata_manager.py, #71).

The hint is a POINTER, the metadata files are the truth (#22); recovery from a
lost hint is automatic only when unambiguous (#60).
"""

import re
import threading
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from .data_structures import TableMetadata
from .exceptions import AmbiguousMetadataError
from .logging_config import get_logger

if TYPE_CHECKING:
    from .lock_provider import LockProvider
    from .storage_backend import StorageBackend

logger = get_logger(__name__)

# Matches both legacy (v3.metadata.json) and current (v3-1a2b3c4d.metadata.json) names
_METADATA_FILE_RE = re.compile(r"^v(\d+)(?:-[0-9a-f]{8})?\.metadata\.json$")


class _VersionHintMixin:
    HINT_PATH: str
    table_path: str
    metadata_path: str
    storage: "StorageBackend"
    lock_provider: "LockProvider"
    current_version: int
    _lock: threading.RLock

    if TYPE_CHECKING:  # provided by MetadataManager

        def _read_metadata_file(self, path: str) -> TableMetadata: ...

        def _release_lock_safely(self) -> None: ...

    def repair_version_hint(self, metadata_file: str) -> TableMetadata:
        """Operator action after AmbiguousMetadataError: point the version hint at
        `metadata_file` (a bare filename under metadata/, e.g. 'v7-1a2b3c4d.metadata.json').

        The file must exist and parse. The choice is logged at WARNING level.
        Returns the metadata that is now current.
        """
        name = metadata_file.replace("\\", "/").rsplit("/", 1)[-1]
        if not _METADATA_FILE_RE.match(name):
            raise ValueError(f"{metadata_file!r} is not a metadata file name (expected vN[-hex].metadata.json)")
        path = f"{self.metadata_path}/{name}"
        metadata = self._read_metadata_file(path)  # raises if missing or corrupt
        with self._lock:
            self.lock_provider.acquire()
            try:
                self.storage.write_file(self.HINT_PATH, name.encode("utf-8"))
                self.current_version = int(_METADATA_FILE_RE.match(name).group(1))  # type: ignore[union-attr]
            finally:
                self._release_lock_safely()
        logger.warning(f"Version hint for {self.table_path} repaired by operator to {name}")
        return metadata


    @staticmethod
    def _parse_hint_content(content: bytes) -> Optional[Tuple[int, str]]:
        """Parse hint file content into (version, metadata_filename).

        Supports the legacy format (bare version number) and the current
        format (full metadata filename). Returns None if unparseable.
        """
        try:
            text = content.decode("utf-8").strip()
        except UnicodeDecodeError:
            return None
        if not text:
            return None
        if text.isdigit():
            # Legacy format: plain version number -> legacy filename
            return int(text), f"v{text}.metadata.json"
        m = _METADATA_FILE_RE.match(text)
        if m:
            return int(m.group(1)), text
        return None

    def _read_version_hint(self) -> Optional[Tuple[int, str]]:
        """Read (version, metadata_filename) from the hint file, or None."""
        try:
            content = self.storage.read_file(self.HINT_PATH)
        except FileNotFoundError:
            return None
        return self._parse_hint_content(content)

    def _recover_version_from_files(self) -> Optional[Tuple[int, str]]:
        """Recover the latest (version, filename) by scanning metadata files.

        Used when the hint is missing/corrupt/stale. Picks the highest version -
        but ONLY when exactly one file carries it. Several files at the same
        version mean one committed and the others were left by committers that
        failed before flipping the hint (lost race, crash); nothing in the files
        distinguishes them, and guessing by mtime once resurrected an uncommitted
        state and dropped committed rows (#60). That case raises
        AmbiguousMetadataError for an operator to resolve with repair_version_hint().
        """
        try:
            all_files = self.storage.list_files(self.metadata_path)
        except Exception:
            return None

        candidates: Dict[int, List[str]] = {}
        for rel_path in all_files:
            norm = rel_path.replace("\\", "/")
            basename = norm.rsplit("/", 1)[-1]
            # Only consider files directly in metadata/ (not metadata/manifests/...)
            parent = norm.rsplit("/", 1)[0] if "/" in norm else ""
            if parent not in ("", self.metadata_path):
                continue
            m = _METADATA_FILE_RE.match(basename)
            if not m:
                continue
            candidates.setdefault(int(m.group(1)), []).append(basename)

        if not candidates:
            return None
        top = max(candidates)
        names = sorted(candidates[top])
        if len(names) > 1:
            raise AmbiguousMetadataError(
                f"Version hint missing or invalid for {self.table_path} and {len(names)} "
                f"metadata files share the highest version {top}: {names}. One was committed "
                f"and the others were left by failed commits; refusing to guess. Identify the "
                f"committed one (e.g. from the writer's logs or the file contents) and call "
                f"Table.repair_version_hint(<filename>)."
            )
        logger.warning(
            f"Version hint missing or invalid for {self.table_path}; "
            f"recovered latest metadata {names[0]} by scanning"
        )
        return top, names[0]

    def _current_version_info(self) -> Optional[Tuple[int, str]]:
        """Resolve the current (version, metadata_filename).

        A parseable hint is trusted here without a separate existence probe (one
        round trip fewer, #67); refresh() falls back to scanning if the file it
        names turns out to be missing. No hint at all -> scan (#22).
        """
        hinted = self._read_version_hint()
        if hinted is not None:
            return hinted
        return self._recover_version_from_files()
