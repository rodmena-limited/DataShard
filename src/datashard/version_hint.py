"""
Version resolution for MetadataManager (#86): the commit point is the exclusive
creation of metadata/v{N}.metadata.json, so the version number IS the commit
identity and metadata/version-hint.text is a pointer readers start from - never
the truth. A hint that lags (a committer crashed between its metadata write and
the hint write) is healed by the next reader, which probes v{N+1}.

Legacy tables (root metadata.version-hint.text, v{N}-{hex} names or snake_case
documents) are detected and refused with the migration instruction (#89).
"""

import re
import threading
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from .data_structures import TableMetadata
from .exceptions import LegacyLayoutError
from .logging_config import get_logger

if TYPE_CHECKING:
    from .lock_provider import LockProvider
    from .storage_backend import StorageBackend

logger = get_logger(__name__)

# Iceberg-style metadata file written since 0.10
METADATA_FILE_RE = re.compile(r"^v(\d+)\.metadata\.json$")
# Names written by 0.8.0 - 0.9.x (0.7.x used the plain form with legacy content)
LEGACY_METADATA_FILE_RE = re.compile(r"^v(\d+)-[0-9a-f]{8}\.metadata\.json$")
# Matches both, for garbage collection of superseded versions
_METADATA_FILE_RE = re.compile(r"^v(\d+)(?:-[0-9a-f]{8})?\.metadata\.json$")

LEGACY_HINT_PATH = "metadata.version-hint.text"
MIGRATED_HINT_PATH = "metadata.version-hint.text.migrated"

# Bound on how far past the hint a reader probes (each probe is one request).
_MAX_PROBE_AHEAD = 10_000


def metadata_file_name(version: int) -> str:
    return f"v{version}.metadata.json"


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

    # ------------------------------------------------------------------ hint I/O

    @staticmethod
    def _parse_hint_content(content: bytes) -> Optional[int]:
        """The version a hint names: a plain integer ('7'), or a metadata file name
        ('v7.metadata.json', as some writers store). None if unparseable."""
        try:
            text = content.decode("utf-8").strip()
        except UnicodeDecodeError:
            return None
        if text.isdigit():
            return int(text)
        m = METADATA_FILE_RE.match(text.rsplit("/", 1)[-1])
        return int(m.group(1)) if m else None

    def _read_version_hint(self) -> Optional[int]:
        try:
            content = self.storage.read_file(self.HINT_PATH)
        except FileNotFoundError:
            return None
        return self._parse_hint_content(content)

    def _advance_hint(
        self,
        version: int,
        known: Optional[Tuple[Optional[int], Optional[str]]] = None,
    ) -> None:
        """Point the hint at `version` unless it already names a higher one.

        Best effort and never raises: after the commit point a hint failure must
        not turn a durable commit into a reported failure. On CAS backends the
        write is conditional on the hint's ETag so a slow writer cannot regress it;
        `known` is the (version, etag) a caller has just read, which saves the
        re-read on the commit path (one S3 round trip per commit).
        """
        content = str(version).encode("utf-8")
        try:
            if not self.storage.supports_cas:
                current = known[0] if known is not None else self._read_version_hint()
                if current is None or current < version:
                    self.storage.write_file(self.HINT_PATH, content)
                return
            from .storage_backend import CASConflictError

            for attempt in range(4):
                if attempt == 0 and known is not None:
                    current, etag = known
                else:
                    try:
                        raw, etag = self.storage.read_file_with_etag(self.HINT_PATH)
                        current = self._parse_hint_content(raw)
                    except FileNotFoundError:
                        current, etag = None, None
                if current is not None and current >= version:
                    return
                try:
                    self.storage.write_file_cas(self.HINT_PATH, content, etag)
                    return
                except CASConflictError:
                    continue  # someone else moved it; re-read and re-check
            logger.warning(f"Version hint for {self.table_path} not advanced to {version} after retries")
        except Exception as e:  # noqa: BLE001 - post-commit, must not raise
            logger.warning(
                f"Version hint for {self.table_path} could not be advanced to {version}: {e}. "
                f"datashard readers heal it; foreign readers may lag until the next commit."
            )

    def heal_hint(self, from_version: int) -> int:
        """Probe forward from `from_version` and move the hint to the highest version
        that exists. Called when a commit finds its target version already taken -
        the signature of a hint that lags behind a durable commit."""
        latest = self._probe_ahead(from_version)
        if latest != from_version:
            logger.warning(
                f"Version hint for {self.table_path} lagged at {from_version}; "
                f"latest committed version is {latest} (healing)"
            )
            self._advance_hint(latest)
        return latest

    # ------------------------------------------------------------ resolution

    def _metadata_versions_on_disk(self) -> Tuple[Dict[int, str], bool]:
        """({version: filename} for Iceberg-form names, legacy_names_present)."""
        try:
            listed = self.storage.list_files(self.metadata_path)
        except Exception:
            return {}, False
        versions: Dict[int, str] = {}
        legacy = False
        for rel in listed:
            norm = rel.replace("\\", "/")
            parent, _, base = norm.rpartition("/")
            if parent not in ("", self.metadata_path):
                continue
            m = METADATA_FILE_RE.match(base)
            if m:
                versions[int(m.group(1))] = base
            elif LEGACY_METADATA_FILE_RE.match(base):
                legacy = True
        return versions, legacy

    def _probe_ahead(self, version: int) -> int:
        """Highest version whose metadata file exists, starting from `version`."""
        v = version
        for _ in range(_MAX_PROBE_AHEAD):
            if not self.storage.exists(f"{self.metadata_path}/{metadata_file_name(v + 1)}"):
                return v
            v += 1
        return v

    def _legacy_layout_present(self) -> bool:
        if self.storage.exists(LEGACY_HINT_PATH):
            return True
        _versions, legacy = self._metadata_versions_on_disk()
        return legacy

    def _raise_legacy(self, detail: str) -> None:
        raise LegacyLayoutError(
            f"Table {self.table_path} uses the pre-0.10 datashard layout ({detail}). "
            f"Run 'datashard migrate {self.table_path}' once (or "
            f"datashard.migrate_table(path)); the table then becomes an Iceberg v2 table "
            f"readable by DuckDB, pyiceberg, Spark and Trino. There is no downgrade."
        )

    def _current_version_info(self, probe: bool = True) -> Optional[Tuple[int, str]]:
        """(version, filename) of the current metadata, or None for no table.

        Hint first (one read). READERS then probe for v{N+1} (one existence check)
        so a hint that lags a durable commit - a writer that died between its
        metadata write and its hint write - can never hide committed rows; the
        healed hint is written back best effort.

        The WRITE path passes probe=False: its own exclusive create of v{N+1} fails
        when the hint lags, and that conflict heals the hint before the retry, so
        the check would only add a round trip to every commit (#86).

        Without a hint the directory is listed. Versions are unique by construction
        (the commit point is an exclusive create), so nothing is ever ambiguous.
        """
        hinted = self._read_version_hint()
        if hinted is not None:
            latest = self.heal_hint(hinted) if probe else hinted
            return latest, metadata_file_name(latest)
        versions, legacy = self._metadata_versions_on_disk()
        if versions:
            top = max(versions)
            # A pre-0.8 table used these same filenames with snake_case CONTENT. Check
            # before touching the hint: refusing a legacy table must write nothing into it.
            from .metadata_serde import is_legacy_document

            try:
                if is_legacy_document(self.storage.read_json(f"{self.metadata_path}/{versions[top]}")):
                    self._raise_legacy(f"snake_case document {versions[top]}")
            except (FileNotFoundError, ValueError) as e:
                if isinstance(e, LegacyLayoutError):
                    raise
            logger.warning(f"Version hint missing for {self.table_path}; using v{top} from the listing")
            self._advance_hint(top)
            return top, versions[top]
        if legacy or self.storage.exists(LEGACY_HINT_PATH):
            self._raise_legacy("root version hint / v{N}-{hex} metadata files")
        return None

    def repair_version_hint(self, metadata_file: str) -> TableMetadata:
        """Operator action: point the hint at `metadata_file` ('v7.metadata.json' or
        '7'). Kept from 0.8 for operators; with the exclusive-create commit point the
        hint can no longer be ambiguous, only stale - and readers heal that alone."""
        version = self._parse_hint_content(metadata_file.encode("utf-8"))
        if version is None:
            raise ValueError(f"{metadata_file!r} is not a version or a vN.metadata.json name")
        metadata = self._read_metadata_file(f"{self.metadata_path}/{metadata_file_name(version)}")
        with self._lock:
            self.lock_provider.acquire()
            try:
                self.storage.write_file(self.HINT_PATH, str(version).encode("utf-8"))
                self.current_version = version
            finally:
                self._release_lock_safely()
        logger.warning(f"Version hint for {self.table_path} set by operator to {version}")
        return metadata


__all__: List[Any] = [
    "_VersionHintMixin",
    "METADATA_FILE_RE",
    "LEGACY_METADATA_FILE_RE",
    "_METADATA_FILE_RE",
    "LEGACY_HINT_PATH",
    "MIGRATED_HINT_PATH",
    "metadata_file_name",
]
