"""
Garbage collection for orphaned data and metadata files.

Safety model (bank-grade, fail closed):
- If ANY reachable manifest list or manifest cannot be read, or fails the length /
  sha256 check recorded at commit time (#58), collection ABORTS without deleting
  anything. Unknown reachability means nothing is deleted.
- Every "older than" decision is relative to the instant GC STARTED (taken before
  the first storage read), never to "now" at deletion time. A file written after
  GC started can never look old enough to delete, however long GC runs (#57).
  That instant is read from the STORAGE's clock (S3: the server's Date header),
  because object timestamps are the server's; a GC host running fast once
  deleted a commit that landed mid-run (#77).
- In-flight markers are loaded BEFORE the table metadata is read, so a
  transaction that commits after the metadata read is still covered by the
  markers it held at that instant (#57). Markers protect files regardless of age
  until they exceed the abandonment timeout.
- Manifest paths and storage listings are normalised by ONE rule (drop the
  leading slash). table_path is never stripped as a string prefix: doing so
  mis-normalised every path of a table whose name is itself a prefix of
  "data"/"metadata" and deleted the whole table (#56).
- The grace period must exceed the longest transaction plus the longest GC run;
  values below MIN_GRACE_PERIOD_MS are refused unless allow_short_grace=True.
"""

import json
import logging
import time
from typing import Callable, Dict, Optional, Set

from .data_structures import ManifestFile, Snapshot, TableMetadata
from .file_manager import FileManager
from .metadata_manager import MetadataManager
from .version_hint import _METADATA_FILE_RE

# Client/server clock difference above which GC warns (age decisions still use the
# storage clock; the warning is for the operator).
CLOCK_SKEW_WARN_MS = 60 * 1000

logger = logging.getLogger(__name__)

# Directory (relative to table root) holding in-flight transaction markers
INFLIGHT_PATH = "metadata/inflight"

# Markers older than this are considered abandoned transactions; their files
# become normal GC candidates. Must comfortably exceed any legitimate
# transaction duration.
DEFAULT_INFLIGHT_TIMEOUT_MS = 24 * 3600 * 1000

# Shortest grace period accepted without an explicit opt-in (#57).
MIN_GRACE_PERIOD_MS = 5 * 60 * 1000


class GarbageCollectionAborted(RuntimeError):
    """Raised when GC aborts because reachability could not be fully computed."""

    pass


class GarbageCollector:
    """Identifies and removes orphaned files."""

    def __init__(
        self,
        table_path: str,
        metadata_manager: MetadataManager,
        file_manager: FileManager
    ):
        self.table_path = table_path
        self.metadata_manager = metadata_manager
        self.file_manager = file_manager
        self.storage = file_manager.storage

    def collect(
        self,
        grace_period_ms: int = 3600000,
        inflight_timeout_ms: int = DEFAULT_INFLIGHT_TIMEOUT_MS,
        allow_short_grace: bool = False,
    ) -> Dict[str, int]:
        """
        Delete files that are not referenced by any valid snapshot and are older than
        grace_period_ms, measured from the instant this call started.

        Args:
            grace_period_ms: Minimum age of orphaned files to delete (milliseconds).
                Default: 1 hour. Must exceed the longest transaction plus the
                longest GC run on this table.
            inflight_timeout_ms: Age after which an in-flight transaction marker
                is considered abandoned. Default: 24 hours.
            allow_short_grace: Accept a grace period below MIN_GRACE_PERIOD_MS.
                Only safe on a table with no concurrent writers (tests).

        Returns:
            Dict with counts of deleted files by type.

        Raises:
            ValueError: grace_period_ms negative, or too short without opt-in.
            GarbageCollectionAborted: If any reachable manifest (list) could not
                be read or verified - nothing is deleted in that case (fail closed).
        """
        if grace_period_ms < 0:
            raise ValueError("grace_period_ms must be >= 0")
        if grace_period_ms < MIN_GRACE_PERIOD_MS and not allow_short_grace:
            raise ValueError(
                f"grace_period_ms={grace_period_ms} is below the minimum {MIN_GRACE_PERIOD_MS} ms. "
                f"A grace period shorter than the longest transaction plus the longest GC run can "
                f"delete live data (#57). Pass allow_short_grace=True only on a table with no "
                f"concurrent writers."
            )

        stats = {"data_files": 0, "manifest_files": 0, "manifest_lists": 0, "metadata_files": 0}

        # 0. The clock for every age decision is taken BEFORE any storage read, from
        # the storage's own clock (#77).
        gc_start_ms = self.storage.clock_ms()
        skew_ms = gc_start_ms - time.time() * 1000
        if abs(skew_ms) > CLOCK_SKEW_WARN_MS:
            logger.warning(
                f"Clock skew of {skew_ms / 1000:+.0f} s between the storage server and this host; "
                f"GC age decisions use the server clock"
            )

        # 1. In-flight markers FIRST (#57): a commit that lands after the metadata
        # read below was necessarily in flight - and marked - at this instant.
        protected_files = self._load_inflight_protection(inflight_timeout_ms, gc_start_ms)

        # 2. Table metadata: the reachability view.
        metadata = self.metadata_manager.refresh()
        if not metadata:
            return stats

        logger.info(f"Starting garbage collection for {self.table_path}")
        if protected_files:
            logger.info(f"Protecting {len(protected_files)} in-flight files from GC")

        # 3. Reachability. ANY failure here aborts the whole collection: deleting
        # based on incomplete reachability deletes live data.
        reachable_data_files: Set[str] = set()
        reachable_manifests: Set[str] = set()
        reachable_manifest_lists: Set[str] = set()

        lists_by_path: Dict[str, Snapshot] = {}
        for snapshot in metadata.snapshots:
            if snapshot.manifest_list:
                lists_by_path.setdefault(self._normalize_path(snapshot.manifest_list), snapshot)
        reachable_manifest_lists.update(lists_by_path)

        manifests_by_path: Dict[str, ManifestFile] = {}
        for m_list_path, snapshot in lists_by_path.items():
            exp_len, exp_sum = FileManager.snapshot_list_integrity(snapshot)
            try:
                manifests = self.file_manager.read_manifest_list_file(
                    m_list_path, expected_length=exp_len, expected_checksum=exp_sum
                )
            except Exception as e:
                raise GarbageCollectionAborted(
                    f"Aborting GC: cannot read reachable manifest list {m_list_path}: {e}. "
                    f"Nothing was deleted."
                ) from e
            for m in manifests:
                if m.manifest_path:
                    manifests_by_path.setdefault(self._normalize_path(m.manifest_path), m)
        reachable_manifests.update(manifests_by_path)

        for m_path, m in manifests_by_path.items():
            try:
                data_files = self.file_manager.read_manifest_file(
                    m_path, expected_length=m.manifest_length, expected_checksum=m.checksum
                )
            except Exception as e:
                raise GarbageCollectionAborted(
                    f"Aborting GC: cannot read reachable manifest {m_path}: {e}. "
                    f"Nothing was deleted."
                ) from e
            for df in data_files:
                reachable_data_files.add(self._normalize_path(df.file_path))

        logger.info(f"Found reachable: {len(reachable_manifest_lists)} manifest lists, "
                    f"{len(reachable_manifests)} manifests, {len(reachable_data_files)} data files")

        # 4. List storage and delete orphans older than the cutoff.
        cutoff_ms = gc_start_ms - grace_period_ms

        stats["data_files"] = self._gc_prefix(
            "data", reachable_data_files | protected_files, cutoff_ms
        )

        # Manifests AND manifest lists are the .avro files under metadata/ (and
        # under metadata/manifests/ for tables written before 0.10). In-flight
        # protection applies here too: a commit in progress has written its
        # manifests before the metadata that makes them reachable. Only .avro
        # files are candidates here - the hint, the metadata versions and the
        # inflight markers under the same prefix are handled elsewhere.
        all_reachable_manifests = reachable_manifests.union(reachable_manifest_lists)
        stats["manifest_files"] = self._gc_prefix(
            self.file_manager.metadata_path,
            all_reachable_manifests | protected_files,
            cutoff_ms,
            candidate=self._is_manifest_candidate,
        )

        # Superseded metadata files (v*.metadata.json outside the current version
        # and the retained metadata log) and temp-file leftovers under metadata/.
        stats["metadata_files"] = self._gc_metadata_files(metadata, cutoff_ms)

        logger.info(f"Garbage collection complete. Deleted: {stats}")
        return stats

    def _is_manifest_candidate(self, norm_path: str) -> bool:
        """True only for OUR manifest / manifest-list objects.

        Since 0.10 they sit DIRECTLY under metadata/ (Iceberg layout); tables
        written before that keep them under metadata/manifests/. Everything else
        below metadata/ - the version hint, metadata versions, inflight markers,
        and anything an operator parked there such as metadata/manifests_archive/
        - is not a candidate. Sweeping the whole metadata/ subtree by suffix once
        deleted a sibling directory's archive (#63 class).
        """
        if not norm_path.endswith(".avro"):
            return False
        parent = norm_path.rpartition("/")[0]
        return parent in (self.file_manager.metadata_path, self.file_manager.legacy_manifests_path)

    def _gc_metadata_files(self, metadata: TableMetadata, cutoff_ms: float) -> int:
        """Delete superseded metadata files older than the cutoff (#68).

        The current version, its metadata_log and the retention depth are re-read
        HERE, at reclaim time: a commit that landed during reachability has moved
        the hint on, and its log names the file that was current when GC started
        (#76). Kept: every metadata file whose version is within
        write.metadata.previous-versions-max of the current version (or ahead of
        it), every file the current log names, and anything newer than the cutoff.
        Everything else under metadata/ that is a v*.metadata.json or a '.tmp.*'
        leftover is reclaimed. Every commit used to leave a full metadata copy
        behind forever, which is quadratic in the number of commits.
        """
        meta_dir = self.metadata_manager.metadata_path
        current = self.metadata_manager.refresh()  # fresh view, not the one from GC start
        if current is None:
            return 0
        info = self.metadata_manager._current_version_info()
        if info is None:
            return 0
        current_version, current_file = info
        raw_max = current.properties.get(MetadataManager.PREVIOUS_VERSIONS_MAX_PROPERTY)
        try:
            keep_versions = (
                int(raw_max) if raw_max is not None else MetadataManager.DEFAULT_PREVIOUS_VERSIONS_MAX
            )
        except (TypeError, ValueError):
            keep_versions = MetadataManager.DEFAULT_PREVIOUS_VERSIONS_MAX
        oldest_kept_version = current_version - max(keep_versions, 0)

        keep: Set[str] = {f"{meta_dir}/{current_file}"}
        for entry in current.metadata_log:
            named = entry.get("metadata-file") if isinstance(entry, dict) else None
            if named:
                try:
                    keep.add(self._normalize_path(self.file_manager.to_relative(named)))
                except ValueError:
                    continue  # a foreign writer's file elsewhere: not under our metadata/

        try:
            listed = self.storage.list_files_with_mtime(meta_dir)
        except Exception as e:
            raise GarbageCollectionAborted(
                f"Aborting GC: cannot list files under {meta_dir}: {e}"
            ) from e

        deleted = 0
        for rel, mtime in listed:
            norm = self._normalize_path(rel)
            parent, _, base = norm.rpartition("/")
            if parent != meta_dir:
                continue  # manifests/, inflight/ are handled elsewhere
            match = _METADATA_FILE_RE.match(base)
            if not (match or base.startswith(".tmp.")):
                continue
            if norm in keep:
                continue
            if match and int(match.group(1)) >= oldest_kept_version:
                continue  # inside the retention window (or ahead of the current version)
            try:
                if mtime * 1000 < cutoff_ms:
                    logger.debug(f"Deleting superseded metadata file: {norm}")
                    self.storage.delete_file(norm)
                    deleted += 1
            except Exception as e:
                logger.warning(f"Failed to process superseded metadata file {norm}: {e}")
        if deleted:
            logger.info(f"Deleted {deleted} superseded metadata file(s) under {meta_dir}")
        return deleted

    def _load_inflight_protection(self, inflight_timeout_ms: int, now_ms: float) -> Set[str]:
        """Collect paths protected by fresh in-flight markers.

        Protection covers every file a transaction has written but not yet made
        reachable - data files AND the manifests / manifest lists of a commit in
        progress. Markers older than the abandonment timeout are deleted; their
        files fall back to normal orphan handling.
        """
        protected: Set[str] = set()
        cutoff = now_ms - inflight_timeout_ms

        try:
            markers = self.storage.list_files_with_mtime(INFLIGHT_PATH)
        except Exception:
            markers = []

        for marker_path, mtime in markers:
            norm_marker = self._normalize_path(marker_path)
            age_ok = mtime * 1000 >= cutoff

            basename = norm_marker.rsplit("/", 1)[-1]
            if not basename.endswith(".inflight"):
                continue
            data_rel = self._marker_target(norm_marker, basename)

            if age_ok:
                protected.add(data_rel)
            else:
                logger.warning(
                    f"Removing abandoned in-flight marker {norm_marker} "
                    f"(older than {inflight_timeout_ms}ms)"
                )
                try:
                    self.storage.delete_file(norm_marker)
                except Exception as e:
                    logger.warning(f"Failed to delete stale marker {norm_marker}: {e}")
                    # Could not remove the marker -> keep protecting its file
                    protected.add(data_rel)

        return protected

    def _marker_target(self, marker_path: str, basename: str) -> str:
        """Resolve which file a marker protects.

        The marker's payload names the protected path explicitly (it may be a
        data file, a manifest, or a manifest list). Markers written by older
        versions carry no payload; for those the historical convention -
        "<data file basename>.inflight" under data/ - is assumed.
        """
        fallback = f"data/{basename[: -len('.inflight')]}"
        try:
            payload = json.loads(self.storage.read_file(marker_path).decode("utf-8"))
            target = payload.get("file_path")
        except Exception:
            return fallback
        if not isinstance(target, str) or not target:
            return fallback
        return self._normalize_path(target)

    def _gc_prefix(
        self,
        prefix: str,
        reachable_set: Set[str],
        cutoff_ms: float,
        candidate: Optional[Callable[[str], bool]] = None,
    ) -> int:
        """Delete unreachable files under `prefix` last modified before cutoff_ms.
        `candidate` restricts which listed paths are considered at all."""
        deleted_count = 0

        try:
            all_files = self.storage.list_files_with_mtime(prefix)
        except Exception as e:
            raise GarbageCollectionAborted(
                f"Aborting GC: cannot list files under {prefix}: {e}"
            ) from e

        for file_rel_path, mtime in all_files:
            norm_path = self._normalize_path(file_rel_path)

            # Independent guard against the #45 class of bug: a listed path that
            # escapes the table root can never be matched against the reachable
            # set, so every live file would look like an orphan. Abort rather
            # than delete on a path we cannot classify (fail closed).
            if not norm_path or norm_path == ".." or norm_path.startswith("../") or "/../" in norm_path:
                raise GarbageCollectionAborted(
                    f"Aborting GC: storage listing under '{prefix}' returned a path outside "
                    f"the table root ({file_rel_path!r}). Reachability cannot be determined."
                )

            if candidate is not None and not candidate(norm_path):
                continue
            if norm_path not in reachable_set:
                # Potential orphan. Its age comes from the listing (#77) and is
                # compared against the GC start clock.
                try:
                    if mtime * 1000 < cutoff_ms:
                        # Per-file detail at DEBUG: a sweep over a large table
                        # would otherwise emit one INFO line per object. The
                        # summary below stays at INFO.
                        logger.debug(f"Deleting orphan file: {file_rel_path}")
                        self.storage.delete_file(file_rel_path)
                        deleted_count += 1
                except Exception as e:
                    # Failing to delete one orphan is not dangerous (nothing
                    # live is at risk); log and continue.
                    logger.warning(f"Failed to process potential orphan {file_rel_path}: {e}")

        if deleted_count:
            logger.info(f"Deleted {deleted_count} orphan file(s) under {prefix}")
        return deleted_count

    @staticmethod
    def _normalize_path(path: str) -> str:
        """Table-relative form of a manifest entry or a storage listing.

        Both sources are already table-relative ('/data/x' Iceberg-style, or
        'data/x' from list_files); the ONLY transformation is dropping the leading
        slash and unifying separators. table_path is deliberately NOT stripped as
        a string prefix (#56).
        """
        return path.replace("\\", "/").lstrip("/")
