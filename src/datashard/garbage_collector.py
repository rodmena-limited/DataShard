"""
Garbage collection for orphaned data and metadata files.

Safety model (bank-grade, fail closed):
- If ANY reachable manifest list or manifest cannot be read, collection ABORTS
  without deleting anything. An unreadable manifest means reachability is
  unknown - deleting on incomplete knowledge deletes live data.
- Files newer than the grace period are never deleted.
- Data files registered by in-flight transactions (marker files under
  metadata/inflight/) are protected regardless of age, until their marker
  exceeds the abandonment timeout. This protects long-running batch loads
  whose files exist before the commit that makes them reachable.
"""

import logging
import time
from typing import Dict, Set

from .file_manager import FileManager
from .metadata_manager import MetadataManager

logger = logging.getLogger(__name__)

# Directory (relative to table root) holding in-flight transaction markers
INFLIGHT_PATH = "metadata/inflight"

# Markers older than this are considered abandoned transactions; their files
# become normal GC candidates. Must comfortably exceed any legitimate
# transaction duration.
DEFAULT_INFLIGHT_TIMEOUT_MS = 24 * 3600 * 1000


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
    ) -> Dict[str, int]:
        """
        Delete files that are not referenced by any valid snapshot and are older than grace_period.

        Args:
            grace_period_ms: Minimum age of orphaned files to delete (milliseconds).
                             Default: 1 hour.
            inflight_timeout_ms: Age after which an in-flight transaction marker
                             is considered abandoned. Default: 24 hours.

        Returns:
            Dict with counts of deleted files by type.

        Raises:
            GarbageCollectionAborted: If any reachable manifest (list) could not
                be read - nothing is deleted in that case (fail closed).
        """
        stats = {"data_files": 0, "manifest_files": 0, "manifest_lists": 0}

        # 1. Refresh metadata to get latest view
        metadata = self.metadata_manager.refresh()
        if not metadata:
            return stats

        logger.info(f"Starting garbage collection for {self.table_path}")

        # 2. Identify all reachable files. ANY failure here aborts the whole
        # collection: deleting based on incomplete reachability deletes live data.
        reachable_data_files: Set[str] = set()
        reachable_manifests: Set[str] = set()
        reachable_manifest_lists: Set[str] = set()

        # Add manifest lists from all snapshots
        for snapshot in metadata.snapshots:
            m_list_path = snapshot.manifest_list
            if m_list_path:
                reachable_manifest_lists.add(self._normalize_path(m_list_path))

        # Process manifest lists to find manifests and data files
        for m_list_path in reachable_manifest_lists:
            try:
                if not self.storage.exists(m_list_path):
                    raise FileNotFoundError(
                        f"Snapshot references missing manifest list: {m_list_path}"
                    )
                manifests = self.file_manager.read_manifest_list_file(m_list_path)
            except Exception as e:
                raise GarbageCollectionAborted(
                    f"Aborting GC: cannot read reachable manifest list {m_list_path}: {e}. "
                    f"Nothing was deleted."
                ) from e
            for m in manifests:
                m_path = m.manifest_path
                if m_path:
                    reachable_manifests.add(self._normalize_path(m_path))

        # Process manifests to find data files
        for m_path in reachable_manifests:
            try:
                if not self.storage.exists(m_path):
                    raise FileNotFoundError(
                        f"Manifest list references missing manifest: {m_path}"
                    )
                data_files = self.file_manager.read_manifest_file(m_path)
            except Exception as e:
                raise GarbageCollectionAborted(
                    f"Aborting GC: cannot read reachable manifest {m_path}: {e}. "
                    f"Nothing was deleted."
                ) from e
            for df in data_files:
                reachable_data_files.add(self._normalize_path(df.file_path))

        logger.info(f"Found reachable: {len(reachable_manifest_lists)} manifest lists, "
                    f"{len(reachable_manifests)} manifests, {len(reachable_data_files)} data files")

        # 3. Load in-flight protection markers (and sweep abandoned ones)
        protected_files = self._load_inflight_protection(inflight_timeout_ms)
        if protected_files:
            logger.info(f"Protecting {len(protected_files)} in-flight data files from GC")

        # 4. List all files in storage and delete orphans

        # GC Data Files
        stats["data_files"] = self._gc_prefix(
            "data", reachable_data_files | protected_files, grace_period_ms
        )

        # GC Manifests (files in the real manifests directory that are NOT reachable)
        all_reachable_manifests = reachable_manifests.union(reachable_manifest_lists)
        stats["manifest_files"] = self._gc_prefix(
            self.file_manager.manifests_path, all_reachable_manifests, grace_period_ms
        )

        logger.info(f"Garbage collection complete. Deleted: {stats}")
        return stats

    def _load_inflight_protection(self, inflight_timeout_ms: int) -> Set[str]:
        """Collect data-file paths protected by fresh in-flight markers.

        Markers older than the abandonment timeout are deleted; their files fall
        back to normal orphan handling.
        """
        protected: Set[str] = set()
        cutoff = (time.time() * 1000) - inflight_timeout_ms

        try:
            markers = self.storage.list_files(INFLIGHT_PATH)
        except Exception:
            markers = []

        for marker_path in markers:
            norm_marker = self._normalize_path(marker_path)
            try:
                age_ok = self.storage.get_modified_time(norm_marker) * 1000 >= cutoff
            except Exception:
                # Can't stat the marker: keep protection (fail closed)
                age_ok = True

            # Marker name is "<data file basename>.inflight"; the file lives in data/
            basename = norm_marker.rsplit("/", 1)[-1]
            if not basename.endswith(".inflight"):
                continue
            data_rel = f"data/{basename[: -len('.inflight')]}"

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

    def _gc_prefix(self, prefix: str, reachable_set: Set[str], grace_period_ms: int) -> int:
        """Garbage collect files in a specific prefix."""
        deleted_count = 0
        cutoff_time = (time.time() * 1000) - grace_period_ms

        try:
            all_files = self.storage.list_files(prefix)
        except Exception as e:
            raise GarbageCollectionAborted(
                f"Aborting GC: cannot list files under {prefix}: {e}"
            ) from e

        for file_rel_path in all_files:
            norm_path = self._normalize_path(file_rel_path)

            if norm_path not in reachable_set:
                # Potential orphan. Check age.
                try:
                    if self.storage.get_modified_time(file_rel_path) * 1000 < cutoff_time:
                        logger.info(f"Deleting orphan file: {file_rel_path}")
                        self.storage.delete_file(file_rel_path)
                        deleted_count += 1
                except Exception as e:
                    # Failing to delete one orphan is not dangerous (nothing
                    # live is at risk); log and continue.
                    logger.warning(f"Failed to process potential orphan {file_rel_path}: {e}")

        return deleted_count

    def _normalize_path(self, path: str) -> str:
        """Normalize path to be relative to table root and strip leading slashes."""
        if path.startswith(self.table_path):
            path = path[len(self.table_path):]
        return path.lstrip("/")
