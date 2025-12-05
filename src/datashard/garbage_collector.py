"""
Garbage collection for orphaned data and metadata files.
"""

import logging
import time
from typing import Dict, Set

from .file_manager import FileManager
from .metadata_manager import MetadataManager

logger = logging.getLogger(__name__)

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

    def collect(self, grace_period_ms: int = 3600000) -> Dict[str, int]:
        """
        Delete files that are not referenced by any valid snapshot and are older than grace_period.

        Args:
            grace_period_ms: Minimum age of orphaned files to delete (milliseconds).
                             Default: 1 hour.

        Returns:
            Dict with counts of deleted files by type.
        """
        stats = {"data_files": 0, "manifest_files": 0, "manifest_lists": 0}

        # 1. Refresh metadata to get latest view
        metadata = self.metadata_manager.refresh()
        if not metadata:
            return stats

        logger.info(f"Starting garbage collection for {self.table_path}")

        # 2. Identify all reachable files
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
                    continue

                manifests = self.file_manager.read_manifest_list_file(m_list_path)
                for m in manifests:
                    m_path = m.manifest_path
                    if m_path:
                        reachable_manifests.add(self._normalize_path(m_path))
            except Exception as e:
                logger.warning(f"Failed to read manifest list {m_list_path}: {e}")

        # Process manifests to find data files
        for m_path in reachable_manifests:
            try:
                if not self.storage.exists(m_path):
                    continue

                data_files = self.file_manager.read_manifest_file(m_path)
                for df in data_files:
                    reachable_data_files.add(self._normalize_path(df.file_path))
            except Exception as e:
                logger.warning(f"Failed to read manifest {m_path}: {e}")

        logger.info(f"Found reachable: {len(reachable_manifest_lists)} manifest lists, "
                    f"{len(reachable_manifests)} manifests, {len(reachable_data_files)} data files")

        # 3. List all files in storage and delete orphans

        # GC Data Files
        stats["data_files"] = self._gc_prefix("data", reachable_data_files, grace_period_ms)

        # GC Manifests (files in 'manifests/' that are NOT in reachable lists)
        all_reachable_manifests = reachable_manifests.union(reachable_manifest_lists)
        stats["manifest_files"] = self._gc_prefix("manifests", all_reachable_manifests, grace_period_ms)

        logger.info(f"Garbage collection complete. Deleted: {stats}")
        return stats

    def _gc_prefix(self, prefix: str, reachable_set: Set[str], grace_period_ms: int) -> int:
        """Garbage collect files in a specific prefix."""
        deleted_count = 0
        cutoff_time = (time.time() * 1000) - grace_period_ms

        try:
            all_files = self.storage.list_files(prefix)
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
                        logger.warning(f"Failed to process potential orphan {file_rel_path}: {e}")

        except Exception as e:
            logger.error(f"Error listing files in {prefix}: {e}")

        return deleted_count

    def _normalize_path(self, path: str) -> str:
        """Normalize path to be relative to table root and strip leading slashes."""
        if path.startswith(self.table_path):
            path = path[len(self.table_path):]
        return path.lstrip("/")
