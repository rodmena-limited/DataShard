"""
GC-protection markers for files a transaction has written but no snapshot references
yet (mixed into Transaction; split out of transaction.py for the 500-line cap).

A marker under metadata/inflight/ names the protected path; the garbage collector
loads markers before it reads metadata and never deletes a marked file (#57).
"""

import json
import uuid
from typing import List, Set, Tuple

from .file_manager import FileManager

# Directory (relative to table root) for in-flight GC-protection markers.
# Kept in sync with garbage_collector.INFLIGHT_PATH.
_INFLIGHT_PATH = "metadata/inflight"


class _InflightMixin:
    file_manager: FileManager
    _inflight_markers: List[str]
    _marked_paths: Set[str]
    _marker_names: Set[str]
    _attempt_files: List[str]

    def _register_inflight(self, file_path: str) -> None:
        """Write a GC-protection marker for a file this transaction is about to
        write but that no snapshot references yet.

        Used for data files AND for the manifests / manifest lists of a commit
        in progress: without a marker, a concurrent garbage collection running
        with a short grace period can delete a file between its write and the
        metadata commit that makes it reachable. Marker write failures
        propagate - a file is never written unprotected (fail closed).
        """
        self._register_inflight_many([file_path])

    def _register_inflight_many(self, file_paths: List[str]) -> None:
        """Write GC-protection markers for several paths in one concurrent batch (#80):
        a commit's manifest and manifest-list markers no longer cost two sequential
        round trips. Same guarantees as _register_inflight; idempotent per path."""
        items: List[Tuple[str, bytes]] = []
        new_rel_paths: List[str] = []
        for file_path in file_paths:
            rel_path = file_path.replace("\\", "/").lstrip("/")
            if rel_path in self._marked_paths:
                continue  # already protected (append_data marks before writing, then queues)
            marker_name = rel_path.rsplit("/", 1)[-1]
            if marker_name in self._marker_names:
                # Two caller-provided files with the same basename in different
                # directories must not share (and overwrite) one marker.
                marker_name = f"{marker_name}.{uuid.uuid4().hex[:8]}"
            marker_path = f"{_INFLIGHT_PATH}/{marker_name}.inflight"
            items.append((marker_path, json.dumps({"file_path": rel_path}).encode("utf-8")))
            self._marker_names.add(marker_name)
            new_rel_paths.append(rel_path)
        if not items:
            return
        # If this raises, nothing was recorded as protected, so no file gets written
        # without a marker (fail closed); markers that did land are swept by GC.
        self.file_manager.storage.write_files(items)
        for (marker_path, _), rel_path in zip(items, new_rel_paths, strict=True):
            self._inflight_markers.append(marker_path)
            self._marked_paths.add(rel_path)
            if rel_path.startswith(self.file_manager.manifests_path + "/"):
                self._attempt_files.append(rel_path)

