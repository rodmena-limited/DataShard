"""
Commit half of Transaction: manifest building, compaction and metadata mutators
(mixed into Transaction; split out of transaction.py for the 500-line file cap, #71).
"""

import uuid
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Set

from .data_structures import DataFile, ManifestContent, ManifestFile, TableMetadata
from .file_manager import FileManager
from .logging_config import get_logger
from .snapshot_manager import SnapshotManager

logger = get_logger(__name__)

# Table property: rewrite the active manifests into one when their count reaches
# this value (0 disables). One manifest per commit, never merged, made scan I/O
# grow with the number of commits ever made (#68).
MANIFEST_COMPACTION_THRESHOLD_PROPERTY = "datashard.manifest.compaction-threshold"
DEFAULT_MANIFEST_COMPACTION_THRESHOLD = 64



class _CommitOpsMixin:
    """Turns a transaction's queued file operations into manifests and a snapshot."""

    file_manager: FileManager
    snapshot_manager: SnapshotManager

    if TYPE_CHECKING:  # provided by Transaction

        def _register_inflight(self, file_path: str) -> None: ...

    def _commit_file_ops(
        self,
        base_metadata: TableMetadata,
        append_files: List[DataFile],
        deleted_paths: Set[str],
        mutator: Optional[Callable[[TableMetadata], None]],
        compact: bool = False,
    ) -> bool:
        """Build manifests for file-level operations and commit the snapshot.

        Returns False when there was nothing to commit (an explicit compaction on a
        table with fewer than two manifests and no other file operation).
        """
        # One snapshot id for EVERYTHING this commit writes: manifest entries,
        # manifest-list filename, and the Snapshot itself must agree, or
        # lineage joins dangle.
        snapshot_id = (uuid.uuid4().int & ((1 << 63) - 1))
        # Same for the Iceberg v2 sequence number: derived from the SAME base
        # metadata the OCC commit will be validated against, so a lost race
        # re-derives it from the fresh base on retry.
        sequence_number = base_metadata.last_sequence_number + 1

        # 1. Read existing manifests from the base snapshot. Failure ABORTS the
        # commit: falling back to an empty manifest set would silently drop
        # every pre-existing file from the new snapshot (and GC would then
        # delete them permanently).
        existing_manifests: List[ManifestFile] = []
        if (
            base_metadata.current_snapshot_id is not None
            and base_metadata.current_snapshot_id != -1
        ):
            base_snapshot = None
            for s in base_metadata.snapshots:
                if s.snapshot_id == base_metadata.current_snapshot_id:
                    base_snapshot = s
                    break

            if base_snapshot is None:
                # Dangling current_snapshot_id: the metadata is inconsistent, so
                # the base file set is UNKNOWN. Continuing with an empty base
                # would write a snapshot that silently omits every pre-existing
                # file (and GC would then delete them). Fail closed.
                raise RuntimeError(
                    f"Table metadata is inconsistent: current_snapshot_id "
                    f"{base_metadata.current_snapshot_id} does not match any snapshot in "
                    f"metadata.snapshots ({[s.snapshot_id for s in base_metadata.snapshots]}). "
                    f"Aborting commit: proceeding would silently drop all prior table data "
                    f"from the new snapshot."
                )

            path = base_snapshot.manifest_list
            if path.startswith("/"):
                path = path.lstrip("/")
            exp_len, exp_sum = FileManager.snapshot_list_integrity(base_snapshot)
            try:
                existing_manifests = self.file_manager.read_manifest_list_file(
                    path, expected_length=exp_len, expected_checksum=exp_sum
                )
            except Exception as e:
                raise RuntimeError(
                    f"Cannot read base snapshot manifest list "
                    f"'{base_snapshot.manifest_list}'. Aborting commit: proceeding "
                    f"would silently drop all prior table data from the new snapshot."
                ) from e

        # 2. Process deletes (rewrite affected manifests). Paths are matched in
        # table-relative form on both sides, and every requested path must be
        # found: a delete that matches nothing must not commit a no-op
        # snapshot and report success (#61).
        final_manifests: List[ManifestFile] = []
        if deleted_paths:
            wanted = {p.replace("\\", "/").lstrip("/") for p in deleted_paths}
            matched: Set[str] = set()
            for manifest in existing_manifests:
                manifest_path = manifest.manifest_path
                if manifest_path.startswith("/"):
                    manifest_path = manifest_path.lstrip("/")

                try:
                    data_files = self.file_manager.read_manifest_file(
                        manifest_path,
                        expected_length=manifest.manifest_length,
                        expected_checksum=manifest.checksum,
                    )
                except Exception as e:
                    # If we can't read a manifest, we can't safely filter it.
                    raise RuntimeError(
                        f"Failed to read manifest {manifest.manifest_path} during delete operation"
                    ) from e

                surviving_files = []
                for f in data_files:
                    rel = f.file_path.replace("\\", "/").lstrip("/")
                    if rel in wanted:
                        matched.add(rel)
                    else:
                        surviving_files.append(f)

                if len(surviving_files) == len(data_files):
                    # No changes, keep manifest
                    final_manifests.append(manifest)
                elif len(surviving_files) > 0:
                    # Partial delete: rewrite. Survivors keep status EXISTING and
                    # their original added_snapshot_id (no falsified history).
                    new_manifest = self.file_manager.create_manifest_file(
                        [],
                        ManifestContent.DATA,
                        snapshot_id,
                        existing_files=surviving_files,
                        sequence_number=sequence_number,
                        pre_write_hook=self._register_inflight,
                    )
                    new_manifest.partition_spec_id = manifest.partition_spec_id
                    final_manifests.append(new_manifest)
                # else: all files deleted -> drop this manifest
            missing = wanted - matched
            if missing:
                raise FileNotFoundError(
                    f"delete_files: {sorted(missing)} are not part of the current snapshot "
                    f"(paths are matched table-relative, with or without a leading '/'). "
                    f"Nothing was deleted and no snapshot was created."
                )
        else:
            final_manifests = list(existing_manifests)

        # 2b. Manifest compaction (#68). When the active set reaches the threshold
        # - or on an explicit compact_manifests() - the small manifests are
        # rewritten into one; entries keep their original snapshot and sequence
        # numbers (status EXISTING), so history is not falsified. The superseded
        # manifests become unreachable and GC reclaims them.
        threshold = self._compaction_threshold(base_metadata)
        if compact or (threshold and len(final_manifests) >= threshold):
            if len(final_manifests) >= 2:
                final_manifests = [
                    self._compact_manifests(final_manifests, snapshot_id, sequence_number)
                ]
            elif compact and not append_files and not deleted_paths:
                return False  # nothing to compact, nothing else to commit

        # 3. Process appends (create new manifest). Existence was checked when the
        # files were queued; a second HEAD per file here bought nothing (#67).
        if append_files:
            new_append_manifest = self.file_manager.create_manifest_file(
                append_files,
                ManifestContent.DATA,
                snapshot_id,
                sequence_number=sequence_number,
                pre_write_hook=self._register_inflight,
            )
            final_manifests.append(new_append_manifest)

        # 4. Create the manifest list (ALL active manifests for the table). Its
        # length and sha256 go into the snapshot summary so a damaged list is
        # rejected on read instead of yielding a partial file set (#58).
        list_info = self.file_manager.create_manifest_list(
            final_manifests, snapshot_id, pre_write_hook=self._register_inflight
        )

        # 5. Commit the snapshot - with the SAME id stamped into the manifests.
        operation = "append" if append_files else ("delete" if deleted_paths else "replace")
        self.snapshot_manager.create_snapshot(
            manifest_list_path=list_info.path,
            summary=list_info.summary(),
            operation=operation,
            parent_snapshot_id=(
                base_metadata.current_snapshot_id
                if base_metadata.current_snapshot_id is not None
                else -1
            ),
            base_metadata=base_metadata,  # Fresh base for OCC
            snapshot_id=snapshot_id,
            metadata_mutator=mutator,
            sequence_number=sequence_number,
        )
        return True

    @staticmethod
    def _compaction_threshold(metadata: TableMetadata) -> int:
        raw = metadata.properties.get(MANIFEST_COMPACTION_THRESHOLD_PROPERTY)
        if raw is None:
            return DEFAULT_MANIFEST_COMPACTION_THRESHOLD
        try:
            return max(0, int(raw))
        except (TypeError, ValueError):
            logger.warning(f"Ignoring invalid {MANIFEST_COMPACTION_THRESHOLD_PROPERTY}={raw!r}")
            return DEFAULT_MANIFEST_COMPACTION_THRESHOLD

    def _compact_manifests(
        self, manifests: List[ManifestFile], snapshot_id: int, sequence_number: int
    ) -> ManifestFile:
        """Rewrite `manifests` into one manifest of EXISTING entries."""
        entries: List[DataFile] = []
        for manifest in manifests:
            manifest_path = manifest.manifest_path.lstrip("/")
            try:
                entries.extend(self.file_manager.read_manifest_file(
                    manifest_path,
                    expected_length=manifest.manifest_length,
                    expected_checksum=manifest.checksum,
                ))
            except Exception as e:
                raise RuntimeError(
                    f"Failed to read manifest {manifest.manifest_path} during compaction"
                ) from e
        merged = self.file_manager.create_manifest_file(
            [],
            ManifestContent.DATA,
            snapshot_id,
            existing_files=entries,
            sequence_number=sequence_number,
            pre_write_hook=self._register_inflight,
        )
        logger.info(f"Compacted {len(manifests)} manifests ({len(entries)} files) into {merged.manifest_path}")
        return merged

    @staticmethod
    def _chain_mutators(
        mutators: List[Callable[[TableMetadata], None]]
    ) -> Optional[Callable[[TableMetadata], None]]:
        if not mutators:
            return None
        if len(mutators) == 1:
            return mutators[0]

        def chained(metadata: TableMetadata) -> None:
            for m in mutators:
                m(metadata)

        return chained

    @staticmethod
    def _make_properties_mutator(
        properties: Dict[str, Optional[str]]
    ) -> Callable[[TableMetadata], None]:
        def mutator(metadata: TableMetadata) -> None:
            for k, v in properties.items():
                if v is None:
                    metadata.properties.pop(k, None)
                else:
                    metadata.properties[k] = v

        return mutator

    @staticmethod
    def _make_expire_mutator(
        cutoff_ms: Optional[int], retain_last: Optional[int] = None
    ) -> Callable[[TableMetadata], None]:
        """Mutator removing snapshots older than cutoff, always keeping the current
        snapshot and the `retain_last` most recent ones."""

        def mutator(metadata: TableMetadata) -> None:
            from .snapshot_manager import repoint_parents_to_surviving_ancestors

            protected = {metadata.current_snapshot_id}
            if retain_last:
                by_time = sorted(metadata.snapshots, key=lambda s: s.timestamp_ms)
                protected.update(s.snapshot_id for s in by_time[-retain_last:])
            kept = [
                s for s in metadata.snapshots
                if s.snapshot_id in protected
                or (cutoff_ms is not None and s.timestamp_ms >= cutoff_ms)
            ]
            kept_ids = {s.snapshot_id for s in kept}
            # Survivors must not keep parent links to expired snapshots.
            repoint_parents_to_surviving_ancestors(metadata.snapshots, kept)
            metadata.snapshots = kept
            metadata.snapshot_log = [
                e for e in metadata.snapshot_log if e.snapshot_id in kept_ids
            ]

        return mutator

