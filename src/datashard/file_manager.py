"""
Manifest management and file-system helpers.

Manifests and manifest lists are Iceberg v2 Avro under <table>/metadata/ (#85);
paths are stored as absolute URIs and handled table-relative internally (#87).
Reads are integrity-checked against the length / sha256 recorded at commit (#58).
"""

import uuid
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Tuple

from .data_operations import DataFileManager
from .data_structures import (
    DataFile,
    ManifestContent,
    ManifestFile,
    PartitionSpec,
    Schema,
    Snapshot,
    TableMetadata,
)
from .integrity import CorruptDataError, IntegrityChecker
from .manifest_codec import decode_manifest, decode_manifest_list
from .manifest_writer import (
    ENTRY_STATUS_ADDED,
    ENTRY_STATUS_EXISTING,
    ManifestEntry,
    encode_manifest,
    encode_manifest_list,
)
from .metadata_manager import MetadataManager
from .metadata_serde import (
    LEGACY_SUMMARY_LIST_LENGTH,
    LEGACY_SUMMARY_LIST_SHA256,
    SUMMARY_LIST_LENGTH,
    SUMMARY_LIST_SHA256,
)
from .storage_backend import StorageBackend
from .table_paths import to_relative

__all__ = [
    "FileManager",
    "ManifestListInfo",
    "ENTRY_STATUS_ADDED",
    "ENTRY_STATUS_EXISTING",
    "SUMMARY_LIST_LENGTH",
    "SUMMARY_LIST_SHA256",
]


class ManifestListInfo(NamedTuple):
    """What a commit must record about the manifest list it wrote."""

    path: str
    length: int
    checksum: str

    def summary(self) -> Dict[str, str]:
        return {SUMMARY_LIST_LENGTH: str(self.length), SUMMARY_LIST_SHA256: self.checksum}


class FileManager:
    """Handles file system operations and manifest management"""

    def __init__(self, table_path: str, metadata_manager: MetadataManager, storage: StorageBackend):
        self.table_path = table_path
        self.metadata_manager = metadata_manager
        self.storage = storage
        self.data_path = "data"
        self.metadata_path = "metadata"
        # Manifests live directly under metadata/ since 0.10 (Iceberg layout); the
        # pre-0.10 subdirectory is still read and garbage-collected.
        self.manifests_path = "metadata"
        self.legacy_manifests_path = "metadata/manifests"
        self.data_file_manager = DataFileManager(self, storage)

    # ------------------------------------------------------------ paths

    def to_relative(self, path: str) -> str:
        """Table-relative form of a path from metadata (URI, '/data/x' or 'data/x')."""
        return to_relative(path, self.metadata_manager.known_locations())

    def validate_file_exists(self, file_path: str) -> bool:
        return self.storage.exists(self.to_relative(file_path))

    def validate_data_files(self, data_files: List[DataFile]) -> bool:
        for data_file in data_files:
            if not self.validate_file_exists(data_file.file_path):
                raise FileNotFoundError(f"Data file does not exist: {data_file.file_path}")
        return True

    def new_manifest_path(self) -> str:
        """Unique table-relative path for a manifest (Iceberg naming)."""
        return f"{self.metadata_path}/{uuid.uuid4()}-m0.avro"

    def new_manifest_list_path(self, snapshot_id: int) -> str:
        """Unique table-relative path for a snapshot's manifest list (Iceberg naming)."""
        return f"{self.metadata_path}/snap-{snapshot_id}-1-{uuid.uuid4()}.avro"

    def _current_schema_and_spec(self, metadata: Optional[TableMetadata]) -> Tuple[Schema, PartitionSpec]:
        if metadata is None:
            metadata = self.metadata_manager.refresh()
        if metadata is None:
            raise RuntimeError("Cannot write a manifest: table has no metadata")
        schema = next((s for s in metadata.schemas if s.schema_id == metadata.current_schema_id), None)
        if schema is None:
            schema = metadata.schemas[0] if metadata.schemas else Schema(schema_id=0, fields=[])
        spec = next((p for p in metadata.partition_specs if p.spec_id == metadata.default_spec_id), None)
        if spec is None:
            spec = PartitionSpec(spec_id=0, fields=[])
        return schema, spec

    # ------------------------------------------------------------ manifests

    def create_manifest_file(
        self,
        data_files: List[DataFile],
        manifest_content: ManifestContent = ManifestContent.DATA,
        snapshot_id: Optional[int] = None,
        existing_files: Optional[List[DataFile]] = None,
        sequence_number: Optional[int] = None,
        pre_write_hook: Optional[Callable[[str], None]] = None,
        manifest_path: Optional[str] = None,
        table_metadata: Optional[TableMetadata] = None,
    ) -> ManifestFile:
        """Write a manifest: `data_files` as ADDED by `snapshot_id`, `existing_files`
        as EXISTING with their original snapshot and sequence numbers (a rewrite never
        re-dates data). `table_metadata` supplies the schema stamped into the header.
        """
        if manifest_content != ManifestContent.DATA:
            raise NotImplementedError("Delete manifests arrive with merge-on-read (1.0)")
        existing_files = existing_files or []
        if manifest_path is None:
            manifest_path = self.new_manifest_path()
        snapshot_id_val = snapshot_id if snapshot_id is not None else (uuid.uuid4().int & ((1 << 63) - 1))
        entries: List[ManifestEntry] = [
            (ENTRY_STATUS_ADDED, snapshot_id_val, sequence_number, df) for df in data_files
        ] + [(ENTRY_STATUS_EXISTING, df.added_snapshot_id, df.sequence_number, df) for df in existing_files]
        return self.write_manifest(
            entries, manifest_path, snapshot_id_val, sequence_number, pre_write_hook, table_metadata
        )

    def write_manifest(
        self,
        entries: List[ManifestEntry],
        manifest_path: str,
        added_snapshot_id: int,
        sequence_number: Optional[int],
        pre_write_hook: Optional[Callable[[str], None]] = None,
        table_metadata: Optional[TableMetadata] = None,
        location: Optional[str] = None,
    ) -> ManifestFile:
        """Low-level manifest write with explicit per-entry status (used by commits
        and by the migration / relocation tools)."""
        schema, spec = self._current_schema_and_spec(table_metadata)
        loc = location or (table_metadata.location if table_metadata else None) or self.metadata_manager.location_uri
        content = encode_manifest(entries, schema, spec, loc)
        if pre_write_hook is not None:
            pre_write_hook(manifest_path)
        self.storage.write_file(manifest_path, content)
        seqs = [seq for _s, _sid, seq, _df in entries if seq is not None]
        added = [df for status, _sid, _seq, df in entries if status == ENTRY_STATUS_ADDED]
        existing = [df for status, _sid, _seq, df in entries if status == ENTRY_STATUS_EXISTING]
        return ManifestFile(
            manifest_path=manifest_path,
            manifest_length=len(content),
            partition_spec_id=spec.spec_id,
            added_snapshot_id=added_snapshot_id,
            added_data_files_count=len(added),
            existing_data_files_count=len(existing),
            deleted_data_files_count=0,
            partitions=[],
            content=ManifestContent.DATA,
            sequence_number=sequence_number,
            min_sequence_number=min(seqs) if seqs else sequence_number,
            checksum=IntegrityChecker.compute_checksum(content),
            added_rows_count=sum(df.record_count for df in added),
            existing_rows_count=sum(df.record_count for df in existing),
        )

    def create_manifest_list(
        self,
        manifest_files: List[ManifestFile],
        snapshot_id: int,
        pre_write_hook: Optional[Callable[[str], None]] = None,
        list_path: Optional[str] = None,
        parent_snapshot_id: Optional[int] = None,
        sequence_number: Optional[int] = None,
        location: Optional[str] = None,
    ) -> ManifestListInfo:
        """Write the manifest list for a snapshot and return path, length and sha256."""
        if list_path is None:
            list_path = self.new_manifest_list_path(snapshot_id)
        loc = location or self.metadata_manager.recorded_location or self.metadata_manager.location_uri
        content = encode_manifest_list(manifest_files, snapshot_id, parent_snapshot_id, sequence_number, loc)
        if pre_write_hook is not None:
            pre_write_hook(list_path)
        self.storage.write_file(list_path, content)
        return ManifestListInfo(path=list_path, length=len(content), checksum=IntegrityChecker.compute_checksum(content))

    def create_manifest_list_file(
        self, manifest_files: List[ManifestFile], snapshot_id: int, pre_write_hook: Optional[Callable[[str], None]] = None
    ) -> str:
        """Compatibility wrapper around create_manifest_list: returns the path only."""
        return self.create_manifest_list(manifest_files, snapshot_id, pre_write_hook).path

    # ------------------------------------------------------------ integrity-checked reads (#58)

    @staticmethod
    def snapshot_list_integrity(snapshot: Optional[Snapshot]) -> Tuple[Optional[int], Optional[str]]:
        """(expected_length, expected_sha256) of a snapshot's manifest list from its
        summary. Both None for snapshots without the datashard keys (a foreign
        writer's, or pre-0.8): unverified, not corrupt."""
        if snapshot is None or not snapshot.summary:
            return None, None
        summary = snapshot.summary
        raw_len = summary.get(SUMMARY_LIST_LENGTH, summary.get(LEGACY_SUMMARY_LIST_LENGTH))
        try:
            length = int(raw_len) if raw_len is not None else None
        except (TypeError, ValueError):
            length = None
        checksum = summary.get(SUMMARY_LIST_SHA256, summary.get(LEGACY_SUMMARY_LIST_SHA256)) or None
        return length, checksum

    @staticmethod
    def _verify_container(
        path: str, content: bytes, expected_length: Optional[int], expected_checksum: Optional[str], kind: str
    ) -> None:
        if expected_length is not None and len(content) != expected_length:
            raise CorruptDataError(
                f"{kind} {path} is {len(content)} bytes but {expected_length} bytes were recorded at "
                f"commit time - truncated or overwritten; refusing to read a partial file list"
            )
        if expected_checksum and not IntegrityChecker.verify_checksum(content, expected_checksum):
            raise CorruptDataError(f"{kind} {path} does not match the sha256 recorded at commit time")

    def _read_container(
        self, path: str, expected_length: Optional[int], expected_checksum: Optional[str], kind: str
    ) -> Tuple[str, bytes]:
        rel = self.to_relative(path)
        try:
            content = self.storage.read_file(rel)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"{kind} file does not exist: {path}") from e
        self._verify_container(rel, content, expected_length, expected_checksum, kind)
        return rel, content

    def read_manifest_file(
        self, manifest_path: str, expected_length: Optional[int] = None, expected_checksum: Optional[str] = None
    ) -> List[DataFile]:
        """Data files of a manifest (any generation), paths table-relative.

        Pass the length / sha256 recorded in the manifest list so a truncated or
        overwritten manifest is rejected instead of read short (#58).
        """
        rel, content = self._read_container(manifest_path, expected_length, expected_checksum, "Manifest")
        return decode_manifest(content, rel, self.to_relative)

    def read_manifest_list_file(
        self, list_path: str, expected_length: Optional[int] = None, expected_checksum: Optional[str] = None
    ) -> List[ManifestFile]:
        """Manifests named by a manifest list (any generation), paths table-relative."""
        rel, content = self._read_container(list_path, expected_length, expected_checksum, "Manifest list")
        return decode_manifest_list(content, rel, self.to_relative)

    # ------------------------------------------------------------ misc

    def cleanup_orphaned_files(self, valid_file_paths: List[str]) -> int:
        """Removed: unsafe legacy cleanup. Use Table.garbage_collect() instead."""
        raise NotImplementedError(
            "FileManager.cleanup_orphaned_files is unsafe and has been removed: it deletes files "
            "without a grace period, without protecting in-flight transactions, and without "
            "failing closed on unreadable metadata. Use Table.garbage_collect(grace_period_ms=...) instead."
        )

    def verify_integrity(self, manifest_files: List[ManifestFile]) -> Dict[str, Any]:
        """Verify the integrity of all files referenced in manifests"""
        report: Dict[str, Any] = {
            "total_files": 0,
            "existing_files": 0,
            "missing_files": [],
            "checksum_mismatches": [],
            "valid_manifests": 0,
            "invalid_manifests": [],
        }
        for manifest in manifest_files:
            try:
                data_files = self.read_manifest_file(
                    manifest.manifest_path, expected_length=manifest.manifest_length, expected_checksum=manifest.checksum
                )
                report["valid_manifests"] += 1
                for data_file in data_files:
                    report["total_files"] += 1
                    if not self.validate_file_exists(data_file.file_path):
                        report["missing_files"].append(data_file.file_path)
                        continue
                    report["existing_files"] += 1
                    if data_file.checksum:
                        try:
                            with self.storage.open_file(self.to_relative(data_file.file_path)) as stream:
                                if not IntegrityChecker.verify_stream_checksum(stream, data_file.checksum):
                                    report["checksum_mismatches"].append(data_file.file_path)
                        except Exception as e:
                            report["checksum_mismatches"].append(f"{data_file.file_path} (read error: {e})")
            except Exception as e:
                report["invalid_manifests"].append({"manifest_path": manifest.manifest_path, "error": str(e)})
        return report
