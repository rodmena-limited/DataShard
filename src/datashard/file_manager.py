"""
File system operations and manifest management for the Python Iceberg implementation

Supports both local filesystem and S3-compatible storage via StorageBackend abstraction
"""

import json
import uuid
from datetime import datetime
from io import BytesIO
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Tuple

import fastavro

from .avro_schemas import MANIFEST_ENTRY_SCHEMA, MANIFEST_FILE_SCHEMA
from .bounds import decode_bound, encode_bound, infer_value_legacy
from .data_operations import DataFileManager
from .data_structures import (
    DataFile,
    ManifestContent,
    ManifestFile,
    Snapshot,
)
from .integrity import CorruptDataError, IntegrityChecker
from .manifest_codec import (
    AVRO_MAGIC,
    avro_records,
    data_file_from_avro,
    data_files_from_json,
    manifest_from_avro,
)
from .metadata_manager import MetadataManager
from .storage_backend import StorageBackend

# Iceberg manifest-entry statuses
ENTRY_STATUS_EXISTING = 0
ENTRY_STATUS_ADDED = 1

# Snapshot summary keys recording the manifest list's size and sha256 at commit
# time, so a truncated or overwritten list is rejected on read (#58).
SUMMARY_LIST_LENGTH = "manifest-list-length"
SUMMARY_LIST_SHA256 = "manifest-list-sha256"

class ManifestListInfo(NamedTuple):
    """What a commit must record about the manifest list it wrote."""

    path: str
    length: int
    checksum: str

    def summary(self) -> Dict[str, str]:
        return {SUMMARY_LIST_LENGTH: str(self.length), SUMMARY_LIST_SHA256: self.checksum}


class FileManager:
    """Handles file system operations and manifest management"""

    def __init__(
        self,
        table_path: str,
        metadata_manager: MetadataManager,
        storage: StorageBackend,
    ):
        self.table_path = table_path
        self.metadata_manager = metadata_manager
        self.storage = storage

        # Path components
        self.data_path = "data"
        self.metadata_path = "metadata"
        self.manifests_path = "metadata/manifests"
        # Directories are created by MetadataManager.initialize_table(); a mere
        # open must not create anything (#72). Writes create parents on demand.

        # Initialize data file manager
        self.data_file_manager = DataFileManager(self, storage)

    def validate_file_exists(self, file_path: str) -> bool:
        """Validate that a file exists in the proper location"""
        # In Iceberg, paths that start with / are relative to the table location
        # So "/data/file.parquet" means "{table_location}/data/file.parquet"
        if file_path.startswith("/"):
            # Iceberg-style path relative to table location
            path = file_path.lstrip("/")
        else:
            # Relative path
            path = file_path

        return self.storage.exists(path)

    def validate_data_files(self, data_files: List[DataFile]) -> bool:
        """Validate that all data files exist and are accessible"""
        for data_file in data_files:
            if not self.validate_file_exists(data_file.file_path):
                raise FileNotFoundError(f"Data file does not exist: {data_file.file_path}")
        return True

    def _safe_int(self, value: Any, default: int = 0) -> int:
        """Safely convert a value to int, returning default if conversion fails"""
        if value is None:
            return default
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            try:
                return int(value)
            except (ValueError, TypeError):
                return default
        if isinstance(value, (float, bool)):
            return int(value)
        return default

    # Bound encoding lives in bounds.py; kept as methods for compatibility.
    _encode_bound = staticmethod(encode_bound)
    _decode_bound = staticmethod(decode_bound)
    _infer_value_legacy = staticmethod(infer_value_legacy)

    def _infer_value(self, value: Any) -> Any:
        """Backward-compatible entry point for bound decoding."""
        return decode_bound(value)

    def create_manifest_file(
        self,
        data_files: List[DataFile],
        manifest_content: ManifestContent = ManifestContent.DATA,
        snapshot_id: Optional[int] = None,
        existing_files: Optional[List[DataFile]] = None,
        sequence_number: Optional[int] = None,
        pre_write_hook: Optional[Callable[[str], None]] = None,
    ) -> ManifestFile:
        """Create a manifest file for the given data files.

        Args:
            data_files: Files newly added by this snapshot (status ADDED).
            manifest_content: DATA or DELETES.
            snapshot_id: The committing snapshot's id, stamped on ADDED entries.
            existing_files: Files carried over from earlier snapshots (e.g.
                survivors of a delete-rewrite). Written with status EXISTING and
                their ORIGINAL added_snapshot_id, so history is not falsified.
            sequence_number: The committing snapshot's Iceberg v2 sequence
                number, stamped on ADDED entries. Carried-over files keep the
                sequence number they were added with (inheritance), so a
                manifest rewrite never re-dates existing data.
            pre_write_hook: Called with the manifest's table-relative path
                immediately BEFORE the file is written. Used to register GC
                protection for a file that is not yet reachable.
        """
        existing_files = existing_files or []

        # Unique manifest file name: timestamp + random suffix. Two concurrent
        # committers in the same microsecond must never collide - a shared name
        # would make one snapshot silently list the other's files.
        timestamp = int(datetime.now().timestamp() * 1000000)  # microseconds
        manifest_filename = f"manifest_{timestamp}_{uuid.uuid4().hex[:8]}.avro"
        manifest_path = f"{self.manifests_path}/{manifest_filename}"

        snapshot_id_val = snapshot_id or int(datetime.now().timestamp() * 1000)

        # Prepare records for Avro
        records = []
        entry_sequence_numbers: List[int] = []
        for df, status in [(f, ENTRY_STATUS_ADDED) for f in data_files] + [
            (f, ENTRY_STATUS_EXISTING) for f in existing_files
        ]:
            if status == ENTRY_STATUS_ADDED:
                entry_snapshot_id: Optional[int] = snapshot_id_val
                entry_sequence_number: Optional[int] = sequence_number
            else:
                # Preserve the original adding snapshot for carried-over files
                entry_snapshot_id = df.added_snapshot_id
                entry_sequence_number = df.sequence_number

            if entry_sequence_number is not None:
                entry_sequence_numbers.append(entry_sequence_number)

            record = {
                "status": status,
                "snapshot_id": entry_snapshot_id,
                "sequence_number": entry_sequence_number,
                "file_sequence_number": entry_sequence_number,
                "data_file": {
                    "file_path": df.file_path,
                    "file_format": df.file_format.value if hasattr(df.file_format, 'value') else str(df.file_format),
                    "partition": {"values": {k: str(v) for k, v in df.partition_values.items()}},
                    "record_count": df.record_count,
                    "file_size_in_bytes": df.file_size_in_bytes,
                    # Avro map keys are strings; convert int field-ids explicitly
                    # (raw int-keyed dicts make fastavro raise, failing the commit)
                    "column_sizes": {str(k): self._safe_int(v) for k, v in df.column_sizes.items()} if df.column_sizes else None,
                    "value_counts": {str(k): self._safe_int(v) for k, v in df.value_counts.items()} if df.value_counts else None,
                    "null_value_counts": {str(k): self._safe_int(v) for k, v in df.null_value_counts.items()} if df.null_value_counts else None,
                    "lower_bounds": {str(k): encode_bound(v) for k, v in df.lower_bounds.items()} if df.lower_bounds else None,
                    "upper_bounds": {str(k): encode_bound(v) for k, v in df.upper_bounds.items()} if df.upper_bounds else None,
                    "checksum": df.checksum,
                }
            }
            records.append(record)

        # Write Avro to BytesIO
        bytes_io = BytesIO()
        fastavro.writer(bytes_io, MANIFEST_ENTRY_SCHEMA, records)
        content = bytes_io.getvalue()

        # Write using storage backend (protect it from GC first, if asked)
        if pre_write_hook is not None:
            pre_write_hook(manifest_path)
        self.storage.write_file(manifest_path, content)

        # Get actual size
        manifest_length = len(content)

        # Return the manifest file structure
        return ManifestFile(
            manifest_path=manifest_path,
            manifest_length=manifest_length,
            partition_spec_id=0,
            added_snapshot_id=snapshot_id_val,
            added_data_files_count=len(data_files),
            existing_data_files_count=len(existing_files),
            deleted_data_files_count=0,
            partitions=[],
            content=manifest_content,
            sequence_number=sequence_number,
            min_sequence_number=(
                min(entry_sequence_numbers) if entry_sequence_numbers else sequence_number
            ),
            checksum=IntegrityChecker.compute_checksum(content),
        )

    # ------------------------------------------------------------------
    # Integrity-checked container reads (#58)
    # ------------------------------------------------------------------

    @staticmethod
    def snapshot_list_integrity(snapshot: Optional[Snapshot]) -> Tuple[Optional[int], Optional[str]]:
        """(expected_length, expected_sha256) of a snapshot's manifest list, from its
        summary. Both None for snapshots written before 0.8.0."""
        if snapshot is None or not snapshot.summary:
            return None, None
        raw_len = snapshot.summary.get(SUMMARY_LIST_LENGTH)
        try:
            length = int(raw_len) if raw_len is not None else None
        except (TypeError, ValueError):
            length = None
        return length, snapshot.summary.get(SUMMARY_LIST_SHA256) or None

    @staticmethod
    def _verify_container(
        path: str,
        content: bytes,
        expected_length: Optional[int],
        expected_checksum: Optional[str],
        kind: str,
    ) -> None:
        if expected_length is not None and len(content) != expected_length:
            raise CorruptDataError(
                f"{kind} {path} is {len(content)} bytes but {expected_length} bytes were "
                f"recorded at commit time - truncated or overwritten; refusing to read a "
                f"partial file list"
            )
        if expected_checksum and not IntegrityChecker.verify_checksum(content, expected_checksum):
            raise CorruptDataError(
                f"{kind} {path} does not match the sha256 recorded at commit time"
            )

    def _read_container(
        self,
        path: str,
        expected_length: Optional[int],
        expected_checksum: Optional[str],
        kind: str,
    ) -> bytes:
        try:
            content = self.storage.read_file(path)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"{kind} file does not exist: {path}") from e
        self._verify_container(path, content, expected_length, expected_checksum, kind)
        return content

    def read_manifest_file(
        self,
        manifest_path: str,
        expected_length: Optional[int] = None,
        expected_checksum: Optional[str] = None,
    ) -> List[DataFile]:
        """Read and parse a manifest file to get data files.

        Pass the manifest_length and checksum recorded in the manifest list: a
        truncated or overwritten manifest is then rejected with CorruptDataError
        instead of being read as a shorter list (#58). Without them only
        structural Avro damage is detected.
        """
        content = self._read_container(manifest_path, expected_length, expected_checksum, "Manifest")
        if content.startswith(AVRO_MAGIC):
            return [
                data_file_from_avro(record)
                for record in avro_records(content, manifest_path, "Manifest")
            ]
        return data_files_from_json(content, manifest_path)

    def create_manifest_list(
        self,
        manifest_files: List[ManifestFile],
        snapshot_id: int,
        pre_write_hook: Optional[Callable[[str], None]] = None,
    ) -> ManifestListInfo:
        """Write the manifest list for a snapshot and return path, length and sha256.

        Args:
            manifest_files: All manifests active in the new snapshot.
            snapshot_id: The committing snapshot's id.
            pre_write_hook: Called with the list's table-relative path before it
                is written (GC protection for a not-yet-reachable file).
        """
        timestamp = int(datetime.now().timestamp() * 1000)
        list_filename = f"manifest_list_{snapshot_id}_{timestamp}_{uuid.uuid4().hex[:8]}.avro"
        list_path = f"{self.manifests_path}/{list_filename}"

        records: List[Dict[str, Any]] = []
        for mf in manifest_files:
            # Ensure content is an integer (handle Enum)
            content_val = int(mf.content.value) if hasattr(mf.content, 'value') else int(mf.content)  # type: ignore

            records.append({
                "manifest_path": mf.manifest_path,
                "manifest_length": mf.manifest_length,
                "partition_spec_id": mf.partition_spec_id,
                "content": content_val,
                "sequence_number": mf.sequence_number,
                "min_sequence_number": mf.min_sequence_number,
                "added_snapshot_id": mf.added_snapshot_id,
                "added_data_files_count": mf.added_data_files_count,
                "existing_data_files_count": mf.existing_data_files_count,
                "deleted_data_files_count": mf.deleted_data_files_count,
                "partitions": [],  # Simplified
                "checksum": mf.checksum,
            })

        bytes_io = BytesIO()
        fastavro.writer(bytes_io, MANIFEST_FILE_SCHEMA, records)
        content = bytes_io.getvalue()

        if pre_write_hook is not None:
            pre_write_hook(list_path)
        self.storage.write_file(list_path, content)

        return ManifestListInfo(
            path=list_path, length=len(content), checksum=IntegrityChecker.compute_checksum(content)
        )

    def create_manifest_list_file(
        self,
        manifest_files: List[ManifestFile],
        snapshot_id: int,
        pre_write_hook: Optional[Callable[[str], None]] = None,
    ) -> str:
        """Compatibility wrapper around create_manifest_list: returns the path only."""
        return self.create_manifest_list(manifest_files, snapshot_id, pre_write_hook).path

    def read_manifest_list_file(
        self,
        list_path: str,
        expected_length: Optional[int] = None,
        expected_checksum: Optional[str] = None,
    ) -> List[ManifestFile]:
        """Read a manifest list and return its manifest files.

        Pass the length/sha256 recorded in the owning snapshot's summary (see
        snapshot_list_integrity) so a damaged list is rejected, not read short (#58).
        """
        content = self._read_container(list_path, expected_length, expected_checksum, "Manifest list")
        if content.startswith(AVRO_MAGIC):
            return [
                manifest_from_avro(record)
                for record in avro_records(content, list_path, "Manifest list")
            ]
        try:
            list_data = json.loads(content.decode("utf-8"))
            return [
                ManifestFile(
                    manifest_path=entry["manifest_path"],
                    manifest_length=entry["manifest_length"],
                    partition_spec_id=entry["partition_spec_id"],
                    added_snapshot_id=entry["added_snapshot_id"],
                    added_data_files_count=entry["added_data_files_count"],
                    existing_data_files_count=entry["existing_data_files_count"],
                    deleted_data_files_count=entry["deleted_data_files_count"],
                    partitions=[],
                    content=ManifestContent(entry["content"]),
                )
                for entry in list_data.get("manifests", [])
            ]
        except Exception as e:
            raise CorruptDataError(
                f"Could not parse manifest list file {list_path} (neither Avro nor JSON)"
            ) from e

    def cleanup_orphaned_files(self, valid_file_paths: List[str]) -> int:
        """Removed: unsafe legacy cleanup. Use Table.garbage_collect() instead.

        The old implementation deleted every data file absent from a
        caller-supplied list, with no grace period, no protection for files
        written by in-flight transactions, and no abort when reachability could
        not be computed - any of which silently destroys live data. It is kept
        only as an explicit failure so existing callers get an actionable error
        rather than data loss.
        """
        raise NotImplementedError(
            "FileManager.cleanup_orphaned_files is unsafe and has been removed: it "
            "deletes files without a grace period, without protecting in-flight "
            "transactions, and without failing closed on unreadable metadata. Use "
            "Table.garbage_collect(grace_period_ms=...) instead."
        )

    def verify_integrity(self, manifest_files: List[ManifestFile]) -> Dict[str, Any]:
        """Verify the integrity of all files referenced in manifests"""
        report: Dict[str, Any] = {
            "total_files": 0,
            "existing_files": 0,
            "missing_files": [],
            "checksum_mismatches": [],  # Would be implemented with actual checksums
            "valid_manifests": 0,
            "invalid_manifests": [],
        }

        for manifest in manifest_files:
            try:
                data_files = self.read_manifest_file(
                    manifest.manifest_path,
                    expected_length=manifest.manifest_length,
                    expected_checksum=manifest.checksum,
                )
                report["valid_manifests"] += 1

                for data_file in data_files:
                    report["total_files"] += 1
                    if self.validate_file_exists(data_file.file_path):
                        report["existing_files"] += 1

                        # Verify checksum if available
                        if data_file.checksum:
                            try:
                                # Read file content using streaming to avoid memory issues
                                clean_path = data_file.file_path.lstrip("/") if data_file.file_path.startswith("/") else data_file.file_path

                                with self.storage.open_file(clean_path) as stream:
                                    if not IntegrityChecker.verify_stream_checksum(stream, data_file.checksum):
                                        report["checksum_mismatches"].append(data_file.file_path)
                            except Exception as e:
                                report["checksum_mismatches"].append(f"{data_file.file_path} (read error: {e})")
                    else:
                        report["missing_files"].append(data_file.file_path)
            except Exception as e:
                report["invalid_manifests"].append(
                    {"manifest_path": manifest.manifest_path, "error": str(e)}
                )

        return report
