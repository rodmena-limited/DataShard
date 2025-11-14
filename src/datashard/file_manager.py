"""
File system operations and manifest management for the Python Iceberg implementation

Supports both local filesystem and S3-compatible storage via StorageBackend abstraction
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from .data_operations import DataFileManager
from .data_structures import DataFile, FileFormat, ManifestContent, ManifestFile
from .metadata_manager import MetadataManager
from .storage_backend import StorageBackend


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

        # Ensure directories exist
        self.storage.makedirs(self.data_path, exist_ok=True)
        self.storage.makedirs(self.manifests_path, exist_ok=True)

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

    def _safe_int(self, value, default=0) -> int:
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
        return default  # type: ignore[return-value]

    def create_manifest_file(
        self,
        data_files: List[DataFile],
        manifest_content: ManifestContent = ManifestContent.DATA,
        snapshot_id: Optional[int] = None,
    ) -> ManifestFile:
        """Create a manifest file containing the specified data files"""
        # Generate a unique manifest file name
        timestamp = int(datetime.now().timestamp() * 1000000)  # microseconds
        manifest_filename = f"manifest_{timestamp}.avro"
        manifest_path = f"{self.manifests_path}/{manifest_filename}"

        # Create manifest content
        manifest_data = {
            "manifest_path": manifest_path,
            "manifest_length": 0,  # Will calculate after writing
            "partition_spec_id": 0,  # Default partition spec
            "added_snapshot_id": snapshot_id or int(datetime.now().timestamp() * 1000),
            "added_data_files_count": len(data_files),
            "existing_data_files_count": 0,
            "deleted_data_files_count": 0,
            "partitions": [],  # This would be populated based on actual partitions
            "content": manifest_content.value,
            "sequence_number": timestamp,
            "min_sequence_number": timestamp,
            "files": [
                {
                    "file_path": df.file_path,
                    "file_format": df.file_format.value,
                    "partition_values": df.partition_values,
                    "record_count": df.record_count,
                    "file_size_in_bytes": df.file_size_in_bytes,
                    "column_sizes": df.column_sizes,
                    "value_counts": df.value_counts,
                    "null_value_counts": df.null_value_counts,
                    "lower_bounds": df.lower_bounds,
                    "upper_bounds": df.upper_bounds,
                }
                for df in data_files
            ],
        }

        # Write the manifest file using storage backend
        self.storage.write_json(manifest_path, manifest_data)

        # Calculate actual file size
        manifest_length = self.storage.get_size(manifest_path)
        manifest_data["manifest_length"] = manifest_length

        # Update the file with correct length
        self.storage.write_json(manifest_path, manifest_data)

        # Return the manifest file structure using safe integer conversions
        return ManifestFile(
            manifest_path=manifest_path,
            manifest_length=self._safe_int(manifest_data["manifest_length"]),
            partition_spec_id=self._safe_int(manifest_data["partition_spec_id"]),
            added_snapshot_id=self._safe_int(manifest_data["added_snapshot_id"], None),
            added_data_files_count=self._safe_int(manifest_data["added_data_files_count"]),
            existing_data_files_count=self._safe_int(manifest_data["existing_data_files_count"]),
            deleted_data_files_count=self._safe_int(manifest_data["deleted_data_files_count"]),
            partitions=(
                manifest_data["partitions"] if isinstance(manifest_data["partitions"], list) else []
            ),
            content=ManifestContent(manifest_data["content"]),
            sequence_number=self._safe_int(manifest_data.get("sequence_number"), None),
            min_sequence_number=self._safe_int(manifest_data.get("min_sequence_number"), None),
        )

    def read_manifest_file(self, manifest_path: str) -> List[DataFile]:
        """Read and parse a manifest file to get data files"""
        if not self.storage.exists(manifest_path):
            raise FileNotFoundError(f"Manifest file does not exist: {manifest_path}")

        manifest_data = self.storage.read_json(manifest_path)

        data_files = []
        for file_entry in manifest_data.get("files", []):
            data_file = DataFile(
                file_path=file_entry["file_path"],
                file_format=FileFormat(file_entry["file_format"]),
                partition_values=file_entry["partition_values"],
                record_count=file_entry["record_count"],
                file_size_in_bytes=file_entry["file_size_in_bytes"],
                column_sizes=file_entry.get("column_sizes"),
                value_counts=file_entry.get("value_counts"),
                null_value_counts=file_entry.get("null_value_counts"),
                lower_bounds=file_entry.get("lower_bounds"),
                upper_bounds=file_entry.get("upper_bounds"),
            )
            data_files.append(data_file)

        return data_files

    def create_manifest_list_file(
        self, manifest_files: List[ManifestFile], snapshot_id: int
    ) -> str:
        """Create a manifest list file for a snapshot"""
        timestamp = int(datetime.now().timestamp() * 1000)
        list_filename = f"manifest_list_{snapshot_id}_{timestamp}.avro"
        list_path = f"{self.manifests_path}/{list_filename}"

        # Create manifest list content
        list_data = {
            "snapshot_id": snapshot_id,
            "timestamp_ms": timestamp,
            "manifests": [
                {
                    "manifest_path": mf.manifest_path,
                    "manifest_length": mf.manifest_length,
                    "partition_spec_id": mf.partition_spec_id,
                    "added_snapshot_id": mf.added_snapshot_id,
                    "content": mf.content.value,
                    "added_data_files_count": mf.added_data_files_count,
                    "existing_data_files_count": mf.existing_data_files_count,
                    "deleted_data_files_count": mf.deleted_data_files_count,
                }
                for mf in manifest_files
            ],
        }

        # Write the manifest list file using storage backend
        self.storage.write_json(list_path, list_data)

        return list_path

    def read_manifest_list_file(self, list_path: str) -> List[ManifestFile]:
        """Read a manifest list file and return manifest files"""
        if not self.storage.exists(list_path):
            raise FileNotFoundError(f"Manifest list file does not exist: {list_path}")

        list_data = self.storage.read_json(list_path)

        manifest_files = []
        for manifest_entry in list_data.get("manifests", []):
            manifest_file = ManifestFile(
                manifest_path=manifest_entry["manifest_path"],
                manifest_length=manifest_entry["manifest_length"],
                partition_spec_id=manifest_entry["partition_spec_id"],
                added_snapshot_id=manifest_entry["added_snapshot_id"],
                added_data_files_count=manifest_entry["added_data_files_count"],
                existing_data_files_count=manifest_entry["existing_data_files_count"],
                deleted_data_files_count=manifest_entry["deleted_data_files_count"],
                partitions=[],  # Would be populated from the manifest file itself
                content=ManifestContent(manifest_entry["content"]),
            )
            manifest_files.append(manifest_file)

        return manifest_files

    def cleanup_orphaned_files(self, valid_file_paths: List[str]) -> int:
        """Clean up data files that are no longer referenced by any manifest"""
        # Get all data files currently in the data directory
        all_data_files_raw = self.storage.list_files(self.data_path)

        # Filter to only data files
        all_data_files = []
        for file_path in all_data_files_raw:
            if file_path.endswith((".parquet", ".avro", ".orc")):
                # Ensure path starts with /
                if not file_path.startswith("/"):
                    file_path = "/" + file_path
                all_data_files.append(file_path)

        # Find files that are not in valid_file_paths
        orphaned_files = [f for f in all_data_files if f not in valid_file_paths]

        # Remove orphaned files
        deleted_count = 0
        for file_path in orphaned_files:
            try:
                # Remove leading slash for storage backend
                path = file_path.lstrip("/")
                if self.storage.exists(path):
                    self.storage.delete_file(path)
                    deleted_count += 1
            except Exception as e:
                print(f"Warning: Could not delete orphaned file {file_path}: {e}")

        return deleted_count

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
                data_files = self.read_manifest_file(manifest.manifest_path)
                report["valid_manifests"] += 1

                for data_file in data_files:
                    report["total_files"] += 1
                    if self.validate_file_exists(data_file.file_path):
                        report["existing_files"] += 1
                    else:
                        report["missing_files"].append(data_file.file_path)
            except Exception as e:
                report["invalid_manifests"].append(
                    {"manifest_path": manifest.manifest_path, "error": str(e)}
                )

        return report
