"""
File system operations and manifest management for the Python Iceberg implementation

Supports both local filesystem and S3-compatible storage via StorageBackend abstraction
"""

from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Optional

import fastavro

from .avro_schemas import MANIFEST_ENTRY_SCHEMA, MANIFEST_FILE_SCHEMA
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

        snapshot_id_val = snapshot_id or int(datetime.now().timestamp() * 1000)

        # Prepare records for Avro
        records = []
        for df in data_files:
            # Flatten structure to match schema
            # content is passed as argument manifest_content, which is Enum

            record = {
                "status": 1, # 1 = ADDED
                "snapshot_id": snapshot_id_val,
                "sequence_number": None,
                "file_sequence_number": None,
                "data_file": {
                    "file_path": df.file_path,
                    "file_format": df.file_format.value if hasattr(df.file_format, 'value') else str(df.file_format),
                    "partition": {"values": {k: str(v) for k, v in df.partition_values.items()}},
                    "record_count": df.record_count,
                    "file_size_in_bytes": df.file_size_in_bytes,
                    "column_sizes": df.column_sizes,
                    "value_counts": df.value_counts,
                    "null_value_counts": df.null_value_counts,
                    # Convert bounds to simple map if present, complex handling skipped for now
                    "lower_bounds": {str(k): str(v) for k, v in df.lower_bounds.items()} if df.lower_bounds else None,
                    "upper_bounds": {str(k): str(v) for k, v in df.upper_bounds.items()} if df.upper_bounds else None,
                    "checksum": df.checksum,
                }
            }
            records.append(record)

        # Write Avro to BytesIO
        bytes_io = BytesIO()
        fastavro.writer(bytes_io, MANIFEST_ENTRY_SCHEMA, records)
        content = bytes_io.getvalue()

        # Write using storage backend
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
            existing_data_files_count=0,
            deleted_data_files_count=0,
            partitions=[],
            content=manifest_content,
            sequence_number=None,
            min_sequence_number=None,
        )

    def _infer_value(self, value: Any) -> Any:
        """Best-effort type inference for bound values stored as strings"""
        if not isinstance(value, str):
            return value
        if value.isdigit():
            return int(value)
        try:
            return float(value)
        except ValueError:
            pass
        if value.lower() == 'true':
            return True
        if value.lower() == 'false':
            return False
        return value

    def read_manifest_file(self, manifest_path: str) -> List[DataFile]:
        """Read and parse a manifest file to get data files"""
        if not self.storage.exists(manifest_path):
            raise FileNotFoundError(f"Manifest file does not exist: {manifest_path}")

        try:
            # Try reading as Avro using streaming I/O
            with self.storage.open_file(manifest_path) as stream:
                reader = fastavro.reader(stream)

                data_files = []
                for record_raw in reader:
                    # Cast record to Dict[str, Any] to satisfy mypy
                    # fastavro reader yields dicts
                    record: Dict[str, Any] = record_raw  # type: ignore

                    # Extract data_file field
                    df_record: Dict[str, Any] = record["data_file"]

                    # Parse bounds: convert keys to int, infer value types
                    lower_bounds = df_record.get("lower_bounds")
                    if lower_bounds:
                        lower_bounds = {int(k): self._infer_value(v) for k, v in lower_bounds.items()}

                    upper_bounds = df_record.get("upper_bounds")
                    if upper_bounds:
                        upper_bounds = {int(k): self._infer_value(v) for k, v in upper_bounds.items()}

                    data_file = DataFile(
                        file_path=df_record["file_path"],
                        file_format=FileFormat(df_record["file_format"]),
                        partition_values=df_record["partition"]["values"],
                        record_count=df_record["record_count"],
                        file_size_in_bytes=df_record["file_size_in_bytes"],
                        column_sizes=df_record.get("column_sizes"),
                        value_counts=df_record.get("value_counts"),
                        null_value_counts=df_record.get("null_value_counts"),
                        lower_bounds=lower_bounds,
                        upper_bounds=upper_bounds,
                        checksum=df_record.get("checksum"),
                    )
                    data_files.append(data_file)
                return data_files

        except (ValueError, IndexError, StopIteration, OSError):
            # Fallback: JSON
            # If Avro parsing fails, we try reading as JSON (backward compatibility)
            pass

        content = self.storage.read_file(manifest_path)
        import json
        try:
            manifest_data = json.loads(content.decode("utf-8"))

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
        except Exception as e:
            # If JSON also fails, raise original error or generic error
            raise ValueError(f"Could not parse manifest file {manifest_path} (tried Avro and JSON)") from e

    def create_manifest_list_file(
        self, manifest_files: List[ManifestFile], snapshot_id: int
    ) -> str:
        """Create a manifest list file for a snapshot"""
        timestamp = int(datetime.now().timestamp() * 1000)
        list_filename = f"manifest_list_{snapshot_id}_{timestamp}.avro"
        list_path = f"{self.manifests_path}/{list_filename}"

        # Prepare records for Avro
        records: List[Dict[str, Any]] = []
        for mf in manifest_files:
            # Ensure content is an integer (handle Enum)
            content_val = int(mf.content.value) if hasattr(mf.content, 'value') else int(mf.content)  # type: ignore

            record: Dict[str, Any] = {
                "manifest_path": mf.manifest_path,
                "manifest_length": mf.manifest_length,
                "partition_spec_id": mf.partition_spec_id,
                "content": content_val,
                "sequence_number": mf.sequence_number or 0,
                "min_sequence_number": mf.min_sequence_number or 0,
                "added_snapshot_id": mf.added_snapshot_id,
                "added_data_files_count": mf.added_data_files_count,
                "existing_data_files_count": mf.existing_data_files_count,
                "deleted_data_files_count": mf.deleted_data_files_count,
                "partitions": [] # Simplified
            }
            records.append(record)

        # Write Avro to BytesIO
        bytes_io = BytesIO()
        fastavro.writer(bytes_io, MANIFEST_FILE_SCHEMA, records)
        content = bytes_io.getvalue()

        # Write using storage backend
        self.storage.write_file(list_path, content)

        return list_path

    def read_manifest_list_file(self, list_path: str) -> List[ManifestFile]:
        """Read a manifest list file and return manifest files"""
        if not self.storage.exists(list_path):
            raise FileNotFoundError(f"Manifest list file does not exist: {list_path}")

        # Try reading as Avro using streaming I/O
        try:
            with self.storage.open_file(list_path) as stream:
                reader = fastavro.reader(stream)

                manifest_files = []
                for record_raw in reader:
                    # Cast to Dict[str, Any] to satisfy mypy
                    record: Dict[str, Any] = record_raw  # type: ignore

                    manifest_file = ManifestFile(
                        manifest_path=record["manifest_path"],
                        manifest_length=record["manifest_length"],
                        partition_spec_id=record["partition_spec_id"],
                        added_snapshot_id=record["added_snapshot_id"],
                        added_data_files_count=record["added_data_files_count"],
                        existing_data_files_count=record["existing_data_files_count"],
                        deleted_data_files_count=record["deleted_data_files_count"],
                        partitions=[],
                        content=ManifestContent(record["content"]),
                        sequence_number=record.get("sequence_number"),
                        min_sequence_number=record.get("min_sequence_number")
                    )
                    manifest_files.append(manifest_file)
                return manifest_files

        except (ValueError, IndexError, StopIteration, OSError):
            # Fallback: JSON
            pass

        content = self.storage.read_file(list_path)

        import json
        try:
            list_data = json.loads(content.decode("utf-8"))

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
        except Exception as e:
            raise ValueError(f"Could not parse manifest list file {list_path}") from e

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

                from .integrity import IntegrityChecker

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
