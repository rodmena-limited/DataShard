"""
Avro / legacy-JSON decoding of manifest entries and manifest-list records (split
out of file_manager.py, #71).
"""

import json
from io import BytesIO
from typing import Any, Dict, List

import fastavro

from .bounds import decode_bound
from .data_structures import DataFile, FileFormat, ManifestContent, ManifestFile
from .integrity import CorruptDataError

AVRO_MAGIC = b"Obj\x01"


def avro_records(content: bytes, path: str, kind: str) -> List[Dict[str, Any]]:
    """Decode every record of an Avro container, or raise CorruptDataError.

    fastavro stops cleanly when a file ends exactly on a block boundary, which
    is why the length/sha256 check above exists; this catches everything else
    (a block cut mid-way, a bad header, trailing garbage).
    """
    bio = BytesIO(content)
    try:
        records = [dict(r) for r in fastavro.reader(bio)]  # type: ignore[arg-type]
    except Exception as e:
        raise CorruptDataError(f"{kind} {path} is not a readable Avro container: {e}") from e
    if bio.tell() != len(content):
        raise CorruptDataError(
            f"{kind} {path} has {len(content) - bio.tell()} unread trailing bytes"
        )
    return records


def data_file_from_avro(record: Dict[str, Any]) -> DataFile:
    df_record: Dict[str, Any] = record["data_file"]

    # Parse bounds: convert keys to int, decode typed values
    lower_bounds = df_record.get("lower_bounds")
    if lower_bounds:
        lower_bounds = {int(k): decode_bound(v) for k, v in lower_bounds.items()}

    upper_bounds = df_record.get("upper_bounds")
    if upper_bounds:
        upper_bounds = {int(k): decode_bound(v) for k, v in upper_bounds.items()}

    # Stats maps: Avro string keys -> int field ids
    column_sizes = df_record.get("column_sizes")
    if column_sizes:
        column_sizes = {int(k): v for k, v in column_sizes.items()}
    value_counts = df_record.get("value_counts")
    if value_counts:
        value_counts = {int(k): v for k, v in value_counts.items()}
    null_value_counts = df_record.get("null_value_counts")
    if null_value_counts:
        null_value_counts = {int(k): v for k, v in null_value_counts.items()}

    return DataFile(
        file_path=df_record["file_path"],
        file_format=FileFormat(df_record["file_format"]),
        partition_values=df_record["partition"]["values"],
        record_count=df_record["record_count"],
        file_size_in_bytes=df_record["file_size_in_bytes"],
        column_sizes=column_sizes,
        value_counts=value_counts,
        null_value_counts=null_value_counts,
        lower_bounds=lower_bounds,
        upper_bounds=upper_bounds,
        checksum=df_record.get("checksum"),
        added_snapshot_id=record.get("snapshot_id"),
        sequence_number=(
            record.get("file_sequence_number")
            if record.get("file_sequence_number") is not None
            else record.get("sequence_number")
        ),
    )

def data_files_from_json(content: bytes, manifest_path: str) -> List[DataFile]:
    """Legacy (pre-Avro) JSON manifests."""
    try:
        manifest_data = json.loads(content.decode("utf-8"))
        data_files = []
        for file_entry in manifest_data.get("files", []):
            data_files.append(DataFile(
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
                checksum=file_entry.get("checksum"),
                added_snapshot_id=file_entry.get("added_snapshot_id"),
                sequence_number=file_entry.get("sequence_number"),
            ))
        return data_files
    except Exception as e:
        raise CorruptDataError(
            f"Could not parse manifest file {manifest_path} (neither Avro nor JSON)"
        ) from e


def manifest_from_avro(record: Dict[str, Any]) -> ManifestFile:
    return ManifestFile(
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
        min_sequence_number=record.get("min_sequence_number"),
        checksum=record.get("checksum"),
    )

