"""
Decoding manifests and manifest lists into DataFile / ManifestFile (#85, #87).

Three generations are readable: Iceberg v2 Avro (written since 0.10, or by a
foreign writer), datashard's pre-0.10 Avro (map<string> statistics, tagged-JSON
bounds) and the pre-Avro JSON of 0.7. Paths come back TABLE-RELATIVE whatever
form they were stored in.
"""

import json
from io import BytesIO
from typing import Any, Callable, Dict, List, Tuple

import fastavro

from .bounds import decode_bound as decode_legacy_bound
from .data_structures import DataFile, FileFormat, ManifestContent, ManifestFile
from .iceberg_avro import count_field_names
from .iceberg_bounds import decode_bound
from .integrity import CorruptDataError

AVRO_MAGIC = b"Obj\x01"

ToRelative = Callable[[str], str]


def avro_container(content: bytes, path: str, kind: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """(header metadata, records) of an Avro container, or CorruptDataError.

    fastavro stops cleanly when a file ends exactly on a block boundary, which is
    why the length/sha256 check in FileManager exists; this catches everything
    else (a block cut mid-way, a bad header, trailing garbage).
    """
    bio = BytesIO(content)
    try:
        reader = fastavro.reader(bio)
        meta = dict(reader.metadata or {})
        records = [dict(r) for r in reader]  # type: ignore[arg-type]
    except Exception as e:
        raise CorruptDataError(f"{kind} {path} is not a readable Avro container: {e}") from e
    if bio.tell() != len(content):
        raise CorruptDataError(f"{kind} {path} has {len(content) - bio.tell()} unread trailing bytes")
    return meta, records


def avro_records(content: bytes, path: str, kind: str) -> List[Dict[str, Any]]:
    """Records only (compatibility helper)."""
    return avro_container(content, path, kind)[1]


def is_iceberg_manifest(header: Dict[str, Any]) -> bool:
    return "schema" in header or "format-version" in header


# ------------------------------------------------------------------ helpers

def _pairs(value: Any) -> Dict[int, Any]:
    """An Avro map (dict) or Iceberg's array-of-{key,value} into {int: value}."""
    out: Dict[int, Any] = {}
    items: Any
    if isinstance(value, dict):
        items = value.items()
    elif isinstance(value, list):
        items = ((e.get("key"), e.get("value")) for e in value if isinstance(e, dict))
    else:
        return out
    for k, v in items:
        try:
            out[int(k)] = v
        except (TypeError, ValueError):
            continue
    return out


def _labels(raw: Any) -> Dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _iceberg_field_types(header: Dict[str, Any]) -> Dict[int, Any]:
    try:
        schema = json.loads(header["schema"])
    except (KeyError, ValueError, TypeError):
        return {}
    return {int(f["id"]): f.get("type") for f in schema.get("fields", []) if "id" in f}


# ------------------------------------------------------------------ manifests

def data_file_from_iceberg(record: Dict[str, Any], types: Dict[int, Any], to_rel: ToRelative) -> DataFile:
    df: Dict[str, Any] = record["data_file"]
    if int(df.get("content", 0) or 0) != 0:
        raise NotImplementedError(
            f"Manifest entry {df.get('file_path')!r} is a delete file (content={df.get('content')}); "
            f"this datashard version reads data files only. Row-level deletes written by "
            f"another engine are not applied - refusing to return wrong rows."
        )
    lower = {k: decode_bound(v, types.get(k)) for k, v in _pairs(df.get("lower_bounds")).items() if k in types}
    upper = {k: decode_bound(v, types.get(k)) for k, v in _pairs(df.get("upper_bounds")).items() if k in types}
    lower = {k: v for k, v in lower.items() if v is not None}
    upper = {k: v for k, v in upper.items() if v is not None}
    seq = record.get("file_sequence_number")
    if seq is None:
        seq = record.get("sequence_number")
    return DataFile(
        file_path="/" + to_rel(df["file_path"]),
        file_format=FileFormat(str(df["file_format"]).lower()),
        partition_values=_labels(df.get("datashard_partition_labels")),
        record_count=int(df["record_count"]),
        file_size_in_bytes=int(df["file_size_in_bytes"]),
        column_sizes=_pairs(df.get("column_sizes")) or None,
        value_counts=_pairs(df.get("value_counts")) or None,
        null_value_counts=_pairs(df.get("null_value_counts")) or None,
        lower_bounds=lower or None,
        upper_bounds=upper or None,
        checksum=df.get("datashard_sha256"),
        split_offsets=list(df["split_offsets"]) if df.get("split_offsets") else None,
        sort_order_id=df.get("sort_order_id"),
        added_snapshot_id=record.get("snapshot_id"),
        sequence_number=seq,
    )


def data_file_from_avro(record: Dict[str, Any]) -> DataFile:
    """datashard's pre-0.10 Avro manifest entry."""
    df_record: Dict[str, Any] = record["data_file"]
    lower_bounds = df_record.get("lower_bounds")
    if lower_bounds:
        lower_bounds = {int(k): decode_legacy_bound(v) for k, v in lower_bounds.items()}
    upper_bounds = df_record.get("upper_bounds")
    if upper_bounds:
        upper_bounds = {int(k): decode_legacy_bound(v) for k, v in upper_bounds.items()}
    seq = record.get("file_sequence_number")
    if seq is None:
        seq = record.get("sequence_number")
    return DataFile(
        file_path=df_record["file_path"],
        file_format=FileFormat(df_record["file_format"]),
        partition_values=df_record["partition"]["values"],
        record_count=df_record["record_count"],
        file_size_in_bytes=df_record["file_size_in_bytes"],
        column_sizes=_pairs(df_record.get("column_sizes")) or None,
        value_counts=_pairs(df_record.get("value_counts")) or None,
        null_value_counts=_pairs(df_record.get("null_value_counts")) or None,
        lower_bounds=lower_bounds or None,
        upper_bounds=upper_bounds or None,
        checksum=df_record.get("checksum"),
        added_snapshot_id=record.get("snapshot_id"),
        sequence_number=seq,
    )


def data_files_from_json(content: bytes, manifest_path: str) -> List[DataFile]:
    """Legacy (pre-Avro, 0.7) JSON manifests."""
    try:
        manifest_data = json.loads(content.decode("utf-8"))
        return [
            DataFile(
                file_path=e["file_path"],
                file_format=FileFormat(e["file_format"]),
                partition_values=e["partition_values"],
                record_count=e["record_count"],
                file_size_in_bytes=e["file_size_in_bytes"],
                column_sizes=e.get("column_sizes"),
                value_counts=e.get("value_counts"),
                null_value_counts=e.get("null_value_counts"),
                lower_bounds=e.get("lower_bounds"),
                upper_bounds=e.get("upper_bounds"),
                checksum=e.get("checksum"),
                added_snapshot_id=e.get("added_snapshot_id"),
                sequence_number=e.get("sequence_number"),
            )
            for e in manifest_data.get("files", [])
        ]
    except Exception as e:
        raise CorruptDataError(f"Could not parse manifest file {manifest_path} (neither Avro nor JSON)") from e


def decode_manifest(content: bytes, path: str, to_rel: ToRelative) -> List[DataFile]:
    """Every entry of a manifest of any generation, paths table-relative."""
    if not content.startswith(AVRO_MAGIC):
        return data_files_from_json(content, path)
    header, records = avro_container(content, path, "Manifest")
    if is_iceberg_manifest(header):
        types = _iceberg_field_types(header)
        return [data_file_from_iceberg(r, types, to_rel) for r in records]
    return [data_file_from_avro(r) for r in records]


# ------------------------------------------------------------------ manifest lists

def manifest_from_iceberg(record: Dict[str, Any], to_rel: ToRelative) -> ManifestFile:
    counts = count_field_names(record)
    content = ManifestContent(int(record.get("content", 0) or 0))
    if content != ManifestContent.DATA:
        raise NotImplementedError(
            f"Manifest {record.get('manifest_path')!r} holds delete files; this datashard version "
            f"reads data manifests only. Refusing to return rows with deletes unapplied."
        )
    return ManifestFile(
        manifest_path=to_rel(record["manifest_path"]),
        manifest_length=int(record["manifest_length"]),
        partition_spec_id=int(record.get("partition_spec_id", 0) or 0),
        added_snapshot_id=int(record["added_snapshot_id"]),
        added_data_files_count=counts["added_files_count"],
        existing_data_files_count=counts["existing_files_count"],
        deleted_data_files_count=counts["deleted_files_count"],
        partitions=[],
        content=content,
        sequence_number=record.get("sequence_number"),
        min_sequence_number=record.get("min_sequence_number"),
        checksum=record.get("datashard_sha256"),
        added_rows_count=record.get("added_rows_count"),
        existing_rows_count=record.get("existing_rows_count"),
    )


def manifest_from_avro(record: Dict[str, Any]) -> ManifestFile:
    """datashard's pre-0.10 manifest-list record."""
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


def decode_manifest_list(content: bytes, path: str, to_rel: ToRelative) -> List[ManifestFile]:
    """Every manifest named by a manifest list of any generation, paths table-relative."""
    if content.startswith(AVRO_MAGIC):
        header, records = avro_container(content, path, "Manifest list")
        if "format-version" in header or "snapshot-id" in header:
            return [manifest_from_iceberg(r, to_rel) for r in records]
        return [manifest_from_avro(r) for r in records]
    try:
        list_data = json.loads(content.decode("utf-8"))
        return [
            ManifestFile(
                manifest_path=e["manifest_path"],
                manifest_length=e["manifest_length"],
                partition_spec_id=e["partition_spec_id"],
                added_snapshot_id=e["added_snapshot_id"],
                added_data_files_count=e["added_data_files_count"],
                existing_data_files_count=e["existing_data_files_count"],
                deleted_data_files_count=e["deleted_data_files_count"],
                partitions=[],
                content=ManifestContent(e["content"]),
            )
            for e in list_data.get("manifests", [])
        ]
    except Exception as e:
        raise CorruptDataError(f"Could not parse manifest list file {path} (neither Avro nor JSON)") from e
