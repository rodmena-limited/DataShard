"""
Migration of pre-0.10 tables to the Iceberg v2 layout, and relocation of moved
tables (#89, #87).

Both rewrite every snapshot's manifest list and manifests (same snapshot ids,
sequence numbers and timestamps; data files untouched) and commit one new metadata
version. Migration additionally retires the legacy root version hint, so 0.9.x and
earlier fail closed on the table instead of committing a divergent lineage. There
is no downgrade.
"""

from copy import deepcopy
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from .data_structures import ManifestFile, PartitionSpec, Snapshot, TableMetadata
from .exceptions import AmbiguousMetadataError
from .file_manager import ENTRY_STATUS_ADDED, ENTRY_STATUS_EXISTING, FileManager
from .logging_config import get_logger
from .manifest_writer import ManifestEntry
from .metadata_manager import MetadataManager
from .metadata_serde import (
    LEGACY_SUMMARY_LIST_LENGTH,
    LEGACY_SUMMARY_LIST_SHA256,
    NAME_MAPPING_PROPERTY,
    is_legacy_document,
    name_mapping_json,
    unrepresentable_fields,
)
from .metadata_serde_legacy import legacy_dict_to_metadata
from .version_hint import (
    LEGACY_HINT_PATH,
    LEGACY_METADATA_FILE_RE,
    METADATA_FILE_RE,
    MIGRATED_HINT_PATH,
)

if TYPE_CHECKING:
    from .table import Table

logger = get_logger(__name__)


def _ordered_snapshots(metadata: TableMetadata) -> List[Snapshot]:
    order = {e.snapshot_id: i for i, e in enumerate(metadata.snapshot_log)}
    return sorted(metadata.snapshots, key=lambda s: (order.get(s.snapshot_id, len(order)), s.timestamp_ms))


def _sequence_numbers(snapshots: List[Snapshot]) -> Dict[int, int]:
    """Snapshot id -> sequence number; snapshots written before sequence numbers
    existed are numbered in commit order (strictly increasing, never reused)."""
    seqs: Dict[int, int] = {}
    last = 0
    for s in snapshots:
        seq = s.sequence_number if s.sequence_number is not None and s.sequence_number > last else last + 1
        seqs[s.snapshot_id] = seq
        last = seq
    return seqs


def rewrite_snapshots(
    fm: FileManager, metadata: TableMetadata, location: str, dry_run: bool = False
) -> Tuple[List[Snapshot], Dict[str, int]]:
    """Rewrite every snapshot's manifests and manifest lists under `location`.

    Manifests shared by several snapshots are rewritten once. Returns the new
    snapshots (same ids) and counts. Nothing is written when dry_run is set.
    """
    ordered = _ordered_snapshots(metadata)
    seqs = _sequence_numbers(ordered)
    stamped = deepcopy(metadata)
    stamped.location = location
    rewritten: Dict[str, ManifestFile] = {}
    counts = {"snapshots": 0, "manifests": 0, "data_files": 0}
    new_snapshots: List[Snapshot] = []
    for snap in ordered:
        exp_len, exp_sum = FileManager.snapshot_list_integrity(snap)
        old_manifests = fm.read_manifest_list_file(snap.manifest_list, expected_length=exp_len, expected_checksum=exp_sum)
        new_manifests: List[ManifestFile] = []
        for om in old_manifests:
            key = fm.to_relative(om.manifest_path)
            if key not in rewritten:
                files = fm.read_manifest_file(om.manifest_path, expected_length=om.manifest_length, expected_checksum=om.checksum)
                entries: List[ManifestEntry] = []
                for f in files:
                    added_by = f.added_snapshot_id if f.added_snapshot_id is not None else om.added_snapshot_id
                    status = ENTRY_STATUS_ADDED if added_by == om.added_snapshot_id else ENTRY_STATUS_EXISTING
                    seq = f.sequence_number if f.sequence_number is not None else seqs.get(added_by, seqs[snap.snapshot_id])
                    entries.append((status, added_by, seq, f))
                counts["manifests"] += 1
                counts["data_files"] += len(entries)
                if dry_run:
                    rewritten[key] = om
                else:
                    rewritten[key] = fm.write_manifest(
                        entries, fm.new_manifest_path(), om.added_snapshot_id,
                        om.sequence_number if om.sequence_number is not None else seqs[snap.snapshot_id],
                        table_metadata=stamped, location=location,
                    )
            new_manifests.append(rewritten[key])
        counts["snapshots"] += 1
        summary = {k: v for k, v in (snap.summary or {}).items() if k not in (LEGACY_SUMMARY_LIST_LENGTH, LEGACY_SUMMARY_LIST_SHA256)}
        parent = snap.parent_snapshot_id if snap.parent_snapshot_id not in (None, -1) else None
        if dry_run:
            new_list_path = snap.manifest_list
        else:
            info = fm.create_manifest_list(
                new_manifests, snap.snapshot_id, parent_snapshot_id=parent,
                sequence_number=seqs[snap.snapshot_id], location=location,
            )
            new_list_path = info.path
            summary.update(info.summary())
        new_snapshots.append(Snapshot(
            snapshot_id=snap.snapshot_id, timestamp_ms=snap.timestamp_ms, manifest_list=new_list_path,
            parent_snapshot_id=parent, operation=snap.operation or "append", summary=summary,
            schema_id=snap.schema_id if snap.schema_id is not None else metadata.current_schema_id,
            sequence_number=seqs[snap.snapshot_id],
        ))
    return new_snapshots, counts


# ------------------------------------------------------------------ migration

def _legacy_current(mm: MetadataManager, metadata_file: Optional[str]) -> Tuple[int, str]:
    """(version, filename) of the legacy table's committed metadata."""
    storage = mm.storage
    if metadata_file is not None:
        m = LEGACY_METADATA_FILE_RE.match(metadata_file) or METADATA_FILE_RE.match(metadata_file)
        if not m:
            raise ValueError(f"{metadata_file!r} is not a metadata file name")
        return int(m.group(1)), metadata_file
    try:
        text = storage.read_file(LEGACY_HINT_PATH).decode("utf-8").strip()
    except FileNotFoundError:
        text = ""
    if text.isdigit():
        return int(text), f"v{text}.metadata.json"
    m = LEGACY_METADATA_FILE_RE.match(text) or METADATA_FILE_RE.match(text)
    if m and storage.exists(f"metadata/{text}"):
        return int(m.group(1)), text
    # No usable hint: scan, refusing to guess between several files at one version (#60)
    candidates: Dict[int, List[str]] = {}
    for rel in storage.list_files("metadata"):
        base = rel.replace("\\", "/").rsplit("/", 1)[-1]
        parent = rel.replace("\\", "/").rsplit("/", 1)[0] if "/" in rel else ""
        if parent not in ("", "metadata"):
            continue
        mm_ = LEGACY_METADATA_FILE_RE.match(base) or METADATA_FILE_RE.match(base)
        if mm_ and is_legacy_document(storage.read_json(f"metadata/{base}")):
            candidates.setdefault(int(mm_.group(1)), []).append(base)
    if not candidates:
        raise ValueError(f"No pre-0.10 datashard table found at {mm.table_path}")
    top = max(candidates)
    names = sorted(candidates[top])
    if len(names) > 1:
        raise AmbiguousMetadataError(
            f"{len(names)} legacy metadata files share version {top}: {names}. One was committed, "
            f"the others were left by failed commits. Pass metadata_file=<the committed one>."
        )
    return top, names[0]


def migrate_table(table_path: str, dry_run: bool = False, metadata_file: Optional[str] = None) -> Dict[str, Any]:
    """Migrate the pre-0.10 table at `table_path` to the Iceberg v2 layout.

    Idempotent: a table that already has an Iceberg metadata version is left
    alone. Returns a report dict; with dry_run nothing is written.
    """
    from .storage_backend import CASConflictError, create_storage_backend

    storage = create_storage_backend(table_path)
    mm = MetadataManager(table_path, storage)
    fm = FileManager(table_path, mm, storage)
    with mm._lock:
        mm.lock_provider.acquire()
        try:
            versions, _legacy_names = mm._metadata_versions_on_disk()
            for v in sorted(versions, reverse=True):
                if not is_legacy_document(storage.read_json(f"metadata/{versions[v]}")):
                    return {"status": "already-migrated", "version": v, "table": table_path}
            legacy_version, legacy_file = _legacy_current(mm, metadata_file)
            legacy_doc = storage.read_json(f"metadata/{legacy_file}")
            if not is_legacy_document(legacy_doc):
                return {"status": "already-migrated", "version": legacy_version, "table": table_path}
            legacy_md = legacy_dict_to_metadata(legacy_doc)
            mm.recorded_location = None  # legacy paths are table-relative
            location = mm.location_uri

            # Pre-0.10 partition specs were never applied to data files (files were not
            # split by partition; `partition_values` were free-form labels). Carrying one
            # into Iceberg metadata would promise a layout the data does not have, so it
            # is dropped - reported, never silently - and dropped BEFORE the manifests are
            # written, since they are stamped with the spec.
            dropped_spec = [pf.name for spec in legacy_md.partition_specs for pf in spec.fields]
            if dropped_spec:
                logger.warning(
                    f"{table_path}: dropping the decorative partition spec {dropped_spec} - pre-0.10 "
                    f"data files were never partitioned by it. Partitioning by value ships in 0.11."
                )
            legacy_md.partition_specs = [PartitionSpec(spec_id=0, fields=[])]
            legacy_md.default_spec_id = 0

            new_snapshots, counts = rewrite_snapshots(fm, legacy_md, location, dry_run=dry_run)
            new_md = deepcopy(legacy_md)
            new_md.location = location
            new_md.snapshots = new_snapshots
            new_md.metadata_log = []
            new_md.last_commit_id = ""
            new_md.last_sequence_number = max((s.sequence_number or 0 for s in new_snapshots), default=legacy_md.last_sequence_number)
            current = next((s for s in new_md.schemas if s.schema_id == new_md.current_schema_id), None)
            unreadable: Dict[str, str] = {}
            if current is not None and current.fields:
                new_md.properties[NAME_MAPPING_PROPERTY] = name_mapping_json(current)
                new_md.last_column_id = max(new_md.last_column_id, max(int(f["id"]) for f in current.fields))
                # Columns Iceberg engines cannot read are migrated as they are - datashard
                # keeps reading them - but the operator must know which ones (#84).
                unreadable = unrepresentable_fields(current)
                if unreadable:
                    logger.warning(
                        f"{table_path}: columns {sorted(unreadable)} use types no Iceberg engine reads "
                        f"reliably; datashard reads them, foreign readers may not. Suggested types: {unreadable}"
                    )
            new_version = legacy_version + 1
            report: Dict[str, Any] = {
                "status": "dry-run" if dry_run else "migrated", "table": table_path, "location": location,
                "from": legacy_file, "to": f"v{new_version}.metadata.json",
                "dropped_partition_fields": dropped_spec,
                "columns_foreign_readers_may_reject": sorted(unreadable),
                **counts,
            }
            if dry_run:
                return report
            try:
                mm._write_metadata_file_exclusive(new_version, new_md)
            except CASConflictError as e:
                raise RuntimeError(f"metadata/v{new_version}.metadata.json already exists; refusing to overwrite") from e
            mm._advance_hint(new_version)
            # Retire the legacy hint: old clients now find no hint and a document
            # they cannot parse - they fail closed instead of forking the lineage.
            try:
                legacy_hint = storage.read_file(LEGACY_HINT_PATH)
                storage.write_file(MIGRATED_HINT_PATH, legacy_hint)
                storage.delete_file(LEGACY_HINT_PATH)
            except FileNotFoundError:
                pass
            mm.current_version = new_version
            logger.warning(f"Migrated {table_path} to Iceberg v2 (v{new_version}); there is no downgrade")
            return report
        finally:
            mm._release_lock_safely()


# ------------------------------------------------------------------ relocation

def relocate_table(table: "Table") -> Dict[str, int]:
    """Rewrite the table's metadata so every path is under the location the table
    is opened at now. A no-op when the recorded location already matches."""
    mm, fm = table.metadata_manager, table.file_manager
    base = mm.refresh()
    if base is None:
        raise ValueError(f"No table at {table.table_path}")
    if base.location == mm.location_uri:
        return {"snapshots": 0, "manifests": 0, "data_files": 0}
    new_snapshots, counts = rewrite_snapshots(fm, base, mm.location_uri)
    new_md = deepcopy(base)
    new_md.snapshots = new_snapshots
    new_md.location = mm.location_uri
    mm.commit(base, new_md, relocate=True)
    return counts
