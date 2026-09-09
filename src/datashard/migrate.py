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
from .manifest_writer import ManifestEntry, encode_manifest, encode_manifest_list
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


# Left in the table ROOT by a migration. A pre-0.10 client reading a migrated table dies
# with a bare KeyError deep in metadata parsing, which reads as "my lake is corrupt"
# rather than "my library is too old" - and that client is already released, so the only
# place left to say so is the directory the operator will look at next (#97).
MIGRATION_NOTICE_PATH = "DATASHARD-MIGRATED-TO-ICEBERG-V2.txt"

_MIGRATION_NOTICE = """This table was migrated to the Apache Iceberg v2 layout by datashard {version}
on {when}.

IF A CLIENT FAILS ON THIS TABLE WITH A BARE KeyError (for example KeyError: 'schema_id'),
THE DATA IS FINE AND THE CLIENT IS TOO OLD. datashard 0.9.x and earlier cannot parse
Iceberg metadata; they read the snake_case format this table no longer uses.

    pip install --upgrade 'datashard>={version}'

There is no downgrade. The pre-0.10 metadata is preserved beside the new metadata until
garbage_collect() reclaims it, and the previous version hint was renamed to
metadata.version-hint.text.migrated.

This table is now readable by any Iceberg engine - DuckDB's iceberg extension, pyiceberg,
Spark, Trino - directly from this directory. datashard must remain its only WRITER until
the REST catalog client in 1.0.

This file is a note for humans. Nothing reads it, and deleting it changes nothing.
"""


def write_migration_notice(storage: Any) -> None:
    """Leave the human-readable breadcrumb described above. Never fails a migration."""
    from datetime import datetime, timezone

    from . import __version__

    try:
        storage.write_file(
            MIGRATION_NOTICE_PATH,
            _MIGRATION_NOTICE.format(
                version=__version__, when=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            ).encode("utf-8"),
        )
    except Exception as e:  # noqa: BLE001 - a note must never fail the operation it describes
        logger.warning(f"Could not write {MIGRATION_NOTICE_PATH}: {e}")


def _metadata_bytes(storage: Any) -> int:
    """Bytes currently under metadata/ - what migration adds to, before GC reclaims it."""
    try:
        return sum(size for _p, size in _sizes(storage, "metadata"))
    except Exception:  # noqa: BLE001 - a size estimate must never fail a migration
        return 0


def _sizes(storage: Any, prefix: str) -> List[Tuple[str, int]]:
    out: List[Tuple[str, int]] = []
    for rel in storage.list_files(prefix):
        try:
            out.append((rel, storage.get_size(rel)))
        except Exception:  # noqa: BLE001
            continue
    return out


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
    snapshots (same ids) and counts, including `metadata_bytes_added`: the exact size
    of the new metadata. On a dry run nothing is written, but the same bytes are
    ENCODED in memory and measured, so the projection is measured rather than guessed
    (#94 - migration writes the new metadata alongside the old, so a table needs the
    headroom before it starts).
    """
    ordered = _ordered_snapshots(metadata)
    seqs = _sequence_numbers(ordered)
    stamped = deepcopy(metadata)
    stamped.location = location
    rewritten: Dict[str, ManifestFile] = {}
    counts = {"snapshots": 0, "manifests": 0, "data_files": 0, "metadata_bytes_added": 0}
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
                seq_for_manifest = (
                    om.sequence_number if om.sequence_number is not None else seqs[snap.snapshot_id]
                )
                if dry_run:
                    # Encode without writing, purely to measure what migration will add.
                    schema, spec = fm._current_schema_and_spec(stamped)
                    counts["metadata_bytes_added"] += len(encode_manifest(entries, schema, spec, location))
                    rewritten[key] = om
                else:
                    rewritten[key] = fm.write_manifest(
                        entries, fm.new_manifest_path(), om.added_snapshot_id, seq_for_manifest,
                        table_metadata=stamped, location=location,
                    )
                    counts["metadata_bytes_added"] += rewritten[key].manifest_length
            new_manifests.append(rewritten[key])
        counts["snapshots"] += 1
        summary = {k: v for k, v in (snap.summary or {}).items() if k not in (LEGACY_SUMMARY_LIST_LENGTH, LEGACY_SUMMARY_LIST_SHA256)}
        parent = snap.parent_snapshot_id if snap.parent_snapshot_id not in (None, -1) else None
        if dry_run:
            new_list_path = snap.manifest_list
            counts["metadata_bytes_added"] += len(encode_manifest_list(
                new_manifests, snap.snapshot_id, parent, seqs[snap.snapshot_id], location))
        else:
            info = fm.create_manifest_list(
                new_manifests, snap.snapshot_id, parent_snapshot_id=parent,
                sequence_number=seqs[snap.snapshot_id], location=location,
            )
            new_list_path = info.path
            summary.update(info.summary())
            counts["metadata_bytes_added"] += info.length
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

    Data files are never rewritten or moved; only metadata is. Idempotent: a table
    that already carries an Iceberg metadata version is left alone.

    **Migration needs headroom.** The new metadata is written ALONGSIDE the old, which
    is only reclaimed by a later ``garbage_collect()``, so the table peaks at roughly
    its current size plus ``metadata_bytes_added`` before it shrinks. On one production
    table that peak was +46 %. Run with ``dry_run=True`` first: it measures the exact
    figure by encoding the new metadata in memory without writing it.

    Args:
        table_path: the table to migrate.
        dry_run: report what would be written and change nothing.
        metadata_file: the committed legacy metadata file, for the rare table whose
            version hint is missing AND whose highest version is ambiguous.

    Returns:
        A report dict:

        ``status``
            ``"migrated"``, ``"dry-run"``, or ``"already-migrated"``.
        ``table`` / ``location``
            the path migrated, and the URI foreign readers should be pointed at.
        ``from`` / ``to``
            the legacy metadata file read, and the Iceberg version written.
        ``snapshots`` / ``manifests`` / ``data_files``
            how many of each the migration rewrote or re-listed. `data_files` counts
            manifest ENTRIES rewritten, not parquet files touched - none are.
        ``metadata_bytes_now`` / ``metadata_bytes_added`` / ``peak_bytes``
            the metadata size before migration, the size migration adds, and the sum -
            the headroom the volume needs before the next ``garbage_collect()``
            reclaims the old metadata. All three are measured, not estimated: a dry
            run encodes the very bytes it would write. All three are absent from an
            ``already-migrated`` report.
        ``dropped_partition_fields``
            partition-spec fields discarded because pre-0.10 data was never
            partitioned by them (empty for almost every table).
        ``columns_foreign_readers_may_reject``
            columns whose type no Iceberg engine reads reliably (``uuid``, ``fixed``);
            datashard still reads them. Empty for almost every table.
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
                    # Idempotent, and it also back-fills the notice on a table migrated
                    # by an earlier version that did not leave one.
                    if not dry_run and not storage.exists(MIGRATION_NOTICE_PATH):
                        write_migration_notice(storage)
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

            # Measured BEFORE anything is written, so the figure means the same thing
            # on a dry run and on the real one.
            metadata_bytes_now = _metadata_bytes(storage)
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
                "metadata_bytes_now": metadata_bytes_now,
                "peak_bytes": metadata_bytes_now + counts["metadata_bytes_added"],
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
            write_migration_notice(storage)
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
