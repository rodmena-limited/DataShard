"""
Data-file compaction: Table.rewrite_data_files (#98).

Partitioning multiplies small files - one file per partition per commit - so it ships
with the tool that merges them back. A rewrite reads the rows of several small files and
writes them as one larger file per partition, then commits a single `replace` snapshot
that removes the inputs and adds the outputs. Nothing is deleted from storage: the old
files become unreachable and garbage_collect() reclaims them after its grace period, so
a reader holding an older snapshot keeps working.
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from .data_structures import DataFile
from .logging_config import get_logger

if TYPE_CHECKING:
    from .data_structures import Schema, TableMetadata

logger = get_logger(__name__)

DEFAULT_TARGET_FILE_SIZE = 128 * 1024 * 1024  # 128 MiB, Iceberg's own default
DEFAULT_MIN_INPUT_FILES = 5


class _CompactionMixin:
    """Mixed into Table."""

    table_path: str
    metadata_manager: Any
    file_manager: Any

    if TYPE_CHECKING:  # provided by Table / _ScanMixin

        def _get_all_data_files(
            self, metadata: Optional["TableMetadata"] = None, snapshot_id: Optional[int] = None
        ) -> List[DataFile]: ...

        def _get_current_schema(self, metadata: Optional["TableMetadata"] = None) -> Optional["Schema"]: ...

        def new_transaction(self) -> Any: ...

        @staticmethod
        def _resolve_verify_mode(param: Any) -> str: ...

        def _read_datafile_table(
            self, data_file: DataFile, columns: Any, compute_expr: Any, verify: Any, pa: Any, pq: Any
        ) -> Any: ...

        @staticmethod
        def _concat_aligned(tables: List[Any], pa: Any) -> Any: ...

    def rewrite_data_files(
        self,
        target_file_size_bytes: int = DEFAULT_TARGET_FILE_SIZE,
        partition: Optional[Dict[str, Any]] = None,
        min_input_files: int = DEFAULT_MIN_INPUT_FILES,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Merge small data files into larger ones, one commit for the whole rewrite.

        Files are grouped by PARTITION and never mixed across partitions - an output file
        carries exactly one partition value, or the table's layout would stop matching
        its manifests. Within a partition, files already at or above the target are left
        alone, and the rest are packed into groups that add up to roughly the target.

        Args:
            target_file_size_bytes: the size to pack up to (default 128 MiB).
            partition: rewrite only this partition, e.g. ``{"hour": 19}``.
            min_input_files: leave a partition alone until it has at least this many
                small files, so a steadily-appending table is not rewritten every commit.
            dry_run: report the plan and change nothing.

        Returns:
            ``{"rewritten_files", "added_files", "partitions", "rows", "bytes_before",
            "bytes_after", "committed"}``. `bytes_after` is 0 on a dry run.
        """
        metadata = self.metadata_manager.refresh()
        schema = self._get_current_schema(metadata)
        if metadata is None or schema is None:
            return _empty_report()
        groups = self._plan_rewrite(
            self._get_all_data_files(metadata), target_file_size_bytes, partition, min_input_files
        )
        report = _empty_report()
        report["partitions"] = len({_key(g[0].partition_values or {}) for g in groups})
        report["rewritten_files"] = sum(len(g) for g in groups)
        report["rows"] = sum(df.record_count for g in groups for df in g)
        report["bytes_before"] = sum(df.file_size_in_bytes for g in groups for df in g)
        if not groups or dry_run:
            return report

        import pyarrow as pa
        import pyarrow.parquet as pq

        mode = self._resolve_verify_mode(None)
        dfm = self.file_manager.data_file_manager
        added: List[Tuple[str, DataFile]] = []
        removed: List[str] = []
        with self.new_transaction() as tx:
            for group in groups:
                tables = [
                    self._read_datafile_table(df, None, None, mode, pa, pq) for df in group
                ]
                merged = self._concat_aligned(tables, pa)
                values = dict(group[0].partition_values or {})
                # Defence in depth: an output file carries ONE partition value, so a group
                # that somehow spanned two would relabel rows. Fail rather than write it.
                for other in group[1:]:
                    if dict(other.partition_values or {}) != values:
                        raise RuntimeError(
                            f"refusing to merge files from different partitions: "
                            f"{values} and {dict(other.partition_values or {})}"
                        )
                path = tx._new_data_file_path(values)
                tx._register_inflight(path)
                data_file = dfm.write_arrow_file(
                    file_path=path, table=merged, iceberg_schema=schema, partition_values=values
                )
                added.append((path, data_file))
                removed.extend(df.file_path for df in group)
            for path, data_file in added:
                tx._queue_written_file(path, data_file)
            tx.delete_files(removed)
            tx.mark_replace()
            try:
                report["committed"] = bool(tx.commit())
            except FileNotFoundError as e:
                # Another rewrite (or a delete) removed these inputs while this one was
                # reading them. Nothing is lost and nothing was committed, but the bare
                # "not part of the current snapshot" reads like corruption.
                raise RuntimeError(
                    f"rewrite_data_files: the files this rewrite was merging are no longer in "
                    f"the table - another rewrite or delete committed first. Nothing was changed "
                    f"and no rows were lost; re-run it to merge what is there now. ({e})"
                ) from e
        report["added_files"] = len(added)
        report["bytes_after"] = sum(df.file_size_in_bytes for _p, df in added)
        logger.info(
            f"{self.table_path}: rewrote {report['rewritten_files']} files into {report['added_files']} "
            f"across {report['partitions']} partition(s); "
            f"{report['bytes_before']} -> {report['bytes_after']} bytes"
        )
        return report

    @staticmethod
    def _plan_rewrite(
        data_files: List[DataFile],
        target: int,
        partition: Optional[Dict[str, Any]],
        min_input_files: int,
    ) -> List[List[DataFile]]:
        """Groups of files to merge: per partition, small files packed up to `target`."""
        by_partition: Dict[Tuple[Any, ...], List[DataFile]] = {}
        for df in data_files:
            values = df.partition_values or {}
            if partition is not None and any(values.get(k) != v for k, v in partition.items()):
                continue
            by_partition.setdefault(_key(values), []).append(df)

        groups: List[List[DataFile]] = []
        for files in by_partition.values():
            small = sorted(
                (f for f in files if f.file_size_in_bytes < target), key=lambda f: f.file_size_in_bytes
            )
            if len(small) < max(2, min_input_files):
                continue
            current: List[DataFile] = []
            size = 0
            for f in small:
                if current and size + f.file_size_in_bytes > target:
                    if len(current) > 1:
                        groups.append(current)
                    current, size = [], 0
                current.append(f)
                size += f.file_size_in_bytes
            if len(current) > 1:
                groups.append(current)
        return groups


def _key(values: Dict[str, Any]) -> Tuple[Any, ...]:
    """A grouping key that cannot collide across distinct partition values.

    `str(v)` alone would make `date(2026, 9, 1)` and the string `"2026-09-01"` the same
    key, and a group that mixed two partitions would be written out under ONE of their
    values - rows silently relabelled. The type name keeps them apart.
    """
    return tuple(sorted((k, type(v).__name__, str(v)) for k, v in (values or {}).items()))


def _empty_report() -> Dict[str, Any]:
    return {
        "rewritten_files": 0, "added_files": 0, "partitions": 0, "rows": 0,
        "bytes_before": 0, "bytes_after": 0, "committed": False,
    }
