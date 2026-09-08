"""
Table.verify(): a health check that actually READS (#94).

`row_count()` answers from metadata alone, so it keeps returning the right number for
a table whose files cannot be read at all - during #93 a broken table reported a
perfectly healthy count and only failed on scan, which meant a health check built on
it reported green while the table was unusable. This exercises the real read path and
reports what it found instead of raising, so it can be wired into a health endpoint.
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional

from .logging_config import get_logger
from .metadata_manager import MetadataManager

if TYPE_CHECKING:
    from .data_structures import DataFile, TableMetadata

logger = get_logger(__name__)


class _VerifyMixin:
    """Mixed into Table."""

    table_path: str
    metadata_manager: MetadataManager

    if TYPE_CHECKING:  # provided by Table / _ScanMixin

        def _get_all_data_files(
            self, metadata: Optional["TableMetadata"] = None, snapshot_id: Optional[int] = None
        ) -> List["DataFile"]: ...

        @staticmethod
        def _resolve_verify_mode(param: Any) -> str: ...

        def _read_datafile_table(
            self, data_file: "DataFile", columns: Optional[List[str]], compute_expr: Any,
            verify: Any, pa: Any, pq: Any,
        ) -> Any: ...

    def verify(
        self,
        deep: bool = False,
        limit: Optional[int] = None,
        snapshot_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Read the table and report whether it is actually readable.

        Unlike :meth:`row_count`, which answers from metadata alone, this opens every
        data file the snapshot references and reads it through the same code path a
        scan uses.

        The two modes answer different questions, and the difference matters:

        * the default answers **"can this table be read, and do the rows match the
          manifests"**. It catches a missing, truncated or unreadable file, a page
          that fails its CRC, and a file whose row count or schema disagrees with the
          metadata. It does NOT catch damage in bytes no read touches - a flipped byte
          in the file's leading magic, say, leaves every row correct and is reported
          as healthy, because the table genuinely is readable;
        * ``deep=True`` answers **"is every byte as it was written"**, by re-hashing
          each file against the sha256 recorded at write time. That catches the case
          above, and costs a full download of every file.

        Args:
            deep: re-hash every file against the checksum recorded at write time.
            limit: check at most this many data files (a sampled check for a very
                large table). The report says how many were checked.
            snapshot_id: verify a historical snapshot instead of the current one.

        Returns:
            A dict that is safe to serialise into a health endpoint::

                {"ok": True, "table": "...", "snapshot_id": 123, "snapshots": 4,
                 "data_files": 12, "checked_files": 12, "rows": 934494,
                 "rows_read": 934494, "deep": False, "errors": []}

            `rows` is the count the metadata claims; `rows_read` is what the files
            actually yielded (equal unless something is wrong, and only comparable
            when every file was checked). `ok` is False whenever `errors` is non-empty.

        This never raises for a broken table - a health check that throws is a health
        check whose caller writes an except clause and loses the diagnosis. Errors are
        collected as strings, most useful first.
        """
        report: Dict[str, Any] = {
            "ok": False, "table": self.table_path, "snapshot_id": snapshot_id, "snapshots": 0,
            "data_files": 0, "checked_files": 0, "rows": 0, "rows_read": 0,
            "deep": bool(deep), "errors": [],
        }
        errors: List[str] = report["errors"]

        try:
            metadata = self.metadata_manager.refresh()
        except Exception as e:  # noqa: BLE001 - the report IS the outcome
            errors.append(f"metadata unreadable: {type(e).__name__}: {e}")
            return report
        if metadata is None:
            errors.append(f"no table at {self.table_path}")
            return report
        report["snapshots"] = len(metadata.snapshots)
        if snapshot_id is None:
            current = metadata.current_snapshot_id
            report["snapshot_id"] = None if current in (None, -1) else current

        try:
            data_files: List[DataFile] = self._get_all_data_files(metadata, snapshot_id)
        except Exception as e:  # noqa: BLE001 - unreadable manifests are the finding
            errors.append(f"snapshot is not traversable: {type(e).__name__}: {e}")
            return report
        report["data_files"] = len(data_files)
        report["rows"] = sum(df.record_count for df in data_files)

        mode = "full" if deep else self._resolve_verify_mode(None)
        if mode == "off" and not deep:
            mode = "page"  # verifying with verification disabled would prove nothing
        import pyarrow as pa
        import pyarrow.parquet as pq

        for data_file in data_files[: limit if limit is not None else len(data_files)]:
            report["checked_files"] += 1
            try:
                table = self._read_datafile_table(data_file, None, None, mode, pa, pq)
                report["rows_read"] += table.num_rows
                if table.num_rows != data_file.record_count:
                    errors.append(
                        f"{data_file.file_path}: manifest says {data_file.record_count} rows, "
                        f"the file holds {table.num_rows}"
                    )
            except Exception as e:  # noqa: BLE001
                errors.append(f"{data_file.file_path}: {type(e).__name__}: {e}")

        report["ok"] = not errors
        if not report["ok"]:
            logger.warning(f"verify({self.table_path}) found {len(errors)} problem(s): {errors[0]}")
        return report
