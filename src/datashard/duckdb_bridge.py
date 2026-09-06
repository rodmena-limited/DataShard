"""
DuckDB analytics layer for Table (#78): hand the verified Arrow result to DuckDB,
or point DuckDB at the parquet files directly.

Mixed into Table. `to_duckdb()` / `sql()` go through `to_arrow()`, so every byte
passed datashard's integrity checks; `parquet_paths()` is the explicit fast path
that lets DuckDB read the files itself and therefore bypasses those checks.
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from .file_manager import FileManager
from .s3_backend import S3StorageBackend
from .storage_backend import StorageBackend

if TYPE_CHECKING:
    from .data_structures import DataFile


def _require_duckdb() -> Any:
    try:
        import duckdb
    except ImportError as e:
        raise ImportError(
            "duckdb is required for Table.to_duckdb() / Table.sql(). "
            "Install with: pip install datashard[duckdb]"
        ) from e
    return duckdb


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class _DuckDBMixin:
    storage: StorageBackend
    file_manager: FileManager

    if TYPE_CHECKING:  # provided by Table / _ScanMixin

        def to_arrow(
            self,
            columns: Optional[List[str]] = None,
            filter: Optional[Dict[str, Any]] = None,
            parallel: Union[bool, int] = False,
            verify_checksums: Optional[Union[bool, str]] = None,
            snapshot_id: Optional[int] = None,
        ) -> Any: ...

        def _get_all_data_files(
            self, metadata: Any = None, snapshot_id: Optional[int] = None
        ) -> List["DataFile"]: ...

    def to_duckdb(
        self, connection: Any = None, view_name: str = "t", **scan_kwargs: Any
    ) -> Any:
        """Register this table's data as a DuckDB view and return the connection.

        Data goes through to_arrow() (page-CRC verified, filter/columns/snapshot_id
        honoured via **scan_kwargs). Pass an existing connection to combine several
        tables; a new in-memory connection is created otherwise.
        """
        duckdb = _require_duckdb()
        con = connection if connection is not None else duckdb.connect()
        con.register(view_name, self.to_arrow(**scan_kwargs))
        return con

    def sql(self, query: str, alias: str = "t", **scan_kwargs: Any) -> Any:
        """Run one DuckDB SQL query over this table (visible as `alias`) and return a
        pyarrow.Table. Example: table.sql("SELECT symbol, sum(qty) FROM t GROUP BY 1").
        """
        duckdb = _require_duckdb()
        con = duckdb.connect()
        try:
            self.to_duckdb(con, view_name=alias, **scan_kwargs)
            return con.execute(query).fetch_arrow_table()
        finally:
            con.close()

    def parquet_paths(self, snapshot_id: Optional[int] = None) -> List[str]:
        """Absolute locations of the snapshot's data files, for DuckDB's native
        read_parquet([...]) or any other engine.

        Local: absolute filesystem paths. S3: s3://bucket/key URIs (configure
        DuckDB's httpfs with duckdb_s3_secret_sql()). This path bypasses datashard's
        page-CRC verification and filters - the engine reads the files itself.
        """
        files = self._get_all_data_files(snapshot_id=snapshot_id)
        if isinstance(self.storage, S3StorageBackend):
            bucket = self.storage.bucket
            return [f"s3://{bucket}/{self.storage._get_s3_key(df.file_path)}" for df in files]
        dfm = self.file_manager.data_file_manager
        return [dfm._get_arrow_path(df.file_path) for df in files]

    def duckdb_s3_secret_sql(self, name: str = "datashard_s3") -> str:
        """The CREATE SECRET statement that lets DuckDB's httpfs read this table's
        bucket with the backend's endpoint and credentials (path-style URLs, as
        S3-compatible providers expect). Only for the S3 backend.
        """
        s = self.storage
        if not isinstance(s, S3StorageBackend):
            raise ValueError("duckdb_s3_secret_sql() applies to S3-backed tables only")
        parts = ["TYPE S3", f"REGION {_sql_quote(s.region)}", "URL_STYLE 'path'"]
        if s.endpoint_url:
            scheme, _, host = s.endpoint_url.partition("://")
            parts.append(f"ENDPOINT {_sql_quote(host.rstrip('/'))}")
            parts.append(f"USE_SSL {'false' if scheme.lower() == 'http' else 'true'}")
        if s.access_key and s.secret_key:
            parts.append(f"KEY_ID {_sql_quote(s.access_key)}")
            parts.append(f"SECRET {_sql_quote(s.secret_key)}")
        else:
            parts.append("PROVIDER credential_chain")
        return f"CREATE OR REPLACE SECRET {name} (" + ", ".join(parts) + ");"
