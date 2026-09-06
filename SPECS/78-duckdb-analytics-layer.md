# Issue #78 — 0.9.0: DuckDB analytics layer - Table.to_arrow(), to_duckdb(), sql(), parquet_paths(), S3 secret helper

Part of the uplift plan (0.9.0). 

EARS SPEC:
- The Table shall expose to_arrow(columns=None, filter=None, parallel=False, verify_checksums=None, snapshot_id=None) returning a pyarrow.Table with the table schema (zero rows for an empty table), with the same filter, integrity and snapshot semantics as scan().
- Where duckdb is installed, Table.to_duckdb(connection=None, view_name="t", **scan_kwargs) shall register the Arrow result as a view on a DuckDB connection and return that connection; Table.sql(query, alias="t", **scan_kwargs) shall run the query against that view and return a pyarrow.Table.
- If duckdb is not installed, then to_duckdb()/sql() shall raise ImportError naming `pip install datashard[duckdb]`.
- The Table shall expose parquet_paths(snapshot_id=None) returning absolute local paths or s3://bucket/key URIs of the snapshot's data files, and duckdb_s3_secret_sql() returning a CREATE SECRET statement (TYPE S3, endpoint host, URL_STYLE 'path', region, key id/secret) for the S3 backend; docs shall state this fast path bypasses page-CRC verification.
- Quantified: table.sql("SELECT count(*) ...") over a 6-file, 12k-row table shall return in < 2x the time of to_arrow() alone (DuckDB registration overhead bounded).

TECHNICAL PROBLEMS: 1. Hand analytical engines the data without a Python-object round trip. 2. Keep the integrity-verified path as the default and make the unverified fast path explicit.
SOLUTION DOMAINS: Apache Arrow as the zero-copy interchange (pyarrow already the storage format); DuckDB Python API (con.register, relation.arrow()); DuckDB S3 secrets (httpfs).
ALTERNATIVES: register parquet paths as a DuckDB view by default [REJECTED: bypasses datashard's integrity checks silently]; Arrow registration default + explicit paths API [CHOSEN]; Polars bridge [DEFERRED: Arrow interchange already covers it].
