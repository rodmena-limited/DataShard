Analytics with DuckDB
=====================

DataShard stores data as Parquet and reads it as Apache Arrow; DuckDB is the query
layer. Install the extra::

   pip install datashard[duckdb]

Query a table
-------------

.. code-block:: python

   from datashard import load_table

   table = load_table("/data/trades")

   # One-shot SQL; the table is visible as `t`, the result is a pyarrow.Table
   by_symbol = table.sql("SELECT symbol, sum(qty) AS qty FROM t GROUP BY 1 ORDER BY 1")

   # Filters, projection and time travel are pushed into DataShard's read path
   failed = table.sql("SELECT count(*) FROM trades WHERE status = 'failed'",
                      alias="trades", snapshot_id=earlier_snapshot_id)

   # Your own connection, several tables, joins
   con = table.to_duckdb(view_name="trades")
   other.to_duckdb(con, view_name="fx")
   con.execute("SELECT ... FROM trades JOIN fx USING (ccy)").fetch_arrow_table()

   # Arrow directly, for Polars / pandas / your own engine
   arrow = table.to_arrow(columns=["symbol", "qty"], filter={"qty": (">", 0)})

Everything above goes through ``to_arrow()``: parquet page checksums are verified,
filters use column statistics to skip files, and ``snapshot_id`` reads history.

Fast path: DuckDB reads the files itself
----------------------------------------

For large scans you can hand DuckDB the parquet files and let its own reader do the
work. This bypasses DataShard's integrity verification and filters.

.. code-block:: python

   con = duckdb.connect()
   con.execute(table.duckdb_s3_secret_sql())        # S3 tables only: httpfs credentials
   paths = table.parquet_paths()                    # current snapshot; pass snapshot_id= for history
   con.execute("SELECT count(*) FROM read_parquet($p)", {"p": paths}).fetchone()

Ingest from Arrow
-----------------

``append_arrow()`` writes a ``pyarrow.Table`` in one pass - the natural path for DuckDB
results, Polars frames and anything that speaks Arrow:

.. code-block:: python

   result = con.execute("SELECT ... ").fetch_arrow_table()
   table.append_arrow(result)

Columns are conformed to the table's schema: unknown columns are refused, absent
optional columns are filled with nulls, columns are reordered and widened within their
type family (``int32`` into ``long``, ``float`` into ``double``). Coercion across
families - the string ``"12"`` into a ``long``, an integer into a ``string`` - is
refused rather than performed silently.

Batch rows per commit. A commit costs a fixed handful of storage round trips whatever
its size, so thousands of rows per append is the right granularity.
