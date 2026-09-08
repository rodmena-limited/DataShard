Interoperability
================

Since 0.10.0 a datashard table **is** an Apache Iceberg v2 table on disk. Any engine that
reads Iceberg reads it: DuckDB, pyiceberg, Spark, Trino. Nothing needs exporting.

The layout::

    <table>/
      metadata/
        v{N}.metadata.json     Iceberg v2 table metadata; creating it IS the commit
        version-hint.text      "N" - a pointer for readers, not the source of truth
        snap-<id>-1-<uuid>.avro   manifest lists
        <uuid>-m0.avro            manifests
      data/
        *.parquet              every column tagged with PARQUET:field_id

Paths inside the metadata are absolute URIs (``file:///...`` or ``s3://bucket/prefix/...``),
which is what foreign readers require. :attr:`Table.location` gives you the URI to point
them at.

DuckDB
------

.. code-block:: sql

   INSTALL iceberg; LOAD iceberg;
   SELECT * FROM iceberg_scan('/lake/trades');
   SELECT * FROM iceberg_scan('/lake/trades', snapshot_from_id => 3410625483784311857);
   SELECT * FROM iceberg_snapshots('/lake/trades');

DuckDB resolves ``metadata/version-hint.text`` when you give it the table directory. Point it
at a specific ``metadata/v{N}.metadata.json`` to pin a version.

For a table on S3-compatible storage, let datashard build the credentials statement so the
endpoint and path-style settings match the backend:

.. code-block:: python

   con.execute(table.duckdb_s3_secret_sql())
   con.execute(f"SELECT count(*) FROM iceberg_scan('{table.location}')")

datashard also ships a first-class DuckDB bridge that goes through Arrow instead of the
Iceberg reader (page-CRC verified, no extension needed) - see :doc:`duckdb`.

pyiceberg
---------

.. code-block:: python

   from pyiceberg.table import StaticTable

   t = StaticTable.from_metadata("/lake/trades/metadata/v7.metadata.json")
   t.scan().to_arrow()
   t.scan(snapshot_id=3410625483784311857).to_arrow()

Pass ``properties={"s3.endpoint": ..., "s3.access-key-id": ..., "s3.secret-access-key": ...}``
for a table on S3. Note that pyiceberg's *string* row filters cannot parse a decimal literal
(``"px == 2.5"`` raises); use the expression API with a ``Decimal`` instead.

Column statistics are consumed by these engines
-----------------------------------------------

Bounds are written in Iceberg's single-value binary form and foreign readers **prune files
on them**: a wrong bound makes them return wrong rows, not merely slow ones. datashard
therefore writes a bound only where the encoding for that type is exact, and omits it
otherwise (no pruning, correct results). The acceptance probe
``audit/evaluations/probe_v0100_foreign_readers.py`` includes a negative control that
falsifies a bound and requires DuckDB and pyiceberg to go wrong, so a regression in this
encoding cannot pass unnoticed.

Files written without field ids
-------------------------------

Tables carry ``schema.name-mapping.default``, so parquet files that have no
``PARQUET:field_id`` - migrated tables and files handed to :meth:`Transaction.append_files`
by other tools - still resolve by column name for DuckDB and pyiceberg. Without both the
field ids and the mapping, DuckDB silently returns all-NULL rows, which is why the mapping
is always written.

Column types
------------

Every type datashard writes maps to an Iceberg primitive that both readers accept, with two
exceptions that are **refused for new tables** since 0.10:

============  ================================================================
``uuid``      datashard writes a parquet *string*; pyiceberg refuses to promote
              it to ``uuid``. Use ``string`` - identical bytes on disk.
``fixed``     Iceberg requires a length (``fixed[16]``); a bare ``fixed`` fails
              pyiceberg's type parser. Use ``binary``.
============  ================================================================

A table created before 0.10 that already has such a column keeps working in datashard, and
``datashard migrate`` lists the affected columns under
``columns_foreign_readers_may_reject``. Change the declared type to the suggestion above
when you can: the parquet files do not need rewriting.

Struct, list and map columns are not supported (datashard's schema validation refuses them).

Limits you must know about
--------------------------

**The local and S3 backends take different read paths** (see :ref:`operating
<commit-cost>`, "Local and S3 are two implementations"), so a claim verified on one is
not automatically true of the other.


**datashard must be the only writer.** pyiceberg and Spark commit with their own metadata
file naming and do not maintain ``version-hint.text``. Their commits are therefore invisible
to datashard and to DuckDB-by-directory, and the files they add are not reachable from
datashard's lineage, so :meth:`Table.garbage_collect` would reclaim them. Reading from any
engine is fully supported; writing from another engine is not, until the REST catalog client
in 1.0. If you need multi-engine writes today, give each engine its own table.

**Merge-on-read tables are refused, not misread.** If a manifest holds delete files
(``content=1`` or ``2``), datashard raises instead of returning the rows those deletes
removed. Applying positional deletes ships in 1.0.

**A crashed writer can leave foreign readers one version behind** for a few seconds: the
commit is durable but the hint has not moved yet. Any datashard read or write heals it.

Moving a table
--------------

Absolute URIs mean a table that is copied or renamed no longer matches its recorded
location. datashard reads it anyway (it resolves paths under the actual root), and DuckDB
can with ``allow_moved_paths => true``. To make it clean for every reader, rewrite the
metadata once::

    datashard relocate /new/path/to/table

Migrating a pre-0.10 table
--------------------------

See :doc:`migration`.
