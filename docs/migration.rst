Migrating to the Iceberg v2 format (0.10.0)
===========================================

Tables written by datashard 0.9.x and earlier use the old datashard layout: a root
``metadata.version-hint.text``, ``v{N}-{hex}.metadata.json`` documents with snake_case keys,
and manifests under ``metadata/manifests/``. Version 0.10.0 reads that layout only to migrate
it.

Opening such a table raises :class:`LegacyLayoutError` and names the command; **nothing is
written when it refuses**::

    LegacyLayoutError: Table /lake/trades uses the pre-0.10 datashard layout
    (root version hint / v{N}-{hex} metadata files). Run 'datashard migrate /lake/trades' once ...

Run it
------

.. code-block:: console

   $ datashard migrate /lake/trades --dry-run
   {"status": "dry-run", "table": "/lake/trades", "from": "v7-a37f3d22.metadata.json",
    "to": "v8.metadata.json", "snapshots": 7, "manifests": 6, "data_files": 8}

   $ datashard migrate /lake/trades
   {"status": "migrated", ...}

or from Python:

.. code-block:: python

   from datashard import migrate_table

   migrate_table("/lake/trades")            # idempotent
   migrate_table("/lake/trades", dry_run=True)

For a table on S3, set the usual ``DATASHARD_S3_*`` environment variables first; the command
uses the same backend as the library.

What it does
------------

* Rewrites every snapshot's manifest list and manifests into the Iceberg v2 form, preserving
  snapshot ids, parent links, timestamps and sequence numbers.
* **Leaves data files exactly where they are** - nothing is copied or rewritten, so the cost
  is proportional to the number of snapshots, not to the size of the table.
* Writes ``schema.name-mapping.default`` so those existing parquet files (which have no
  ``PARQUET:field_id``) resolve for DuckDB and pyiceberg.
* Converts column bounds only where the Iceberg binary encoding is exact for the type, and
  drops them otherwise. A dropped bound costs pruning; a wrong one would cost correctness.
* Commits one new metadata version through the normal commit point, under the table lock.
* Renames the old root hint to ``metadata.version-hint.text.migrated``.

It is idempotent: running it on a migrated table reports ``already-migrated`` and changes
nothing.

There is no downgrade
---------------------

After migration, datashard 0.9.x and earlier cannot read or write the table - they fail
closed rather than committing a divergent lineage. This is verified in the harness against
the released 0.7.2 and 0.9.1 packages: the old client raises, and it writes nothing.

If you need a rollback path, copy the table directory before migrating.

Verify it
---------

.. code-block:: python

   from datashard import load_table

   t = load_table("/lake/trades")
   print(t.row_count(), len(t.snapshots()))

.. code-block:: sql

   -- and from an engine that never knew datashard existed
   SELECT count(*) FROM iceberg_scan('/lake/trades');

If the version hint is missing and several legacy metadata files share the highest version
(one committed, the rest left by failed commits), migration refuses rather than guessing.
Pass the committed one explicitly::

    datashard migrate /lake/trades --metadata-file v7-a37f3d22.metadata.json
