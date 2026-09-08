Operating a datashard table
===========================

Everything here comes from running datashard in production. Read the first section
before you write a recorder; it is the one that costs real money if you meet it by
surprise.

.. _commit-cost:

What a commit costs
-------------------

**One append is one commit, and one commit rewrites the table's whole metadata
document.** That document lists every snapshot the table has, so it grows with the
number of commits — and because each commit writes a fresh copy, the metadata a table
accumulates grows with the **square** of the commit count.

A production recorder that appended one row every 28 seconds for a day, per symbol,
measured this on a single table::

    *.parquet    3,027 files      9.5 MB   <- the data, mean 3.3 KB per file
    *.avro      12,104 files  1,508.6 MB
    *.json       3,029 files  1,962.0 MB   <- one per commit, mean 648 KB

9.5 MB of data carried by 3.5 GB of metadata. Nothing was wrong with the table; that
is simply what 3,027 commits cost.

Since 0.10.2 datashard tells you while it is happening: when the metadata document
passes 1 MiB, a WARNING names the size, the snapshot count, the cost model and the two
remedies, and repeats only when the size doubles. Set the
``datashard.metadata.warn-bytes`` table property to move the threshold, or ``0`` to
silence it.

**Batch your appends.** The fix is one transaction per batch instead of one per row:

.. code-block:: python

   # one commit per row: 3,027 commits an hour, and the cost above
   for row in stream:
       table.append_records([row], schema)

   # one commit per batch: same rows, ~1 % of the metadata
   buffer = []
   for row in stream:
       buffer.append(row)
       if len(buffer) >= 5_000:          # or every N seconds - whichever your latency budget allows
           table.append_records(buffer, schema)
           buffer.clear()

For several operations in one atomic commit — appends plus a delete, or appends across
calls — use a transaction directly:

.. code-block:: python

   with table.new_transaction() as tx:
       tx.append_records(morning, schema)
       tx.append_records(afternoon, schema)
       tx.commit()                        # ONE snapshot, one metadata write

Every row group is written per data file, so a batch also produces bigger, faster-
scanning parquet files. A commit is atomic whatever its size: a batch is not a
durability trade-off.

Keeping a long-running table small
----------------------------------

Two steps, and they do different things. Running only the first frees nothing.

.. code-block:: python

   removed = table.expire_snapshots(retain_last=48)   # 1. shorten the history
   stats   = table.garbage_collect()                  # 2. delete what that made unreachable

:meth:`~datashard.Table.expire_snapshots` drops old snapshots from the metadata
document — which is what stops the quadratic growth — and, since 0.10.2, folds
manifest compaction into the *same* commit, so the manifest chain collapses too. It
returns how many snapshots were **removed from the metadata**; it deletes no files and
frees no bytes.

:meth:`~datashard.Table.garbage_collect` is what frees bytes. Two things about it
surprise people:

* a file's age is its **modification time on the storage**, not its logical age. A
  table you just copied, restored or unpacked has fresh mtimes on every file, so
  nothing in it can be collected until the grace period has passed. That is the guard
  that stops a concurrent writer's in-flight files being deleted;
* the default grace period is an hour, and anything under five minutes is refused
  unless you pass ``allow_short_grace=True`` — safe only when no other writer exists.

A daily maintenance job is usually all a busy table needs:

.. code-block:: python

   t = load_table(path)
   t.expire_snapshots(retain_last=100)       # keep ~a day of time travel
   t.garbage_collect()                       # reclaim what that released

Is this table actually readable?
--------------------------------

:meth:`~datashard.Table.row_count` answers from the manifests without opening a single
data file. That is what makes it fast, and it is also why **it is not a health check**:
it keeps returning the right number for a table whose files cannot be read at all. A
monitor built on it reports green while the table is unusable.

:meth:`~datashard.Table.verify` reads:

.. code-block:: python

   report = table.verify()
   # {"ok": True, "table": "...", "snapshot_id": 123, "snapshots": 4,
   #  "data_files": 12, "checked_files": 12, "rows": 934494, "rows_read": 934494,
   #  "deep": False, "errors": []}

It opens every data file the current snapshot references, through the same code path a
scan uses. It **never raises** for a broken table — the report is the answer, and
``errors`` names each bad file — so it drops straight into a health endpoint, and the
``datashard verify <table>`` command exits 1 when ``ok`` is false:

.. code-block:: console

   $ datashard verify /lake/trades || alert "trades unreadable"

The two modes answer different questions:

* the **default** answers *can this table be read, and do the rows match the
  manifests*. It catches a missing, truncated or unreadable file, a page that fails
  its CRC, and a file whose row count or schema disagrees with the metadata. It does
  not catch damage in bytes that no read touches — a flipped byte in a file's leading
  magic leaves every row correct, and is reported as healthy, because the table
  genuinely is readable;
* ``verify(deep=True)`` (``--deep``) answers *is every byte as it was written*, by
  re-hashing each file against the checksum recorded at write time. It catches the
  case above, and downloads every byte to do it.

``verify(limit=n)`` (``--limit n``) checks a sample of ``n`` files, for a table too
large to read whole on every probe.

Filters, and what pruning does and does not do
----------------------------------------------

Filters are always **correct**, whatever datashard knows about the layout. Pruning only
changes how much is read:

.. code-block:: python

   rows = table.scan(filter={"hour": ("between", (19, 19))})

Today a filter is applied at two levels: files whose recorded column bounds cannot
match are skipped entirely, and inside each remaining file parquet's own row-group
statistics skip most of the rest. Partitioning by value — which prunes at the directory
level, before a file is opened at all — ships in 0.11. Until then a filtered scan reads
more files than it strictly must, and returns exactly the same rows: one production
table measured 0.09 s slower on a filtered scan without partition pruning. There is no
reason to rewrite a query layer while waiting for it.

Local and S3 are two implementations, not one
---------------------------------------------

The two backends take genuinely different code paths on read: local files are opened by
path so pyarrow reads them natively, while S3 objects are read through datashard's own
range reader over a file object (pyarrow's S3 client sends a header some providers
reject). They are pinned against each other by tests, but **"it works on S3" and "it
works locally" are separate claims**, and a report about one should say which it was.
The same holds for the storage guarantees: the S3 commit point is a conditional PUT,
the local one is a hard link.
