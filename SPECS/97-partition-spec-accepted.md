# #97 — create_table must accept a partition spec it cannot apply (0.10.5)

Reported by the crypto-trader platform, 2026-09-09, against a 429-table production lake.

## Requirements (EARS)

- **When** `create_table` receives a `partition_spec` whose fields are non-empty, datashard
  **shall** accept the call, persist an unpartitioned spec, and log a WARNING naming the dropped
  fields, why they are dropped, and that filters still return correct rows meanwhile.
- The requested fields **shall** be recorded in `datashard.requested-partition-fields`, so 0.11 can
  offer to apply them rather than the intent having been lost.
- The CHANGELOG **shall** describe this as a REMOVAL that 0.10.0 made, in the 0.10.0 entry an
  upgrader reads, not only in the entry that fixes it.
- `migrate_table` **shall** leave a breadcrumb in the table root, because a pre-0.10 client reading
  a migrated table dies with a bare `KeyError` that reads as data corruption.
- Verification **shall** include a REAL S3 bucket, not only a local filesystem or moto.

## Evidence

    NotImplementedError: Partition specs with fields are not supported by this version
                         (partitioning by value ships in 0.11)
    datashard/table.py:73  _initialize_table

0.7.2 accepted the same call, and all 429 of the reporter's tables were created with it. So this
was a capability removed, while the message framed it as one not yet added.

**The failure shape is what made it urgent.** The raise fires only on CREATE. Their recorder makes
one table per (symbol, day) at that day's first write, so after an upgrade the service restarts
cleanly, appends successfully all day, and dies at 00:00Z building the next day's tables — every
smoke test green, then a timed outage with nobody watching. They found it at 23:16Z, roughly 40
minutes before it would have fired.

## Synthesis (localised)

- **[CHOSEN] Accept and drop, with a warning**, matching what `migrate_table` already does for the
  same specs on pre-0.10 tables. Removes the timed outage now and keeps the metadata honest.
- [REJECTED] Accept and record the fields in the metadata: data files carry empty partition
  structs, so DuckDB, pyiceberg and Spark would prune against a layout that does not exist. This
  is what the original raise was protecting against, and it is still right.
- [REJECTED] Wait for 0.11: the correct end state, weeks away, and it leaves a dated outage armed.

## Verification

`tests/test_partition_spec_accepted.py`: the reporter's exact call succeeds and the table works;
the warning names the field, 0.11, and that column statistics still serve filters; the fields are
recorded as a property and survive a reopen; the persisted metadata has an empty spec and
`last-partition-id` 999; filters return identical rows with and without the spec; an empty spec is
untouched and silent; a spec without a schema takes the other branch and is also accepted; a
migrated table carries the note, a dry run does not write it, and it is back-filled for tables
migrated earlier.

On a **real OVH bucket**: the full S3 integration suite (15 tests) plus the reporter's own shape —
one table per (symbol, day) with their spec, across a day boundary — 4 tables, filters correct on
each, `verify()` green.

## Outcome
(filled at release)
