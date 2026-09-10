# #92 — uuid and fixed[L] columns, as Iceberg requires (0.11.2)

Until 0.11.2 datashard wrote a parquet **string** for a column declared `uuid`, and had no way to
spell `fixed` at all because Iceberg's `fixed` carries a width. pyiceberg refuses the first
(`Cannot promote an string to uuid`) and the second (`Could not match fixed, expected format
fixed[22]`), so 0.10 responded by **refusing both types at create time** — the workaround being
`string` / `binary`, which have identical bytes.

## Requirements (EARS)

- When a schema declares `uuid`, datashard **shall** write a parquet `fixed_size_binary(16)` column
  tagged with the Iceberg `uuid` type, and **shall** accept `str`, `uuid.UUID` or 16 raw bytes on
  write.
- When a schema declares `fixed[L]`, datashard **shall** write `fixed_size_binary(L)` and emit the
  Iceberg type `fixed[L]`; a bare `fixed` **shall** stay refused.
- Reads **shall** return a stable Python type for these columns — a canonical **string** for `uuid`,
  `bytes` for `fixed[L]` — from every read API, and existing tables (parquet strings tagged `uuid`)
  **shall** keep reading, in the same scan as files written since.
- Verification **shall** add a uuid and a fixed[L] column to a foreign-reader probe and require
  DuckDB **and** pyiceberg to match datashard row for row.

## What the checks caught

Writing the encoder was the small half. Every boundary where a uuid changes representation had a
defect, and each was found by a test or a probe rather than by reading the code:

- **A filter on a uuid column matched nothing.** The predicate was pushed into the parquet reader,
  which compared the caller's 36-character string against 16 stored bytes. A uuid predicate is now
  split out and applied after the column is decoded, where old string files and new binary files
  look the same; its values are canonicalised, so a `str`, a `uuid.UUID` and raw bytes all select
  the same rows. Column bounds are kept as text for the same reason, and they still prune (3 files
  → 1 on an equality lookup).
- **`rewrite_data_files()` aborted on a table holding both encodings** — concatenating a string
  column with a binary one casts the strings to 36-byte binary. Files are now decoded per file
  before merging, which also makes a rewrite the way to convert an old file to the Iceberg encoding.
- **The partitioned append path and the pandas path build their own Arrow table**, so neither saw
  the conversion; both were failing with a pyarrow message about a length-36 value.
- **A uuid or fixed partition value is refused** (`bucket[N]` is offered instead): Iceberg readers
  disagree about how such a value is spelled in the manifest's Avro struct and in the path, and a
  partition value a reader misreads is worse than no partitioning. `bucket[N]` on a uuid is exact —
  Iceberg hashes the same 16 bytes datashard stores.
- **A migrated table's report keeps naming its uuid columns.** The type is writable now, but a table
  old enough to migrate stores that column as a string and migration does not rewrite data files, so
  `columns_foreign_readers_may_reject` still lists it, with `rewrite_data_files()` as the fix.

## Measured

- `audit/evaluations/probe_v0112_uuid_and_fixed_foreign_readers.py`: 6 rows written as `str` /
  `uuid.UUID` / raw bytes; DuckDB **==** datashard and pyiceberg **==** datashard on values;
  pyiceberg sees `extension<arrow.uuid>` and `fixed_size_binary[4]`.
- **Negative control:** the pre-0.11.2 representation (a parquet string tagged `uuid`) is built
  deliberately and pyiceberg still refuses it — `ResolveError: Cannot promote an string to uuid` —
  so the PASS above is the new encoding being read, not pyiceberg being lenient.
- Backward compatibility: a string-backed table reads, accepts an append that writes 16 bytes, and
  scans both files together (`verify()` ok); a lookup finds a row in either kind of file.
- `tests/test_uuid_and_fixed.py`: 12 tests over the round trip, every read API, every write API,
  bounds and pruning, bucket partitioning, the bare-`fixed` refusal and the mixed-encoding rewrite.

## Outcome

Released in **0.11.2**. Not exercised: schema evolution *into* a uuid column (0.11 evolution does
not change a column's storage), and partitioning by uuid other than `bucket[N]`, which is refused.
