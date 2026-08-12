# SPEC 0054 — S3 parquet reads go through our own backend

**Ticket:** #54
**Status:** implemented, 2026-08-12 (released in 0.7.2)

## The defect

Against OVH Object Storage, every parquet read failed:

```
AWS Error [code 134] during GetObject operation:
Value for x-amz-checksum-mode header is invalid.
```

pyarrow's `S3FileSystem` sends an `x-amz-checksum-mode` header on GetObject that
OVH rejects. **boto3 reads the identical object, from the identical bucket, with
the identical credentials, without complaint** — as do rclone and datashard's own
`S3StorageBackend`. So this was never a credential, endpoint or permission
problem; it was two different HTTP clients in one library, only one of which
works against this provider.

The failure mode was cruel: writes go through boto3, so `create_table` succeeded
and the table metadata appeared in the bucket. The **first append** then died
inside `_validate_file_schema`, reading back the file it had just written. A user
sees a correctly-created table that cannot accept a single row.

Measured directly against OVH with pyarrow 24.0.0:

| operation | pyarrow `S3FileSystem` | boto3 |
|---|---|---|
| LIST | ok | ok |
| WRITE | ok | ok |
| **READ** | **fails** | ok |

## Requirements

**R54.1** The library shall read parquet data through its own storage backend
rather than through pyarrow's S3 filesystem.

**R54.2** Where the backend is S3, the library shall provide a **seekable** file
object, so pyarrow reads only the parquet footer and the column chunks a scan
needs.

**R54.3** The library shall not download an entire data file in order to read its
schema.

**R54.4** Where the backend is local, behaviour and performance shall be
unchanged.

**R54.5** The path-traversal guard (#47) shall still run on every read. A
manifest entry pointing outside the table shall raise
`ValueError("Path traversal…")`, not merely fail to open.

**R54.6** Adding this capability shall not break third-party `StorageBackend`
subclasses.

## How each was verified

| Req | Verification |
|---|---|
| R54.1 | live round trip against OVH: create, append, scan, append again, scan — 3 rows back |
| R54.2 | `test_reports_itself_as_seekable`, and the byte measurements below |
| R54.3 | `test_reading_the_footer_transfers_only_the_footer` asserts the requested range is 8 bytes, so a whole-object read fails it |
| R54.4 | 152 tests pass on the local backend; `test_local_backend_open_seekable_round_trips` |
| R54.5 | the audit suite's `test_read_path_rejects_absolute_path_outside_table` still passes — see the note below, it caught a real regression |
| R54.6 | `open_seekable` is deliberately **not** `@abstractmethod`; asserted at runtime in the release checks |

**The traversal guard caught a genuine regression during this work.** The first
implementation read straight through the backend, which still *contained* the
path (the local resolver joins it under the table root) but reported
`FileNotFoundError` instead of refusing. Containment that reports "not found"
invites someone to "fix" it later by loosening the resolver. The guard is now
called explicitly on the read path, for the check rather than the value.

## Performance

Measured against the live OVH bucket, counting bytes actually requested:

| file validated | bytes fetched | range requests |
|---|---|---|
| 641 B | 641 B | 1 |
| 361 KB | 65 KB | 1 |
| 2.9 MB | 462 KB | 2 |

Reads are **footer-scoped, not whole-file**: roughly a sixth of the object at
these sizes, and the ratio held across an 8× size increase. The obvious simpler
fix — `pq.ParquetFile(BytesIO(storage.read_file(path)))` — would have been
correct and would have downloaded **100%** of every file to answer "what is its
schema", turning an O(footer) check into O(file). That is why `S3RangeFile`
implements `seek`/`tell` over ranged GETs instead.

The reader is wrapped in `io.BufferedReader` because pyarrow issues many small
reads while walking a footer, and unbuffered each would be its own HTTP request.

**Not a regression against pyarrow's own S3 path**: that also did footer reads.
This keeps the same access pattern with a client that works.

## Known gaps

* `tests/test_s3_integration.py` has **two failures unrelated to this change**,
  present before and after: they assert `storage.exists("metadata")`, but
  `exists()` deliberately only falls back to a prefix listing for paths written
  as directories (trailing `/`). That restraint is intentional — answering True
  for `data/x.parquet` because objects exist *under* it would let a missing data
  file pass validation. **The test's expectation is wrong, not the code**, and it
  was left alone rather than loosening a safety property to make a suite green.
* OVH does not support `If-None-Match`, so `DATASHARD_S3_USE_CONDITIONAL_WRITES`
  must be `false` there, which downgrades locking to best-effort polling. Under
  contention two writers can both believe they hold the lock and a commit can be
  lost. That is a separate, pre-existing limitation and it is logged loudly at
  runtime.
