# #93 — Reads must not infer partitioning from the filesystem path (0.10.1)

## Requirements (EARS)

- When datashard reads a data file, it **shall not** infer partitioning from the file's
  filesystem path. A table's partitioning is described by its Iceberg metadata; the directory
  is a location, not a partition scheme.
- The rows and the schema a scan returns **shall** depend only on the table's data and
  metadata, and **shall** be identical for the same table stored at any path.
- **When** the table's path contains a Hive-style `k=v` segment whose key matches a column,
  every read API (`scan`, `to_arrow`, `to_pandas`, `iter_records`, `scan_batches`) **shall**
  return the value stored in the column, never the value parsed from the directory name.
- **When** the path contains a `k=v` segment whose key matches no column, no extra column
  **shall** appear in the result.
- Every parquet read **shall** go through one helper that disables inference, and a test
  **shall** fail the build if a bare `pq.read_table` call is reintroduced.

## Evidence (reproduced live on 0.10.0, pyarrow 22.0.0)

1. **Colliding key — hard failure.** A table created under `.../symbol=ZEN-USD/day=2026-09-08/`
   whose rows carry `symbol="ZEN-USDT"`:

       pyarrow.lib.ArrowTypeError: Unable to merge: Field symbol has incompatible types:
       string vs dictionary<values=string, indices=int32, ordered=0>

   raised from `pq.read_table` → `ParquetDataset` → `ds.dataset(partitioning="hive")`.
   Reported by crypto-trader (Farshid) on 2026-09-08 as blocking their 0.10 upgrade. Reproduced
   here with **one** data file as well as two, so the reporter's "needs two files" condition is
   not required on pyarrow 22.
2. **Non-colliding key — silent.** A file under `.../venue=binance/` reads back as
   `['symbol', 'n', 'venue']` with `venue='binance'` taken from the directory name. No error and
   no warning: the scan returns a column that is not in the table's schema. Nobody reported this
   because it does not crash.
3. `pq.read_table(path, partitioning=None)` returns the true column value and no injected
   column, and works with `columns=`, `filters=`, a file object and a `BytesIO`.

## Scope

Local backend only: S3 reads pass a file object, which never triggers dataset discovery. The
defect arrived when local reads switched from a file object to a filesystem path for the
pyarrow-22 exit-abort fix (#74), i.e. **0.8.0 through 0.10.0**; 0.7.2 is unaffected, which
matches the report.

Aggravating: `row_count()` reads metadata only, so an affected table reports a healthy count and
fails only on read — a health check built on `row_count()` reports green. And because 0.10 refuses
an un-migrated table, the failure surfaces only *after* the one-way migration.

## Synthesis (localised)

- **[CHOSEN] Disable filesystem partition inference on every read.** The table format already
  carries the partitioning, so path inference can only ever contradict it.
- [REJECTED] Skip only the inferred fields that collide with a schema column: leaves the
  phantom-column injection in place and keeps the physical path semantically load-bearing.
- [REJECTED] Document a naming restriction on table paths: turns a lake layout the user already
  has into a datashard incompatibility, and does nothing for tables already written.

## Verification

- `tests/test_hive_path_inference.py`: colliding key with one and with two files; non-colliding
  key; the same table read at three different paths; all integrity modes and parallel reads;
  `append_files` under such a path; an S3 table under a `symbol=…` prefix (the reporter could not
  test S3). Each test first asserts, through raw pyarrow, that the fixture really does trigger
  inference — otherwise a passing datashard read would prove nothing.
- A structural test fails the build if any module regains a bare `pq.read_table` call.

## Outcome

**Shipped in 0.10.1 (2026-09-08):** https://pypi.org/project/datashard/0.10.1/ · commit d5e074c · tag v0.10.1.

Exercised: 260 unit tests (8 new in `tests/test_hive_path_inference.py`, each paired with a raw-pyarrow
control proving the fixture really triggers inference); 34/34 probes including the new
`probe_v0101_path_is_not_a_partition_scheme.py`; the reporter's own script run against the built wheel
and then against the wheel installed back from PyPI, covering the crash variant with one and two data
files and the silent-injection variant. Affected versions determined by running the reproduction against
the released 0.8.1 and 0.9.1 packages from PyPI - both fail, so the reporter's inferred 0.8.x guess was
correct; 0.7.2 was retested clean.

Not exercised: a real S3 provider (the S3 leg runs against moto; S3 never took this path because its
reads pass a file object), and pyarrow versions other than 22.0.0.
