# #53 — Pin pyarrow below 25.0.0 so datashard installs on FreeBSD

Ticket: `issuedb-cli context 53` (also `#53` in this repo).

## EARS SPEC

- When a user installs datashard on a platform whose package repositories
  provide pyarrow 24.0.0 as the newest available version, then the datashard
  distribution shall declare a pyarrow dependency ceiling below 25.0.0 so the
  available 24.0.0 satisfies it.
- While FreeBSD ports provide `py312-pyarrow-24.0.0`, the datashard
  distribution shall declare `pyarrow>=10.0.0,<25.0.0`.
- The datashard 0.7.1 distribution shall pass its test suite with pyarrow
  24.0.0 installed.
- The datashard 0.7.1 source tree shall pass `mypy src/datashard` and
  `ruff check .` with pyarrow 24.0.0 installed (CI runs these on every push).

## Context / decision

FreeBSD has no pyarrow wheel above 24.0.0; the old `pyarrow>=10.0.0` lets pip
pick a version with no FreeBSD wheel and fail the install at source-build
time.

An "optional extra" (`datashard[parquet]`) was proposed instead. Rejected for
this release: the data plane (`DataFileWriter`/`DataFileReader` implement only
`FileFormat.PARQUET` — AVRO/ORC data formats are declared but unimplemented),
the S3 filesystem (`pyarrow.fs`) and the filter-compute engine
(`pyarrow.compute`) are all pyarrow, so a pyarrow-less datashard cannot store,
read or filter data. FreeBSD already ships pyarrow 24, so the pin alone fixes
the resolution failure.

The floor stays `>=10.0.0` (not raised to 24) to avoid forcing existing
consumers to upgrade.
