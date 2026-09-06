# #83 — Iceberg v2 foreign-reader spike (input to 0.10.0)

Run 2026-09-06 against **duckdb 1.5.5 + iceberg extension 45163a28 (+ httpfs)** and **pyiceberg 0.12.0**,
with fastavro 1.12.1 / pyarrow 22.0.0 writing the files. Scripts: `audit/spike_iceberg_v2/`.
Oracle: full-row value equality against the written data (not counts).

## Requirements (EARS) — met
- The spike shall hand-write a minimal Iceberg v2 table and prove both readers return the written rows. **Met** (42-check matrix, 36 PASS; every non-PASS is a documented reader limitation, see below).
- The spike shall record the answers to hint handling, Avro strictness, bounds encoding, name-mapping, positional deletes and path tolerance. **Met** (table below).
- Nothing in `src/` changes. **Met.**

## Findings

| Question | DuckDB 1.5.5 | pyiceberg 0.12.0 | Consequence for 0.10.0 |
|---|---|---|---|
| Reads a hand-written v2 table (metadata.json + manifest list + manifest + parquet with field ids) | PASS via root (hint) and via metadata path; `snapshot_from_id` time travel PASS; predicate filters PASS | PASS via `from_metadata(metadata.json)` and via the table root (hint); `snapshot_id=` PASS | Layout in the plan is correct as written |
| `version-hint.text` | **Load-bearing**: no hint → error "No version was provided and no version-hint could be found, globbing … disabled by default"; `version='1'` works; a **stale hint is trusted silently** (v1 read while v2 exists) | Honoured for root reads | Hint must be written after every commit with only-if-greater semantics; document "foreign readers may lag one version after a crash" |
| Metadata filename | `v{N}.metadata.json` accepted | accepted | Keep plain integer `v{N}` |
| Paths in manifests / manifest list | Relative `data/x` and datashard-0.8-style `/data/x` **fail** unless `allow_moved_paths=true`; `file:///abs` and bare `/abs` both work; a moved table works only with `allow_moved_paths=true` | Relative and `/data/x` **fail**; `file://` and bare absolute work | **Absolute URIs are mandatory**; `datashard relocate` needed for moved tables |
| Parquet without field ids + `schema.name-mapping.default` | **PASS** (mapping honoured) | **PASS** | Migration keeps old data files untouched and writes the name mapping; **no data rewrite** |
| Parquet without field ids and no mapping | **Silently returns all-NULL rows** (3 rows, every column NULL) | Raises `ValueError: Parquet file does not have field-ids and the table does not have schema.name-mapping.default` | Migration must always write the mapping; the datashard writer must always emit `PARQUET:field_id`; the acceptance probe must include this negative case |
| Bounds (`lower_bounds`/`upper_bounds`, Iceberg single-value binary) | **Consumed for pruning**: deliberately wrong bounds (id in [1000,2000]) made `WHERE id = 2` return 0 rows; real long/decimal/timestamptz/string bounds prune correctly | Same: wrong bounds → 0 rows; real bounds correct | Bounds are a correctness input for foreign readers, not a hint. Encode exactly (LE long, minimal big-endian two's-complement unscaled decimal, LE micros, UTF-8) or **omit** the column's bounds. Migration converts only unambiguous types |
| Manifest without statistics (all optional fields null) | PASS | PASS | Omitting bounds is safe |
| v1 count names (`added_data_files_count`…) in a v2 manifest list | PASS | PASS | Both resolve Avro fields by `field-id`; use the v2 names anyway |
| Positional deletes (`content=1`, parquet `file_path`/`pos` with reserved ids 2147483546/2147483545) | **Applied** | **Applied** | Merge-on-read is viable for 1.0 as planned |
| S3 (moto, path-style, custom endpoint) | PASS via `httpfs` + `CREATE SECRET (TYPE S3, ENDPOINT, URL_STYLE 'path', USE_SSL false)` | PASS via `s3.endpoint`/`s3.access-key-id`/… properties | `duckdb_s3_secret_sql()` from 0.9.0 already emits the right shape |
| Reverse direction: pyiceberg `SqlCatalog.register_table` + `append` on our table | Reads the pyiceberg-written metadata by path: PASS (all 3 snapshots) | Writes `00000-<uuid>.metadata.json` (its own naming) and **does not touch `version-hint.text`** | A foreign writer desynchronises the hint and the `v{N}` sequence: DuckDB-by-root and datashard would keep seeing the old version. Until the REST catalog (1.0), **datashard is the only writer of hint-based tables**; state this in the docs and have `refresh()` warn when `metadata-log` names a file outside the `v{N}` sequence |
| Introspection | `iceberg_snapshots()`, `iceberg_metadata()` PASS | – | Use in the acceptance probe |
| pyiceberg string filter with a decimal literal (`"px == 2.5"`) | – | `ValueError: Could not convert 2.5 into a decimal(18, 8)`; the expression API with `Decimal("2.50000000")` works | Docs note only; pyiceberg literal-parsing limitation |
| DuckDB Python API | `fetch_arrow_table()` is deprecated in 1.5.5 in favour of `to_arrow_table()` | – | `duckdb_bridge.sql()` must prefer `to_arrow_table` with a fallback (folded into 0.10.0) |

## Design decisions taken for 0.10.0
1. Layout, commit point and hint semantics as in the approved plan; hint written after every commit, only-if-greater.
2. Absolute URIs everywhere (`file:///…`, `s3://bucket/prefix/…`); `location` is a URI; `datashard relocate` for moves.
3. Migration = one new commit with name mapping; data files untouched; bounds converted for long/int/double/float/date/time/timestamp(tz)/string/decimal/boolean, dropped otherwise.
4. Every parquet file written by 0.10+ carries `PARQUET:field_id`.
5. Bounds are emitted only when the encoding is exact for the type; a foreign-reader **negative** case (wrong bounds → wrong rows) is part of the acceptance probe so a regression cannot pass silently.
6. Positional deletes stay on the 1.0 roadmap (both readers apply them).
7. Foreign writers are documented as unsupported on hint-based tables until the catalog client exists.

## Outcome
Closed 2026-09-06; findings feed tickets #84+ (0.10.0 work items).
