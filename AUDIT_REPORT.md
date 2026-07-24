# datashard — Full Audit & Bank-Grade Certification Report

**Audit ticket:** issuedb #10
**Date:** 2026-07-24
**Version audited:** pyproject `0.5.4` (package `__version__` reports `0.3.2` — see #43)
**Scope:** all of `src/datashard/` (19 modules, ~5,310 LoC), test suite, static analysis, repo hygiene, and fidelity to Apache Iceberg semantics.
**Method:** full manual read of every source module; four parallel adversarial review lenses (concurrency/OCC, data-integrity/Iceberg, security, scan/filter/API); test-suite + `mypy` + `ruff` execution; and **executable reproductions** for the highest-severity defects (see Appendix A).

---

## 0. Remediation status (updated 2026-07-24, same day)

All 31 fix tickets were implemented in one pass. **40 tickets closed; 1 open (#13).**

- **Every P0–P2 code defect (#14–#42) is fixed and regression-tested.** A new
  `tests/test_audit_fixes.py` (28 tests) exercises the previously-missed failure
  paths: post-commit fault injection, fail-closed scan/GC, checksum verification,
  version-hint recovery, concurrent-append lost-update (content assertion),
  FileLock timeout, and every corrected API. **Suite: 107 passed / 3 skipped
  (S3); mypy --strict clean; ruff clean.**
- **#13 remains OPEN and is the one blocker only you can clear:** the hardcoded
  secrets were removed from the working tree (now read from env), but the
  exposed keys must still be **rotated** and **purged from git history**
  (`git-filter-repo` + force-push). Until then, treat both endpoints as
  compromised.
- The verdict below is the original point-in-time audit. **Net position now:**
  the code is materially safer and fails closed; full re-certification still
  requires (a) #13 completed and (b) an independent re-review of the new commit/
  lock/GC protocols (esp. the S3 CAS-lock + fencing path, which local CI cannot
  exercise).

---

## 1. Certification verdict

> ## ❌ NOT CERTIFIED — bank-grade certification is **DENIED**.
>
> The library is **not fit for storing data that must not be lost**. Multiple independent, reproduced defects convert *transient, expected failures* (a single S3 hiccup, one corrupt file, a missing 2-byte hint file, a slightly-too-slow batch load) into **silent, permanent data loss or silently wrong query results** — the exact failure class a table format exists to prevent. In addition, a set of **live cloud credentials is committed to a public repository**.

Against the four bank-grade pillars:

| Pillar | Verdict | Basis |
|---|---|---|
| **Correctness** | ❌ Fail | Documented happy path (`create_table(schema)` → append) silently writes **zero columns**; `is_null` filters return all rows; `overwrite_by_filter`/`expire_snapshots` are no-ops returning success; unknown operators coerce to `==`. All reproduced. |
| **Security** | ❌ Fail | Two live S3 credential sets committed to a **public** GitHub repo + a real-looking `.env` on disk; path-traversal guard bypassable by sibling-prefix/symlink. |
| **Auditability** | ❌ Fail | Manifest `snapshot_id`s dangle to non-existent snapshots (lineage joins fail); delete-rewrites falsify file history; `format_version: 2` is claimed but sequence numbers/metadata-log are inert. |
| **Test rigor** | ❌ Fail | 79 passing tests but they cover only the single-writer happy path; the reproduced blockers (post-commit rollback, GC fail-open, schema-less append, `is_null`, wrong-prefix GC) were **all missed**. No multi-process, crash-injection, or S3-lock tests. |

**Re-certification requires:** clearing every P0 (tickets #13–#22) and the P1 correctness/security/concurrency set (#23–#31), plus a hardened test suite (crash-injection, multi-process OCC, S3 lock, fault-injection on every `except`). See the bugfix plan in §6.

---

## 2. Test & static-analysis results

- **pytest:** `79 passed, 3 skipped` (S3 integration skipped — no creds configured). Runtime ~4s.
- **mypy** (strict config): `Success: no issues found in 19 source files`.
- **ruff:** `All checks passed`.

**Interpretation:** green tooling here is a *false comfort*. The suite exercises the narrow happy path; every reproduced blocker below passed CI. `mypy`/`ruff` cannot see semantic no-ops, swallowed exceptions, or fail-open GC. Coverage of failure paths is effectively zero.

---

## 3. Severity summary

| Sev | Count | Theme |
|---|---|---|
| **BLOCKER (P0)** | 10 | Silent data loss / silently wrong results / committed secrets (#13–#22) |
| **HIGH (P1)** | 9 | Wrong results, broken lineage, unsafe locks, path traversal (#23–#31) |
| **MEDIUM (P2)** | 11 | GC no-op, durability, lossy stats, silent retention, S3 gaps (#32–#42) |
| **LOW (P3)** | 1 (cluster) | Version mismatch, dead code, README, hygiene (#43) |

Every item below was verified against source (`file:line`). Items tagged **[REPRODUCED]** have an executable proof in Appendix A.

---

## 4. Blockers (P0) — certification stoppers

**#13 — [security] Live S3 credentials committed to a public repo.**
`tests/create_s3_bucket.py:10-12` and `tests/verify_docs.py:134-137` contain real access/secret keys; remote is public `github.com/rodmena-limited/datashard`. Untracked `.env` holds another live-looking set. Anyone can read/write/destroy the backing buckets. *Requires key rotation + history purge — an operational action for the maintainer (see §6, Phase 0).*

**#14 — [data-loss, REPRODUCED] Post-commit exception deletes committed data.**
`transaction.py:372` runs `refresh()` *after* the durable commit point (`create_snapshot()`); the generic `except` (`:398-401`) calls `_rollback()`, which deletes `_written_files` (`:426-429`) — files the committed snapshot now references. Injecting an `OSError` on the post-commit refresh deleted a committed parquet; `scan()` silently lost the row.

**#15 — [data-loss] Commit falls back to an empty manifest list on read error.**
`transaction.py:277-286`: `except Exception: existing_manifests = []`. A transient failure reading the base manifest list produces a snapshot that references only the newly-appended file — all prior data becomes unreferenced and is later GC-deleted.

**#16 — [data-loss] Garbage collector fails *open*.**
`garbage_collector.py:60-83` swallows manifest-read errors (`warning; continue`); `:100-123` then deletes every file that the unreadable manifest would have protected. One transient S3 error during GC = permanent deletion of live data.

**#17 — [data-loss] GC vs. long-running transaction race.**
Data files are written at `append_data()` but become reachable only at commit. A batch load taking longer than `grace_period_ms` (default 1h) has its not-yet-referenced files deleted by a concurrent `garbage_collect()`; the subsequent commit references missing files.

**#18 — [data-loss, REPRODUCED] `create_table(schema=...)` ignores the schema.**
`iceberg.py:44-47` never persists the schema; metadata defaults to empty `Schema(0, [])`. A schema-less `append_records` then writes **zero-column** parquet. Verified: `create_table(schema)` + `append_records([...])` → returns `True`, `row_count()==0`, `scan()==[]`. **The most basic documented usage silently loses all data.**

**#19 — [correctness, REPRODUCED] Scan paths swallow all read errors.**
`transaction.py:798-805, 916-923, 1026-1027, 1097-1098, 1113-1114, 1128-1129` return `None`/`[]` on any exception. A corrupt or transiently-unreadable file drops its rows from `scan()` with no error; `row_count()` disagrees. "Empty table" and "broken table" are indistinguishable.

**#20 — [correctness, REPRODUCED] `is_null`/`is_not_null` filters silently dropped.**
`filters.py:127-129` skips null-checks in `to_pyarrow_filter` and nothing re-applies them. `scan(filter={"c": ("is_null", True)})` returns **all** rows. `scan_batches()` (compute-expression path) returns the correct rows — the two engines disagree.

**#21 — [correctness, REPRODUCED] `overwrite_by_filter` is a silent no-op.**
Queued (`:201-208`), "applied" as a `pass` (`:463-466`), ignored by commit (`:260-264`). `overwrite_by_filter(...)` + `commit()` returns `True` with data unchanged.

**#22 — [data-loss] Missing/corrupt version-hint triggers destructive re-init.**
A missing or non-numeric `metadata.version-hint.text` makes `refresh()` return `None` (`metadata_manager.py:351-358`); `create_table`/`initialize_table` (`iceberg.py:38-43`, `metadata_manager.py:38-55`) then re-initialize unconditionally (new `table_uuid`, hint `0`), orphaning all real data for GC. One lost 2-byte file → total table loss. Init is also unlocked (concurrent-create race).

---

## 5. High / Medium / Low findings

### HIGH (P1)
- **#23 [REPRODUCED]** `expire_snapshots()` is a no-op that *adds* a snapshot (`transaction.py:246-247/468-471`).
- **#24** `delete_snapshot(current)` repoints to `max(snapshot_id)` over **random** UUIDs → jumps to an arbitrary historical snapshot (`snapshot_manager.py:199-207`).
- **#25 [REPRODUCED]** Unknown filter operators coerce to `==` (`filters.py:104,165-168`); `startswith` returned equality matches.
- **#26** Appends aren't validated against the schema: extra fields silently dropped; a divergent-schema append later makes `pa.concat_tables` raise and **bricks `scan()`** for the whole table.
- **#27** Manifest `snapshot_id` (`transaction.py:252`) ≠ actual `Snapshot` id (`snapshot_manager.py:60`) → dangling lineage, broken audit joins.
- **#28** Manifest filenames use microsecond timestamps only (`file_manager.py:88-90`) → concurrent committers collide; one snapshot ends up listing another's files.
- **#29** Path-traversal guard uses `startswith` without a separator and no `realpath` (`storage_backend.py:128-133`) → sibling-dir/symlink escape.
- **#30** S3 locks (`lock_provider.py`) have a polling mutual-exclusion hole, unconditional stale-break, and unconditional heartbeat renewal, with **no fencing** before the unconditional version-hint write → mutual-exclusion failure becomes a silent lost update.
- **#31** `FileLock` blocking path (`file_lock.py:80-91`) never enforces its timeout; a wedged holder blocks all committers forever; no-primitive fallback gives zero exclusion.

### MEDIUM (P2)
- **#32 [REPRODUCED]** Manifest GC scans prefix `"manifests"` instead of `"metadata/manifests"` → always deletes 0; orphan manifests accumulate forever.
- **#33** Parquet data files aren't fsync'd before rename (`data_operations.py:247-289`) while metadata is → power-loss can leave committed metadata pointing at unpersisted data.
- **#34** Bounds are stringified then heuristically re-inferred (`file_manager.py:115-116, 148-162`) → wrong file pruning for timestamps; inverted min/max for numeric-looking strings.
- **#35** Silent snapshot expiry at 100, keyed to the wrong Iceberg property (`snapshot_manager.py:83-100`) → time travel silently stops working.
- **#36** User-populated column stats crash commit (int-keyed dict into Avro `map` → `fastavro` `ValueError` → rollback deletes data) (`data_structures.py:107-109`, `file_manager.py:111-113`).
- **#37** Delete-rewrites mark surviving files `ADDED` with the new snapshot id (`file_manager.py:100-104`) → falsified history.
- **#38** `SnapshotManager` creates local dirs for S3 tables (`snapshot_manager.py:20-23`) → stray `test_table_*` dirs, breaks read-only FS.
- **#39** `S3StorageBackend.exists()` has no retry (`storage_backend.py:436-454`) — on hot paths, amplifies #14.
- **#40** `write_pandas_file` sizes the wrong path and computes no stats/checksum (`data_operations.py:553-557`).
- **#41** Duplicate schema field-ids not validated (`data_structures.py:40-59`) → colliding bounds → wrong pruning.
- **#42** Checksums are computed at write but **never verified on read** (`scan`/`to_pandas`/`read_data_file`); JSON fallback drops the checksum.

### LOW (P3) — cluster #43
Version mismatch (`__init__` `0.3.2` vs pyproject `0.5.4`); tracked `data_operations.py.bak`; dead landmine code (`_apply_operation` writes a thread id into `last_updated_ms`; `_create_manifest_list` JSON placeholder bypasses storage); README examples that raise (`FileManager(...)` missing `storage`) and overstate time-travel/pruning; `begin()` doesn't reset `_operations` (double-apply on reuse); empty commits create snapshots; non-SQL null semantics; INFO logging per write; unbounded `TransactionManager` map; correlated S3 lock retry jitter.

---

## 6. What is genuinely solid

To keep this fair — the bones are good and worth preserving:

- The **single-writer commit path** is well-constructed: temp-file + `fsync` + atomic `os.replace` + directory `fsync` for metadata, versioned `v{n}.metadata.json` behind an atomic version-hint flip, with the hint write as a clean commit point (orphans-on-crash, never dangling references — for metadata).
- **OCC is real**: lock → re-read base → compare `current_snapshot_id`/`last_updated_ms` → retry-on-fresh-base correctly *merges* concurrent appends rather than clobbering them.
- **Local multi-process locking** via `fcntl.flock` on a persistent inode is correct (the "don't delete the lock file" reasoning is right).
- UUID-based snapshot ids (63-bit) avoid the timestamp-collision trap.
- No dangerous deserialization (`pickle`/`eval`/`yaml.load`), safe `tempfile` usage, SHA-256 checksums, clean `mypy --strict`.

The problem is not the core design — it is that **every failure edge fails in the unsafe direction**, and several advertised features are inert.

---

## 7. Bugfix plan

Phased, dependency-ordered. Each ticket carries its EARS spec in issuedb (retrieve with `issuedb-cli context <ID>`) and in `SPECS/audit-bugfix-specs.md`.

**Phase 0 — Security containment (do first, blocks release; maintainer action):** #13.
Rotate both S3 credential sets and the `.env` set *now*; remove the files/values; purge history (`git-filter-repo`) and force-push; treat both endpoints as compromised until rotated. *This is the only item requiring your credentials/decision — I have not touched git history or keys.*

**Phase 1 — Stop the data loss (P0 core):** #14, #15, #16, #17, #22.
Common principle: **fail closed**. Turn every `except: <return empty / continue>` on the commit, GC, and read paths into an abort; make the version-hint recoverable from metadata files and make `initialize_table` refuse an existing table; coordinate GC with in-flight writers. These are the certification gate.

**Phase 2 — Stop the silent wrong answers (P0 correctness):** #18, #19, #20, #21.
Persist the schema in `create_table`; propagate read errors out of `scan()`; apply `is_null`/`is_not_null`; make `overwrite_by_filter` either work or raise `NotImplementedError`.

**Phase 3 — Correctness & safety hardening (P1):** #23–#31.
No-op APIs (#23) → implement or raise; `delete_snapshot` lineage (#24); operator validation (#25); append schema validation (#26); lineage id unification (#27); manifest filename UUIDs (#28); path guard `realpath`+`commonpath` (#29); S3 lock fencing + real mutual exclusion (#30); `FileLock` timeout (#31).

**Phase 4 — Robustness & fidelity (P2):** #32–#42.
GC prefix fix (#32); data-file fsync (#33); type-faithful bounds (#34); explicit retention (#35); stats serialization (#36); manifest status semantics (#37); backend-routed dirs (#38); `exists()` retry (#39); `write_pandas_file` (#40); field-id uniqueness (#41); read-path checksum verification (#42).

**Phase 5 — Hygiene:** #43.

**Cross-cutting (gates re-certification):** a failure-path test suite — crash injection at every commit ordering point, multi-process OCC with content assertions (not just snapshot counts), S3 lock contention/expiry, and fault injection on every storage `except`. Today's suite would pass all of the above blockers unchanged.

**Suggested first PR (fast, high-value, low-risk):** #18 + #20 + #21 + #23 + #32 + #43 — each is a localized, independently-testable fix that removes a *reproduced* silent failure.

---

## Appendix A — Reproductions

Executable proofs (run against `src/datashard` @ this revision) for #14, #18, #20, #21, #23, #25, #32. Representative results:

```
[#18] create_table(schema) + append_records([...]) -> append True, row_count()=0, scan()=[]
[#21] overwrite_by_filter(...) + commit() -> True, rows unchanged
[#23] expire_snapshots(all) + commit() -> True, snapshot count 3 -> 4 (increased)
[#20] scan(filter={name: is_null}) -> returns all 3 rows (expected 1)
[#25] scan(filter={name: (startswith, "a")}) -> returns only name=="a"
[#32] garbage_collect() -> manifest_files deleted always 0; 6 manifests remain on disk
[#14] inject OSError on post-commit refresh -> committed parquet deleted;
      snapshot references a missing file; scan() silently drops the committed row
```

## Appendix B — Traceability

Audit ticket **#10**; fix tickets **#13–#43**. Durable invariants recorded in issuedb memory (`issuedb-cli memory list -c spec`). Specs mirrored in `SPECS/`.
