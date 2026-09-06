# datashard 0.7.2 — Adversarial Audit (trading-desk data lake)

**Audit ticket:** issuedb #55 · **Date:** 2026-09-06 · **Version audited:** 0.7.2 (`main` @ 1d7f383)
**Method:** `mission-critical-audit` — every module read end to end by one auditor; every claim in README, docs, SPECS and the two prior audit reports treated as something to falsify; every finding reproduced live through the public API against a real counterparty (local FS, a moto S3 server, and the configured OVH endpoint) or labelled SUSPECTED. Prior "fixed" statuses were not inherited.
**Environment:** Python 3.13.3, pyarrow 22.0.0, fastavro 1.12.1, boto3 1.41.3, moto 5.2.2 (server mode), OVH `s3.uk.io.cloud.ovh.net`.
**Harness:** `audit/evaluations/` — 24 probes, `run_all.sh`; baseline on 0.7.2: **21 probes FAIL, 1 PASS** (external probes opt-in via `AUDIT_ALLOW_EXTERNAL=1`). PASS = claim holds; FAIL = defect reproduced.

Baseline as the authors run it: pytest 152 passed / 3 skipped, mypy strict clean, ruff clean. The suite is green and proves less than it appears to: none of the 21 failing probes is covered by it.

---

## 0. Remediation status (2026-09-06, release 0.8.0)

Every ticket opened by this audit is fixed, regression-tested and closed; #13 (key rotation) is the
operator's. `audit/evaluations/run_all.sh` on 0.8.0: every probe PASSES (the GitHub-workflow house
rule is reported as INFO because removal waits for the CI migration). Regression tests:
`tests/test_audit_55_fixes.py` (24 tests); suite 184 passed, mypy strict clean over 33 files, ruff clean.

| Ticket | Sev | Status |
|---|---|---|
| #56 | P0 | ✅ single path normaliser; table named `data` keeps its files |
| #57 | P0 | ✅ markers before metadata, cutoff from GC start, markers for `append_files`, grace floor |
| #58 | P0 | ✅ manifests / lists verified against recorded length + sha256; GC aborts on mismatch |
| #59 | P0 | ✅ CAS auto-detected; unsafe lock refused without opt-in; OVH verified live |
| #60 | P0 | ✅ clean failures delete their metadata file; ambiguity raises; `repair_version_hint` |
| #61–#65 | P1 | ✅ delete validation, table-order layout, `/` delimiter, `SchemaMismatchError`, live S3 tests |
| #66–#70 | P2 | ✅ page-CRC default, 37 → 16 S3 calls, metadata GC + compaction + maintenance API, 1 row group, disk floor |
| #71–#74 | P3 | ✅ modules split, docs corrected, `decimal`/`timestamptz`, suspected items fixed with tests |
| #13 | — | ⏳ rotate the `.env` OVH keys (operator) |

Measured on the real endpoint after remediation: single-row commit p50 5.4 s → 3.1 s (one writer),
10.3 s → 6.5 s (four writers), 20/20 commits kept. One new defect was found and fixed on the way:
pyarrow 22 on CPython 3.13 aborts the interpreter at exit after threaded reads over a Python file
object, which the 0.7.2 S3 read path triggered in every process that wrote and then read.

---

## 1. Verdict

> **NOT production-worthy for data you cannot lose. Certification denied.**
> Five reproduced P0 data-loss / lost-commit paths (#56–#60), five P1 correctness defects (#61–#65), and a performance profile (5.4 s per single-row commit on the production endpoint, unbounded metadata growth, whole-file reads on every projected scan) that contradicts the README's claims.
>
> The single most consequential fact is operational and needs no code: **the production endpoint honours S3 conditional writes** (If-None-Match → 412, If-Match → 200/412, verified live), yet production runs `DATASHARD_S3_USE_CONDITIONAL_WRITES=false`, i.e. the lock the code itself documents as "BEST-EFFORT ONLY". With that flag, two concurrent writers can both return `True` and one snapshot vanishes (reproduced). Flipping the flag was verified end to end on OVH with 4 concurrent processes (20/20 commits kept). **Do this first.**

What held up: local multi-process OCC (8 processes × 25 appends, 200/200 commits, p50 8 ms), the CAS lock's lease in both directions (blocks while valid, taken over after expiry, stale holder fenced), `expire_snapshots` + GC reclaiming manifest lists, checksum verification detecting a flipped byte, timezone normalisation of tz-aware timestamps.

## 2. Findings, ranked by customer harm

| # | Sev | Status | Finding | Probe |
|---|---|---|---|---|
| #56 | P0 | CONFIRMED | `garbage_collect()` deletes **every live file** when the table path is a string prefix of `data`/`metadata` — e.g. a table named `data` (local and S3). `_normalize_path` strips `table_path` as a plain prefix. | probe_gc_table_path_prefix_collision |
| #57 | P0 | CONFIRMED | GC deletes a file (and its manifests) **committed while GC is running**: markers are loaded after reachability, the cutoff is taken at listing time. On large S3 tables the reachability phase outlasts the default 1 h grace. Table left unreadable. | probe_gc_race_commit_during_slow_reachability |
| #58 | P0 | CONFIRMED | A manifest **truncated at an Avro block boundary reads as a shorter list** with no error (142 of 3000 entries); `row_count` wrong; GC deleted 2858 live files. `manifest_length` is recorded but never checked; no integrity check on manifests, lists or metadata. | probe_truncated_manifest_silent_partial |
| #59 | P0 | CONFIRMED | **Polling S3 lock admits two holders → lost commit**: both `append_records()` return `True`, one row and its data file vanish. Production runs this lock on OVH, which **does** support conditional writes (verified). Same orchestration with CAS on: nothing lost. | probe_s3_polling_lock_lost_commit, probe_external_ovh_* |
| #60 | P0 | CONFIRMED | **Hint recovery resurrects an uncommitted metadata file** of the same version (a crashed/lost-race writer's leftover, newer mtime): committed rows disappear and the next commit builds on the wrong base, dropping them from the lineage permanently. | probe_hint_recovery_picks_uncommitted_metadata |
| #61 | P1 | CONFIRMED | `delete_files()` of an unknown or slash-mismatched path **silently succeeds** and commits an empty `delete` snapshot (violates memory `no_silent_noop_apis`). | probe_delete_files_silent_noop |
| #62 | P1 | CONFIRMED | Schema validation ignores **field order**; a reordered schema from a fresh process is accepted and every scan then raises `ArrowInvalid: Schema at index 1 was different` — the #49 "table bricked by one append" class, new instance. | probe_schema_field_order_bricks_scan |
| #63 | P1 | CONFIRMED | S3 GC deletes **sibling-prefix objects** (`<table>/data_export/…`, `metadata/manifests_archive/…`): `list_files` lists the prefix without a `/` delimiter. | probe_s3_gc_prefix_overreach |
| #64 | P1 | CONFIRMED | `create_table(path, schema=B)` on a table with schema A **silently returns A** — no error, no warning. | probe_create_table_conflicting_schema |
| #65 | P1 | CONFIRMED | The repo's **S3 integration tests fail (2 of 3) against any S3 server** (`exists("metadata")` assertions broken since the strict `exists()` of #50) and never run in CI; the S3 path has had no automated coverage since 0.7.0. Test tables are never cleaned up (7 `test_table_*` leftovers in the bucket). | pytest vs moto |
| #66 | P2 | CONFIRMED | **Default checksum verification reads whole files**: a 1-column projection read 31.9 MB of a 31.9 MB table; a pruned point lookup read the whole 5.3 MB file (3–6× slower); `scan_batches` materialises whole files. Projection, pushdown and "streaming" are defeated by the default. | probe_verify_checksums_defeats_projection |
| #67 | P2 | CONFIRMED | **S3 request amplification**: 37 boto3 calls per single-row append (+ pyarrow's PUTs), 4 per `current_snapshot()`, ~5 per file per scan; missing keys take 3.1 s (404 retried 5×). On OVH: **5.4 s p50 per single-row commit alone, 10.3 s p50 / 53 s max with 4 writers**. README: "~50 ms". | probe_s3_request_count, probe_s3_missing_key_latency, probe_external_ovh_cas_commit_e2e |
| #68 | P2 | CONFIRMED | **Unbounded metadata**: per-commit metadata linear in commits (141 KiB at 300, ~46 MiB at 100k, re-read ≥4× per commit); superseded `v*.metadata.json` **never reclaimed** (21.9 MiB at 300 commits, quadratic → TiB at 100k); one manifest per commit, never compacted (scan = 905 storage calls at 300 tiny commits); the only retention lever is a table property **no public API can set**. | probe_metadata_growth, probe_snapshot_retention_and_expiry |
| #69 | P2 | CONFIRMED | Write path: **1000-row row groups** (50 per 50k-row file, 3× slower column reads), records converted to Arrow twice, `append_pandas` via `to_dict('records')` (6–11× slower than native), `append_records([])` commits an empty file + snapshot. | probe_row_group_layout, probe_append_pandas_path |
| #70 | P2 | CONFIRMED | Disk guard refuses **all writes at 95 % used regardless of free bytes** (400 GB free on 10 TB → `OSError`), making the lake read-only. | probe_disk_threshold_blocks_writes_with_free_space |
| #71 | P3 | CONFIRMED | House rules: 6 source files + 1 test exceed 500 lines (`transaction.py` 1329); GitHub Actions workflow present (left untouched, flagged); `__version__` reports 0.5.1 in the dev install. | probe_house_rules_and_claims |
| #72 | P3 | CONFIRMED | Docs overclaim: time-travel **reads** ("query data as it existed at any point in time" — `scan()` has no snapshot parameter), Avro/ORC formats (writer raises), ~50 ms S3 writes, "partition pruning" (partition values never used), implied Iceberg interoperability (formats are not spec-compliant); `load_table()` creates directories on a failed open. | probe_house_rules_and_claims, probe_load_table_side_effects |
| #73 | P3 | CONFIRMED | No `decimal(P,S)`, no `timestamptz` — financial amounts can only be float/double. | probe_types_decimal_timestamptz |
| #74 | P3 | SUSPECTED | Not reproduced: `os.write` return value ignored (short write → truncated metadata passes fsync+rename); same-millisecond OCC window for metadata-only commits; botocore retries × datashard retries (up to 24 attempts); `.tmp.*` and multipart leftovers never reclaimed; OCC retries leave orphan manifests. | — |
| #13 | — | open | Credential rotation still pending; this audit used the `.env` OVH keys for its external probes (objects written only under `audit-probe-<uuid>/`, all deleted). | — |

## 3. The P0s in detail

**#56 — GC vs a table called `data`.** `create_table("data")` (local, relative) or an S3 logical path `data`; three appends; `garbage_collect(grace_period_ms=0)` → `gc_stats data_files=3`, parquet files 3→0, `scan()` → `FileNotFoundError`. Control tables `sales`/`trades` untouched. Root: `garbage_collector.py:266-270` `if path.startswith(self.table_path): path = path[len(table_path):]`. Smallest fix: one canonical table-relative normaliser on the storage backend used for both the reachable set and the listing; abort on anything it cannot normalise. Class: the #45 class (two path sources, string comparison) — one instance was fixed, the class was not.

**#57 — GC vs a concurrent commit.** GC read metadata (snapshot S0), a writer committed S1 (file F, marker written then removed at commit), GC's slow phase outlasted the 2 s grace, listing found F unreachable/unprotected/old → deleted F and both new manifests; `scan()` → "references missing manifest list". Realistic trigger: the reachability phase costs 3 S3 requests per manifest (see #67/#68), so a table with tens of thousands of commits spends longer than the default 1 h grace there. Smallest fix: load markers **before** reading metadata; compute the cutoff from the GC start instant; register markers for `append_files()` files at queue time; refuse grace < 5 min without an explicit flag. Class: guard counting the wrong population (time at listing vs time of the metadata read).

**#58 — Truncated manifest.** 3000-entry manifest (340 788 B) cut after its first block (17 395 B): `read_manifest_file` returned 142 entries, no exception (fastavro ends cleanly at a block boundary); `row_count()` 1420 instead of 30000; `garbage_collect()` deleted 2858 live data files. Smallest fix: compare bytes consumed with `manifest_length` (already recorded) and raise; add sha256 for manifests in the manifest list and for the list in the snapshot summary; GC aborts on any mismatch. Class: fail-open parser at a container boundary; the same shape applies to the manifest list.

**#59 — Polling lock lost commit; OVH supports CAS.** Lock level: B `HEAD`s (no lock), A creates+verifies, B's delayed `PUT` lands, B verifies its own id → both hold. Commit level with the real `Transaction`/`MetadataManager` code (only timing injected): A passes its fence, B's lock PUT lands, B refreshes (hint unchanged), commits `vB`, A's delayed hint PUT then writes `vA` → both returned `True`, `row_count` 2 of 3, B's data file orphaned. Natural probability needs two latency spikes to coincide, but the desk runs this on every commit. Against the real endpoint: `PUT If-None-Match:*` on an existing key → **412 PreconditionFailed**; `If-Match` current → 200; stale → 412. With `DATASHARD_S3_USE_CONDITIONAL_WRITES=true`: 4 procs × 5 appends → 20/20 rows, 0 errors. Smallest fix: flip the flag in production **today**; then auto-detect CAS at backend init and fail closed on providers without it. Class: mocked counterparty encoding belief — the "OVH lacks If-None-Match" comment was never tested against OVH.

**#60 — Hint recovery picks an uncommitted file.** Committed `v2-<a>` (rows A, B) plus leftover `v2-deadbeef` with a newer mtime (what a crashed or lost-race writer leaves behind); hint removed → `load_table` picked `v2-deadbeef`, rows 1; the next append committed `v3` on that base → B dropped permanently. Smallest fix: delete the uncommitted metadata file on known-clean failures; when more than one file exists at the top version and the hint is gone, **raise** with the candidates and offer an explicit repair call. Class: recovery heuristic that guesses instead of failing closed (contradicts memory `fail_closed_invariant`).

## 4. Remediation plan (order matters)

0. **Ops, now, no release needed:** set `DATASHARD_S3_USE_CONDITIONAL_WRITES=true` for every OVH deployment (verified). Rotate the `.env` keys (#13). Do not name any table `data`/`metadata` (or any prefix of them) until #56 ships. Do not run `garbage_collect` with a grace period shorter than your longest GC run + longest transaction until #57 ships.
1. **Release 0.8.0 — P0 code fixes** (#56, #57, #58, #59, #60), each landing with its probe turned into a regression test; re-run `audit/evaluations/run_all.sh` and require the five P0 probes to PASS. Estimate: 2–3 focused days.
2. **P1 correctness** (#61–#65): small, localised; #65 adds a moto-server S3 job to the house CI so the S3 path is finally exercised. ~1 day.
3. **P2 performance** (#66–#70): page-CRC integrity default (#66), request de-duplication and no 404 retry (#67), metadata-file GC + `set_properties` + manifest compaction (#68 — blocked by #56/#57 in issuedb), single Arrow conversion + proper row groups + native pandas (#69), absolute free-space floor (#70). Re-measure with the probes; targets are in each ticket. ~1 week.
4. **P3** (#71–#74): file splits (do them while touching the modules in steps 1–3), docs corrections, decimal/timestamptz, and one probe per SUSPECTED item before fixing it.

Every ticket carries its EARS spec, evidence, chosen/rejected alternatives and the probe that must go green; copies live in `SPECS/56-*.md` … `SPECS/74-*.md`.

## 5. Closing statement

**What was exercised.** All 19 source modules were read in full. 24 probes ran through the public API against: the local filesystem (tmp dirs), a moto 5.2.2 S3 server (protocol-level S3, both lock providers, GC, scans, request counting), and the real OVH endpoint (conditional-write capability with three PUT preconditions; end-to-end commits with 1 and 4 concurrent processes × 5 single-row appends under a throwaway prefix, deleted afterwards). Local multi-process contention was exercised at 8 processes × 25 appends. Metadata growth was measured to 300 commits. The lock guards were tested in both directions (block and release). Every CONFIRMED finding has a runnable probe that reproduces it.

**What was not tested.** Sustained load (hours, thousands of commits) on the real endpoint; GC against the production tables or any real bucket; disk-full and process-kill fault injection against a live commit (the atomic-write reasoning is from code); NFS or Windows lock behaviour; the polling-lock lost commit under *natural* (uninjected) timing — its probability on OVH is unknown, only its possibility is proven; pyarrow's S3 write path on OVH beyond the 20 + 5 appends run here; multi-writer S3 contention beyond 4 processes; the `AmbiguousCommitError` path; tables created by versions before 0.6.0.

**What remains uncertain.** How often #59 fires in production today (the desk's real write concurrency and OVH's latency distribution decide it); whether the leftover `test_table_*` prefixes in the bucket are the only stray data; the #74 items, which are reasoned from code and carry no reproduction; whether the prior audits' individually fixed items (#45–#52) still hold — this audit re-verified their *classes* (two new instances found: #56 for the #45 class, #62 for the #49 class) rather than re-running each original reproduction.
