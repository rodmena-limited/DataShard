# datashard 0.6.0 — Re-Audit & Production-Worthiness Verdict

**Audit ticket:** issuedb #44
**Date:** 2026-07-24
**Version audited:** 0.6.0 (git `main` @ 8d16599, post audit-#10 remediation)
**Method:** 10-dimension multi-agent review (correctness/commit, concurrency/OCC, security, Iceberg fidelity, scan/filter, GC/durability, S3 backend, test rigor, API/versioning, error handling) with independent adversarial verification of every finding; ~170 raw findings triaged, 23 refuted as false positives; independent manual re-read of all load-bearing modules; empirical runs (pytest, mypy, ruff, git-history secret sweep); **executable reproductions** for both new serious defects.

---

## 0. Remediation status (2026-07-25)

**All findings from this audit are fixed and closed: #45–#49 (blockers) and #50–#52 (bank-grade clusters).**
The verdict below is the state of 0.6.0 as audited; §6 records what changed since.

| Ticket | Sev | Status |
|---|---|---|
| #45 | P0 | ✅ Fixed — canonical base path in `list_files` + fail-closed GC guard; original reproduction re-run clean |
| #46 | P1 | ✅ Fixed — `in`/`not_in` never match NULL; original reproduction re-run clean |
| #47 | P1 | ✅ Fixed — read path sandboxed to the table root |
| #48 | P1 | ✅ Fixed — dangling `current_snapshot_id` aborts commit **and** raises on read |
| #49 | P1 | ✅ Fixed — `append_files()` validates the parquet footer schema |
| #50 | P2 | ✅ Fixed — S3 retries/permanent-error fast-fail, strict `exists()`, polling-lock lease fencing |
| #51 | P2 | ✅ Fixed — sequence numbers, `schema_id`, `metadata_log`, lineage repointing, in-flight manifest protection, legacy cleanup disabled |
| #52 | P3 | ✅ Fixed — docs version, classifier, console script, dead code, `{"col": None}`, logging, excepts, temp-file leak, py3.10 floor |
| #13 | — | ⏳ Open — key rotation (operational, not code) |

Gate after remediation: **141 passed / 3 skipped** (S3, no creds), `mypy src/datashard` clean, `ruff check .` clean.
34 new regression tests (`tests/test_audit_44_fixes.py`, `tests/test_audit_44_p2_fixes.py`); 27 of them fail
against the pre-fix source, verified by stashing the fixes.

---

## 1. Verdict

> ## ⚠️ NOT YET PRODUCTION-WORTHY — conditionally close: **one reproduced P0 + four P1s stand between 0.6.0 and a production pass.**
>
> The v0.6.0 remediation is real: **every prior P0 blocker (#14–#24 code defects) is verifiably fixed in source and regression-tested** — the commit protocol, GC, filters, schema handling, locks, and path guards now genuinely fail closed. The architecture (OCC + CAS-fenced commit point + fail-closed GC) is production-grade in design. But this re-audit found **one new reproduced data-loss P0** (#45) and **four P1s** (#46–#49) in paths the remediation didn't touch, plus the still-open credential-rotation ticket (#13). Bank-grade certification remains **denied** until those are cleared; ordinary production use (single-writer, local or CAS-capable S3, no symlinked roots) is close — the fixes are small and well-localized.

### Pillar scorecard

| Pillar | 0.5.4 (audit #10) | 0.6.0 (this audit) | Basis |
|---|---|---|---|
| **Correctness** | ❌ Fail | ⚠️ Near pass | All 10 prior P0s fixed & tested. New: #45 (symlink-root GC deletes live data, reproduced), #46 (`not_in` returns NULL rows, reproduced), #48 (dangling snapshot-id fail-open). |
| **Security** | ❌ Fail | ⚠️ Near pass | No live secrets in working tree or git history (see §3). #47: read path bypasses the table-root sandbox. #13: key rotation unverifiable, stays open. |
| **Auditability** | ❌ Fail | ⚠️ Partial | Lineage joins fixed (#25/#30). Still inert: sequence numbers, `schema_id`, `metadata_log` (#51). |
| **Test rigor** | ❌ Fail | ⚠️ Improved | 107 pass / 3 skip; real fault-injection + multi-process OCC tests now exist. Gaps: no S3 tests in CI (moto absent), no symlink/read-path traversal tests — exactly where #45/#47 hid. |

---

## 2. Empirical results

- **pytest:** 107 passed, 3 skipped (S3 integration — no creds), ~4s.
- **mypy** (strict): clean, 19 files. **ruff:** clean.
- **Prior-fix re-verification:** 123 of the ~170 reviewed findings confirm prior fixes as genuinely present and correct. Independent manual reads confirmed the highest-stakes ones: fail-closed commit (`transaction.py:283–426`), AmbiguousCommitError semantics, fail-closed GC (`garbage_collector.py`), CAS lock with fencing (`lock_provider.py:217+`, `metadata_manager.py:215`), FileLock timeout, realpath+commonpath traversal guard (`storage_backend.py:148–181`), is_null in all scan APIs, version-hint recovery, no-clobber `initialize_table`.

## 3. Security: the #13 picture, corrected

A full `git rev-list --all` sweep shows the only "AWS key" ever committed is `AKIAIOSFODNN7EXAMPLE` — AWS's official documentation example — plus MinIO placeholder pairs (`rodmena`/`pleasebeready`, `minioadmin`) with a private endpoint (`s3.rodmena.co.uk`, currently unreachable). **No real AWS credentials are in git history.** The prior audit's "live credentials in a public repo" was overstated; the residual risk is the weak MinIO password pair in history and the real-looking key set in the local, untracked, gitignored `.env`. **#13 stays open but downgraded in urgency:** rotate the MinIO pair and the `.env` keys; history purge is optional hardening rather than an emergency.

**Note:** `.env` currently sets `DATASHARD_S3_USE_CONDITIONAL_WRITES=false` — this selects `S3PollingLockProvider`, the documented-weaker non-CAS lock (#50). For production S3 use, run with conditional writes ON.

## 4. New findings (all ticketed, adversarially verified)

| Ticket | Sev | Finding |
|---|---|---|
| **#45** | **P0** | **[REPRODUCED]** Opening a table through a **symlinked root** makes `LocalStorageBackend.list_files` (`storage_backend.py:304`) emit `../real_table/...` paths (relpath against raw base vs realpath-walked tree). GC then classifies **every live file as an orphan and deletes the whole table**. Proof: 2-row table, GC via symlink → all data + manifests deleted, table unreadable. |
| **#46** | P1 | **[REPRODUCED]** `not_in` keeps NULL rows (`filters.py:176`): `not_in [2]` over `[1,2,NULL,3]` → `[1,NULL,3]`, contradicting the API doc at `transaction.py:929` ("in/not_in never match NULL"). Silently wrong query results. |
| **#47** | P1 | Read path bypasses the table sandbox: `_get_arrow_path` (`data_operations.py:344`) returns true absolute paths **as-is**, so a tampered manifest entry reads any file on disk — the #27 guard protects writes/GC but not reads. |
| **#48** | P1 | Fail-open remnant: if `current_snapshot_id` dangles (corrupt metadata), `_commit_file_ops` (`transaction.py:404`) proceeds with an **empty** base manifest set — new snapshot silently drops all prior data. Same class as fixed #15. |
| **#49** | P1 | `append_files()` validates only existence, not schema (`transaction.py:88`); a schema-divergent parquet commits fine and bricks subsequent scans. Its sibling `append_data()` does validate. |
| **#50** | P2 | S3/locking robustness cluster: polling-lock (non-CAS) lost-update windows, no retry on delete/stat/list, permanent-error retry waste, `exists()` prefix false-positive. |
| **#51** | P2 | Iceberg-fidelity cluster: sequence numbers inert despite `format_version: 2`, `schema_id` never set, `metadata_log` unpopulated, expire/delete leave dangling lineage refs, GC markers don't protect in-flight manifests, legacy `cleanup_orphaned_files` lacks all safety rails. |
| **#52** | P3 | Hygiene cluster: `docs/conf.py` says 0.1.5, Alpha classifier, no console script, dead `to_pyarrow_filter` (drops IS_NULL), `{"col": None}` returns zero rows, GC log flood, blanket excepts, temp-file leak, EOL py3.7 floor. |

Twenty-three further reviewer claims (incl. re-litigations of #27/#30/#32/#36 and a TOCTOU-symlink-swap theory) were **refuted** by adversarial verification and are not carried.

## 5. Path to production sign-off

1. **#45** — one-line class of fix (`relpath` against `os.path.realpath(self.base_path)`) + symlink GC regression test. *Blocker.*
2. **#46–#49** — each is a small, localized fix (add `field.is_valid()` to not_in; route reads through `_resolve_path`; raise on dangling snapshot id; validate parquet footer schema in `append_files`). *Required for certification.*
3. **#13** — rotate the MinIO pair + `.env` keys (10 minutes); history purge optional.
4. **#50–#51** — required for *bank-grade* certification, not for ordinary production use.
5. Re-run this audit's reproductions + suite; then certify.

## 6. Post-audit remediation (2026-07-25)

All eight tickets are closed; specs in `SPECS/45-49-certification-blockers.md` and
`SPECS/50-52-bank-grade-clusters.md`, changes summarised in `CHANGELOG.md` (Unreleased).

- **#45** `LocalStorageBackend._real_base_path()` is now the single canonical base for
  `_resolve_path` and `list_files`; `list_files` raises if a produced path escapes, and
  `GarbageCollector._gc_prefix` independently aborts on any `..` path (fail closed).
- **#46** `in`/`not_in` drop NULL from the value set and AND with `field.is_valid()`.
- **#47** `_get_arrow_path` routes table-relative paths through `_resolve_path` and accepts a
  true absolute path only when it is contained in the canonical table root.
- **#48** Dangling `current_snapshot_id` raises in `_commit_file_ops` **and** in
  `_get_all_data_files` (a broken table is never reported as an empty one).
- **#49** `Transaction._validate_file_schema` compares the parquet footer schema to the
  table schema with `equals(check_metadata=False)` — precisely the condition
  `pa.concat_tables` requires.
- **#50/#51/#52** as tabulated in §0.

Two behaviour changes worth noting for users: manifest paths are strictly table-relative on
read as well as write, and `{"col": None}` now raises instead of silently matching nothing.

**Remaining before certification:** **#13** only — rotate the MinIO pair and the local `.env`
keys. That is an operational step; no code change is outstanding.

**Bottom line:** 0.6.0 is a dramatic, genuine improvement — the core engine now deserves the trust the previous version only claimed. It is suitable **today** for non-critical/internal workloads (single-writer, no symlinked roots, CAS-enabled S3 or local). It is **not yet certifiable for production data you cannot lose** until #45–#49 land, which is roughly a day of focused work.
