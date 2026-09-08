# #96 — A lock left by a dead process must be breakable in one acquire (0.10.4)

## Requirements (EARS)

- The metadata lock's acquire deadline **shall** exceed its lease, so a lock abandoned by a dead
  or killed process is always taken over within a single `acquire()` call rather than after a
  retry the caller has to know to make.
- **When** `acquire()` does time out, the error **shall** state the age of the blocking lock and
  that it is being renewed, so an operator can tell contention from a corpse — and **shall** warn
  against deleting a lock that is still held.
- A lock a live holder is renewing **shall** still be refused (both directions of the guard).

## Evidence (reproduced live against OVH `s3.eu-west-par.io.cloud.ovh.net`, 2026-09-08)

    planted a stale lock at <table>/.locks/metadata.lock
    lock timeout=30.0s  lease=60s   -> takeover needs age > lease
    FAILED after 32s: Failed to acquire S3 lock at .../.locks/metadata.lock within 30.0s
    second attempt acquired after 5s (lock was >60s old)

`S3LockProvider._try_takeover_expired` only breaks a lock older than `lease_seconds` (60), while
`acquire()` gave up at `self.timeout` (30), which `MetadataManager` hardcodes. So the first
attempt after any abandoned lock always failed, and only a retry made more than 60 s later
succeeded. Reported by crypto-trader, whose S3 migration failed with exactly this error.

Aggravating: migration is one-way, so the error appeared at the moment an operator is least
willing to retry blindly, and nothing in the message said it was transient.

After the fix, on the same bucket:

    1. stale lock: ACQUIRED after 64s (was: TimeoutError at 30s)
    2. live holder: correctly refused after 76s
       message: ... It was last renewed 13s ago (lease 60s). ...

## Process failure, recorded so it is not repeated

`migrate` on S3 shipped in 0.10.0 with "S3 migration not exercised" in the closing statement
instead of a test. S3 reads, CAS commits under multi-process contention, GC and DuckDB-over-S3
were all exercised, including against real OVH; the migration path specifically was not. Naming a
gap is not the same as closing it, and for a one-way operation the gap is where the cost lands.
Migration on real OVH has now been run end to end — dry-run, migrate, verify, post-migration
append; 4 snapshots in 31 s — and works.

## Verification

- `tests/test_lock_takeover.py`: the deadline always outlives the lease (asserted against the
  shipped default, 60 + 15); a planted stale lock is taken over, and the waiting really happened;
  a holder that keeps renewing is refused and never stolen, with the age in the message; a lock
  that cannot be inspected still produces the timeout rather than masking it.
- `audit/evaluations/probe_v0104_stale_lock_takeover.py`: both directions, plus a **control** that
  puts the deadline back inside the lease and shows the identical stale lock becoming unbreakable.
- Both directions re-run against the real OVH bucket.

## Outcome

**Shipped in 0.10.4 (2026-09-08):** https://pypi.org/project/datashard/0.10.4/ · tag v0.10.4.

Exercised: 277 unit tests (4 new); 36/36 probes; and the reported failure re-run end to end against
the **served** wheel on the real OVH bucket — a stale lock planted exactly as a killed migration
would leave one, then `migrate_table`, which completed in 71 s where 0.10.3 raised TimeoutError,
followed by `verify()` green. Both directions of the lease guard were re-run on that bucket before
and after the fix. The test prefixes were deleted afterwards; no `datashard-*` objects remain.

Not exercised: a provider other than OVH for the lock path, and the polling (non-CAS) lock, which
remains opt-in and best-effort.
