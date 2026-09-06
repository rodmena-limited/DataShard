# Issue #77 — S3 GC measured object age against the client clock

Found by re-audit #75 (2026-09-06) of 0.8.0. Priority: high (conditional on clock skew).
Probe: `audit/evaluations/probe_v080_gc_clock_skew.py`.

## EARS SPEC

- When collecting on an S3 backend, the garbage collector shall take its start instant (and
  the in-flight marker cutoff) from the storage server's clock (HTTP Date header of an S3
  response), not from the client's clock.
- If the server and client clocks differ by more than 60 s, then garbage_collect shall log a
  WARNING naming the skew.
- The garbage collector shall take object modification times from the listing (LastModified)
  instead of one HEAD per candidate, so a sweep over many orphans is O(pages), not O(objects).
- Verification: the probe above shall PASS (commit during GC + client clock 2 h fast);
  regression tests `test_s3_gc_survives_a_fast_client_clock`,
  `test_gc_uses_listing_mtimes_not_one_stat_per_object`.

## Evidence (CONFIRMED live on 0.8.0, moto)

Client clock +2 h, grace 1 h, one commit landing during reachability: GC deleted its data file,
both manifests and its metadata file; the table became unreadable ("references missing manifest
list"). Reachable files are never candidates, so exactly that window is exposed.

## Synthesis

Distributed clocks: compare timestamps against the clock that produced them - the object
store's - read from the response Date header [CHOSEN] vs requiring NTP on GC hosts [REJECTED:
operational, unverifiable from the library] vs refusing GC when skew is detected [REJECTED as
the only measure: still leaves the client clock in the decision].
