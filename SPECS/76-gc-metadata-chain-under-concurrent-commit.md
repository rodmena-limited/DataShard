# Issue #76 — GC reclaims the previously-current metadata file when a commit lands during GC

Found by re-audit #75 (2026-09-06) of 0.8.0. Priority: medium. Probe:
`audit/evaluations/probe_v080_gc_metadata_chain_under_concurrent_commit.py`.

## EARS SPEC

- When garbage_collect() reclaims superseded metadata files, it shall determine the current
  version and the metadata_log at reclaim time (not from the metadata view read at GC start),
  so a commit that landed during the run keeps every file its log references.
- The garbage collector shall additionally keep every metadata file whose version number is
  within write.metadata.previous-versions-max of the current version (or ahead of it),
  independent of the log contents.
- Verification: the probe above shall PASS; regression test
  `tests/test_reaudit_75_fixes.py::test_gc_keeps_metadata_chain_when_a_commit_lands_during_gc`.

## Evidence

15 commits, GC started at v15; a commit to v16 landed during reachability; GC reclaimed
`v15-*.metadata.json` although v16's metadata_log names it (rows intact). Impact: auditability
of the chain; a stale reader hits FileNotFoundError before the recovery fallback. No data loss.

## Synthesis (localised)

Retention window keyed by monotonic version numbers (Iceberg's previous-versions-max) plus a
fresh read of the log at reclaim time [CHOSEN] vs re-reading the log only [REJECTED: still racy
against a second commit during reclamation].
