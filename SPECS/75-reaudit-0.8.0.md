# Issue #75 — Re-audit datashard 0.8.0 (falsify the remediation itself)

Ticket: issuedb #75. Method: `mission-critical-audit`. Baseline: `audit/evaluations/run_all.sh`
22/22 PASS on 0.8.0 (2ec68f8).

## EARS SPEC

- The auditor shall run the existing harness first and treat its PASS as the baseline, not as
  evidence that the new code is correct.
- The auditor shall treat every guarantee introduced by 0.8.0 as a claim to falsify: GC age
  decisions relative to the start instant (including client/server clock skew on S3),
  markers-before-metadata ordering, superseded-metadata reclamation vs the metadata_log chain
  under a concurrent commit, manifest compaction under interleaved appends/deletes and under
  multi-process contention (local and S3), CAS auto-detection and refusal, hint repair,
  page-CRC verification of files written without CRCs, and time-travel reads across compaction.
- The auditor shall exercise the upgrade path with a table written by the released 0.7.2 package
  (read, append, GC with 0.8.0; read back with 0.7.2).
- If a defect is confirmed, then it shall be ticketed, fixed with a regression test and probe,
  and released as 0.8.1.
- The auditor shall close with what was exercised, what was not, and what remains uncertain.

New probes carry the prefix `probe_v080_`.
