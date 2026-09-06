# Issue #57 — [P0 data loss] GC deletes files committed while GC runs: markers loaded after reachability, cutoff taken at listing time

Found by adversarial audit #55 (2026-09-06). Priority: critical. Probe convention: PASS = claim holds, FAIL = defect reproduced.

EARS SPEC:
- The garbage collector shall load in-flight protection markers BEFORE it reads table metadata, so a commit landing after the metadata read is still covered by the markers it had at that instant.
- The garbage collector shall compute the deletion cutoff as (GC start instant, taken before the metadata read) minus grace_period_ms, never as (now at listing) minus grace.
- When append_files() queues caller-provided files, the transaction shall register in-flight markers for them at queue time (today only append_data files and manifests get markers).
- If grace_period_ms < 300000 (5 min), then garbage_collect shall refuse unless allow_short_grace=True is passed; the docstring shall state grace must exceed the longest transaction plus the longest GC run.
- Verification: audit/evaluations/probe_gc_race_commit_during_slow_reachability.py shall PASS.

EVIDENCE (CONFIRMED live): with grace 2 s, a commit landing between GC's reachability phase and its listing lost its data file and both manifests (gc_stats data_files=1, manifest_files=2); the table became unreadable: 'Current snapshot ... references missing manifest list'. On S3 the reachability phase of a large table takes longer than the default 1 h grace (3 requests per manifest; see perf ticket), so this happens with default settings.

TECHNICAL PROBLEMS:
1. Consistency between three point-in-time observations (metadata, markers, listing) made at different instants.

SOLUTION DOMAINS:
- Iceberg remove_orphan_files semantics (older-than relative to the operation start, never 'now'); codebase pattern: in-flight markers (garbage_collector.py, transaction.py:_register_inflight).

ALTERNATIVES:
- List storage first, then read metadata [REJECTED: still loses long-running append_files transactions whose files predate the markers].
- Markers-first ordering + start-time cutoff + markers for append_files [CHOSEN].
- GC takes the metadata lock for its whole run [REJECTED: blocks writers for hours on large S3 tables].
