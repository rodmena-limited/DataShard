# Issue #60 — [P0 data loss] Version-hint recovery can pick a never-committed same-version metadata file; committed data disappears and later commits build on it

Found by adversarial audit #55 (2026-09-06). Priority: critical. Probe convention: PASS = claim holds, FAIL = defect reproduced.

EARS SPEC:
- When a commit fails after writing its vN metadata file but before the hint flip with a KNOWN-clean failure (CAS conflict, local atomic-write failure, lock lost), the MetadataManager shall delete that uncommitted metadata file before raising.
- If the hint is missing or invalid and more than one metadata file exists at the highest version, then refresh() shall raise AmbiguousMetadataError listing the candidates instead of choosing by mtime.
- The MetadataManager shall expose repair_version_hint(metadata_file) for an operator to resolve the ambiguity explicitly, logging the choice.
- Verification: audit/evaluations/probe_hint_recovery_picks_uncommitted_metadata.py shall PASS.

EVIDENCE (CONFIRMED live): committed v2-<a> (rows A,B) plus a leftover v2-deadbeef (a racing writer's file, never committed, newer mtime); hint removed -> load_table picked v2-deadbeef, row_count 1 (B gone); the next append committed v3 on that base -> B permanently dropped from the lineage (rows 2 instead of 3). Root cause: metadata_manager.py:576-620 _recover_version_from_files prefers the most recently modified file among equal versions; nothing distinguishes committed from uncommitted files.

TECHNICAL PROBLEMS: 1. Commit-point authority without a catalog; 2. Recovery heuristics that guess instead of failing closed.
SOLUTION DOMAINS: Iceberg's catalog-as-commit-authority; issuedb memory 'version_hint_is_a_hint' (must be amended: recovery may only be automatic when unambiguous).
ALTERNATIVES: prefer oldest mtime [REJECTED: still a guess; S3 clock skew]; per-version 'committed' marker [REJECTED: same race as the hint]; delete-on-known-failure + fail closed on ambiguity + explicit repair [CHOSEN].
