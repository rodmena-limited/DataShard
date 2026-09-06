# #89 — [0.10.0] datashard migrate CLI: legacy layout to Iceberg v2 in one commit, no downgrade; GC understands both layouts

EARS SPEC:
- 'datashard migrate <table> [--dry-run]' (and Table.migrate_to_iceberg()) shall, under the table lock, rewrite every snapshot's manifest list and manifests into the Iceberg form (same snapshot ids, sequence numbers, timestamps; bounds converted only where the encoding is exact; data files untouched), write v{N+1}.metadata.json exclusively, write the hint, then rename the legacy root hint to metadata.version-hint.text.migrated so 0.8.x/0.9.x fail closed.

Design input: SPECS/83-iceberg-v2-spike.md; plan /home/farshid/.claude/plans/ok-let-s-plan-for-greedy-rainbow.md (0.10.0).

## Outcome
(filled at release)
