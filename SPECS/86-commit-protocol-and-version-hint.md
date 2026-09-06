# #86 — [0.10.0] Commit protocol: create-if-absent v{N}.metadata.json as the commit point, advisory only-if-greater version-hint.text, legacy layout detection

EARS SPEC:
- The commit point shall be the exclusive creation of metadata/v{N+1}.metadata.json (S3: If-None-Match:*; local: temp+fsync+os.link, EEXIST = conflict, temp unlinked); a conflict shall raise ConcurrentModificationException and never leave a file at v{N+1}.

Design input: SPECS/83-iceberg-v2-spike.md; plan /home/farshid/.claude/plans/ok-let-s-plan-for-greedy-rainbow.md (0.10.0).

## Outcome
(filled at release)
