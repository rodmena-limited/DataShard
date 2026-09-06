# Issue #56 — [P0 data loss] GC deletes every live file when table_path is a string prefix of 'data'/'metadata' (e.g. a table named 'data')

Found by adversarial audit #55 (2026-09-06). Priority: critical. Probe convention: PASS = claim holds, FAIL = defect reproduced.

EARS SPEC:
- The garbage collector shall normalise reachable paths and listed paths through ONE canonical table-relative normaliser provided by the storage backend, and shall never strip table_path as a plain string prefix.
- If a listed path cannot be normalised to a table-relative path, then garbage_collect shall abort (GarbageCollectionAborted) without deleting anything.
- When a table is created with any path (including 'data', 'metadata', 'd', 'meta', 'data/' and S3 logical prefixes of the same shape), garbage_collect shall delete zero files referenced by the current snapshot.
- Verification: audit/evaluations/probe_gc_table_path_prefix_collision.py shall PASS for local and S3 (moto).

EVIDENCE (CONFIRMED live, 2026-09-06):
- local create_table("data") + 3 appends + garbage_collect(grace=0): gc_stats data_files=3, parquet files 3->0, scan -> FileNotFoundError.
- S3 (moto) table_path "data": data keys 3->0, scan -> FileNotFoundError. Control tables 'sales'/'trades' untouched.
- Root cause: garbage_collector.py:266-270 _normalize_path does `if path.startswith(self.table_path): path = path[len(table_path):]`, so 'data/auto_x.parquet' becomes 'auto_x.parquet' for a table called 'data' while the reachable set holds 'data/auto_x.parquet'.

TECHNICAL PROBLEMS:
1. Two path sources (manifest entries vs storage listing) canonicalised by different code; the GC compares them by string equality.

SOLUTION DOMAINS:
- Filesystem/object-store path semantics (POSIX relpath; S3 key prefix + '/' delimiter). Codebase pattern: LocalStorageBackend._real_base_path()/list_files already return canonical table-relative paths (#45 fix) - extend that contract to the backend interface.

ALTERNATIVES:
- Strip only `table_path + '/'` [REJECTED: still string-based; wrong for trailing-slash paths and S3 logical prefixes].
- StorageBackend.to_table_relative(path) as the single normaliser used by GC for both sets; manifests keep table-relative paths [CHOSEN].
- Special-case reserved table names [REJECTED: whack-a-mole].

PLAN:
1. Add StorageBackend.to_table_relative(path) (local: realpath+relpath against canonical base; S3: strip the configured prefix only when followed by '/').
2. GC uses it for reachable_* sets and for every listed path; abort on any path it cannot normalise.
3. Regression tests: table names data / metadata / d / data/ on local and moto S3, asserting GC deletes nothing.
4. Re-run the probe -> PASS.
