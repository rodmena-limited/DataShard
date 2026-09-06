"""House engineering rules and documentation claims that are cheap to falsify:
file-size cap, no GitHub Actions, __version__ single source of truth, documented file
formats, time-travel READS, and empty appends.
"""
import glob
import inspect
import os
import re
import tempfile

import _harness as H

H.local_env()
H.quiet_logs()
import pyarrow as pa  # noqa: E402

import datashard  # noqa: E402
from datashard import FileFormat, Table, create_table  # noqa: E402
from datashard.data_operations import DataFileWriter  # noqa: E402

root = H.ROOT
over = []
for f in glob.glob(os.path.join(root, "src", "datashard", "*.py")) + glob.glob(os.path.join(root, "tests", "*.py")):
    n = sum(1 for _ in open(f, encoding="utf-8"))
    if n > 500:
        over.append((os.path.relpath(f, root), n))
H.report("no-source-file-exceeds-500-lines (house rule: 500 soft / 550 hard)", not over, str(sorted(over, key=lambda x: -x[1])))
wf = [os.path.relpath(w, root) for w in glob.glob(os.path.join(root, ".github", "workflows", "*.yml"))]
# House rule: no GitHub Actions - but existing workflows are left alone until the operator asks
# for the migration to ci.rodmena.co.uk, so this is flagged rather than failed.
print(f"INFO github-actions-workflows-present (house rule: migrate to ci.rodmena.co.uk when asked): {wf}")
pv = re.search(r'^version = "([^"]+)"', open(os.path.join(root, "pyproject.toml")).read(), re.M).group(1)
H.report("__version__-matches-pyproject", datashard.__version__ == pv, f"datashard.__version__={datashard.__version__} pyproject={pv}")
perf_doc = open(os.path.join(root, "docs", "performance.rst"), encoding="utf-8").read()
claims_formats = "Good for schema evolution scenarios" in perf_doc or "Optimized for read-heavy workloads" in perf_doc
for fmt in (FileFormat.AVRO, FileFormat.ORC):
    try:
        DataFileWriter(os.path.join(tempfile.mkdtemp(), "x"), fmt, pa.schema([("a", pa.int64())])).open()
        refused, msg = False, "opened"
    except ValueError as e:
        refused, msg = True, f"refused: {str(e)[:50]}"
    H.report(
        f"file-format-{fmt.value}: docs and writer agree (parquet only)",
        refused and not claims_formats,
        f"writer {msg}; docs/performance.rst still advertises Avro/ORC={claims_formats}",
    )
params = list(inspect.signature(Table.scan).parameters)
H.report(
    "scan-can-read-a-historical-snapshot (docs: 'query data as it existed at any point in time')",
    any(p in params for p in ("snapshot_id", "as_of", "timestamp")),
    f"Table.scan parameters: {params}",
)
t = create_table(os.path.join(tempfile.mkdtemp(), "e"), H.simple_schema())
ok = t.append_records([], H.simple_schema())
H.report(
    "empty-append-is-a-no-op-or-rejected",
    len(t.snapshots()) == 0,
    f"append_records([]) returned {ok}; snapshots={len(t.snapshots())}; data files committed={len(t._get_all_data_files())}",
)
H.finish()
