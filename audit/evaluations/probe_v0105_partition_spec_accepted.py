"""Claim (#97, #98): create_table accepts a partition spec, and since 0.11 the metadata
and the data files AGREE about it.

0.7.2 accepted such a spec; 0.10.0-0.10.4 raised NotImplementedError, which removed a
capability - and only on CREATE, so an upgraded recorder that makes one table per day ran
green all day and would have failed at the day boundary. 0.10.5 accepted the call without
applying the spec; 0.11 applies it.

The invariant that outlives both releases is the second claim here: whatever the metadata
says about partitioning, the files must match it, because DuckDB and pyiceberg prune on
what the metadata says.
"""
import glob
import json
import os
import tempfile

import _harness as H

H.local_env()
H.quiet_logs()
from datashard import (  # noqa: E402
    PartitionField,
    PartitionSpec,
    Schema,
    create_table,
)

SCHEMA = Schema(schema_id=1, fields=[
    {"id": 1, "name": "hour", "type": "int", "required": True},
    {"id": 2, "name": "px", "type": "double"},
])
SPEC = PartitionSpec(spec_id=0, fields=[   # the reporter's exact spec
    PartitionField(source_id=1, field_id=1000, name="hour", transform="identity")])
tmp = tempfile.mkdtemp(prefix="audit_partspec_")


def build(name, spec):
    path = os.path.join(tmp, name)
    t = create_table(path, schema=SCHEMA, partition_spec=spec)
    for h in (9, 9, 19, 19, 23):
        t.append_records([{"hour": h, "px": float(h)}], SCHEMA)
    return t


try:
    t = build("with_spec", SPEC)
    created, err = t.created, None
except Exception as e:  # noqa: BLE001
    t, created, err = None, False, f"{type(e).__name__}: {str(e)[:110]}"
H.report(
    "create_table-accepts-a-partition-spec-it-cannot-apply",
    created,
    "the call the reporter's 429 tables were created with succeeds"
    if created else f"still raising: {err}",
)

if t is not None:
    doc = json.load(open(sorted(glob.glob(os.path.join(t.table_path, "metadata", "v*.metadata.json")))[-1]))
    specs = doc["partition-specs"]
    declared = [f["name"] for spec in specs for f in spec["fields"]]
    on_disk = {tuple(sorted((df.partition_values or {}).keys())) for df in t._get_all_data_files()}
    expected = {tuple(sorted(declared))} if declared else {()}
    H.report(
        "the-metadata-and-the-files-agree-about-partitioning",
        on_disk == expected,
        f"metadata declares {declared or 'no partitioning'} and every data file carries "
        f"{sorted(on_disk)} - foreign readers PRUNE on the metadata, so a file whose partition "
        f"struct does not match it would make them skip real rows",
    )
    layout = sorted({os.path.basename(os.path.dirname(f))
                     for f in glob.glob(os.path.join(t.table_path, "data", "*", "*.parquet"))})
    H.report(
        "a-partitioned-table-is-laid-out-by-its-spec",
        layout == ["hour=19", "hour=23", "hour=9"],
        f"data/ contains {layout or 'no partition directories'}",
    )
    plain = build("no_spec", None)
    same = all(t.scan(filter=f) == plain.scan(filter=f) != []
               for f in ({"hour": 19}, {"hour": ("between", (19, 19))}, {"hour": (">=", 19)}))
    H.report(
        "filters-return-the-same-rows-with-and-without-the-spec",
        same,
        "what a caller relies on until 0.11: pruning changes how much is READ, never what is returned",
    )
H.finish()
