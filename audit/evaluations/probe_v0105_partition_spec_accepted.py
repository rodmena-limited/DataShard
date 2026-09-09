"""Claim (#97): a partition spec datashard cannot APPLY is still ACCEPTED at create,
and the metadata never claims a partitioning the data files do not have.

0.7.2 accepted such a spec; 0.10.0-0.10.4 raised NotImplementedError, which removed a
capability - and only on CREATE, so an upgraded recorder that makes one table per day
ran green all day and would have failed at the day boundary.

Both halves are probed: the call must succeed, AND the persisted spec must stay empty,
because DuckDB and pyiceberg prune on a partition spec and these files carry empty
partition structs.
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
    load_table,
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
    H.report(
        "the-metadata-never-claims-a-partitioning-the-files-do-not-have",
        specs == [{"spec-id": 0, "fields": []}],
        f"persisted partition-specs={specs} - foreign readers PRUNE on this, and these data "
        f"files carry empty partition structs, so a recorded field would make them skip real rows",
    )
    H.report(
        "the-requested-fields-are-recorded-not-lost",
        load_table(t.table_path).properties().get("datashard.requested-partition-fields") == "hour",
        f"datashard.requested-partition-fields={load_table(t.table_path).properties().get('datashard.requested-partition-fields')!r} "
        f"so 0.11 can offer to apply it",
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
