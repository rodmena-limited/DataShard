"""Hand-written Iceberg v2 table generator for the 0.10.0 spike (#83). No datashard code involved."""
import json, os, struct, time, uuid
from decimal import Decimal
from datetime import datetime, timezone
import fastavro, pyarrow as pa, pyarrow.parquet as pq

SCHEMA_FIELDS = [
    {"id": 1, "name": "id", "required": True, "type": "long"},
    {"id": 2, "name": "sym", "required": False, "type": "string"},
    {"id": 3, "name": "px", "required": False, "type": "decimal(18,8)"},
    {"id": 4, "name": "ts", "required": False, "type": "timestamptz"},
]
ICEBERG_SCHEMA = {"type": "struct", "schema-id": 0, "fields": SCHEMA_FIELDS}

def rows(n0=1, n=3):
    return {
        "id": list(range(n0, n0 + n)),
        "sym": [f"S{i}" if i % 3 else None for i in range(n0, n0 + n)],
        "px": [Decimal(f"{i}.50000000") for i in range(n0, n0 + n)],
        "ts": [datetime(2026, 9, 6, 12, i % 60, tzinfo=timezone.utc) for i in range(n0, n0 + n)],
    }

def arrow_table(data, field_ids=True):
    fs = []
    types = {"id": pa.int64(), "sym": pa.string(), "px": pa.decimal128(18, 8), "ts": pa.timestamp("us", tz="UTC")}
    for f in SCHEMA_FIELDS:
        md = {b"PARQUET:field_id": str(f["id"]).encode()} if field_ids else None
        fs.append(pa.field(f["name"], types[f["name"]], nullable=not f["required"], metadata=md))
    return pa.table(data, schema=pa.schema(fs))

# ---- Iceberg single-value binary bounds
def b_long(v): return struct.pack("<q", v)
def b_str(v): return v.encode()
def b_dec(v: Decimal):
    unscaled = int(v.scaleb(8))
    n = max(1, (unscaled.bit_length() + 8) // 8)
    return unscaled.to_bytes(n, "big", signed=True)
def b_ts(v: datetime): return struct.pack("<q", int(v.timestamp() * 1_000_000))

def bounds(data, fake=False):
    ids = data["id"]; syms = [s for s in data["sym"] if s is not None]
    lo = {1: b_long(min(ids)), 2: b_str(min(syms)), 3: b_dec(min(data["px"])), 4: b_ts(min(data["ts"]))}
    hi = {1: b_long(max(ids)), 2: b_str(max(syms)), 3: b_dec(max(data["px"])), 4: b_ts(max(data["ts"]))}
    if fake:  # deliberately wrong: claims id in [1000, 2000] -> a reader that trusts bounds prunes the file
        lo[1], hi[1] = b_long(1000), b_long(2000)
    return lo, hi

# ---- Avro schemas (Iceberg v2, field-ids on every field)
def _map(name, kid, vid, vtype):
    return ["null", {"type": "array", "logicalType": "map", "items": {"type": "record", "name": f"k{kid}_v{vid}",
            "fields": [{"name": "key", "type": "int", "field-id": kid}, {"name": "value", "type": vtype, "field-id": vid}]}}]

def manifest_entry_schema(partition_fields=()):
    data_file = {"type": "record", "name": "r2", "fields": [
        {"name": "content", "type": "int", "field-id": 134},
        {"name": "file_path", "type": "string", "field-id": 100},
        {"name": "file_format", "type": "string", "field-id": 101},
        {"name": "partition", "type": {"type": "record", "name": "r102", "fields": list(partition_fields)}, "field-id": 102},
        {"name": "record_count", "type": "long", "field-id": 103},
        {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
        {"name": "column_sizes", "type": _map("column_sizes", 117, 118, "long"), "field-id": 108, "default": None},
        {"name": "value_counts", "type": _map("value_counts", 119, 120, "long"), "field-id": 109, "default": None},
        {"name": "null_value_counts", "type": _map("null_value_counts", 121, 122, "long"), "field-id": 110, "default": None},
        {"name": "nan_value_counts", "type": _map("nan_value_counts", 138, 139, "long"), "field-id": 137, "default": None},
        {"name": "lower_bounds", "type": _map("lower_bounds", 126, 127, "bytes"), "field-id": 125, "default": None},
        {"name": "upper_bounds", "type": _map("upper_bounds", 129, 130, "bytes"), "field-id": 128, "default": None},
        {"name": "key_metadata", "type": ["null", "bytes"], "field-id": 131, "default": None},
        {"name": "split_offsets", "type": ["null", {"type": "array", "items": "long", "element-id": 133}], "field-id": 132, "default": None},
        {"name": "equality_ids", "type": ["null", {"type": "array", "items": "int", "element-id": 136}], "field-id": 135, "default": None},
        {"name": "sort_order_id", "type": ["null", "int"], "field-id": 140, "default": None},
    ]}
    return {"type": "record", "name": "manifest_entry", "fields": [
        {"name": "status", "type": "int", "field-id": 0},
        {"name": "snapshot_id", "type": ["null", "long"], "field-id": 1, "default": None},
        {"name": "sequence_number", "type": ["null", "long"], "field-id": 3, "default": None},
        {"name": "file_sequence_number", "type": ["null", "long"], "field-id": 4, "default": None},
        {"name": "data_file", "type": data_file, "field-id": 2},
    ]}

def manifest_list_schema(v1_names=False):
    a, e, d = ("added_data_files_count", "existing_data_files_count", "deleted_data_files_count") if v1_names else \
              ("added_files_count", "existing_files_count", "deleted_files_count")
    summary = {"type": "record", "name": "r508", "fields": [
        {"name": "contains_null", "type": "boolean", "field-id": 509},
        {"name": "contains_nan", "type": ["null", "boolean"], "field-id": 518, "default": None},
        {"name": "lower_bound", "type": ["null", "bytes"], "field-id": 510, "default": None},
        {"name": "upper_bound", "type": ["null", "bytes"], "field-id": 511, "default": None}]}
    return {"type": "record", "name": "manifest_file", "fields": [
        {"name": "manifest_path", "type": "string", "field-id": 500},
        {"name": "manifest_length", "type": "long", "field-id": 501},
        {"name": "partition_spec_id", "type": "int", "field-id": 502},
        {"name": "content", "type": "int", "field-id": 517},
        {"name": "sequence_number", "type": "long", "field-id": 515},
        {"name": "min_sequence_number", "type": "long", "field-id": 516},
        {"name": "added_snapshot_id", "type": "long", "field-id": 503},
        {"name": a, "type": "int", "field-id": 504},
        {"name": e, "type": "int", "field-id": 505},
        {"name": d, "type": "int", "field-id": 506},
        {"name": "added_rows_count", "type": "long", "field-id": 512},
        {"name": "existing_rows_count", "type": "long", "field-id": 513},
        {"name": "deleted_rows_count", "type": "long", "field-id": 514},
        {"name": "partitions", "type": ["null", {"type": "array", "items": summary, "element-id": 508}], "field-id": 507, "default": None},
        {"name": "key_metadata", "type": ["null", "bytes"], "field-id": 519, "default": None},
    ]}

def write_avro(path, schema, records, meta):
    with open(path, "wb") as fh:
        fastavro.writer(fh, fastavro.parse_schema(schema), records, metadata=meta, codec="deflate")
    return os.path.getsize(path)

class TableWriter:
    """Builds a table snapshot by snapshot. `uri(path)` decides how paths appear in metadata."""
    def __init__(self, root, uri_style="file", field_ids=True, name_mapping=False, v1_names=False,
                 hint=True, fake_bounds=False, minimal_manifest=False, location_style="file"):
        self.root = os.path.abspath(root); os.makedirs(f"{self.root}/metadata"); os.makedirs(f"{self.root}/data")
        self.uri_style, self.field_ids, self.name_mapping, self.v1_names = uri_style, field_ids, name_mapping, v1_names
        self.hint, self.fake_bounds, self.minimal = hint, fake_bounds, minimal_manifest
        self.location_style = location_style
        self.uuid = str(uuid.uuid4()); self.version = 0; self.seq = 0; self.snapshots = []; self.metadata_log = []
        self.manifests = []  # (manifest_file record) accumulated for the manifest list
        self.last_metadata = None

    def uri(self, rel):
        if self.uri_style == "file": return f"file://{self.root}/{rel}"
        if self.uri_style == "abs": return f"{self.root}/{rel}"
        if self.uri_style == "rel": return rel                 # "data/x.parquet"
        if self.uri_style == "slashrel": return "/" + rel      # datashard 0.8 style "/data/x.parquet"
        raise ValueError(self.uri_style)

    def location(self):
        return f"file://{self.root}" if self.location_style == "file" else self.root

    def _write_manifest(self, entries, content, sid):
        name = f"{uuid.uuid4()}-m0.avro"; path = f"{self.root}/metadata/{name}"
        meta = {"schema": json.dumps(ICEBERG_SCHEMA), "schema-id": "0", "partition-spec": "[]", "partition-spec-id": "0",
                "format-version": "2", "content": "data" if content == 0 else "deletes"}
        length = write_avro(path, manifest_entry_schema(), entries, meta)
        n_rows = sum(e["data_file"]["record_count"] for e in entries)
        rec = {"manifest_path": self.uri(f"metadata/{name}"), "manifest_length": length, "partition_spec_id": 0,
               "content": content, "sequence_number": self.seq, "min_sequence_number": self.seq, "added_snapshot_id": sid,
               "added_rows_count": n_rows, "existing_rows_count": 0, "deleted_rows_count": 0, "partitions": [], "key_metadata": None}
        a, e, d = ("added_data_files_count", "existing_data_files_count", "deleted_data_files_count") if self.v1_names else \
                  ("added_files_count", "existing_files_count", "deleted_files_count")
        rec.update({a: len(entries), e: 0, d: 0})
        return rec

    def _entry(self, rel, size, count, content, table=None, data=None):
        df = {"content": content, "file_path": self.uri(rel), "file_format": "PARQUET", "partition": {}, "record_count": count,
              "file_size_in_bytes": size, "column_sizes": None, "value_counts": None, "null_value_counts": None,
              "nan_value_counts": None, "lower_bounds": None, "upper_bounds": None, "key_metadata": None,
              "split_offsets": None, "equality_ids": None, "sort_order_id": None}
        if data is not None and not self.minimal:
            lo, hi = bounds(data, fake=self.fake_bounds)
            df["lower_bounds"] = [{"key": k, "value": v} for k, v in lo.items()]
            df["upper_bounds"] = [{"key": k, "value": v} for k, v in hi.items()]
            df["value_counts"] = [{"key": f["id"], "value": count} for f in SCHEMA_FIELDS]
            df["null_value_counts"] = [{"key": f["id"], "value": sum(x is None for x in data[f["name"]])} for f in SCHEMA_FIELDS]
        return {"status": 1, "snapshot_id": None, "sequence_number": None, "file_sequence_number": None, "data_file": df}

    def append(self, data):
        self.seq += 1; sid = int(time.time() * 1000) * 1000 + self.seq
        name = f"{uuid.uuid4()}.parquet"; path = f"{self.root}/data/{name}"
        pq.write_table(arrow_table(data, self.field_ids), path)
        entry = self._entry(f"data/{name}", os.path.getsize(path), len(data["id"]), 0, data=data)
        self.manifests.append(self._write_manifest([entry], 0, sid))
        return self._commit(sid, "append", {"added-data-files": "1", "added-records": str(len(data["id"]))})

    def positional_delete(self, data_rel, positions):
        """content=1 positional delete file against `data_rel` (path as written in the data manifest)."""
        self.seq += 1; sid = int(time.time() * 1000) * 1000 + self.seq
        name = f"{uuid.uuid4()}-deletes.parquet"; path = f"{self.root}/data/{name}"
        t = pa.table({"file_path": pa.array([self.uri(data_rel)] * len(positions), pa.string()),
                      "pos": pa.array(positions, pa.int64())},
                     schema=pa.schema([pa.field("file_path", pa.string(), False, metadata={b"PARQUET:field_id": b"2147483546"}),
                                       pa.field("pos", pa.int64(), False, metadata={b"PARQUET:field_id": b"2147483545"})]))
        pq.write_table(t, path)
        entry = self._entry(f"data/{name}", os.path.getsize(path), len(positions), 1)
        self.manifests.append(self._write_manifest([entry], 1, sid))
        return self._commit(sid, "delete", {"added-delete-files": "1", "added-position-deletes": str(len(positions))})

    def _commit(self, sid, op, summary):
        # every manifest list re-lists all live manifests (no manifest compaction here)
        ml_name = f"snap-{sid}-1-{uuid.uuid4()}.avro"; ml_path = f"{self.root}/metadata/{ml_name}"
        parent = self.snapshots[-1]["snapshot-id"] if self.snapshots else None
        meta = {"snapshot-id": str(sid), "parent-snapshot-id": str(parent) if parent else "null",
                "sequence-number": str(self.seq), "format-version": "2"}
        write_avro(ml_path, manifest_list_schema(self.v1_names), self.manifests, meta)
        now = int(time.time() * 1000)
        snap = {"snapshot-id": sid, "sequence-number": self.seq, "timestamp-ms": now,
                "manifest-list": self.uri(f"metadata/{ml_name}"), "summary": {"operation": op, **summary}, "schema-id": 0}
        if parent: snap["parent-snapshot-id"] = parent
        self.snapshots.append(snap)
        if self.last_metadata:
            self.metadata_log.append({"metadata-file": self.uri(f"metadata/v{self.version}.metadata.json"), "timestamp-ms": now})
        self.version += 1
        props = {"write.format.default": "parquet"}
        if self.name_mapping:
            props["schema.name-mapping.default"] = json.dumps([{"field-id": f["id"], "names": [f["name"]]} for f in SCHEMA_FIELDS])
        md = {"format-version": 2, "table-uuid": self.uuid, "location": self.location(), "last-sequence-number": self.seq,
              "last-updated-ms": now, "last-column-id": 4, "current-schema-id": 0, "schemas": [ICEBERG_SCHEMA],
              "default-spec-id": 0, "partition-specs": [{"spec-id": 0, "fields": []}], "last-partition-id": 999,
              "default-sort-order-id": 0, "sort-orders": [{"order-id": 0, "fields": []}], "properties": props,
              "current-snapshot-id": sid, "snapshots": self.snapshots,
              "snapshot-log": [{"snapshot-id": s["snapshot-id"], "timestamp-ms": s["timestamp-ms"]} for s in self.snapshots],
              "metadata-log": self.metadata_log, "refs": {"main": {"snapshot-id": sid, "type": "branch"}}}
        mpath = f"{self.root}/metadata/v{self.version}.metadata.json"
        with open(mpath, "w") as fh: json.dump(md, fh, indent=1)
        if self.hint:
            with open(f"{self.root}/metadata/version-hint.text", "w") as fh: fh.write(str(self.version))
        self.last_metadata = mpath
        return sid, mpath
