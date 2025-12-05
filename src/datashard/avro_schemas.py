# Avro schemas for Iceberg-like manifests

MANIFEST_ENTRY_SCHEMA = {
    "type": "record",
    "name": "manifest_entry",
    "fields": [
        {"name": "status", "type": "int"},
        {"name": "snapshot_id", "type": ["null", "long"], "default": None},
        {"name": "sequence_number", "type": ["null", "long"], "default": None},
        {"name": "file_sequence_number", "type": ["null", "long"], "default": None},
        {
            "name": "data_file",
            "type": {
                "type": "record",
                "name": "data_file",
                "fields": [
                    {"name": "file_path", "type": "string"},
                    {"name": "file_format", "type": "string"},
                    {
                        "name": "partition",
                        "type": {
                            "type": "record",
                            "name": "partition_data",
                            "fields": [
                                {"name": "values", "type": {"type": "map", "values": "string"}}
                            ]
                        }
                    },
                    {"name": "record_count", "type": "long"},
                    {"name": "file_size_in_bytes", "type": "long"},
                    {"name": "column_sizes", "type": ["null", {"type": "map", "values": "long"}], "default": None},
                    {"name": "value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": None},
                    {"name": "null_value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": None},
                    {"name": "lower_bounds", "type": ["null", {"type": "map", "values": "string"}], "default": None},
                    {"name": "upper_bounds", "type": ["null", {"type": "map", "values": "string"}], "default": None},
                    {"name": "checksum", "type": ["null", "string"], "default": None}
                ]
            }
        }
    ]
}

# Updated schema: content is an int in standard Iceberg (0=DATA, 1=POSITION DELETE, 2=EQUALITY DELETE)
# But here we just use 0 (DATA) and 1 (DELETES) based on our Enum.
MANIFEST_FILE_SCHEMA = {
    "type": "record",
    "name": "manifest_file",
    "fields": [
        {"name": "manifest_path", "type": "string"},
        {"name": "manifest_length", "type": "long"},
        {"name": "partition_spec_id", "type": "int"},
        # content is an integer in the schema
        {"name": "content", "type": "int"},
        {"name": "sequence_number", "type": ["null", "long"], "default": None},
        {"name": "min_sequence_number", "type": ["null", "long"], "default": None},
        {"name": "added_snapshot_id", "type": ["null", "long"], "default": None},
        {"name": "added_data_files_count", "type": "int"},
        {"name": "existing_data_files_count", "type": "int"},
        {"name": "deleted_data_files_count", "type": "int"},
        {"name": "partitions", "type": {"type": "array", "items": "string"}}
    ]
}
