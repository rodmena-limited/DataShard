import os
import time

from datashard import Schema, create_table


def test_garbage_collection_local(tmp_path):
    table_path = str(tmp_path / "gc_table")

    schema = Schema(
        schema_id=1,
        fields=[{"id": 1, "name": "id", "type": "int", "required": True}]
    )

    table = create_table(table_path, schema)

    # Create 3 snapshots
    for i in range(3):
        table.append_records([{"id": i}], schema)

    # Verify we have 3 snapshots
    assert len(table.snapshots()) == 3

    # Manually create a dummy file in data/ directory (simulate orphan)
    orphan_file = os.path.join(table_path, "data", "orphan.parquet")
    os.makedirs(os.path.dirname(orphan_file), exist_ok=True)
    with open(orphan_file, "wb") as f:
        f.write(b"fake parquet content")

    # Set mtime of orphan file to 2 hours ago
    two_hours_ago = time.time() - 7200
    os.utime(orphan_file, (two_hours_ago, two_hours_ago))

    # Run GC with 1 hour grace period
    # This should delete the orphan file
    stats = table.garbage_collect(grace_period_ms=3600000)

    assert stats["data_files"] == 1
    assert not os.path.exists(orphan_file)

    # Verify valid files are still there
    # Scan should still work
    records = table.scan()
    assert len(records) == 3

def test_garbage_collection_grace_period(tmp_path):
    table_path = str(tmp_path / "gc_table_grace")

    schema = Schema(
        schema_id=1,
        fields=[{"id": 1, "name": "id", "type": "int", "required": True}]
    )

    table = create_table(table_path, schema)

    # Manually create a dummy file in data/ directory
    orphan_file = os.path.join(table_path, "data", "fresh_orphan.parquet")
    os.makedirs(os.path.dirname(orphan_file), exist_ok=True)
    with open(orphan_file, "wb") as f:
        f.write(b"fake parquet content")

    # File is fresh (created just now)

    # Run GC with 1 hour grace period
    # This should NOT delete the fresh orphan file
    stats = table.garbage_collect(grace_period_ms=3600000)

    assert stats["data_files"] == 0
    assert os.path.exists(orphan_file)
