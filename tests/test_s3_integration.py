"""
Integration tests for the S3-compatible storage backend.

They run against a moto_server started by the `s3_env` fixture, or against the
provider configured through DATASHARD_S3_* (AWS S3, MinIO, OVH, ...). Every
table they create is deleted on teardown (#65).
"""
import pytest

from datashard import Schema, create_table, load_table

SCHEMA = Schema(
    schema_id=1,
    fields=[
        {"id": 1, "name": "id", "type": "long", "required": True},
        {"id": 2, "name": "message", "type": "string", "required": False},
        {"id": 3, "name": "count", "type": "long", "required": False},
    ],
)


def test_s3_storage_create_table(s3_env):
    name = s3_env.table_name()
    table = create_table(name, SCHEMA)
    assert type(table.storage).__name__ == "S3StorageBackend"
    assert table.created
    assert table.current_snapshot() is None  # initialised, empty
    # The hint and the v0 metadata are real objects under the table prefix.
    assert table.storage.exists("metadata.version-hint.text")
    assert any(k.endswith(".metadata.json") for k in s3_env.keys_under(name + "/metadata/"))
    assert load_table(name).row_count() == 0


def test_s3_storage_write_and_read(s3_env):
    name = s3_env.table_name()
    table = create_table(name, SCHEMA)
    records = [
        {"id": 1, "message": "Hello S3", "count": 100},
        {"id": 2, "message": "MinIO test", "count": 200},
        {"id": 3, "message": "DataShard rocks", "count": 300},
    ]
    with table.transaction_manager.begin_transaction() as txn:
        txn.append_data(records, SCHEMA)

    loaded = load_table(name)
    assert len(loaded.snapshots()) == 1
    assert loaded.row_count() == 3
    assert {r["id"]: r["count"] for r in loaded.scan()} == {1: 100, 2: 200, 3: 300}
    assert loaded.scan(filter={"count": (">", 150)}, columns=["id"]) == [{"id": 2}, {"id": 3}]
    for df in loaded._get_all_data_files():
        assert loaded.storage.exists(df.file_path.lstrip("/"))


def test_s3_multiple_transactions(s3_env):
    name = s3_env.table_name()
    schema = Schema(
        schema_id=1,
        fields=[
            {"id": 1, "name": "batch_id", "type": "long", "required": True},
            {"id": 2, "name": "item_id", "type": "long", "required": True},
            {"id": 3, "name": "data", "type": "string", "required": False},
        ],
    )
    create_table(name, schema)
    for batch in range(3):
        loaded = load_table(name)  # a fresh reader sees the cumulative snapshots
        with loaded.transaction_manager.begin_transaction() as txn:
            txn.append_data(
                [{"batch_id": batch, "item_id": i, "data": f"batch_{batch}_item_{i}"} for i in range(5)],
                schema,
            )
    loaded = load_table(name)
    assert len(loaded.snapshots()) == 3
    assert loaded.row_count() == 15
    assert len(loaded.scan(filter={"batch_id": 1})) == 5


def test_s3_conditional_writes_are_autodetected(s3_env):
    table = create_table(s3_env.table_name(), SCHEMA)
    assert table.storage.supports_cas, "moto / every supported provider honours If-None-Match"


def test_s3_unsafe_polling_lock_is_refused_without_opt_in(s3_env, monkeypatch):
    monkeypatch.setenv("DATASHARD_S3_USE_CONDITIONAL_WRITES", "false")
    with pytest.raises(RuntimeError, match="LOST"):
        create_table(s3_env.table_name(), SCHEMA)


def test_s3_unsafe_polling_lock_allowed_with_explicit_opt_in(s3_env, monkeypatch):
    monkeypatch.setenv("DATASHARD_S3_USE_CONDITIONAL_WRITES", "false")
    monkeypatch.setenv("DATASHARD_S3_ALLOW_UNSAFE_LOCK", "1")
    table = create_table(s3_env.table_name(), SCHEMA)
    assert not table.storage.supports_cas
    table.append_records([{"id": 1, "message": "m", "count": 1}], SCHEMA)
    assert load_table(table.table_path).row_count() == 1


def test_s3_garbage_collect_keeps_live_data(s3_env):
    name = s3_env.table_name()
    table = create_table(name, SCHEMA)
    for i in range(3):
        table.append_records([{"id": i, "message": "m", "count": i}], SCHEMA)
    stats = table.garbage_collect(grace_period_ms=0, allow_short_grace=True)
    assert stats == {"data_files": 0, "manifest_files": 0, "manifest_lists": 0}
    assert load_table(name).row_count() == 3
