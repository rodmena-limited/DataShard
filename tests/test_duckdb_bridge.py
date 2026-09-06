"""0.9.0: DuckDB analytics layer (#78) and Arrow ingestion (#79)."""
import os
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from datashard import Schema, create_table

duckdb = pytest.importorskip("duckdb")

SCHEMA = Schema(schema_id=1, fields=[
    {"id": 1, "name": "symbol", "type": "string", "required": True},
    {"id": 2, "name": "qty", "type": "long", "required": True},
    {"id": 3, "name": "px", "type": "decimal(18,8)", "required": False},
])


def _table(tmp_path):
    t = create_table(str(tmp_path / "t"), SCHEMA)
    t.append_records([{"symbol": "AAA", "qty": 10, "px": Decimal("1.5")},
                      {"symbol": "BBB", "qty": 5, "px": None}], SCHEMA)
    t.append_records([{"symbol": "AAA", "qty": 7, "px": Decimal("2.25")}], SCHEMA)
    return t


def test_to_arrow_matches_scan_and_keeps_schema_when_empty(tmp_path):
    t = _table(tmp_path)
    arrow = t.to_arrow()
    assert arrow.num_rows == 3 and arrow.column_names == ["symbol", "qty", "px"]
    assert sorted(arrow.to_pylist(), key=lambda r: (r["symbol"], r["qty"])) == sorted(t.scan(), key=lambda r: (r["symbol"], r["qty"]))
    first = t.snapshots()[0]["snapshot_id"]
    assert t.to_arrow(snapshot_id=first).num_rows == 2
    empty = create_table(str(tmp_path / "e"), SCHEMA).to_arrow(columns=["qty"])
    assert empty.num_rows == 0 and empty.schema.names == ["qty"] and empty.schema.field("qty").type == pa.int64()
    assert t.to_arrow(filter={"qty": (">", 100)}).num_rows == 0


def test_sql_aggregates_match(tmp_path):
    t = _table(tmp_path)
    out = t.sql("SELECT symbol, sum(qty) AS q FROM t GROUP BY symbol ORDER BY symbol")
    assert out.to_pylist() == [{"symbol": "AAA", "q": 17}, {"symbol": "BBB", "q": 5}]
    out = t.sql("SELECT count(*) AS n FROM trades", alias="trades", filter={"symbol": "AAA"})
    assert out.to_pylist() == [{"n": 2}]


def test_to_duckdb_registers_view_on_given_connection(tmp_path):
    t = _table(tmp_path)
    con = duckdb.connect()
    assert t.to_duckdb(con, view_name="v") is con
    assert con.execute("SELECT count(*) FROM v").fetchone() == (3,)
    con.close()


def test_parquet_paths_are_readable_by_duckdb_directly(tmp_path):
    t = _table(tmp_path)
    paths = t.parquet_paths()
    assert len(paths) == 2 and all(os.path.isabs(p) and os.path.exists(p) for p in paths)
    con = duckdb.connect()
    n = con.execute("SELECT count(*) FROM read_parquet($paths)", {"paths": paths}).fetchone()[0]
    assert n == 3


def test_duckdb_s3_secret_sql_shape_and_local_refusal(tmp_path, s3_env):
    t = create_table(s3_env.table_name(), SCHEMA)
    sql = t.duckdb_s3_secret_sql()
    assert sql.startswith("CREATE OR REPLACE SECRET datashard_s3 (TYPE S3")
    assert "URL_STYLE 'path'" in sql and "ENDPOINT '127.0.0.1:" in sql and "USE_SSL false" in sql
    assert "KEY_ID 'testing'" in sql and "SECRET 'testing'" in sql
    assert t.parquet_paths() == []
    t.append_records([{"symbol": "AAA", "qty": 1, "px": None}], SCHEMA)
    (uri,) = t.parquet_paths()
    assert uri.startswith(f"s3://{s3_env.bucket}/") and uri.endswith(".parquet")


def test_duckdb_s3_secret_sql_refused_for_local_tables(tmp_path):
    with pytest.raises(ValueError):
        _table(tmp_path).duckdb_s3_secret_sql()


def test_append_arrow_conforms_reorders_casts_and_fills_optional(tmp_path):
    t = create_table(str(tmp_path / "t"), SCHEMA)
    # int32 qty, columns out of order, optional px absent
    arrow = pa.table({"qty": pa.array([1, 2], pa.int32()), "symbol": ["X", "Y"]})
    assert t.append_arrow(arrow) is True
    got = t.to_arrow()
    assert got.column_names == ["symbol", "qty", "px"]
    assert got.column("qty").type == pa.int64() and got.column("px").null_count == 2
    (f,) = t.parquet_paths()
    assert pq.read_schema(f).names == ["symbol", "qty", "px"]


def test_append_arrow_rejects_unknown_columns_nulls_in_required_and_bad_types(tmp_path):
    t = create_table(str(tmp_path / "t"), SCHEMA)
    with pytest.raises(ValueError, match="not in the table schema"):
        t.append_arrow(pa.table({"symbol": ["X"], "qty": [1], "extra": [1]}))
    with pytest.raises(ValueError, match="null"):
        t.append_arrow(pa.table({"symbol": ["X"], "qty": pa.array([None], pa.int64())}))
    with pytest.raises(ValueError, match="not compatible"):
        t.append_arrow(pa.table({"symbol": ["X"], "qty": ["not a number"]}))
    assert t.append_arrow(pa.table({"symbol": pa.array([], pa.string()), "qty": pa.array([], pa.int64())})) is True
    assert len(t.snapshots()) == 0  # empty append queued nothing


def test_duckdb_result_round_trips_into_the_table(tmp_path):
    t = _table(tmp_path)
    con = duckdb.connect()
    derived = con.execute("SELECT 'ZZZ' AS symbol, 42::BIGINT AS qty, 3.5::DECIMAL(18,8) AS px").fetch_arrow_table()
    t.append_arrow(derived)
    assert t.sql("SELECT qty FROM t WHERE symbol = 'ZZZ'").to_pylist() == [{"qty": 42}]
