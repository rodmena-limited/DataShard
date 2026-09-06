import os, sys, shutil, subprocess, time, json, traceback
sys.path.insert(0, os.path.dirname(__file__))
from decimal import Decimal
import duckdb, pyarrow as pa, boto3
from gen import TableWriter, rows, arrow_table
from pyiceberg.table import StaticTable
from pyiceberg.expressions import EqualTo
from run import norm, expect  # noqa: re-running run.py's matrix is cheap enough (rebuilds tables)
BASE = os.path.join(os.path.dirname(__file__), "tables2"); shutil.rmtree(BASE, ignore_errors=True); os.makedirs(BASE)
con = duckdb.connect(); con.execute("INSTALL httpfs; LOAD iceberg; LOAD httpfs;")
d1 = rows(1, 3)
print("\n==== run2 ====")
# C2 raw: what does duckdb return with no field ids and no mapping?
w = TableWriter(f"{BASE}/C2", field_ids=False, name_mapping=False); sid1, m1 = w.append(d1)
print("C2 duckdb raw rows (no field ids, no mapping):", con.execute(f"SELECT * FROM iceberg_scan('{w.root}')").fetchall())

# pyiceberg decimal filter through the expression API
w = TableWriter(f"{BASE}/E2"); sid1, m1 = w.append(d1)
t = StaticTable.from_metadata(m1)
got = norm(t.scan(row_filter=EqualTo("px", Decimal("2.50000000"))).to_arrow())
print("pyiceberg EqualTo(px, Decimal) ->", "PASS" if got == [r for r in expect(d1) if r["id"] == 2] else f"MISMATCH {got}")

# ---- S3 via moto: write the table with s3:// URIs, read with duckdb httpfs + pyiceberg
port = 5601
proc = subprocess.Popen([sys.executable, "-m", "moto.server", "-p", str(port)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
try:
    for _ in range(50):
        try:
            import urllib.request; urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=1); break
        except Exception: time.sleep(0.2)
    os.environ.update(AWS_ACCESS_KEY_ID="testing", AWS_SECRET_ACCESS_KEY="testing", AWS_DEFAULT_REGION="us-east-1")
    s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{port}")
    s3.create_bucket(Bucket="lake")
    class S3Writer(TableWriter):
        def uri(self, rel): return f"s3://lake/tbl/{rel}"
        def location(self): return "s3://lake/tbl"
    w = S3Writer(f"{BASE}/S3local"); sid1, m1 = w.append(d1); sid2, m2 = w.append(rows(4, 3))
    for dp, dn, fn in os.walk(w.root):
        for f in fn:
            full = os.path.join(dp, f); key = "tbl/" + os.path.relpath(full, w.root)
            s3.upload_file(full, "lake", key)
    con.execute(f"""CREATE OR REPLACE SECRET moto (TYPE S3, KEY_ID 'testing', SECRET 'testing', REGION 'us-east-1',
                    ENDPOINT '127.0.0.1:{port}', URL_STYLE 'path', USE_SSL false)""")
    try:
        got = norm(con.execute("SELECT * FROM iceberg_scan('s3://lake/tbl')").to_arrow_table())
        print("S3 duckdb iceberg_scan(s3://) via httpfs ->", "PASS" if got == expect(d1, rows(4, 3)) else f"MISMATCH {got}")
    except Exception as e: print("S3 duckdb ERROR", type(e).__name__, str(e).splitlines()[0][:200])
    try:
        t = StaticTable.from_metadata("s3://lake/tbl/metadata/v2.metadata.json", properties={
            "s3.endpoint": f"http://127.0.0.1:{port}", "s3.access-key-id": "testing", "s3.secret-access-key": "testing", "s3.region": "us-east-1"})
        got = norm(t.scan().to_arrow())
        print("S3 pyiceberg StaticTable(s3://) ->", "PASS" if got == expect(d1, rows(4, 3)) else f"MISMATCH {got}")
    except Exception as e: print("S3 pyiceberg ERROR", type(e).__name__, str(e).splitlines()[0][:200]); traceback.print_exc()

    # ---- reverse direction: pyiceberg (SqlCatalog + register_table) appends to our hand-written S3 table, duckdb reads
    from pyiceberg.catalog.sql import SqlCatalog
    cat = SqlCatalog("spike", **{"uri": f"sqlite:///{BASE}/catalog.db", "warehouse": "s3://lake/wh",
        "s3.endpoint": f"http://127.0.0.1:{port}", "s3.access-key-id": "testing", "s3.secret-access-key": "testing", "s3.region": "us-east-1"})
    cat.create_namespace("ns")
    tbl = cat.register_table("ns.t", "s3://lake/tbl/metadata/v2.metadata.json")
    d3 = rows(7, 2)
    tbl.append(arrow_table(d3))
    print("pyiceberg appended; new metadata:", tbl.metadata_location)
    got = norm(con.execute(f"SELECT * FROM iceberg_scan('{tbl.metadata_location}')").to_arrow_table())
    print("reverse: duckdb reads pyiceberg-appended table ->", "PASS" if got == expect(d1, rows(4, 3), d3) else f"MISMATCH {got}")
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket="lake", Prefix="tbl/metadata/")["Contents"]]
    print("metadata objects after pyiceberg append:", sorted(k.rsplit('/',1)[1] for k in keys if k.endswith(".json")))
    md = json.loads(s3.get_object(Bucket="lake", Key=tbl.metadata_location.replace("s3://lake/", ""))["Body"].read())
    print("pyiceberg wrote snapshots:", len(md["snapshots"]), "last-sequence-number:", md["last-sequence-number"], "hint object present:", any(k.endswith("version-hint.text") and False for k in keys))
    hint = s3.get_object(Bucket="lake", Key="tbl/metadata/version-hint.text")["Body"].read().decode()
    print("version-hint after pyiceberg append (was it bumped?):", hint)
finally:
    proc.kill(); proc.wait()
