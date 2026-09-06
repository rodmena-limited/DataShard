import os, shutil, sys, traceback, json
sys.path.insert(0, os.path.dirname(__file__))
import duckdb, pyarrow as pa, pyarrow.compute as pc
from gen import TableWriter, rows, arrow_table
from pyiceberg.table import StaticTable

BASE = os.path.join(os.path.dirname(__file__), "tables"); shutil.rmtree(BASE, ignore_errors=True); os.makedirs(BASE)
con = duckdb.connect(); con.execute("LOAD iceberg;")
results = []

def norm(t: pa.Table):
    t = t.select(["id", "sym", "px", "ts"]).cast(arrow_table(rows()).schema.remove_metadata())
    return t.sort_by("id").to_pylist()

def expect(*datas):
    return sorted([dict(zip(d.keys(), vals)) for d in datas for vals in zip(*d.values())], key=lambda r: r["id"])

def duck(sql):
    return con.execute(sql).to_arrow_table()

def check(name, fn, want):
    try:
        got = fn(); ok = got == want
        results.append((name, "PASS" if ok else "MISMATCH", "" if ok else f"got {len(got)} rows: {got[:2]}..."))
    except Exception as e:
        results.append((name, "ERROR", f"{type(e).__name__}: {str(e).splitlines()[0][:160]}"))

def py_read(meta, snapshot_id=None, row_filter=None):
    t = StaticTable.from_metadata(meta)
    sc = t.scan(snapshot_id=snapshot_id) if snapshot_id else t.scan()
    if row_filter: sc = sc.filter(row_filter)
    return norm(sc.to_arrow())

# ---------- A: baseline, file:// URIs, field ids, hint, two appends
w = TableWriter(f"{BASE}/A"); d1, d2 = rows(1, 3), rows(4, 3)
sid1, m1 = w.append(d1); sid2, m2 = w.append(d2)
check("A duckdb iceberg_scan(root via hint)", lambda: norm(duck(f"SELECT * FROM iceberg_scan('{w.root}')")), expect(d1, d2))
check("A duckdb iceberg_scan(metadata.json path)", lambda: norm(duck(f"SELECT * FROM iceberg_scan('{m2}')")), expect(d1, d2))
check("A duckdb snapshot_from_id (time travel)", lambda: norm(duck(f"SELECT * FROM iceberg_scan('{w.root}', snapshot_from_id={sid1})")), expect(d1))
check("A duckdb filter id>=4", lambda: norm(duck(f"SELECT * FROM iceberg_scan('{w.root}') WHERE id >= 4")), expect(d2))
check("A pyiceberg StaticTable.from_metadata", lambda: py_read(m2), expect(d1, d2))
check("A pyiceberg time travel", lambda: py_read(m2, snapshot_id=sid1), expect(d1))
check("A pyiceberg filter id>=4", lambda: py_read(m2, row_filter="id >= 4"), expect(d2))
check("A pyiceberg from root dir (hint)", lambda: py_read(w.root), expect(d1, d2))

# ---------- B: stale hint (v1 while v2 exists): does the reader trust it?
w = TableWriter(f"{BASE}/B"); sid1, m1 = w.append(d1); w.hint = False; sid2, m2 = w.append(d2)
check("B duckdb stale hint -> sees only v1 (lag)", lambda: norm(duck(f"SELECT * FROM iceberg_scan('{w.root}')")), expect(d1))
w = TableWriter(f"{BASE}/B2", hint=False); w.append(d1)
check("B2 duckdb no hint at all (root)", lambda: norm(duck(f"SELECT * FROM iceberg_scan('{w.root}')")), expect(d1))
check("B2 duckdb no hint, version='1'", lambda: norm(duck(f"SELECT * FROM iceberg_scan('{w.root}', version='1')")), expect(d1))

# ---------- C: parquet WITHOUT field ids + schema.name-mapping.default
w = TableWriter(f"{BASE}/C", field_ids=False, name_mapping=True); sid1, m1 = w.append(d1)
check("C duckdb name-mapping (no parquet field ids)", lambda: norm(duck(f"SELECT * FROM iceberg_scan('{w.root}')")), expect(d1))
check("C pyiceberg name-mapping", lambda: py_read(m1), expect(d1))
w = TableWriter(f"{BASE}/C2", field_ids=False, name_mapping=False); sid1, m1 = w.append(d1)
check("C2 duckdb no field ids, no mapping", lambda: norm(duck(f"SELECT * FROM iceberg_scan('{w.root}')")), expect(d1))
check("C2 pyiceberg no field ids, no mapping", lambda: py_read(m1), expect(d1))

# ---------- D: path styles
for style in ("abs", "rel", "slashrel"):
    w = TableWriter(f"{BASE}/D_{style}", uri_style=style, location_style="abs" if style == "abs" else "file"); sid1, m1 = w.append(d1)
    check(f"D duckdb paths={style}", lambda w=w: norm(duck(f"SELECT * FROM iceberg_scan('{w.root}')")), expect(d1))
    check(f"D duckdb paths={style} allow_moved_paths", lambda w=w: norm(duck(f"SELECT * FROM iceberg_scan('{w.root}', allow_moved_paths=true)")), expect(d1))
    check(f"D pyiceberg paths={style}", lambda m1=m1: py_read(m1), expect(d1))
# moved table: write with file:// URIs then move the directory
w = TableWriter(f"{BASE}/D_moved_src"); sid1, m1 = w.append(d1); shutil.move(w.root, f"{BASE}/D_moved"); moved = f"{BASE}/D_moved"
check("D duckdb moved table (absolute URIs stale)", lambda: norm(duck(f"SELECT * FROM iceberg_scan('{moved}')")), expect(d1))
check("D duckdb moved table allow_moved_paths", lambda: norm(duck(f"SELECT * FROM iceberg_scan('{moved}', allow_moved_paths=true)")), expect(d1))

# ---------- E: are bounds consumed? fake bounds claim id in [1000,2000]; a bounds-aware reader wrongly prunes on id=2
w = TableWriter(f"{BASE}/E", fake_bounds=True); sid1, m1 = w.append(d1)
check("E duckdb WHERE id=2 with FAKE bounds (0 rows => bounds used)", lambda: norm(duck(f"SELECT * FROM iceberg_scan('{w.root}') WHERE id = 2")), [])
check("E pyiceberg id=2 with FAKE bounds (0 rows => bounds used)", lambda: py_read(m1, row_filter="id == 2"), [])
check("E duckdb WHERE id=1500 FAKE bounds (reads file, 0 rows)", lambda: norm(duck(f"SELECT * FROM iceberg_scan('{w.root}') WHERE id = 1500")), [])
w = TableWriter(f"{BASE}/E2"); sid1, m1 = w.append(d1)
check("E2 duckdb WHERE px = 2.5 real decimal bounds", lambda: norm(duck(f"SELECT * FROM iceberg_scan('{w.root}') WHERE px = 2.5")), [r for r in expect(d1) if r["id"] == 2])
check("E2 pyiceberg px == 2.5 real decimal bounds", lambda: py_read(m1, row_filter="px == 2.5"), [r for r in expect(d1) if r["id"] == 2])
check("E2 duckdb ts filter real ts bounds", lambda: norm(duck(f"SELECT * FROM iceberg_scan('{w.root}') WHERE ts >= TIMESTAMPTZ '2026-09-06 12:02:00+00'")), [r for r in expect(d1) if r["id"] >= 2])
check("E2 duckdb sym filter", lambda: norm(duck(f"SELECT * FROM iceberg_scan('{w.root}') WHERE sym = 'S2'")), [r for r in expect(d1) if r["id"] == 2])

# ---------- F: positional deletes (content=1)
w = TableWriter(f"{BASE}/F"); sid1, m1 = w.append(d1)
data_rel = "data/" + [f for f in os.listdir(f"{w.root}/data")][0]
sid2, m2 = w.positional_delete(data_rel, [1])  # delete row at position 1 -> id 2
check("F duckdb positional delete applied", lambda: norm(duck(f"SELECT * FROM iceberg_scan('{w.root}')")), [r for r in expect(d1) if r["id"] != 2])
check("F pyiceberg positional delete applied", lambda: py_read(m2), [r for r in expect(d1) if r["id"] != 2])

# ---------- G: minimal manifest (no stats), v1-style count names
w = TableWriter(f"{BASE}/G", minimal_manifest=True); sid1, m1 = w.append(d1)
check("G duckdb minimal manifest (no stats)", lambda: norm(duck(f"SELECT * FROM iceberg_scan('{w.root}')")), expect(d1))
check("G pyiceberg minimal manifest", lambda: py_read(m1), expect(d1))
w = TableWriter(f"{BASE}/G2", v1_names=True); sid1, m1 = w.append(d1)
check("G2 duckdb v1 count names in v2 list", lambda: norm(duck(f"SELECT * FROM iceberg_scan('{w.root}')")), expect(d1))
check("G2 pyiceberg v1 count names in v2 list", lambda: py_read(m1), expect(d1))

# ---------- H: metadata introspection helpers DuckDB exposes
try:
    w = TableWriter(f"{BASE}/H"); w.append(d1)
    snaps = con.execute(f"SELECT snapshot_id, sequence_number FROM iceberg_snapshots('{w.root}')").fetchall()
    results.append(("H duckdb iceberg_snapshots()", "PASS" if len(snaps) == 1 else "MISMATCH", str(snaps)))
    md = con.execute(f"SELECT manifest_path, status, content, record_count FROM iceberg_metadata('{w.root}')").fetchall()
    results.append(("H duckdb iceberg_metadata()", "PASS" if len(md) == 1 else "MISMATCH", str(md)[:150]))
except Exception as e:
    results.append(("H duckdb introspection", "ERROR", f"{type(e).__name__}: {str(e).splitlines()[0][:160]}"))

ext = con.execute("select extension_version from duckdb_extensions() where extension_name='iceberg'").fetchone()[0]
print(f"duckdb {duckdb.__version__} iceberg ext {ext}; pyiceberg {__import__('pyiceberg').__version__}")
for name, st, info in results:
    print(f"{st:8} {name:60} {info}")
