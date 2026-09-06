# Iceberg v2 foreign-reader spike (#83, 0.10.0 design input)

`gen.py` hand-writes a minimal Iceberg v2 table (no datashard code). `run.py` reads the variants with
DuckDB `iceberg_scan` and pyiceberg `StaticTable`; `run2.py` adds S3 via a moto server, the pyiceberg
reverse-append and the decimal-filter check. Findings and decisions: `SPECS/83-iceberg-v2-spike.md`.

    .venv/bin/python audit/spike_iceberg_v2/run.py
    .venv/bin/python audit/spike_iceberg_v2/run2.py   # needs pyiceberg[sql-sqlite], moto[server]
