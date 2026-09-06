# Issue #79 — 0.9.0: Transaction.append_arrow / Table.append_arrow - Arrow tables written without a dict round trip

Part of the uplift plan (0.9.0). 

EARS SPEC:
- The Transaction shall expose append_arrow(table: pyarrow.Table, partition_values=None) that validates the table against the persisted schema (unknown columns raise; required columns present and null-free; types castable to the table's Arrow schema) and writes it through DataFileManager._write_arrow_table as one data file with statistics and checksum, exactly like append_data.
- The Table shall expose append_arrow(table) as a one-transaction convenience returning True.
- An empty Arrow table shall queue nothing (no data file, no snapshot), matching append_data.
- Quantified: appending a 300k-row Arrow table shall take < 1.5x the time of pq.write_table alone on local storage.

Synthesis (localised): reuse validate_arrow_table_strict / _write_arrow_table (codebase pattern from #69); cast with table.cast(arrow_schema) so int64->long etc. are accepted [CHOSEN] vs strict schema equality [REJECTED: pandas/DuckDB often produce compatible-but-not-identical Arrow types].
