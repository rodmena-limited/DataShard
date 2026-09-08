# #95 — Five small defects in the 0.10.x releases (0.10.3)

Found by sweeping the 0.10.x releases after the production review (#94) was shipped.

## Requirements (EARS)

1. The README's data-type list **shall** state that `uuid` and `fixed` are refused for NEW tables
   (since 0.10.0) and name the replacements, so it cannot contradict the error a caller gets.
2. The S3 storage doc's layout diagram **shall** show the 0.10 on-disk layout, not the pre-0.10 one.
3. Every class the API reference documents **shall** be importable from the `datashard` package, so
   no section renders empty.
4. The CLI **shall** expose `datashard verify <table> [--deep] [--limit N] [--snapshot-id ID]`,
   printing the report as JSON and exiting non-zero when the table is not readable. It **shall**
   report rather than raise for a missing or un-migrated table.
5. The CLI **shall** accept `--version`.

## Evidence

- README line 669 listed `uuid, fixed` among the supported types while `create_table` refuses both.
- `docs/s3_storage.rst` showed `v0.metadata.json` (versions start at v1), a `metadata/manifests/`
  subdirectory and `manifest_*.avro` names — none of which have existed since 0.10.0 — plus a
  malformed duplicate `metadata/` entry left by an earlier edit.
- A sphinx build emitted `failed to import X from module 'datashard'` for seven names, so those
  API sections rendered empty.
- `datashard verify` was an invalid choice; `datashard --version` was an unrecognised argument.

**Correction to the assessment that opened this ticket:** `Table` itself was never missing from the
API reference — `api/iceberg.rst` autoclasses `datashard.Table` with `:members:`, and `Table.verify`
renders. The defect was the seven unimportable names.

**Found while fixing #4:** `verify()`'s description overstated its default mode. A byte flipped in a
file's leading magic leaves every row readable and correct, so reporting the table healthy is the
right answer; only `deep=True` claims every byte is as written. Both are now stated precisely, and
the boundary is pinned by a test rather than left to be "fixed" later into a slow full-file hash.

## Synthesis (localised)

- **[CHOSEN]** Export the seven names from the package: the API reference is the contract, and a
  blank section is worse than an absent one because the page still promises the class is documented.
- [REJECTED] Delete the failing autoclass directives: hides the gap rather than closing it.

## Verification

`tests/test_operational_cost_and_health.py`: the CLI returns 0 for a healthy table under every flag
combination, 1 for a missing table, 1 for a corrupted one and 1 for an un-migrated one, and
`--version` exits 0; a byte flip outside any page is reported healthy by the default mode, with the
rows still correct, and unhealthy by `deep`; every class the API reference documents is importable
and present in `__all__`. The docs build emits zero autodoc import failures (was seven).

## Outcome

**Shipped in 0.10.3 (2026-09-08):** https://pypi.org/project/datashard/0.10.3/ · tag v0.10.3.

Exercised: 273 unit tests (4 new); 35/35 probes; a docs build with **zero** autodoc import failures
(was seven); and all five fixes re-checked against the wheel installed back from PyPI - the served
long_description no longer lists `uuid`/`fixed` and explains the refusal, all seven classes import,
`datashard --version` prints, and `datashard verify` exits 0 on a healthy table, 1 on a missing one
and 1 on a corrupted one.

Not exercised: the rendered readthedocs pages (the docs build was local; publication follows the
push), and the CLI against an S3-backed table.
