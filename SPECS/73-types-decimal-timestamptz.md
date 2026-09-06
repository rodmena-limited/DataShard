# Issue #73 — [P3 functional gap] No decimal(P,S) and no timestamptz types: prices/quantities can only be stored as float/double

Found by adversarial audit #55 (2026-09-06). Priority: low. Probe convention: PASS = claim holds, FAIL = defect reproduced.

EARS SPEC:
- Schema shall accept 'decimal(P,S)' mapped to pa.decimal128(P,S) with a typed bounds encoding ('dec' tag, string repr) so pruning works on decimal columns.
- Schema shall accept 'timestamptz' mapped to pa.timestamp('us', tz='UTC'); tz-aware datetimes written into 'timestamp' shall keep the current UTC normalisation and the behaviour shall be documented.
- Verification: audit/evaluations/probe_types_decimal_timestamptz.py shall PASS.

EVIDENCE: Schema rejects decimal(18,8), decimal and timestamptz (data_structures.py:41-45 valid_primitive_types); tz-aware datetimes are normalised to naive UTC correctly (12:00+02:00 -> 10:00).
