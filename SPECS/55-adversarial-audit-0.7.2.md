# Issue #55 — Adversarial audit of datashard 0.7.2 (trading-desk data lake)

Ticket: issuedb #55 (critical, tag: audit). Method: `mission-critical-audit` skill —
falsify, don't confirm; every finding reproduced live through the public API or
labelled SUSPECTED; probes persisted under `audit/evaluations/`.

## EARS SPEC

- The auditor shall read every module under `src/datashard` and build a falsifiable
  claim inventory from README, `docs/`, `SPECS/` and prior audit reports, treating every
  prior finding and "fixed" status as a claim to re-verify rather than a fact to inherit.
- The auditor shall reproduce each candidate defect live through datashard's public API
  (`create_table`/`load_table`/`Table`/`Transaction`/`garbage_collect`) or its storage
  backends against real counterparties (local filesystem, a moto S3 server, the configured
  OVH endpoint), and shall label every finding CONFIRMED (reproduced) or SUSPECTED
  (reasoned from code only).
- The auditor shall persist every reproduction as a probe under `audit/evaluations/` that
  prints PASS/FAIL plus observed evidence, with `audit/evaluations/run_all.sh` running the
  safe set and exiting non-zero on any FAIL; destructive or external probes shall refuse to
  run without an explicit opt-in variable (`AUDIT_ALLOW_EXTERNAL=1`).
- If a finding can cause data loss, a lost commit, wrong query results, or a silent no-op
  reported as success, then the auditor shall rank it above every performance, hygiene or
  documentation finding.
- When the audit completes, the auditor shall open one issuedb ticket per confirmed defect
  or defect cluster carrying an EARS bugfix spec and a remediation plan, and shall record a
  closing statement naming what was exercised, what was not, and what remains uncertain.
- The auditor shall not modify `src/` during the audit; fixes are planned in tickets and
  executed afterwards.

## Synthesis (collapsed — an audit adds no component)

- Technical problems: (1) falsification of correctness claims (ACID, OCC, GC safety, filter
  semantics) on a storage-backed metadata protocol; (2) quantifying performance behaviour
  (I/O count, metadata growth, scan cost) as a function of commit count.
- Solution domain: falsification-driven auditing (`mission-critical-audit` skill); S3
  counterparty = moto server (protocol-faithful, local) + the real OVH endpoint for
  provider-capability facts.
- Alternatives: live probe harness through the public API [CHOSEN] vs code-reading only
  [REJECTED: candidates, not findings] vs multi-agent fan-out [REJECTED: loses
  cross-subsystem context; operator did not ask for it].

## Probe convention

PASS = the claim holds (correct behaviour observed). FAIL = defect reproduced. After
remediation every probe must PASS; a probe that FAILs again reopens its finding.

## Outcome (2026-09-06)

Certification of 0.7.2 denied; 19 tickets (#56–#74) opened with EARS specs, all fixed and closed
the same day, released as 0.8.0. Probe harness: 21 of 22 probes failed on 0.7.2, all pass on 0.8.0.
Report: `../AUDIT_REPORT_3.md`.
