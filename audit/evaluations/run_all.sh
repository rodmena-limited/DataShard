#!/usr/bin/env bash
# Runs the SAFE probe set (local tmp dirs + a local moto server). External /
# destructive probes (probe_external_*) only run with AUDIT_ALLOW_EXTERNAL=1.
set -u
cd "$(dirname "$0")"
PY="${PYTHON:-../../.venv/bin/python}"
[ -x "$PY" ] || PY=python3
fail=0; pass=0; skipped=0
for p in probe_*.py; do
  case "$p" in
    probe_external_*) if [ "${AUDIT_ALLOW_EXTERNAL:-0}" != "1" ]; then echo "SKIP $p (set AUDIT_ALLOW_EXTERNAL=1)"; skipped=$((skipped+1)); continue; fi;;
  esac
  echo "=== $p ==="
  if "$PY" "$p"; then pass=$((pass+1)); else fail=$((fail+1)); fi
done
echo "----------------------------------------"
echo "probes passed: $pass  failed: $fail  skipped: $skipped"
[ "$fail" -eq 0 ]
