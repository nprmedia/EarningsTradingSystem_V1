#!/usr/bin/env bash
set -euo pipefail
date_in="${1:-$(date -u +%Y%m%d)}"
session="${2:-amc}"
outdir="out"

# Normalize inputs
date_compact="${date_in//-/}"
date_dashed="$(printf '%s-%s-%s' "${date_compact:0:4}" "${date_compact:4:2}" "${date_compact:6:2}")"

files_compact=(
  "${outdir}/${date_compact}_${session}_factors.csv"
  "${outdir}/${date_compact}_${session}_scores.csv"
  "${outdir}/${date_compact}_${session}_trades.csv"
  "${outdir}/${date_compact}_${session}_pulls.csv"
  "${outdir}/${date_compact}_${session}_telemetry.csv"
)
files_dashed=(
  "${outdir}/${date_dashed}_${session}_factors.csv"
  "${outdir}/${date_dashed}_${session}_scores.csv"
  "${outdir}/${date_dashed}_${session}_trades.csv"
  "${outdir}/${date_dashed}_${session}_pulls.csv"
  "${outdir}/${date_dashed}_${session}_telemetry.csv"
)

check_set () {
  local miss=0
  for f in "$@"; do
    if [ -s "$f" ]; then echo "[OK] $f"; else echo "[MISS] $f"; miss=1; fi
  done
  return $miss
}

echo "[CHECK] compact names:"
if check_set "${files_compact[@]}"; then
  echo "[PASS] compact set found."
  exit 0
fi

echo "[CHECK] dashed names:"
if check_set "${files_dashed[@]}"; then
  echo "[PASS] dashed set found."
  exit 0
fi

echo "[FAIL] Neither filename style is fully present."
exit 1
