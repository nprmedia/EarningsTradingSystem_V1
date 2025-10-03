#!/usr/bin/env bash
set -euo pipefail
DATE=$(date +%F)
ets-run --session amc --date "$DATE" --tickers tickers.csv
echo "Wrote out/${DATE//-/}_amc_{factors,scores,trades}.csv"
