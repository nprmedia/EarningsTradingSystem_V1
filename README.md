# ETS (Pre-earnings Screener)
## Install
pip install -e .
## Run
ets-run --session amc --date YYYY-MM-DD --tickers tickers.csv
## Outcomes (T+1)
ets-outcomes YYYY-MM-DD out/YYYYMMDD_amc_trades.csv
