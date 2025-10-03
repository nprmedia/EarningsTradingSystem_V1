run:
	ets-run --session amc --date $(DATE) --tickers tickers.csv
outcomes:
	ets-outcomes $(DATE) out/$(shell echo $(DATE) | tr -d -)_amc_trades.csv
