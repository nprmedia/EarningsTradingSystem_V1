SHELL := /bin/bash
.DEFAULT_GOAL := help

help:
	@echo "Targets: bootstrap, dry, run, test, clean"
	@echo "Optional env: SESSION=amc|bmo  TICKERS=path/to.csv"

bootstrap:
	@python3 -m venv .venv || true
	@. .venv/bin/activate; pip install -U pip wheel || true
	@if [ -f requirements.txt ]; then . .venv/bin/activate; pip install -r requirements.txt; fi
	@if [ -f pyproject.toml ]; then . .venv/bin/activate; pip install -e .; fi

dry:
	@SESSION="$${SESSION:-amc}"; TICKERS="$${TICKERS:-tickers.csv}"; \
	DATE="$$(date -u +%Y%m%d)"; \
	echo "[DRY] DATE=$${DATE} SESSION=$${SESSION} TICKERS=$${TICKERS}"; \
	ETS_MODE=dry ets-run --session "$${SESSION}" --date "$${DATE}" --tickers "$${TICKERS}" --dry

run:
	@SESSION="$${SESSION:-amc}"; TICKERS="$${TICKERS:-tickers.csv}"; \
	DATE="$$(date -u +%Y%m%d)"; \
	echo "[LIVE] DATE=$${DATE} SESSION=$${SESSION} TICKERS=$${TICKERS}"; \
	ETS_MODE=live ets-run --session "$${SESSION}" --date "$${DATE}" --tickers "$${TICKERS}"

test:
	@PYTHONPATH=src pytest -q || true

clean:
	@rm -rf __pycache__ .pytest_cache *.pyc .venv
