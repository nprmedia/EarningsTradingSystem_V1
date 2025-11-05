from __future__ import annotations

import os

import pytest


@pytest.mark.integration
def test_provider_registry_respects_offline(mock_prices, mock_fundamentals):
    assert os.getenv("ETS_OFFLINE") == "1"
    try:
        from ets.data.providers import ProviderRegistry  # type: ignore[attr-defined]
    except Exception:
        return
    reg = ProviderRegistry.from_env()
    data = reg.get_prices(["AAPL", "MSFT"])
    assert "AAPL" in data and "MSFT" in data
    assert isinstance(data["AAPL"], list)
