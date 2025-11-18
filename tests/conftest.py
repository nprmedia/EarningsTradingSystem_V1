from __future__ import annotations

import json
import os
import pathlib
import random
import socket

import pytest


@pytest.fixture(autouse=True)
def _deterministic_env(monkeypatch):
    monkeypatch.setenv("PYTHONHASHSEED", "0")
    monkeypatch.setenv("ETS_STRICT_DETERMINISM", "1")
    monkeypatch.setenv("ETS_OFFLINE", os.getenv("ETS_OFFLINE", "1"))
    random.seed(int(os.getenv("ETS_SEED", "1337")))


@pytest.fixture(autouse=True)
def _disable_network(monkeypatch):
    def guard(*a, **k):
        raise RuntimeError("Network disabled in tests.")

    monkeypatch.setattr(socket, "create_connection", guard)
    monkeypatch.setattr(socket.socket, "connect", guard, raising=False)


@pytest.fixture(scope="session")
def fixtures_dir() -> pathlib.Path:
    return pathlib.Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def mock_prices(fixtures_dir: pathlib.Path) -> dict:
    return json.loads((fixtures_dir / "mock_prices.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def mock_fundamentals(fixtures_dir: pathlib.Path) -> dict:
    return json.loads((fixtures_dir / "mock_fundamentals.json").read_text(encoding="utf-8"))


@pytest.fixture
def tmp_out(tmp_path: pathlib.Path) -> pathlib.Path:
    out = tmp_path / "out"
    out.mkdir(parents=True, exist_ok=True)
    return out
