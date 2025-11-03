# src/ets/config/env_loader.py
from __future__ import annotations


def load_env(verbose: bool = False) -> None:
    """
    Load environment variables from a local .env if present.
    Safe in CI: does nothing if package/file missing.
    """
    try:
        from dotenv import load_dotenv  # provided by python-dotenv
    except Exception:
        return
    loaded = load_dotenv()
    if verbose:
        print(f"ENV LOADER: .env loaded={loaded}")
