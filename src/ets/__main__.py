# ruff: noqa: E402
"""
CLI entrypoint for `python -m ets`.
Ensures deterministic env + config are applied before any imports.
"""

from ets.config.env_loader import load_env
from ets.main import main

# Apply environment constraints (deterministic mode) before execution.
load_env(verbose=False)


if __name__ == "__main__":
    main()
