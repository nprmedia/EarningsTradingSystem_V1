import logging
import os

logger = logging.getLogger(__name__)


def ensure_dir(path: str) -> None:
    """Ensure that a directory exists (mkdir -p equivalent)."""
    if not path:
        logger.warning("ensure_dir called with empty path.")
        return
    os.makedirs(path, exist_ok=True)
    logger.debug(f"[utils] Ensured directory exists: {path}")
