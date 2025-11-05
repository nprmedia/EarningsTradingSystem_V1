import os


def load_env(dotenv_path: str = ".env") -> None:
    try:
        from dotenv import load_dotenv

        load_dotenv(dotenv_path)
    except Exception:
        pass  # ok if dotenv not installed


def require_env(key: str, message: str | None = None) -> str:
    val = os.getenv(key, "").strip()
    if not val:
        raise RuntimeError(message or f"Missing required environment variable: {key}")
    return val
