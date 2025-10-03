import logging, os
from datetime import datetime

def get_logger(name: str, logs_dir: str = "logs"):
    os.makedirs(logs_dir, exist_ok=True)
    log_path = os.path.join(logs_dir, f"{datetime.utcnow().strftime('%Y%m%d')}.log")
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fh = logging.FileHandler(log_path)
        ch = logging.StreamHandler()
        fmt = logging.Formatter("%(asctime)s %(levelname)s | %(name)s | %(message)s", "%Y-%m-%d %H:%M:%S")
        fh.setFormatter(fmt); ch.setFormatter(fmt)
        logger.addHandler(fh); logger.addHandler(ch)
    return logger
