# app/logger.py

import logging
from .config import settings

def setup_logging():
    # Берём поле UBOT_LOG_LEVEL, которое мы только что переименовали
    level_name = settings.UBOT_LOG_LEVEL.upper()
    level = getattr(logging, level_name, logging.INFO)

    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)s [%(name)s]: %(message)s',
        handlers=[logging.StreamHandler()]
    )
    logger = logging.getLogger("tg_ubot")
    logger.info(f"Logging configured for tg_ubot at level={level_name}")
    return logger
