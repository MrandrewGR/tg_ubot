# tg_ubot/app/logger.py

import logging
from .config import settings

def setup_logging():
    # Раньше было: level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
    # Теперь:
    level = getattr(logging, settings.LOG_LEVEL_.upper(), logging.INFO)

    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)s [%(name)s]: %(message)s',
        handlers=[logging.StreamHandler()]
    )
    logger = logging.getLogger("tg_ubot")
    logger.info("Logging configured for tg_ubot.")
    return logger