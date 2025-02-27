# tg_ubot/app/utils.py

import os
import asyncio
import random
import logging
from zoneinfo import ZoneInfo
from datetime import datetime, time
from .config import settings

logger = logging.getLogger("utils")


def ensure_dir(path: str):
    """
    Убеждаемся, что каталог существует.
    """
    try:
        os.makedirs(path, exist_ok=True)
    except Exception as e:
        logger.exception(f"Could not create directory {path}: {e}")


def get_current_time_moscow():
    return datetime.now(ZoneInfo("Europe/Moscow"))


def is_night_time():
    """
    Простая логика: ночь, если >=22:00 или <06:00 по Москве.
    """
    current_time = get_current_time_moscow().time()
    return current_time >= time(22, 0) or current_time < time(6, 0)


async def human_like_delay(delay_min: float, delay_max: float):
    """
    Случайная пауза (сек) в заданном диапазоне, имитация "человеческих" задержек.
    """
    delay = random.uniform(delay_min, delay_max)
    logger.debug(f"[human_like_delay] Sleeping ~{delay:.1f}s.")
    await asyncio.sleep(delay)


def get_delay_settings(delay_type: str):
    """
    Возвращает (min_delay, max_delay) для чата или канала, учитывая день/ночь.
    """
    if delay_type == "chat":
        if is_night_time():
            return (settings.CHAT_DELAY_MIN_NIGHT, settings.CHAT_DELAY_MAX_NIGHT)
        else:
            return (settings.CHAT_DELAY_MIN_DAY, settings.CHAT_DELAY_MAX_DAY)
    else:
        # считаем, что это "канал"
        if is_night_time():
            return (settings.CHANNEL_DELAY_MIN_NIGHT, settings.CHANNEL_DELAY_MAX_NIGHT)
        else:
            return (settings.CHANNEL_DELAY_MIN_DAY, settings.CHANNEL_DELAY_MAX_DAY)
