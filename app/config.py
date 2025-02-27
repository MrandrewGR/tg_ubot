# tg_ubot/app/config.py

"""
Конфигурация tg_ubot,
наследуемся от BaseConfig вашей библиотеки mirco_services_data_management для Kafka/DB и пр.
Если не используете PostgreSQL/DB, можно упростить.
"""

import os
from pydantic import BaseSettings
from typing import Optional, List

# Импортируем BaseConfig (Kafka/DB) из библиотеки mirco_services_data_management,
# если она вам нужна. Иначе закомментируйте и используйте чистый Pydantic.
from mirco_services_data_management.config import BaseConfig


class TGUBotSettings(BaseSettings, BaseConfig):
    """
    Наследуем функционал из BaseConfig (mirco_services_data_management), в т.ч.:
     - KAFKA_BROKER
     - DB_HOST / DB_NAME / ...
     - POLL_INTERVAL_DB
    и т.д.

    Дополним собственными настройками для Telegram и задержек (день/ночь).
    """

    TELEGRAM_API_ID: int
    TELEGRAM_API_HASH: str

    # Kafka: топик для сообщений, которые генерирует наш юзербот
    UBOT_PRODUCE_TOPIC: str = os.getenv("KAFKA_UBOT_OUTPUT_TOPIC", "tg_ubot_output")

    # gap-scan
    KAFKA_GAP_SCAN_TOPIC: str = os.getenv("KAFKA_GAP_SCAN_TOPIC", "gap_scan_request")
    KAFKA_GAP_SCAN_RESPONSE_TOPIC: str = os.getenv("KAFKA_GAP_SCAN_RESPONSE_TOPIC", "gap_scan_response")

    # Исключаем чаты по ID/username
    EXCLUDED_CHAT_IDS: Optional[List[int]] = []
    EXCLUDED_USERNAMES: Optional[List[str]] = []

    # Настройки задержек (день/ночь) — для имитации "человеческого" поведения
    TRANSITION_START_TO_NIGHT: str = "20:00"
    TRANSITION_END_TO_NIGHT: str = "22:00"
    TRANSITION_START_TO_DAY: str = "06:00"
    TRANSITION_END_TO_DAY: str = "08:00"

    CHAT_DELAY_MIN_DAY: float = 1.0
    CHAT_DELAY_MAX_DAY: float = 3.0
    CHAT_DELAY_MIN_NIGHT: float = 2.0
    CHAT_DELAY_MAX_NIGHT: float = 6.0

    CHANNEL_DELAY_MIN_DAY: float = 5.0
    CHANNEL_DELAY_MAX_DAY: float = 10.0
    CHANNEL_DELAY_MIN_NIGHT: float = 10.0
    CHANNEL_DELAY_MAX_NIGHT: float = 20.0

    TELEGRAM_TARGET_IDS: Optional[List[int]] = []
    SESSION_FILE: str = os.getenv("SESSION_FILE", "userbot.session")

    # Пример: лог-уровень (если не берём из BaseConfig)
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    class Config:
        # Можно указать свой путь к .env, либо брать по умолчанию
        env_file = "/app/env/tg_ubot.env"
        env_file_encoding = "utf-8"


settings = TGUBotSettings()
