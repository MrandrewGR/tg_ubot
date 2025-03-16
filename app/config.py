import os
from typing import Optional, List
from pydantic import BaseSettings, Field
from mirco_services_data_management.config import BaseConfig


class TGUBotSettings(BaseSettings, BaseConfig):
    TELEGRAM_API_ID: int
    TELEGRAM_API_HASH: str

    UBOT_PRODUCE_TOPIC: str = os.getenv("KAFKA_UBOT_OUTPUT_TOPIC", "tg_ubot_output")
    KAFKA_GAP_SCAN_TOPIC: str = os.getenv("KAFKA_GAP_SCAN_TOPIC", "gap_scan_request")
    KAFKA_GAP_SCAN_RESPONSE_TOPIC: str = os.getenv("KAFKA_GAP_SCAN_RESPONSE_TOPIC", "gap_scan_response")

    EXCLUDED_CHAT_IDS: Optional[List[int]] = []
    EXCLUDED_USERNAMES: Optional[List[str]] = []

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

    # Поле с лог-уровнем; в переменной окружения ожидается LOG_LEVEL,
    # но внутри класса сохраняется как UBOT_LOG_LEVEL
    UBOT_LOG_LEVEL: str = Field("INFO", alias="LOG_LEVEL")

    # Новое поле: ограничение глубины backfill по дням (0 = без ограничения)
    BACKFILL_MAX_DAYS: int = 2

    # Новые параметры для Kafka consumer (настраиваются через переменные окружения)
    KAFKA_CONSUMER_HEARTBEAT_INTERVAL_MS: int = 3000
    KAFKA_CONSUMER_SESSION_TIMEOUT_MS: int = 10000

    class Config:
        env_file = "/app/env/tg_ubot.env"
        env_file_encoding = "utf-8"

    @property
    def LOG_LEVEL(self) -> str:
        return self.UBOT_LOG_LEVEL


settings = TGUBotSettings()
