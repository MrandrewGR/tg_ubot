# tg_ubot/app/kafka/producer.py

import json
import logging
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from app.config import settings

logger = logging.getLogger("kafka_producer")

class KafkaMessageProducer:
    """
    Асинхронный KafkaProducer (aiokafka).
    """

    def __init__(self):
        self.producer = None

    async def initialize(self):
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=settings.KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            await self.producer.start()
            logger.info("AIOKafkaProducer started.")
        except Exception as e:
            logger.error(f"Error creating AIOKafkaProducer: {e}")
            raise

    async def send_message(self, topic: str, message: dict):
        if not self.producer:
            raise Exception("Kafka producer not initialized.")
        try:
            await self.producer.send_and_wait(topic, message)
            name_uname = message.get("name_uname", "Unknown")
            month_part = message.get("month_part", "N/A")
            logger.info(f"Sent message to {topic}. name_uname={name_uname}, month_part={month_part}.")
        except KafkaError as e:
            logger.error(f"Error sending message: {e}")
            raise

    async def close(self):
        if self.producer:
            await self.producer.stop()
            logger.info("AIOKafkaProducer closed.")
