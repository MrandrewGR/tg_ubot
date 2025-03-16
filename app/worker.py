import asyncio
import logging

# BaseWorker из mirco_services_data_management
from mirco_services_data_management.base_worker import BaseWorker

from app.telegram.backfill import BackfillManager
from app.telegram.gaps import LocalGapsManager

logger = logging.getLogger("worker")


class TGUBotWorker(BaseWorker):
    """
    Основной класс-воркер для userbot-а:
      - Наследует BaseWorker для использования общей логики (например, Kafka producer),
        который всегда запускается.
      - Обработка входящих сообщений (Kafka consumer) становится опциональной.
      - Также запускаются фоновые задачи: backfill и поиск "дыр" (gap finder).
    """

    def __init__(self, config, client, chat_id_to_data, state_mgr, message_callback):
        super().__init__(config)
        self.client = client
        self.chat_id_to_data = chat_id_to_data
        self.state_mgr = state_mgr
        self.message_callback = message_callback
        self.stop_event = asyncio.Event()
        self.enable_kafka_consumer = config.ENABLE_KAFKA_CONSUMER

        # Бэкфилл
        self.backfill_manager = BackfillManager(
            client=self.client,
            state_mgr=self.state_mgr,
            message_callback=self.message_callback,
            chat_id_to_data=self.chat_id_to_data
        )
        # Локальное сканирование дыр
        self.gaps_manager = LocalGapsManager(
            state_mgr=self.state_mgr,
            client=self.client,
            chat_id_to_data=self.chat_id_to_data
        )

    async def start(self):
        # Всегда запускаем BaseWorker для инициализации продюсера и фоновых задач.
        await super().start()
        if not self.enable_kafka_consumer:
            logger.info("Kafka consumer disabled by configuration. Incoming messages will be ignored.")

    async def _after_baseworker_started(self):
        # После старта Kafka-процессов запускаем задачи бэкфилла и локального gap-сканирования
        asyncio.create_task(self._backfill_loop(), name="backfill_loop")
        asyncio.create_task(self._gap_finder_loop(), name="gap_finder_loop")

    async def _backfill_loop(self):
        logger.info("[TGUBotWorker] backfill_manager started.")
        await self.backfill_manager.run()
        logger.info("[TGUBotWorker] backfill_manager stopped.")

    async def _gap_finder_loop(self):
        """
        Периодически (примерно раз в 30 мин) вызывает LocalGapsManager, чтобы искать пропуски.
        """
        logger.info("[TGUBotWorker] local gap_finder started.")
        try:
            while not self.stop_event.is_set():
                for chat_id in self.chat_id_to_data:
                    await self.gaps_manager.find_and_fill_gaps_for_chat(chat_id)
                await asyncio.sleep(1800)
        except asyncio.CancelledError:
            logger.info("[TGUBotWorker] local gap_finder cancelled.")
        except Exception as e:
            logger.exception(f"[TGUBotWorker] local gap_finder error: {e}")

    async def shutdown(self):
        logger.info("[TGUBotWorker] shutdown() called.")
        self.stop_event.set()
        self.backfill_manager.stop()
        await super().shutdown()

    async def handle_message(self, message: dict):
        """
        Если BaseWorker получает сообщение из Kafka, то:
          - Если ENABLE_KAFKA_CONSUMER True – обрабатываем сообщение.
          - Если False – просто игнорируем.
        """
        if not self.enable_kafka_consumer:
            logger.debug(f"[TGUBotWorker.handle_message] Kafka consumer disabled; ignoring message: {message}")
            return

        logger.debug(f"[TGUBotWorker.handle_message] got message: {message}")
        # Реализуйте здесь необходимую логику обработки входящих сообщений,
        # если хотите использовать режим потребителя.
        # Пока оставляем pass.
        pass

    def stop(self):
        self.stop_event.set()
        super().stop()
