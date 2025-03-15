# tg_ubot/app/main.py

import asyncio
import signal
import logging
import os
import base64
from telethon import TelegramClient

from app.config import settings
from app.logger import setup_logging
from app.utils import ensure_dir
from app.telegram.chat_info import get_all_chats_info
from app.telegram.state_manager import StateManager
from app.telegram.state import MessageCounter
from app.worker import TGUBotWorker

# mirco_services_data_management -> send_message, если нужно публиковать сообщения
from mirco_services_data_management.kafka_io import send_message

logger = logging.getLogger("main")

# >>> NEW CODE <<<: Функция, которая декодирует session из SESSION_FILE_BASE64 и кладёт в файл
def decode_session_file():
    session_b64 = os.getenv("SESSION_FILE_BASE64", "")
    session_path = settings.SESSION_FILE  # "userbot.session" по умолчанию
    if session_b64.strip():
        try:
            data = base64.b64decode(session_b64)
            with open(session_path, "wb") as f:
                f.write(data)
            logger.info(f"Decoded Telegram session into '{session_path}'")
        except Exception as e:
            logger.exception(f"Failed to decode SESSION_FILE_BASE64: {e}")
    else:
        logger.warning("SESSION_FILE_BASE64 is empty; no preloaded session will be used.")

async def run_tg_ubot():
    setup_logging()
    ensure_dir("/app/data")
    ensure_dir("/app/logs")

    decode_session_file()

    client = TelegramClient(settings.SESSION_FILE, settings.TELEGRAM_API_ID, settings.TELEGRAM_API_HASH)
    await client.start()

    # Если файл сессии устарел или невалидный — авторизации не будет
    if not await client.is_user_authorized():
        logger.error("Telegram client not authorized (session invalid or expired). Exiting.")
        return

    # Собираем информацию о доступных чатах/каналах
    chat_id_to_data = await get_all_chats_info(client)
    logger.info(f"[main] Discovered {len(chat_id_to_data)} chats/channels after exclusions.")

    # StateManager хранит информацию о бэкфиллах, пропущенных ID и т.п.
    state_mgr = StateManager("/app/data/state.json")

    # MessageCounter — простой счётчик обработанных сообщений.
    msg_counter = MessageCounter(client, threshold=100)

    async def message_callback(data: dict):
        """
        Колбэк, вызывающийся при получении нового/бэкфил/редактированного сообщения:
         1) отправить data в Kafka (topic = settings.UBOT_PRODUCE_TOPIC)
         2) обновить счётчик
        """
        topic = settings.UBOT_PRODUCE_TOPIC
        if worker.producer:
            await send_message(worker.producer, topic, data)
            await msg_counter.increment()
            message_id = data.get("message_id", "unknown")
            name_uname = data.get("name_uname", "unknown")
            month_part = data.get("month_part", "unknown")
            logger.debug(f"Message processed: id={message_id}, name_uname={name_uname}, month_part={month_part}")
        else:
            logger.warning("[message_callback] Producer not ready yet!")

    # Создаём основной worker
    worker = TGUBotWorker(
        config=settings,
        client=client,
        chat_id_to_data=chat_id_to_data,
        state_mgr=state_mgr,
        message_callback=message_callback
    )

    message_buffer = asyncio.Queue()
    userbot_active = asyncio.Event()
    userbot_active.set()  # включаем обработку новых сообщений

    # Регистрируем обработчики Telethon (пришли новые сообщения, отредактированные и т.п.)
    from app.telegram.handlers import register_unified_handler
    register_unified_handler(
        client=client,
        message_buffer=message_buffer,
        userbot_active=userbot_active,
        chat_id_to_data=chat_id_to_data,
        state_mgr=state_mgr
    )

    stop_event = asyncio.Event()

    def handle_signal(signum, frame):
        logger.warning(f"Signal {signum} received, stopping worker.")
        stop_event.set()
        worker.stop()

    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, handle_signal, s, None)
        except NotImplementedError:
            pass

    async def worker_main():
        start_task = asyncio.create_task(worker.start(), name="worker_start")
        await asyncio.sleep(1)
        await worker._after_baseworker_started()
        await start_task

    worker_task = asyncio.create_task(worker_main(), name="tg_ubot_worker")
    await stop_event.wait()

    logger.info("[main] Shutting down worker...")
    worker.stop()
    worker_task.cancel()
    await asyncio.gather(worker_task, return_exceptions=True)

    await client.disconnect()
    logger.info("tg_ubot service terminated.")


def main():
    asyncio.run(run_tg_ubot())


if __name__ == "__main__":
    main()
