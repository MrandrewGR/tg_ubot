import logging
import asyncio
from telethon import events
from telethon.tl.types import Message

from app.config import settings
from app.utils import human_like_delay, get_delay_settings
from app.process_messages import serialize_message
# Replaced import: мы теперь используем upsert
from mirco_services_data_management.db import ensure_partitioned_parent_table, upsert_partitioned_record

logger = logging.getLogger("unified_handler")


def register_unified_handler(
    client,
    message_buffer: asyncio.Queue,
    userbot_active: asyncio.Event,
    chat_id_to_data: dict,
    state_mgr=None
):
    """
    Регистрирует обработчики для новых и отредактированных сообщений,
    ограниченные чатами из `target_ids`.
    """
    target_ids = list(chat_id_to_data.keys())
    logger.info(f"Registering unified_handler for chats: {target_ids}")

    @client.on(events.NewMessage(chats=target_ids))
    async def on_new_message(event):
        if state_mgr is not None:
            state_mgr.record_new_message()
        if not userbot_active.is_set():
            return
        await process_message_event(event, "new_message", message_buffer, chat_id_to_data)

    @client.on(events.MessageEdited(chats=target_ids))
    async def on_edited_message(event):
        if state_mgr is not None:
            state_mgr.record_new_message()
        if not userbot_active.is_set():
            return
        await process_message_event(event, "edited_message", message_buffer, chat_id_to_data)


async def process_message_event(event, event_type, message_buffer, chat_id_to_data):
    """
    Обрабатывает событие нового/отредактированного сообщения:
      1) Вставляет задержку для имитации "человеческого" поведения,
      2) Сериализует сообщение (включая данные о реакциях),
      3) Помещает данные в очередь (для последующей отправки в Kafka),
      4) Выполняет upsert в базу данных.
    """
    try:
        msg: Message = event.message
        chat_info = chat_id_to_data.get(msg.chat_id)
        if not chat_info:
            logger.warning(f"No chat_info for chat_id={msg.chat_id}, skipping.")
            return

        # Имитация случайной задержки
        dmin, dmax = get_delay_settings("chat")
        await human_like_delay(dmin, dmax)

        # Сериализация
        data = serialize_message(msg, event_type, chat_info)
        if not data:
            return

        # Помещаем данные в очередь для Kafka
        topic = settings.UBOT_PRODUCE_TOPIC
        await message_buffer.put((topic, data))

        # Формирование имени таблицы (например, "messages_<chat_username>" или "messages_<chat_id>")
        if chat_info.get("chat_username"):
            table_suffix = chat_info["chat_username"].lstrip("@").lower()
        else:
            table_suffix = str(msg.chat_id)
        table_name = "messages_" + table_suffix

        # Убедимся, что родительская partition-таблица создана
        ensure_partitioned_parent_table(table_name)

        # Upsert в БД
        inserted = upsert_partitioned_record(table_name, data)
        if inserted:
            logger.info(f"[unified_handler] Inserted new row for msg_id={msg.id} in {table_name}.")
        else:
            logger.info(f"[unified_handler] Updated existing row for msg_id={msg.id} in {table_name}.")

        logger.info(f"[unified_handler] Processed {event_type} msg_id={msg.id} chat_id={msg.chat_id}")

    except Exception as e:
        logger.exception(f"[unified_handler] Error: {e}")


# Новый обработчик для команды "push" от администратора.
# При получении текстового сообщения "push" от пользователя с username, равным settings.ADMIN_USERNAME,
# инициируется публикация в канал, независимо от времени.
@events.register(events.NewMessage)
async def on_admin_push(event):
    logger.info("on_admin_push вызван!")
    try:
        text = event.message.raw_text.strip().lower()
        if text != "push":
            return

        sender = await event.get_sender()
        if not sender or not getattr(sender, "username", None):
            return

        sender_username = "@" + sender.username.lower()
        logger.info(f"ADMIN_USERNAME from config: {settings.ADMIN_USERNAME}")
        logger.info(f"Sender username: {sender_username}")

        if sender_username != settings.ADMIN_USERNAME.lower():
            logger.info("Sender is not admin, ignoring push command.")
            return

        logger.info(f"Received admin push command from {sender_username}.")

        # Здесь можно реализовать логику публикации. Пример с тестовыми данными:
        top_msg = {
            "message_id": 9999,
            "clown_count": 12,
            "chat_id": -10012345678,
            "text": "This is the best 🤡 message",
        }
        publish_text = (
            f"**Top clown-emoji message (24h)**\n\n"
            f"Message ID: {top_msg['message_id']}\n"
            f"Clown Count: {top_msg['clown_count']}\n"
            f"Chat ID: {top_msg['chat_id']}\n"
            f"Text: {top_msg['text'] or '(no text)'}"
        )

        await event.client.send_message(settings.PUBLISH_CHANNEL, publish_text)
        logger.info("Publication triggered by admin push command.")
        await event.reply("Publication triggered.")
    except Exception as e:
        logger.exception(f"Error in admin push handler: {e}")
        await event.reply("Failed to trigger publication.")
