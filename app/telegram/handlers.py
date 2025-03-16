import logging
import asyncio
from telethon import events
from telethon.tl.types import Message

from app.config import settings
from app.utils import human_like_delay, get_delay_settings
from app.process_messages import serialize_message
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
    ограниченные чатами из target_ids.
    Если получено сообщение с текстом "push" от администратора (ADMIN_USERNAME),
    инициируется публикация в канал (PUBLISH_CHANNEL).
    """
    target_ids = list(chat_id_to_data.keys())
    logger.info(f"Registering unified_handler for chats: {target_ids}")

    @client.on(events.NewMessage(chats=target_ids))
    async def on_new_message(event):
        try:
            text = event.message.raw_text.strip().lower()
        except Exception as e:
            logger.error(f"Error reading message text: {e}")
            return

        # Если текст равен "push", проверяем отправителя
        if text == "push":
            sender = await event.get_sender()
            sender_username = "@" + (sender.username.lower() if sender and sender.username else "")
            logger.info(f"Received push command. Sender username: {sender_username}, expected admin: {settings.ADMIN_USERNAME.lower()}")
            if sender_username == settings.ADMIN_USERNAME.lower():
                logger.info(f"Admin push command triggered by {sender_username}.")
                # Пример публикации (здесь можно заменить на реальную логику)
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
                return

        # Если не команда push, обрабатываем сообщение стандартным образом
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

        dmin, dmax = get_delay_settings("chat")
        await human_like_delay(dmin, dmax)

        data = serialize_message(msg, event_type, chat_info)
        if not data:
            return

        topic = settings.UBOT_PRODUCE_TOPIC
        await message_buffer.put((topic, data))

        if chat_info.get("chat_username"):
            table_suffix = chat_info["chat_username"].lstrip("@").lower()
        else:
            table_suffix = str(msg.chat_id)
        table_name = "messages_" + table_suffix

        ensure_partitioned_parent_table(table_name)
        inserted = upsert_partitioned_record(table_name, data)
        if inserted:
            logger.info(f"[unified_handler] Inserted new row for msg_id={msg.id} in {table_name}.")
        else:
            logger.info(f"[unified_handler] Updated existing row for msg_id={msg.id} in {table_name}.")

        logger.info(f"[unified_handler] Processed {event_type} msg_id={msg.id} chat_id={msg.chat_id}")

    except Exception as e:
        logger.exception(f"[unified_handler] Error: {e}")
