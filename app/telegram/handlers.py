# app/telegram/handlers.py

import logging
import asyncio
from telethon import events
from telethon.tl.types import Message

from app.config import settings
from app.utils import human_like_delay, get_delay_settings
from app.process_messages import serialize_message
# Replaced import: we now do upsert
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
    Registers handlers for new messages and edited messages, limited to chat IDs in `target_ids`.
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
    Handle a single new/edited message event:
    1) Possibly insert a 'human-like' delay,
    2) Serialize the message (with reaction data),
    3) Put it on the queue (for Kafka or whatever),
    4) Upsert into the DB so only the latest state remains.
    """
    try:
        msg: Message = event.message
        chat_info = chat_id_to_data.get(msg.chat_id)
        if not chat_info:
            logger.warning(f"No chat_info for chat_id={msg.chat_id}, skipping.")
            return

        # Imitate a random delay
        dmin, dmax = get_delay_settings("chat")
        await human_like_delay(dmin, dmax)

        # Serialize
        data = serialize_message(msg, event_type, chat_info)
        if not data:
            return

        # Optionally put to a queue => which your code later sends to Kafka
        topic = settings.UBOT_PRODUCE_TOPIC
        await message_buffer.put((topic, data))

        # Build a partitioned table name, e.g. "messages_<chat_username>" or "messages_<chat_id>"
        if chat_info.get("chat_username"):
            table_suffix = chat_info["chat_username"].lstrip("@").lower()
        else:
            table_suffix = str(msg.chat_id)
        table_name = "messages_" + table_suffix

        # Ensure the partitioned table is created
        ensure_partitioned_parent_table(table_name)

        # Upsert into DB
        inserted = upsert_partitioned_record(table_name, data)
        if inserted:
            logger.info(f"[unified_handler] Inserted new row for msg_id={msg.id} in {table_name}.")
        else:
            logger.info(f"[unified_handler] Updated existing row for msg_id={msg.id} in {table_name}.")

        logger.info(f"[unified_handler] Processed {event_type} msg_id={msg.id} chat_id={msg.chat_id}")

    except Exception as e:
        logger.exception(f"[unified_handler] Error: {e}")
