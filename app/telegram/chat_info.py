# tg_ubot/app/telegram/chat_info.py

import logging
from telethon import TelegramClient
from telethon.tl.types import User, Chat, Channel, ChatForbidden

from app.config import settings

logger = logging.getLogger("chat_info")


async def get_all_chats_info(client: TelegramClient):
    """
    Возвращает dict {chat_id: {...}} с метаданными о чатах, исключая
    те, что прописаны в EXCLUDED_CHAT_IDS/EXCLUDED_USERNAMES.
    """
    chats_info = {}
    all_dialogs = await client.get_dialogs()

    excluded_ids = set(settings.EXCLUDED_CHAT_IDS or [])
    excluded_unames = [u.lower() for u in (settings.EXCLUDED_USERNAMES or [])]

    for dialog in all_dialogs:
        entity = dialog.entity
        if not entity:
            continue

        raw_id = getattr(entity, 'id', None)
        if raw_id is None:
            continue

        raw_uname = (getattr(entity, 'username', '') or '').lower()
        if raw_uname in excluded_unames:
            logger.info(f"Excluding by username={raw_uname}, id={raw_id}")
            continue

        if raw_id in excluded_ids:
            logger.info(f"Excluding by chat_id={raw_id}")
            continue

        target_id, entity_type = _get_target_id_and_type(entity)
        if target_id is None:
            continue

        chat_title = _get_chat_title(entity)
        chat_username = getattr(entity, 'username', '') or ''
        name_uname = _get_name_or_username(entity)

        chats_info[target_id] = {
            "target_id": target_id,
            "chat_title": chat_title,
            "chat_username": chat_username,
            "name_uname": name_uname,
            "entity_type": entity_type
        }

    logger.info(f"Total dialogs after exclusion: {len(chats_info)}")
    return chats_info


def _get_target_id_and_type(entity):
    if isinstance(entity, Channel):
        if getattr(entity, 'broadcast', False) or getattr(entity, 'megagroup', False):
            return int(f"-100{entity.id}"), "ChannelOrSupergroup"
        else:
            return int(f"-100{entity.id}"), "UnknownChannel"
    elif isinstance(entity, Chat):
        return entity.id, "Chat"
    elif isinstance(entity, User):
        return entity.id, "User"
    elif isinstance(entity, ChatForbidden):
        return None, "ChatForbidden"
    return None, "UnknownEntity"


def _get_chat_title(entity):
    if hasattr(entity, 'title'):
        return entity.title
    if isinstance(entity, User):
        fn = entity.first_name or ""
        ln = entity.last_name or ""
        return (fn + " " + ln).strip()
    return ""


def _get_name_or_username(entity):
    uname = getattr(entity, 'username', '') or ''
    if uname:
        return f"@{uname}"
    if hasattr(entity, 'title'):
        return entity.title or "Unknown"
    if isinstance(entity, User):
        fn = entity.first_name or ""
        ln = entity.last_name or ""
        if fn or ln:
            return (fn + " " + ln).strip()
    return "Unknown"
