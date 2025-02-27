# tg_ubot/app/telegram/state.py

import asyncio
import logging
import json
import os
from telethon import TelegramClient

logger = logging.getLogger("state")


class MessageCounter:
    """
    Простой счётчик обработанных сообщений:
    каждые threshold отправляет уведомление себе (Saved Messages).
    """

    def __init__(self, client: TelegramClient, threshold: int = 100):
        self.client = client
        self.threshold = threshold
        self.count = 0
        self.lock = asyncio.Lock()
        self.state_file = "/app/data/state.json"
        self.state = self._load_state()

        if "message_count" in self.state:
            self.count = self.state["message_count"]

    def _load_state(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.exception(f"Failed to load state: {e}")
        return {}

    def _save_state(self):
        self.state["message_count"] = self.count
        try:
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(self.state, f)
        except Exception as e:
            logger.exception(f"Failed to save state: {e}")

    async def increment(self):
        async with self.lock:
            self.count += 1
            self._save_state()
            logger.debug(f"Processed messages: {self.count}")
            if self.count % self.threshold == 0:
                await self.notify()

    async def notify(self):
        try:
            saved = await self.client.get_entity("me")
            text = f"Processed {self.count} messages so far."
            await self.client.send_message(saved, text)
            logger.info(f"Notification sent for {self.count} messages.")
        except Exception as e:
            logger.exception(f"Failed to notify: {e}")
