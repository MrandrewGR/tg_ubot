# tg_ubot/app/telegram/state_manager.py

import os
import json
import asyncio
import logging

logger = logging.getLogger("state_manager")

class StateManager:
    """
    Хранит:
      - backfill_from_id для каждого чата
      - missing_ranges
      - временные метки новых сообщений (для понимания, были ли "свежие" сообщения)
    """

    def __init__(self, state_file="/app/data/state.json"):
        self.state_file = state_file
        self.state = self._load_state()
        self.new_msg_timestamps = []
        self.lock = asyncio.Lock()

    def _load_state(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.exception(f"Could not load state: {e}")
                return {}
        else:
            logger.warning(f"{self.state_file} not found, using empty state.")
            return {}

    def _save_state(self):
        tmp_file = self.state_file + ".tmp"
        try:
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(self.state, f, ensure_ascii=False, indent=2)
            os.replace(tmp_file, self.state_file)
            logger.debug(f"Saved state to {self.state_file}")
        except Exception as e:
            logger.exception(f"Error saving state: {e}")

    # --- backfill_from_id ---
    def get_backfill_from_id(self, chat_id: int):
        return self.state.get(f"chat_{chat_id}_backfill_from_id", None)

    def update_backfill_from_id(self, chat_id: int, new_val: int):
        self.state[f"chat_{chat_id}_backfill_from_id"] = new_val
        self._save_state()

    # --- missing_ranges ---
    def get_missing_ranges(self, chat_id: int) -> list:
        return self.state.get(f"chat_{chat_id}_missing_ranges", [])

    def set_missing_ranges(self, chat_id: int, missing_ranges: list):
        self.state[f"chat_{chat_id}_missing_ranges"] = missing_ranges
        self._save_state()

    # --- new messages count ---
    def record_new_message(self):
        now = asyncio.get_event_loop().time()
        self.new_msg_timestamps.append(now)

    def pop_new_messages_count(self, interval: float) -> int:
        now = asyncio.get_event_loop().time()
        cutoff = now - interval
        valid_times = [t for t in self.new_msg_timestamps if t > cutoff]
        count = len(valid_times)
        self.new_msg_timestamps = valid_times
        return count

    def get_chats_needing_backfill(self):
        """
        Возвращает chat_ids, у которых backfill_from_id > 1.
        """
        result = []
        for k, v in self.state.items():
            if k.endswith("_backfill_from_id"):
                if isinstance(v, int) and v > 1:
                    try:
                        cid = int(k.replace("chat_", "").replace("_backfill_from_id", ""))
                        result.append(cid)
                    except ValueError:
                        logger.warning(f"Invalid chat id in key: {k}")
        return result
