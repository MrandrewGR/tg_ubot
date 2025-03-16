import asyncio
import logging
from datetime import timedelta
from telethon import errors

from app.utils import human_like_delay, get_delay_settings, get_current_time_moscow
from app.process_messages import serialize_message
from app.config import settings

logger = logging.getLogger("backfill_manager")

class BackfillManager:
    """
    Менеджер бэкфилла. Проходит "назад" по старым сообщениям чата,
    записывает их в Kafka/DB, пока не достигнет конца (ID=1) или сообщения старше указанного порога.
    """

    def __init__(
        self,
        client,
        state_mgr,
        message_callback,
        chat_id_to_data,
        new_msgs_threshold=0,
        idle_timeout=10,
        batch_size=50,
        flood_wait_delay=60,
        max_total_wait=300
    ):
        self.client = client
        self.state_mgr = state_mgr
        self.message_callback = message_callback
        self.chat_id_to_data = chat_id_to_data

        self.new_msgs_threshold = new_msgs_threshold
        self.idle_timeout = idle_timeout
        self.batch_size = batch_size
        self.flood_wait_delay = flood_wait_delay
        self.max_total_wait = max_total_wait

        self._stop_event = asyncio.Event()

    def stop(self):
        self._stop_event.set()

    async def run(self):
        logger.info("BackfillManager started.")
        while not self._stop_event.is_set():
            await asyncio.sleep(self.idle_timeout)
            new_count = self.state_mgr.pop_new_messages_count(self.idle_timeout)
            if new_count > self.new_msgs_threshold:
                logger.debug("[Backfill] new messages => skip this round")
                continue

            chats_to_backfill = self.state_mgr.get_chats_needing_backfill()
            if not chats_to_backfill:
                logger.debug("[Backfill] No chats needing backfill.")
                continue

            chats_to_backfill.sort(
                key=lambda cid: self.state_mgr.get_backfill_from_id(cid) or 1,
                reverse=True
            )

            for cid in chats_to_backfill:
                if self._stop_event.is_set():
                    break
                await self._fill_missing_ranges(cid)
                await self._do_chat_backfill(cid)

        logger.info("BackfillManager stopped.")

    async def _fill_missing_ranges(self, chat_id: int):
        missing_ranges = self.state_mgr.get_missing_ranges(chat_id)
        if not missing_ranges:
            return

        logger.debug(f"[Backfill] Chat {chat_id} has gaps: {missing_ranges}")
        missing_ranges.sort(key=lambda rng: rng[1], reverse=True)
        new_missing = []

        for (start_id, end_id) in missing_ranges:
            if self._stop_event.is_set():
                break
            offset = end_id + 1
            try:
                logger.info(f"[Backfill] Filling gaps {start_id}..{end_id} for chat {chat_id}")
                current_off = offset
                stop_processing = False
                while current_off > start_id:
                    msgs = await self.client.get_messages(
                        entity=chat_id,
                        limit=self.batch_size,
                        offset_id=current_off,
                        reverse=False
                    )
                    if not msgs:
                        break
                    min_id = current_off
                    for m in msgs:
                        if m.id >= current_off:
                            continue

                        if settings.BACKFILL_MAX_DAYS > 0:
                            cutoff_date = get_current_time_moscow() - timedelta(days=settings.BACKFILL_MAX_DAYS)
                            message_date = m.date.astimezone(cutoff_date.tzinfo)
                            if message_date < cutoff_date:
                                logger.info(f"[Backfill] Message {m.id} date {message_date} is older than cutoff {cutoff_date}. Stop filling gaps for chat {chat_id}.")
                                stop_processing = True
                                break

                        dmin, dmax = get_delay_settings("chat")
                        await human_like_delay(dmin, dmax)

                        data = serialize_message(m, "missing_message", self.chat_id_to_data.get(chat_id, {}))
                        await self.message_callback(data)
                        if m.id < min_id:
                            min_id = m.id
                    if stop_processing or min_id >= current_off:
                        break
                    current_off = min_id
                    if current_off <= start_id:
                        break
            except asyncio.CancelledError:
                raise
            except errors.FloodWaitError as e:
                wait_sec = min(e.seconds + self.flood_wait_delay, self.max_total_wait)
                logger.warning(f"[Backfill] FloodWaitError => wait {wait_sec}s.")
                await asyncio.sleep(wait_sec)
            except Exception as e:
                logger.exception(f"[Backfill] Error filling {start_id}..{end_id} for chat {chat_id}: {e}")
                new_missing.append([start_id, end_id])
                continue

        if new_missing:
            logger.info(f"[Backfill] Remaining gaps for chat {chat_id}: {new_missing}")
        else:
            logger.info(f"[Backfill] All gaps filled for chat {chat_id}")
        self.state_mgr.set_missing_ranges(chat_id, new_missing)

    async def _do_chat_backfill(self, chat_id: int):
        offset = self.state_mgr.get_backfill_from_id(chat_id)
        if not offset or offset <= 1:
            logger.debug(f"[Backfill] Chat {chat_id} fully backfilled.")
            return

        logger.info(f"[Backfill] Backfill from offset={offset} for chat {chat_id}")
        try:
            msgs = await self.client.get_messages(
                entity=chat_id,
                limit=self.batch_size,
                offset_id=offset,
                reverse=False
            )
            if not msgs:
                logger.info(f"[Backfill] No older msgs for chat {chat_id}, set backfill=1")
                self.state_mgr.update_backfill_from_id(chat_id, 1)
                return

            min_id = offset
            for m in msgs:
                if m.id >= offset:
                    continue

                if settings.BACKFILL_MAX_DAYS > 0:
                    cutoff_date = get_current_time_moscow() - timedelta(days=settings.BACKFILL_MAX_DAYS)
                    message_date = m.date.astimezone(cutoff_date.tzinfo)
                    if message_date < cutoff_date:
                        logger.info(f"[Backfill] Message {m.id} date {message_date} is older than cutoff {cutoff_date}. Stop backfill for chat {chat_id}.")
                        self.state_mgr.update_backfill_from_id(chat_id, 1)
                        return

                dmin, dmax = get_delay_settings("chat")
                await human_like_delay(dmin, dmax)

                data = serialize_message(m, "backfill_message", self.chat_id_to_data.get(chat_id, {}))
                await self.message_callback(data)
                if m.id < min_id:
                    min_id = m.id

            if min_id < offset:
                offset = min_id
            self.state_mgr.update_backfill_from_id(chat_id, offset)
            logger.info(f"[Backfill] Updated chat {chat_id} => backfill_from_id={offset}")

        except asyncio.CancelledError:
            raise
        except errors.FloodWaitError as e:
            wait_sec = min(e.seconds + self.flood_wait_delay, self.max_total_wait)
            logger.warning(f"[Backfill] FloodWait => wait {wait_sec}s.")
            await asyncio.sleep(wait_sec)
        except Exception as e:
            logger.exception(f"[Backfill] Error in backfill for chat {chat_id}: {e}")
