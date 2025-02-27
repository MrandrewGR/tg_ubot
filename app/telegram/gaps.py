# tg_ubot/app/telegram/gaps.py

import os
import logging
import psycopg2
import psycopg2.extras
from telethon.errors import FloodWaitError

from mirco_services_data_management.db import get_connection

logger = logging.getLogger("gaps_manager_local")


class LocalGapsManager:
    """
    Локальный поиск "дыр" в базе (если вы используете БД).
    Если не используете PostgreSQL — уберите этот класс или закомментируйте.
    """

    def __init__(self, state_mgr, client, chat_id_to_data):
        self.state_mgr = state_mgr
        self.client = client
        self.chat_id_to_data = chat_id_to_data
        self.schema_name = os.getenv("TG_UBOT_SCHEMA", "public")

    def _get_all_tables_in_schema(self):
        """
        Поиск всех таблиц в схеме, чьи имена начинаются на messages_.
        """
        conn = get_connection()  # из mirco_services_data_management.db
        table_list = []
        try:
            with conn.cursor() as cur:
                sql = """
                    SELECT c.relname
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = %s
                      AND c.relkind IN ('r', 'p')
                      AND c.relname LIKE 'messages_%%'
                    ORDER BY c.relname
                """
                cur.execute(sql, (self.schema_name,))
                rows = cur.fetchall()
                table_list = [row[0] for row in rows]
        except Exception as e:
            logger.debug(f"Error fetching tables in {self.schema_name}: {e}")
        finally:
            conn.close()
        return table_list

    def _fetch_all_message_ids_across_schema(self, chat_id: int):
        all_ids = []
        tables = self._get_all_tables_in_schema()
        if not tables:
            return []

        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                for table_name in tables:
                    sql = f"""
                        SELECT (data->>'message_id')::bigint AS msgid
                        FROM {self.schema_name}.{table_name}
                        WHERE (data->>'chat_id')::bigint = %s
                    """
                    try:
                        cur.execute(sql, (chat_id,))
                        rows = cur.fetchall()
                        for r in rows:
                            if r and "msgid" in r:
                                all_ids.append(r["msgid"])
                    except Exception as e:
                        logger.debug(f"Failed to fetch IDs from {table_name}: {e}")
        except Exception as e:
            logger.debug(f"_fetch_all_message_ids_across_schema({chat_id}) error: {e}")
        finally:
            conn.close()

        all_ids.sort()
        return all_ids

    async def _get_earliest_in_telegram(self, chat_id: int):
        """
        Для примера: получаем самый ранний ID сообщения в Telegram (offset_id=0, reverse=True).
        """
        try:
            msgs = await self.client.get_messages(chat_id, limit=1, offset_id=0, reverse=True)
            return msgs[0].id if msgs else None
        except Exception as e:
            logger.debug(f"_get_earliest_in_telegram({chat_id}) error: {e}")
            return None

    def _find_missing_ranges(self, sorted_ids):
        if not sorted_ids:
            return []
        missing = []
        prev_id = sorted_ids[0]
        for current_id in sorted_ids[1:]:
            if current_id > prev_id + 1:
                missing.append((prev_id + 1, current_id - 1))
            prev_id = current_id
        return missing

    async def find_and_fill_gaps_for_chat(self, chat_id: int):
        """
        Ищет пропущенные ID, записывает их в state_mgr.
        """
        logger.info(f"[LocalGapsManager] Checking gaps for chat {chat_id}")
        sorted_ids = self._fetch_all_message_ids_across_schema(chat_id)
        earliest_in_db = sorted_ids[0] if sorted_ids else None
        earliest_in_tg = await self._get_earliest_in_telegram(chat_id)

        logger.info(f"Chat {chat_id}: earliest_in_db={earliest_in_db}, earliest_in_tg={earliest_in_tg}")

        if earliest_in_db is None:
            if earliest_in_tg:
                self.state_mgr.update_backfill_from_id(chat_id, earliest_in_tg)
            self.state_mgr.set_missing_ranges(chat_id, [])
            logger.info(f"Chat {chat_id}: No DB rows => backfill set to {earliest_in_tg}")
            return

        # Если в БД записано, что самый ранний id > earliest_in_tg + 1, обновим backfill
        if earliest_in_tg and earliest_in_db > earliest_in_tg + 1:
            self.state_mgr.update_backfill_from_id(chat_id, earliest_in_db)
            logger.info(f"Chat {chat_id}: Backfill updated => {earliest_in_db}")

        missing_ranges = self._find_missing_ranges(sorted_ids)
        total_missing = sum((end - start + 1) for start, end in missing_ranges)
        self.state_mgr.set_missing_ranges(chat_id, [list(r) for r in missing_ranges])
        logger.info(f"Chat {chat_id}: Total missing messages: {total_missing}")
