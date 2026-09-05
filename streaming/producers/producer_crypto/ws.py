import websockets
import time
import json
import asyncio
import logging

from asyncio import Task
from dataclasses import dataclass
from websockets.protocol import State

from streaming.plugins.redis_utils import RateLimiter

# from dwh.dbms_utils import DBMSManager
from dwh.postgres_utils import PGManager
from dwh.queries.core.dml import insert_in_table
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


@dataclass
class WSInfo:
    """Данные о ws"""

    exchange: str = None
    ws: websockets.ClientConnection = None
    ws_url: str = None
    ws_ping_interval: float = None
    health: bool = False
    created_at: int = None
    started_at: int = None
    pairs: list[str] = None
    sub_msg: dict = None
    unplanned_reconnects: int = 0
    filter_ping_pong: str = None  # для 'bybit': "op"


class WSManager:
    def __init__(
        self,
        ws_url,
        ws_ping_interval,
        sub_msg: dict,
        raw_queue: asyncio.Queue,
        limiter: RateLimiter,
        async_pg_manager: PGManager,
        ws_info: WSInfo,
    ):
        self.ws_url = ws_url
        self.ws_ping_interval = ws_ping_interval
        self.sub_msg = sub_msg
        self.raw_queue = raw_queue
        self.limiter = limiter
        self.async_pg_manager = async_pg_manager

        self._read_task: Task = None
        self.info = ws_info

    async def _create_connection(self):
        ws = await websockets.connect(self.ws_url, ping_interval=self.ws_ping_interval)
        self.info.created_at = int(time.time())
        return ws

    async def _subscribe(self, ws: websockets.ClientConnection):
        await ws.send(json.dumps(self.sub_msg))
        await ws.recv()
        self.info.started_at = int(time.time())
        self.info.health = True

    async def _reconnect(self, planned=True):
        if not await self.limiter.access():
            return

        new_ws = await self._create_connection()
        await self._subscribe(new_ws)
        new_task = asyncio.create_task(self._put_raw(new_ws))
        await asyncio.sleep(5)
        old_ws = self.info.ws
        old_task = self._read_task
        self.info.ws = new_ws
        self._read_task = new_task

        if old_task:
            old_task.cancel()
            try:
                await old_task
            except asyncio.CancelledError:
                pass

        if old_ws and not old_ws.state == State.CLOSED:
            print("удаляем старый ws")
            await asyncio.sleep(1)
            await old_ws.close()
            await self.limiter.manager.r.decrby(
                name=f"{self.info.exchange}:connections", amount=1
            )

        logger.info("Переподключение установлено успешно")

        if not planned:
            time_connection = datetime.fromtimestamp(int(time.time()), tz=timezone.utc)
            await self._writer_event_log(
                event_time=time_connection, event_type="reconnection"
            )
            logger.info("Данные о времени переподключения успешно сохранены")

    async def _put_raw(self, ws=None):
        try:
            async for msg in ws or self.info.ws:
                # Фильтр для бирж, где pong — это простая текстовая строка (OKX)
                if isinstance(msg, str) and msg.strip() == "pong":
                    continue

                raw = json.loads(msg)
                # Для Bybit
                if self.info.filter_ping_pong:
                    raw = dict(raw)
                    filter_msg = raw.get(self.info.filter_ping_pong)
                    if filter_msg == "ping":
                        # Получен прикладной ping от сервера, нужно сразу ответить pong, самостоятельно
                        await self.info.ws.send(json.dumps({"op": "pong"}))
                        continue
                    elif filter_msg == "pong":
                        # Пропуск прикладного pong от bybit
                        continue

                await self.raw_queue.put(raw)

        except websockets.ConnectionClosed:
            shutdown_time = datetime.fromtimestamp(int(time.time()), tz=timezone.utc)
            await self._writer_event_log(
                event_time=shutdown_time, event_type="emergency_shutdown"
            )
            logger.warning("Соединение закрылось аварийно. Переподключение...")
            self.info.unplanned_reconnects += 1
            self.info.health = False
            await self._reconnect(planned=False)

        except Exception as e:
            logger.error(f"Ошибка в _put_message: {e}")

    async def _writer_event_log(self, event_time: int, event_type: str):
        if event_type not in ["emergency_shutdown", "connection", "reconnection"]:
            raise ValueError(f"Пришёл неверное событие: {event_type}")
        data = []
        for pair in self.info.pairs:
            data.append(
                {
                    "exchange": self.info.exchange,
                    "pair": pair,
                    "event_type": event_type,
                    "event_time": event_time,
                }
            )
            await self.async_pg_manager.execute_async(
                insert_in_table,
                table=self.async_pg_manager.models["event_log"],
                values=data,
            )

    async def run(self):
        ws = await self._create_connection()
        self.info.ws = ws
        await self._subscribe(ws=ws)
        self._read_task = asyncio.create_task(self._put_raw(ws=ws))
        time_connection = datetime.fromtimestamp(int(time.time()), tz=timezone.utc)
        await self._writer_event_log(
            event_time=time_connection, event_type="connection"
        )
        while True:
            await asyncio.sleep(23 * 3600 + 30 * 60)
            logger.warning("Плановое переподключение WS")
            await self._reconnect(planned=True)
