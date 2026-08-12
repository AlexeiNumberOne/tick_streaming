import websockets
import time
import json
import asyncio

from dataclasses import dataclass

from streaming.plugins.redis_client import RateLimiter


@dataclass
class WSInfo:
    """Данные о ws"""

    ws: websockets.ClientConnection | None = None
    health: bool = False
    created_at: int | None = None
    started_at: int | None = None
    pairs: list[str] | None = None
    sub_msg: dict | None = None
    last_tick_time: int | None = None
    reconnects: int = 0
    filter_ping_pong: None | str = None  # для 'bybit': "op"


class WSConnectionManager:
    def __init__(self, url: str, ping_interval=None, filter_ping_pong=None):
        self._url = url
        self._ping_interval = ping_interval
        self.filter_ping_pong = filter_ping_pong
        self.info = WSInfo()

    async def connect(self):
        ws = await websockets.connect(self._url, ping_interval=self._ping_interval)
        self.info.ws = ws
        self.info.created_at = int(time.time())

    async def subscribe(self):
        if self.info.sub_msg is None:
            raise ValueError("Нет сообщения для подписки")
        await self.info.ws.send(json.dumps(self.info.sub_msg))
        await self.info.ws.recv()
        self.info.started_at = int(time.time())
        self.info.health = True

    async def reconnect(self):
        if self.info.ws:
            await self.info.ws.close()

        await self.connect()
        self.info.reconnects += 1

    async def get_message(self):
        async for msg in self.info.ws:
            # Фильтр для бирж, где pong — это простая текстовая строка (OKX)
            if isinstance(msg, str) and msg.strip() == "pong":
                continue

            raw = json.loads(msg)

            # Для Bybit
            if self.info.filter_ping_pong:
                filter_msg = raw.get(self.info.filter_ping_pong)
                if filter_msg == "ping":
                    # Получен прикладной ping от сервера, нужно сразу ответить pong, самостоятельно
                    await self.info.ws.send(json.dumps({"op": "pong"}))
                    continue
                elif filter_msg == "pong":
                    # Пропуск прикладного pong от bybit
                    continue

            return raw


class WSConnectionHandler:
    def __init__(
        self,
        limiter: RateLimiter,
        ws_url: str,
        batch: list,
        exchange_state: dict,
        build_sub_messages_func,
        exchange_info,
        process_message_func,
        queue: asyncio.Queue,
        filter_ping_pong: str | None = None,
        ping_interval: int | None = None,
    ):
        self.limiter = limiter
        self.ws_url = ws_url
        self.batch = batch
        self.exchange_state = exchange_state
        self.build_sub_messages_func = build_sub_messages_func
        self.exchange_info = exchange_info
        self.process_message_func = process_message_func
        self.queue = queue
        self.filter_ping_pong = filter_ping_pong
        self.ping_interval = ping_interval

    async def run(self):
        """Создаёт websocket соединение"""
        if not await self.limiter.access():
            return
        manager = WSConnectionManager(
            url=self.ws_url,
            ping_interval=self.ping_interval,
            filter_ping_pong=self.filter_ping_pong,
        )
        await manager.connect()
        self.exchange_state[
            f"{self.exchange_info.exchange}-{self.exchange_info.market_type}"
        ].add_manager(manager=manager)
        manager.info.pairs = self.batch
        manager.info.sub_msg = self.build_sub_messages_func(self.batch)
        await manager.subscribe()

        while True:
            try:
                raw = await manager.get_message()
                self.exchange_info.log_count_received_ticks += 1
                for msg in self.process_message_func(raw):
                    await self.queue.put(msg)
            except websockets.ConnectionClosed:
                # #🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️
                # loss_time = int(time.time())
                # #await async_save_in_db(manager.info.pairs, loss_time)
                manager.info.health = False
                if not await self.limiter.access():
                    return
                await manager.reconnect()
                await manager.subscribe()
