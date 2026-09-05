import asyncio
import logging

from abc import ABC, abstractmethod
from aiokafka import AIOKafkaProducer
from typing import Iterator

from streaming.producers.producer_crypto.state import exchange_state, ExchangeInfo
from streaming.plugins.redis_utils import RateLimiter
from streaming.producers.producer_crypto.ws import WSManager, WSInfo
from streaming.plugins.kafka_utils import KafkaWriter
from streaming.plugins.redis_utils import RedisManager
from dwh.dbms_utils import DBMSManager

logger = logging.getLogger(__name__)


class MarketStream(ABC):
    def __init__(
        self,
        source_name: str,
        market_type: str,
        pairs: list,
        producer: AIOKafkaProducer,
        redis_manager: RedisManager,
        async_pg_manager: DBMSManager,
        ws_url: str,
        limit_connections: int,
        limit_attempt: int,
        ttl_attempt: int,
        queue_size: int = 10_000,
        filter_ping_pong: str = None,
        ws_ping_interval: float = 20.0,
    ):
        self.source_name = source_name
        self.market_type = market_type
        self.pairs = pairs
        self.producer = producer
        self.redis_manager = redis_manager
        self.async_pg_manager = async_pg_manager
        self.ws_url = ws_url
        self.limit_connections = limit_connections
        self.limit_attempt = limit_attempt
        self.ttl_attempt = ttl_attempt
        self.filter_ping_pong = filter_ping_pong
        self.ws_ping_interval = ws_ping_interval
        self.raw_queue = asyncio.Queue(maxsize=queue_size)
        self.queue = asyncio.Queue(maxsize=queue_size)

    @abstractmethod
    def process_message(self, raw: dict) -> Iterator[dict]:
        """Обрабатывает полученные сообщения и нормализует"""
        pass

    @abstractmethod
    def create_batches_pairs(self, pairs: list) -> list[list]:
        """Принимает список валютных пар и
        возвращает их в виде списка батчей валютных пар,
        которые умещаются в лимиты для подписки в одном ws-соединении"""
        pass

    @abstractmethod
    def build_sub_messages(self, batch: list) -> dict:
        """Возвращает сообщение для подписки"""
        pass

    # async def _keepalive(self, ws):
    #     """Метод для кастомного поддержания соединения.
    #     Требуется не для всех бирж, поэтому не abstract
    #     """
    #     pass

    async def process_raw(self):
        while True:
            raw = await self.raw_queue.get()
            for msg in self.process_message(raw):
                await self.queue.put(msg)

    async def run(self):
        """Метод для запуска подписки websocket соединений бирж и записи в kafka тиков"""
        batches = self.create_batches_pairs(self.pairs)
        exchange_info = ExchangeInfo(
            exchange=self.source_name,
            market_type=self.market_type,
            expected_ws=len(batches),
        )
        exchange_state[f"{self.source_name}-{self.market_type}"] = exchange_info
        writer = KafkaWriter(
            queue=self.queue, producer=self.producer, exchange_info=exchange_info
        )
        asyncio.create_task(writer.run())
        limiter = RateLimiter(
            manager=self.redis_manager,
            main_key=self.source_name,
            limit_attempt=self.limit_attempt,
            limit_connections=self.limit_connections,
            ttl_attempt=self.ttl_attempt,
        )

        ws_managers: list[WSManager] = []

        for batch in batches:
            if not await limiter.access():
                logger.warning("Превышено кол-во одновременных подключений")
                return

            sub_msg = self.build_sub_messages(batch)
            ws_info = WSInfo(
                exchange=self.source_name,
                ws_url=self.ws_url,
                ws_ping_interval=self.ws_ping_interval,
                pairs=batch,
                sub_msg=sub_msg,
                filter_ping_pong=self.filter_ping_pong,
            )
            ws_manager = WSManager(
                ws_url=self.ws_url,
                ws_ping_interval=self.ws_ping_interval,
                sub_msg=sub_msg,
                raw_queue=self.raw_queue,
                limiter=limiter,
                async_pg_manager=self.async_pg_manager,
                ws_info=ws_info,
            )

            ws_managers.append(ws_manager)
            exchange_info.add_manager(manager=ws_manager)

        reader_tasks = [asyncio.create_task(m.run()) for m in ws_managers]
        process_raw_task = asyncio.create_task(self.process_raw())

        await asyncio.gather(*reader_tasks, process_raw_task)
