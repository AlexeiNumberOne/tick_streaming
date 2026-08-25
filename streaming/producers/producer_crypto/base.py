import asyncio

from abc import ABC, abstractmethod
from aiokafka import AIOKafkaProducer
from typing import Iterator

from streaming.producers.producer_crypto.state import exchange_state, ExchangeInfo
from streaming.plugins.redis_utils import RateLimiter
from streaming.producers.producer_crypto.ws import WSConnectionHandler
from streaming.plugins.kafka_utils import KafkaWriter
from streaming.plugins.redis_utils import RedisManager
from dwh.dbms_utils import DBMSManager


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
        filter_ping_pong: str | None = None,
        ping_interval: int | None = 20,
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
        self.ping_interval = ping_interval
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
        readers = []
        for batch in batches:
            reader = WSConnectionHandler(
                limiter=limiter,
                async_pg_manager=self.async_pg_manager,
                ws_url=self.ws_url,
                batch=batch,
                exchange_state=exchange_state,
                build_sub_messages_func=self.build_sub_messages,
                exchange_info=exchange_info,
                process_message_func=self.process_message,
                queue=self.queue,
                filter_ping_pong=self.filter_ping_pong,
                ping_interval=self.ping_interval,
            )
            readers.append(asyncio.create_task(reader.run()))
        if readers:
            await asyncio.gather(*readers)
